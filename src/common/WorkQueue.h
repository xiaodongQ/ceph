// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_WORKQUEUE_H
#define CEPH_WORKQUEUE_H

#if defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
// for ObjectStore.h
struct ThreadPool {
  struct TPHandle {
  };
};

#else

#include <atomic>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "common/ceph_mutex.h"
#include "include/unordered_map.h"
#include "common/config_obs.h"
#include "common/HeartbeatMap.h"
#include "common/Thread.h"
#include "include/common_fwd.h"
#include "include/Context.h"
#include "common/HBHandle.h"


/// Pool of threads that share work submitted to multiple work queues.
class ThreadPool : public md_config_obs_t {
protected:
  CephContext *cct;
  // 线程池名字
  std::string name;
  // 也是线程池名字？
  std::string thread_name;
  // 锁的名字
  std::string lockname;
  // 工作队列的互斥锁
  ceph::mutex _lock;
  // 锁对应的条件变量，实际是std::condition_variable，ceph做了一层包装
  ceph::condition_variable _cond;
  // 线程池是否停止
  bool _stop;
  // 暂时中止标志
  int _pause;
  int _draining;
  ceph::condition_variable _wait_cond;

public:
  // 定义一个内部类(public)，用做超时检查
  class TPHandle : public HBHandle {
    friend class ThreadPool;
    CephContext *cct;
    // 心跳，记录了相关信息，并把该结构添加到HeartbeatMap的系统链表中保存。OSD会有一个定时器，定时检查是否超时
    ceph::heartbeat_handle_d *hb;
    // 超时，每次线程函数执行时，都会设置一个grace超时时间，当线程执行超过该时间，就认为是unhealthy的状态
    ceph::timespan grace;
    // 自杀的超时时间，当执行时间超过suicide_grace时，OSD就会产生断言而导致自杀
    ceph::timespan suicide_grace;
  public:
    TPHandle(
      CephContext *cct,
      ceph::heartbeat_handle_d *hb,
      ceph::timespan grace,
      ceph::timespan suicide_grace)
      : cct(cct), hb(hb), grace(grace), suicide_grace(suicide_grace) {}
    void reset_tp_timeout() override final;
    void suspend_tp_timeout() override final;
  };
protected:

  // 内部类(protected)，工作队列的基本接口(包含纯虚函数，抽象类)
  /// Basic interface to a work queue used by the worker threads.
  struct WorkQueue_ {
    std::string name;
    ceph::timespan timeout_interval;
    ceph::timespan suicide_interval;
    // 用初始化列表来在构造函数本体之前初始化参数(而不是赋值，初始化效率更高)(Effective C++ 条款04：确定对象被使用前已先被初始化)
    WorkQueue_(std::string n, ceph::timespan ti, ceph::timespan sti)
      : name(std::move(n)), timeout_interval(ti), suicide_interval(sti)
    { }
    virtual ~WorkQueue_() {}
    // 纯虚类，子类进行实现
    // 移除工作队列中的所有元素
    /// Remove all work items from the queue.
    virtual void _clear() = 0;
    /// Check whether there is anything to do.
    virtual bool _empty() = 0;
    /// Get the next work item to process.
    // 获取下一个待处理的工作
    virtual void *_void_dequeue() = 0;
    /** @brief Process the work item.
     * This function will be called several times in parallel
     * and must therefore be thread-safe. */
    // 处理工作
    virtual void _void_process(void *item, TPHandle &handle) = 0;
    /** @brief Synchronously finish processing a work item.
     * This function is called after _void_process with the global thread pool lock held,
     * so at most one copy will execute simultaneously for a given thread pool.
     * It can be used for non-thread-safe finalization. */
    virtual void _void_process_finish(void *) = 0;
  };

  // track thread pool size changes
  unsigned _num_threads;
  std::string _thread_num_option;
  const char **_conf_keys;

  const char **get_tracked_conf_keys() const override {
    return _conf_keys;
  }
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set <std::string> &changed) override;

public:
  /** @brief Templated by-value work queue.
   * Skeleton implementation of a queue that processes items submitted by value.
   * This is useful if the items are single primitive values or very small objects
   * (a few bytes). The queue will automatically add itself to the thread pool on
   * construction and remove itself on destruction. */
  // 任务队列，定义模板类，按值提交，继承上面的内部接口WorkQueue_(抽象类)，实现其中的接口方法
  template<typename T, typename U = T>
  class WorkQueueVal : public WorkQueue_ {
    ceph::mutex _lock = ceph::make_mutex("WorkQueueVal::_lock");
    ThreadPool *pool; // 线程池
    std::list<U> to_process; // 要处理的任务队列(用list来模拟deque)
    std::list<U> to_finish;  // 已处理完的任务
    virtual void _enqueue(T) = 0; // 用来向线程池pool添加任务，下面的queue会调用该接口
    virtual void _enqueue_front(T) = 0;
    bool _empty() override = 0;
    virtual U _dequeue() = 0;
    virtual void _process_finish(U) {}

    // 获取下一个待处理的工作，放到处理列表中(原队列同时删除该工作)
    void *_void_dequeue() override {
      {
	std::lock_guard l(_lock);
	if (_empty())
	  return 0;
	U u = _dequeue();
	to_process.push_back(u);
      }
      return ((void*)1); // Not used
    }
    // 处理任务，并放入to_finish队列
    void _void_process(void *, TPHandle &handle) override {
      _lock.lock();
      ceph_assert(!to_process.empty());
      // 拿to_process队列头中的任务
      U u = to_process.front();
      to_process.pop_front();
      _lock.unlock();

      // 根据传入的句并来进行处理，_process 是本类中定义的纯虚函数(即本类也是一个抽象类)
      _process(u, handle);

      _lock.lock();
      // 处理完之后放入 to_finish 队列(list来模拟deque)
      to_finish.push_back(u);
      _lock.unlock();
    }

    // 结束任务前的处理，to_finish队列头出队，并执行该任务结束前的处理
    void _void_process_finish(void *) override {
      _lock.lock();
      ceph_assert(!to_finish.empty());
      U u = to_finish.front();
      to_finish.pop_front();
      _lock.unlock();

      // 具体结束前要执行什么操作，由子类实现虚函数_process_finish
      _process_finish(u);
    }

    void _clear() override {}

  public:
    // 构造的时侯纠添加到线程池的工作队列了
    WorkQueueVal(std::string n,
		 ceph::timespan ti,
		 ceph::timespan sti,
		 ThreadPool *p)
      : WorkQueue_(std::move(n), ti, sti), pool(p) {
      pool->add_work_queue(this);
    }
    ~WorkQueueVal() override {
      pool->remove_work_queue(this);
    }
    // 向线程池pool的任务队列中添加一个任务，并通过(线程池中的)条件变量进行通知
    void queue(T item) {
      std::lock_guard l(pool->_lock);
      _enqueue(item);
      pool->_cond.notify_one();
    }
    void queue_front(T item) {
      std::lock_guard l(pool->_lock);
      // 添加到队列头
      _enqueue_front(item);
      pool->_cond.notify_one();
    }
    void drain() {
      pool->drain(this);
    }
  protected:
    void lock() {
      pool->lock();
    }
    void unlock() {
      pool->unlock();
    }
    virtual void _process(U u, TPHandle &) = 0;
  };

  /** @brief Template by-pointer work queue.
   * Skeleton implementation of a queue that processes items of a given type submitted as pointers.
   * This is useful when the work item are large or include dynamically allocated memory. The queue
   * will automatically add itself to the thread pool on construction and remove itself on
   * destruction. */
  // 任务队列，按指针提交的任务(上面的WorkQueueVal中任务是按值提交)，也实现WorkQueue_接口
  template<class T>
  class WorkQueue : public WorkQueue_ {
    ThreadPool *pool;
    
    // 入队一个任务，添加到队列
    /// Add a work item to the queue.
    virtual bool _enqueue(T *) = 0;
    // 出队一个已提交的任务，任务信息放到传入的指针中
    /// Dequeue a previously submitted work item.
    virtual void _dequeue(T *) = 0;
    // 出队一个已提交的任务，任务信息通过返回值返回
    /// Dequeue a work item and return the original submitted pointer.
    virtual T *_dequeue() = 0;
    // 结束前的处理
    virtual void _process_finish(T *) {}

    // 实现继承的抽象类方法
    // implementation of virtual methods from WorkQueue_
    void *_void_dequeue() override {
      return (void *)_dequeue();
    }
    void _void_process(void *p, TPHandle &handle) override {
      _process(static_cast<T *>(p), handle);
    }
    void _void_process_finish(void *p) override {
      _process_finish(static_cast<T *>(p));
    }

  protected:
    /// Process a work item. Called from the worker threads.
    virtual void _process(T *t, TPHandle &) = 0;

  public:
    WorkQueue(std::string n,
	      ceph::timespan ti, ceph::timespan sti,
	      ThreadPool* p)
      : WorkQueue_(std::move(n), ti, sti), pool(p) {
      pool->add_work_queue(this);
    }
    ~WorkQueue() override {
      pool->remove_work_queue(this);
    }
    
    bool queue(T *item) {
      pool->_lock.lock();
      bool r = _enqueue(item);
      pool->_cond.notify_one();
      pool->_lock.unlock();
      return r;
    }
    void dequeue(T *item) {
      pool->_lock.lock();
      _dequeue(item);
      pool->_lock.unlock();
    }
    void clear() {
      pool->_lock.lock();
      _clear();
      pool->_lock.unlock();
    }

    void lock() {
      pool->lock();
    }
    void unlock() {
      pool->unlock();
    }
    /// wake up the thread pool (without lock held)
    void wake() {
      pool->wake();
    }
    /// wake up the thread pool (with lock already held)
    void _wake() {
      pool->_wake();
    }
    void _wait() {
      pool->_wait();
    }
    void drain() {
      pool->drain(this);
    }

  };

  template<typename T>
  class PointerWQ : public WorkQueue_ {
  public:
    ~PointerWQ() override {
      m_pool->remove_work_queue(this);
      ceph_assert(m_processing == 0);
    }
    void drain() {
      {
        // if this queue is empty and not processing, don't wait for other
        // queues to finish processing
        std::lock_guard l(m_pool->_lock);
        if (m_processing == 0 && m_items.empty()) {
          return;
        }
      }
      m_pool->drain(this);
    }
    void queue(T *item) {
      std::lock_guard l(m_pool->_lock);
      m_items.push_back(item);
      m_pool->_cond.notify_one();
    }
    bool empty() {
      std::lock_guard l(m_pool->_lock);
      return _empty();
    }
  protected:
    PointerWQ(std::string n,
	      ceph::timespan ti, ceph::timespan sti,
	      ThreadPool* p)
      : WorkQueue_(std::move(n), ti, sti), m_pool(p), m_processing(0) {
    }
    void register_work_queue() {
      m_pool->add_work_queue(this);
    }
    void _clear() override {
      ceph_assert(ceph_mutex_is_locked(m_pool->_lock));
      m_items.clear();
    }
    bool _empty() override {
      ceph_assert(ceph_mutex_is_locked(m_pool->_lock));
      return m_items.empty();
    }
    void *_void_dequeue() override {
      ceph_assert(ceph_mutex_is_locked(m_pool->_lock));
      if (m_items.empty()) {
        return NULL;
      }

      ++m_processing;
      T *item = m_items.front();
      m_items.pop_front();
      return item;
    }
    void _void_process(void *item, ThreadPool::TPHandle &handle) override {
      process(reinterpret_cast<T *>(item));
    }
    void _void_process_finish(void *item) override {
      ceph_assert(ceph_mutex_is_locked(m_pool->_lock));
      ceph_assert(m_processing > 0);
      --m_processing;
    }

    virtual void process(T *item) = 0;
    void process_finish() {
      std::lock_guard locker(m_pool->_lock);
      _void_process_finish(nullptr);
    }

    T *front() {
      ceph_assert(ceph_mutex_is_locked(m_pool->_lock));
      if (m_items.empty()) {
        return NULL;
      }
      return m_items.front();
    }
    void requeue_front(T *item) {
      std::lock_guard pool_locker(m_pool->_lock);
      _void_process_finish(nullptr);
      m_items.push_front(item);
    }
    void requeue_back(T *item) {
      std::lock_guard pool_locker(m_pool->_lock);
      _void_process_finish(nullptr);
      m_items.push_back(item);
    }
    void signal() {
      std::lock_guard pool_locker(m_pool->_lock);
      m_pool->_cond.notify_one();
    }
    ceph::mutex &get_pool_lock() {
      return m_pool->_lock;
    }
  private:
    ThreadPool *m_pool;
    std::list<T *> m_items;
    uint32_t m_processing;
  };
protected:
  std::vector<WorkQueue_*> work_queues;
  int next_work_queue = 0;
 

  // threads
  struct WorkThread : public Thread {
    ThreadPool *pool;
    // cppcheck-suppress noExplicitConstructor
    WorkThread(ThreadPool *p) : pool(p) {}
    void *entry() override {
      pool->worker(this);
      return 0;
    }
  };
  
  std::set<WorkThread*> _threads;
  std::list<WorkThread*> _old_threads;  ///< need to be joined
  int processing;

  void start_threads();
  // 把旧的工作线程释放掉
  void join_old_threads();
  // 线程池的执行函数
  virtual void worker(WorkThread *wt);

public:
  ThreadPool(CephContext *cct_, std::string nm, std::string tn, int n, const char *option = NULL);
  ~ThreadPool() override;

  /// return number of threads currently running
  int get_num_threads() {
    std::lock_guard l(_lock);
    return _num_threads;
  }
  
  /// assign a work queue to this thread pool
  void add_work_queue(WorkQueue_* wq) {
    std::lock_guard l(_lock);
    work_queues.push_back(wq);
  }
  /// remove a work queue from this thread pool
  void remove_work_queue(WorkQueue_* wq) {
    std::lock_guard l(_lock);
    unsigned i = 0;
    while (work_queues[i] != wq)
      i++;
    for (i++; i < work_queues.size(); i++) 
      work_queues[i-1] = work_queues[i];
    ceph_assert(i == work_queues.size());
    work_queues.resize(i-1);
  }

  /// take thread pool lock
  void lock() {
    _lock.lock();
  }
  /// release thread pool lock
  void unlock() {
    _lock.unlock();
  }

  /// wait for a kick on this thread pool
  void wait(ceph::condition_variable &c) {
    std::unique_lock l(_lock, std::adopt_lock);
    c.wait(l);
  }

  // 外部已经加锁情况下通知(即调用该接口前外面已经加锁了)
  /// wake up a waiter (with lock already held)
  void _wake() {
    _cond.notify_all();
  }
  // 没有加锁情况下进行通知(函数里面进行一次手动加锁)
  /// wake up a waiter (without lock held)
  void wake() {
    std::lock_guard l(_lock);
    _cond.notify_all();
  }
  void _wait() {
    std::unique_lock l(_lock, std::adopt_lock);
    _cond.wait(l);
  }

  // 启动线程池，其在加锁的情况下，调用函数start_threads
  /// start thread pool thread
  void start();
  /// stop thread pool thread
  void stop(bool clear_after=true);
  /// pause thread pool (if it not already paused)
  void pause();
  /// pause initiation of new work
  void pause_new();
  /// resume work in thread pool.  must match each pause() call 1:1 to resume.
  void unpause();
  /** @brief Wait until work completes.
   * If the parameter is NULL, blocks until all threads are idle.
   * If it is not NULL, blocks until the given work queue does not have
   * any items left to process. */
  void drain(WorkQueue_* wq = 0);
};

class GenContextWQ :
  public ThreadPool::WorkQueueVal<GenContext<ThreadPool::TPHandle&>*> {
  std::list<GenContext<ThreadPool::TPHandle&>*> _queue;
public:
  GenContextWQ(const std::string &name, ceph::timespan ti, ThreadPool *tp)
    : ThreadPool::WorkQueueVal<
      GenContext<ThreadPool::TPHandle&>*>(name, ti, ti*10, tp) {}
  
  void _enqueue(GenContext<ThreadPool::TPHandle&> *c) override {
    _queue.push_back(c);
  }
  void _enqueue_front(GenContext<ThreadPool::TPHandle&> *c) override {
    _queue.push_front(c);
  }
  bool _empty() override {
    return _queue.empty();
  }
  GenContext<ThreadPool::TPHandle&> *_dequeue() override {
    ceph_assert(!_queue.empty());
    GenContext<ThreadPool::TPHandle&> *c = _queue.front();
    _queue.pop_front();
    return c;
  }
  void _process(GenContext<ThreadPool::TPHandle&> *c,
		ThreadPool::TPHandle &tp) override {
    c->complete(tp);
  }
};

class C_QueueInWQ : public Context {
  GenContextWQ *wq;
  GenContext<ThreadPool::TPHandle&> *c;
public:
  C_QueueInWQ(GenContextWQ *wq, GenContext<ThreadPool::TPHandle &> *c)
    : wq(wq), c(c) {}
  void finish(int) override {
    wq->queue(c);
  }
};

/// Work queue that asynchronously completes contexts (executes callbacks).
/// @see Finisher
class ContextWQ : public ThreadPool::PointerWQ<Context> {
public:
  ContextWQ(const std::string &name, ceph::timespan ti, ThreadPool *tp)
    : ThreadPool::PointerWQ<Context>(name, ti, ceph::timespan::zero(), tp) {
    this->register_work_queue();
  }

  void queue(Context *ctx, int result = 0) {
    if (result != 0) {
      std::lock_guard locker(m_lock);
      m_context_results[ctx] = result;
    }
    ThreadPool::PointerWQ<Context>::queue(ctx);
  }
protected:
  void _clear() override {
    ThreadPool::PointerWQ<Context>::_clear();

    std::lock_guard locker(m_lock);
    m_context_results.clear();
  }

  void process(Context *ctx) override {
    int result = 0;
    {
      std::lock_guard locker(m_lock);
      ceph::unordered_map<Context *, int>::iterator it =
        m_context_results.find(ctx);
      if (it != m_context_results.end()) {
        result = it->second;
        m_context_results.erase(it);
      }
    }
    ctx->complete(result);
  }
private:
  ceph::mutex m_lock = ceph::make_mutex("ContextWQ::m_lock");
  ceph::unordered_map<Context*, int> m_context_results;
};

// 如果任务之间有互斥性，那么正在处理该任务的两个线程有一个必须等待另一个处理完成后才能处理，从而导致线程的阻塞，性能下降
// ShardedThreadPool对上述的任务调度方式做了改进，其在线程的执行函数里，添加了表示线程的thread_index
// 其基本的思想就是：每个线程对应一个任务队列，所有需要顺序执行的任务都放在同一个线程的任务队列里，全部由该线程执行
class ShardedThreadPool {

  CephContext *cct;
  std::string name;
  std::string thread_name;
  std::string lockname;
  ceph::mutex shardedpool_lock;
  ceph::condition_variable shardedpool_cond;
  ceph::condition_variable wait_cond;
  uint32_t num_threads;

  // 从C++11开始，对列表初始化（List Initialization）的功能进行了扩充
  // C++11列表初始化还可以应用于容器，终于可以摆脱 push_back() 调用了，C++11中可以直观地初始化容器
  // 如：vector<string> vs={"first", "second", "third"};
  std::atomic<bool> stop_threads = { false };
  std::atomic<bool> pause_threads = { false };
  std::atomic<bool> drain_threads = { false };

  uint32_t num_paused;
  uint32_t num_drained;

public:

  class BaseShardedWQ {
  
  public:
    ceph::timespan timeout_interval, suicide_interval;
    BaseShardedWQ(ceph::timespan ti, ceph::timespan sti)
      :timeout_interval(ti), suicide_interval(sti) {}
    virtual ~BaseShardedWQ() {}

    virtual void _process(uint32_t thread_index, ceph::heartbeat_handle_d *hb ) = 0;
    virtual void return_waiting_threads() = 0;
    virtual void stop_return_waiting_threads() = 0;
    virtual bool is_shard_empty(uint32_t thread_index) = 0;
  };

  template <typename T>
  class ShardedWQ: public BaseShardedWQ {
  
    ShardedThreadPool* sharded_pool;

  protected:
    virtual void _enqueue(T&&) = 0;
    virtual void _enqueue_front(T&&) = 0;


  public:
    ShardedWQ(ceph::timespan ti,
	      ceph::timespan sti, ShardedThreadPool* tp)
      : BaseShardedWQ(ti, sti), sharded_pool(tp) {
      tp->set_wq(this);
    }
    ~ShardedWQ() override {}

    void queue(T&& item) {
      _enqueue(std::move(item));
    }
    void queue_front(T&& item) {
      _enqueue_front(std::move(item));
    }
    void drain() {
      sharded_pool->drain();
    }
    
  };

private:

  BaseShardedWQ* wq;
  // threads
  struct WorkThreadSharded : public Thread {
    ShardedThreadPool *pool;
    uint32_t thread_index;
    WorkThreadSharded(ShardedThreadPool *p, uint32_t pthread_index): pool(p),
      thread_index(pthread_index) {}
    void *entry() override {
      pool->shardedthreadpool_worker(thread_index);
      return 0;
    }
  };

  std::vector<WorkThreadSharded*> threads_shardedpool;
  void start_threads();
  // 在线程执行函数里，加了表示线程的 thread_index
  void shardedthreadpool_worker(uint32_t thread_index);
  void set_wq(BaseShardedWQ* swq) {
    wq = swq;
  }



public:

  ShardedThreadPool(CephContext *cct_, std::string nm, std::string tn, uint32_t pnum_threads);

  ~ShardedThreadPool(){};

  /// start thread pool thread
  void start();
  /// stop thread pool thread
  void stop();
  /// pause thread pool (if it not already paused)
  void pause();
  /// pause initiation of new work
  void pause_new();
  /// resume work in thread pool.  must match each pause() call 1:1 to resume.
  void unpause();
  /// wait for all work to complete
  void drain();

};

#endif

#endif
