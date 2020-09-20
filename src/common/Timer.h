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

#ifndef CEPH_TIMER_H
#define CEPH_TIMER_H

#include <map>
#include "include/common_fwd.h"
#include "ceph_time.h"
#include "ceph_mutex.h"

class Context;
class SafeTimerThread;

// 类SafeTimer实现了定时器的功能
class SafeTimer
{
  CephContext *cct;
  ceph::mutex& lock;
  ceph::condition_variable cond;
  bool safe_callbacks;

  friend class SafeTimerThread;
  // 定时器执行线程
  SafeTimerThread *thread;

  // 本函数一次检查scheduler中的任务是否到期，其循环检查任务是否到期执行
  // 任务在schedule中是按照时间升序排列的。首先检查，如果第一任务没有到时间，后面的任务就不用检查了，直接终止循环
  void timer_thread();
  void _shutdown();

  using clock_t = ceph::real_clock;
  // (multimap<clock_t::time_point, Context*>)目标时间和定时任务执行函数Context
  // multimap 容器保存的是有序的键/值对，但它可以保存重复的元素
  // multimap 大部分成员函数的使用方式和 map 相同。因为重复键的原因，multimap 有一些函数的使用方式和 map 有一些区别
  // multimap 不支持下标运算符，因为键并不能确定一个唯一元素
  // 使用key方式：auto iter = people.find(name); if (iter ! = std::end (people)){xxx}
  using scheduled_map_t = std::multimap<clock_t::time_point, Context*>;
  scheduled_map_t schedule;
  using event_lookup_map_t = std::map<Context*, scheduled_map_t::iterator>;
  // map<定时任务, 定时任务在schedule中的位置映射(迭代器)>
  event_lookup_map_t events;
  bool stopping;

  void dump(const char *caller = 0) const;

public:
  // This class isn't supposed to be copied
  SafeTimer(const SafeTimer&) = delete;
  SafeTimer& operator=(const SafeTimer&) = delete;

  /* Safe callbacks determines whether callbacks are called with the lock
   * held.
   *
   * safe_callbacks = true (default option) guarantees that a cancelled
   * event's callback will never be called.
   *
   * Under some circumstances, holding the lock can cause lock cycles.
   * If you are able to relax requirements on cancelled callbacks, then
   * setting safe_callbacks = false eliminates the lock cycle issue.
   * */
  SafeTimer(CephContext *cct, ceph::mutex &l, bool safe_callbacks=true);
  virtual ~SafeTimer();

  /* Call with the event_lock UNLOCKED.
   *
   * Cancel all events and stop the timer thread.
   *
   * If there are any events that still have to run, they will need to take
   * the event_lock first. */
  void init();
  void shutdown();

  // 添加定时任务，可以多少秒后才进行调度
  /* Schedule an event in the future
   * Call with the event_lock LOCKED */
  Context* add_event_after(double seconds, Context *callback);
  // 添加定时任务
  Context* add_event_at(clock_t::time_point when, Context *callback);

  /* Cancel an event.
   * Call with the event_lock LOCKED
   *
   * Returns true if the callback was cancelled.
   * Returns false if you never added the callback in the first place.
   */
  // 取消定时任务
  bool cancel_event(Context *callback);

  /* Cancel all events.
   * Call with the event_lock LOCKED
   *
   * When this function returns, all events have been cancelled, and there are no
   * more in progress.
   */
  void cancel_all_events();

};

#endif
