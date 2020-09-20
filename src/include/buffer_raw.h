// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_BUFFER_RAW_H
#define CEPH_BUFFER_RAW_H

#include <map>
#include <utility>
#include <type_traits>
#include "common/ceph_atomic.h"
#include "include/buffer.h"
#include "include/mempool.h"
#include "include/spinlock.h"

// Buffer就是一个命名空间，在这个命名空间下定义了Buffer相关的数据结构，这些数据结构在Ceph的源代码中广泛使用
namespace ceph::buffer {
inline namespace v15_2_0 {

  // buffer::raw类是基础类，其子类完成了Buffer数据空间的分配
  // 类buffer::raw是一个原始的数据Buffer，在其基础之上添加了长度、引用计数和额外的crc校验信息
  class raw {
  public:
    // In the future we might want to have a slab allocator here with few
    // embedded slots. This would allow to avoid the "if" in dtor of ptr_node.
    std::aligned_storage<sizeof(ptr_node),
			 alignof(ptr_node)>::type bptr_storage;
  protected:
    // 数据指针
    char *data;
    // 数据长度
    unsigned len;
  public:
    // 引用计数(无锁，CAS)
    ceph::atomic<unsigned> nref { 0 };
    int mempool;

    // 下面两个pair是crc32校验信息
    // 数据段的起始和结束
    std::pair<size_t, size_t> last_crc_offset {std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max()};
    // pair<base crc32校验码, 加上数据段后计算出的crc32校验码>
    std::pair<uint32_t, uint32_t> last_crc_val;

    // 自旋锁，ceph自己实现的，基于std::atomic_flag来进行循环地test_and_set
    // 相较与互斥锁(mutex)将等待锁的线程置于等待队列中，自旋锁会忙等待并不停的进行锁请求(busy-waiting类型)，直到得到这个锁为止。所以，自旋锁一般用于多核的服务器。
    // 自旋锁适用于锁使用者保持锁时间比较短的情况下
    // 看《Ceph源码分析》中，用的是读写锁
    mutable ceph::spinlock crc_spinlock;

    explicit raw(unsigned l, int mempool=mempool::mempool_buffer_anon)
      : data(nullptr), len(l), nref(0), mempool(mempool) {
      mempool::get_pool(mempool::pool_index_t(mempool)).adjust_count(1, len);
    }
    raw(char *c, unsigned l, int mempool=mempool::mempool_buffer_anon)
      : data(c), len(l), nref(0), mempool(mempool) {
      mempool::get_pool(mempool::pool_index_t(mempool)).adjust_count(1, len);
    }
    virtual ~raw() {
      mempool::get_pool(mempool::pool_index_t(mempool)).adjust_count(
	-1, -(int)len);
    }

    void _set_len(unsigned l) {
      mempool::get_pool(mempool::pool_index_t(mempool)).adjust_count(
	-1, -(int)len);
      len = l;
      mempool::get_pool(mempool::pool_index_t(mempool)).adjust_count(1, len);
    }

    void reassign_to_mempool(int pool) {
      if (pool == mempool) {
	return;
      }
      mempool::get_pool(mempool::pool_index_t(mempool)).adjust_count(
	-1, -(int)len);
      mempool = pool;
      mempool::get_pool(mempool::pool_index_t(pool)).adjust_count(1, len);
    }

    void try_assign_to_mempool(int pool) {
      if (mempool == mempool::mempool_buffer_anon) {
	reassign_to_mempool(pool);
      }
    }

private:
    // no copying.
    // cppcheck-suppress noExplicitConstructor
    raw(const raw &other) = delete;
    const raw& operator=(const raw &other) = delete;
public:
    char *get_data() const {
      return data;
    }
    unsigned get_len() const {
      return len;
    }
    virtual raw* clone_empty() = 0;
    ceph::unique_leakable_ptr<raw> clone() {
      raw* const c = clone_empty();
      memcpy(c->data, data, len);
      return ceph::unique_leakable_ptr<raw>(c);
    }
    bool get_crc(const std::pair<size_t, size_t> &fromto,
		 std::pair<uint32_t, uint32_t> *crc) const {
      std::lock_guard lg(crc_spinlock);
      if (last_crc_offset == fromto) {
        *crc = last_crc_val;
        return true;
      }
      return false;
    }
    void set_crc(const std::pair<size_t, size_t> &fromto,
		 const std::pair<uint32_t, uint32_t> &crc) {
      std::lock_guard lg(crc_spinlock);
      last_crc_offset = fromto;
      last_crc_val = crc;
    }
    void invalidate_crc() {
      std::lock_guard lg(crc_spinlock);
      last_crc_offset.first = std::numeric_limits<size_t>::max();
      last_crc_offset.second = std::numeric_limits<size_t>::max();
    }
  };

} // inline namespace v15_2_0
} // namespace ceph::buffer

#endif // CEPH_BUFFER_RAW_H
