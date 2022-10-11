//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // 1. 如果事务状态abort，直接false
  // 2. 隔离级别是读未提交，直接abort，不需要读锁
  // 3. 如果不是两阶段锁的加锁阶段，直接abort
  // 4. 从事务的读锁集合判断当前行是否已经获得锁，已经获得，直接true
  // 5. 封装锁请求，插入到锁请求队列
  // 6. 事务的读锁集合插入行编号
  // 7. 锁请求队列的条件变量等待，直到满足条件，每次条件变量被唤醒都要检查当前事务是否abort
  //   a. 条件，如果锁请求队列的头部锁请求是读锁，那么满足条件
  //   b. 否则不满足条件，需要等待，可以尝试将锁请求队列中的某些锁请求对应事务abort
  //   c.
  //   遍历锁请求队列每个锁请求，如果遍历锁请求事务编号更大，也就是更年轻，并且锁类型是读锁，将锁请求对应的事务状态设置为abort
  //   d. 锁请求队列条件变量通知所有阻塞的，那些事务状态被设置位abort的将会abort

  if (CheckAbort(txn)) {
    return false;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // read uncommitted don't need LockShared.
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> guard(latch_);
  LockRequestQueue *lock_queue = &lock_table_[rid];
  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::SHARED);
  lock_queue->request_queue_.emplace_back(lock_request);
  txn->GetSharedLockSet()->emplace(rid);

  while (NeedWait(txn, lock_queue)) {
    lock_queue->cv_.wait(guard);
    LOG_DEBUG("%d: Awake and check itself.", txn->GetTransactionId());
    if (CheckAbort(txn)) {
      return false;
    }
  }

  for (auto &iter : lock_queue->request_queue_) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      iter.granted_ = true;
    }
  }
  txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 1. 如果事务状态abort，直接false
  // 2. 如果不是两阶段锁的加锁阶段，直接abort
  // 3. 如果事务的写锁集合有当前行编号，直接true
  // 4. 从锁管理器的锁哈希表，用当前行编号，获得锁请求队列
  // 5. 封装锁请求，插入锁请求队列
  // 6. 事务的写锁集合插入当前行编号
  // 7. 锁请求队列的条件变量等待，直到满足条件，每次条件变量被唤醒都需要检查当前事务是否abort
  //   a. 条件，锁请求队列的尾部是当前锁请求，如果锁请求队列大小为1，满足条件
  //   b. 否则，不满足条件，需要等待，可以尝试将锁请求队列中的某写锁请求的对应的事务abort
  //   c. 遍历锁请求队列每个锁请求，如果遍历锁请求事务编号更大，也就是更年轻，就将遍历到的锁请求的事务的状态设置为abort
  //   d. 锁请求队列条件变量通知所有阻塞的，那些事务状态被设置位abort的将会abort。

  if (CheckAbort(txn)) {
    return false;
  }

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> guard(latch_);
  LockRequestQueue *lock_queue = &lock_table_[rid];
  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_queue->request_queue_.emplace_back(lock_request);
  txn->GetExclusiveLockSet()->emplace(rid);

  while (NeedWait(txn, lock_queue)) {
    LOG_DEBUG("%d: Wait for exclusive lock", txn->GetTransactionId());
    lock_queue->cv_.wait(guard);
    LOG_DEBUG("%d: Awake and check itself.", txn->GetTransactionId());
    if (CheckAbort(txn)) {
      return false;
    }
  }

  LOG_DEBUG("%d: Get exclusive lock", txn->GetTransactionId());
  for (auto &iter : lock_queue->request_queue_) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      iter.granted_ = true;
    }
  }
  txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 1. 事务状态是abort，直接false
  // 2. 两阶段锁不是加锁阶段，直接abort
  // 3. 事务的写锁集合已经有当前行编号，直接true
  // 4. 从锁管理器中用行编号取出锁请求队列
  // 5. 锁请求队列条件变量等待，直到满足条件，每次条件变量被唤醒都检查当前事务是否被abort
  // 6.
  // 从锁请求队列中找到唯一一个锁请求的事务编号和当前事务编号相同的锁请求，将锁请求的锁类型变为写锁。从事务的读锁集合删除当前行编号，从事务的写锁集合插入当前行编号

  if (CheckAbort(txn)) {
    return false;
  }

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> guard(latch_);

  LockRequestQueue *lock_queue = &lock_table_[rid];

  while (NeedWaitUpdate(txn, lock_queue)) {
    lock_queue->cv_.wait(guard);
    if (CheckAbort(txn)) {
      return false;
    }
  }

  for (auto iter : lock_queue->request_queue_) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      iter.granted_ = true;
      iter.lock_mode_ = LockMode::EXCLUSIVE;
      txn->SetState(TransactionState::GROWING);
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->emplace(rid);
      break;
    }
  }
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // 1. 事务状态是abort，直接false
  // 2. 两阶段锁不是加锁阶段，直接abort
  // 3. 事务的写锁集合已经有当前行编号，直接true
  // 4. 从锁管理器中用行编号取出锁请求队列
  // 5. 锁请求队列条件变量等待，直到满足条件，每次条件变量被唤醒都检查当前事务是否被abort
  //   a. 条件，遍历锁请求队列
  //   b. 如果遍历锁请求的事务编号比当前锁请求的事务编号更年轻，遍历锁请求的事务状态设置为abort
  //   c. 锁请求队列条件变量通知所有阻塞的，那些事务状态如果被设置为abort将会真正abort
  // 6.
  // 从锁请求队列中找到唯一一个锁请求的事务编号和当前事务编号相同的锁请求，将锁请求的锁类型变为写锁。从事务的读锁集合删除当前行编号，从事务的写锁集合插入当前行编号

  LOG_DEBUG("%d: Unlock", txn->GetTransactionId());
  if (!txn->IsSharedLocked(rid) && !txn->IsExclusiveLocked(rid)) {
    return false;
  }

  std::unique_lock<std::mutex> guard(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  if (lock_queue.upgrading_ == txn->GetTransactionId()) {
    lock_queue.upgrading_ = INVALID_TXN_ID;
  }
  bool found = false;
  for (auto iter = lock_queue.request_queue_.begin(); iter != lock_queue.request_queue_.end(); iter++) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      found = true;
      lock_queue.request_queue_.erase(iter);

      lock_queue.cv_.notify_all();
      break;
    }
  }

  if (!found) {
    return false;
  }

  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

bool LockManager::NeedWait(Transaction *txn, LockRequestQueue *lock_queue) {
  auto self = lock_queue->request_queue_.back();

  auto first_iter = lock_queue->request_queue_.begin();
  if (self.lock_mode_ == LockMode::SHARED) {
    if (first_iter->txn_id_ == txn->GetTransactionId() || first_iter->lock_mode_ == LockMode::SHARED) {
      return false;
    }
  } else {
    if (first_iter->txn_id_ == txn->GetTransactionId()) {
      return false;
    }
  }

  // need wait, try to prevent it.
  bool need_wait = false;
  bool has_aborted = false;

  for (auto iter = first_iter; iter->txn_id_ != txn->GetTransactionId(); iter++) {
    if (iter->txn_id_ > txn->GetTransactionId()) {
      bool situation1 = self.lock_mode_ == LockMode::SHARED && iter->lock_mode_ == LockMode::EXCLUSIVE;
      bool situation2 = self.lock_mode_ == LockMode::EXCLUSIVE;
      if (situation1 || situation2) {
        // abort younger
        Transaction *younger_txn = TransactionManager::GetTransaction(iter->txn_id_);
        if (younger_txn->GetState() != TransactionState::ABORTED) {
          LOG_DEBUG("%d: Abort %d", txn->GetTransactionId(), iter->txn_id_);
          younger_txn->SetState(TransactionState::ABORTED);
          has_aborted = true;
        }
      }
      continue;
    }

    if (self.lock_mode_ == LockMode::EXCLUSIVE) {
      need_wait = true;
    }

    if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
      need_wait = true;
    }
  }

  if (has_aborted) {
    lock_queue->cv_.notify_all();
  }

  return need_wait;
}

bool LockManager::NeedWaitUpdate(Transaction *txn, LockRequestQueue *lock_queue) {
  bool need_wait = false;
  bool has_aborted = false;

  for (auto iter = lock_queue->request_queue_.begin(); iter->txn_id_ != txn->GetTransactionId(); iter++) {
    if (iter->txn_id_ > txn->GetTransactionId()) {
      LOG_DEBUG("%d: Abort %d", txn->GetTransactionId(), iter->txn_id_);
      Transaction *younger_txn = TransactionManager::GetTransaction(iter->txn_id_);
      if (younger_txn->GetState() != TransactionState::ABORTED) {
        younger_txn->SetState(TransactionState::ABORTED);
        has_aborted = true;

      continue;
    }

    need_wait = true;
  }

  if (has_aborted) {
    lock_queue->cv_.notify_all();
  }

  return need_wait;
}

bool LockManager::CheckAbort(Transaction *txn) { return txn->GetState() == TransactionState::ABORTED; }

}  // namespace bustub
