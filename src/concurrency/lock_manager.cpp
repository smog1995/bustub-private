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

#include "concurrency/lock_manager.h"
#include <memory>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    throw "logic_exception!";
  }
  //  若为收缩阶段
  if (txn->GetState() == TransactionState::SHRINKING) {
    //  第一种: 读未提交，读锁禁用，收缩阶段也不能上写锁（对应两种异常）   第二种: 读已提交,收缩阶段只能上读锁，意向读锁
    //  第三种: 可重复读, 收缩阶段不能上锁
    if ((txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
         (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mode != LockMode::SHARED &&
         lock_mode != LockMode::INTENTION_SHARED) ||
        (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
        (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
         lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  //  若为增长阶段，读未提交是不能进行加读锁，意向读锁的
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }
  //  对锁表上锁，找到该数据项对应的请求队列,找完立即释放锁表的锁
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  //  将请求队列上锁
  lock_request_queue->latch_.lock();
  for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end();) {
    //  首先判断是否为一次事务锁升级，即如果请求队列中能找到该事务之前的上锁记录，则尝试升级锁
    if ((*ele)->txn_id_== txn->GetTransactionId()) {
      if (lock_mode == (*ele)->lock_mode_) {  //  假设两次请求一样
        return true; 
      }
      //  不允许多个事务同时尝试锁升级
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      //  如果升级不兼容，抛出异常
      if (((*ele)->lock_mode_ == LockMode::SHARED &&
           lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) ||
          ((*ele)->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
           lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) ||
          ((*ele)->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) ||
          ((*ele)->lock_mode_ == LockMode::EXCLUSIVE)) {
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      //  若以上情况均没有，则执行锁升级
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      auto lock_request = new LockRequest((*ele)->txn_id_, lock_mode, oid);
      lock_request_queue->request_queue_.push_back(lock_request);
      ele = lock_request_queue->request_queue_.erase(ele);

    }
    ele++;
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
