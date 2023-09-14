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
#include <algorithm>
#include <memory>
#include <unordered_set>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/index_scan_executor.h"

namespace bustub {

void PrintState(Transaction *txn) {
  if (txn->GetState() == TransactionState::GROWING) {
    std::cout << "Growing";
  } else if (txn->GetState() == TransactionState::SHRINKING) {
    std::cout << "Shrinking";
  } else if (txn->GetState() == TransactionState::ABORTED) {
    std::cout << "Aborted";
  } else {
    std::cout << "Commited";
  }
}
void PrintIsolationLevel(Transaction *txn) {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    std::cout << "Read_uncommited";
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    std::cout << "Read_commited";
  } else {
    std::cout << "Repeatable _read";
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return false;
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
      std::cout << "locktable:  lock on shrink " << std::endl;
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
        (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
         lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      std::cout << "locktable:LOCK_SHARED_ON_READ_UNCOMMITTED " << std::endl;
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }
  //  若为增长阶段，读未提交是不能进行加读锁，意向读锁的
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  //  对锁表上锁，找到该数据项对应的请求队列,找完立即释放锁表的锁
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = table_lock_map_[oid];
  //  将请求队列上锁
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();
  //  首先判断是否为一次事务锁升级，即如果请求队列中能找到该事务之前的上锁记录，则尝试升级锁
  bool is_lock_upgrade = false;
  for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end();) {
    if ((*ele)->txn_id_ == txn->GetTransactionId()) {
      if (lock_mode == (*ele)->lock_mode_) {  //  假设两次请求一样
        return true;
      }
      txn->LockTxn();
      //  不允许多个事务同时尝试锁升级
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        std::cout << txn->GetTransactionId() << ":locktable:UPGRADE_CONFLICT " << std::endl;
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      //  如果升级不兼容，抛出异常
      if (!update_lock_[static_cast<int>((*ele)->lock_mode_)][static_cast<int>(lock_mode)]) {
        std::cout << "locktable: INCOMPATIBLE_UPGRADE " << std::endl;
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        return false;
      }
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      //  尝试锁升级的事务肯定是上一个锁已经拿到了的，不然还会被阻塞在原队列，不可能跑来这里进行锁升级
      DeleteTableLockInTransaction(txn, (*ele)->lock_mode_, oid);
      is_lock_upgrade = true;
      ele = lock_request_queue->request_queue_.erase(ele);
      txn->UnlockTxn();
      break;
    }
    ele++;
  }

  while (!GrantLock(lock_request_queue.get(), lock_mode, txn->GetTransactionId())) {
    lock_request_queue->cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    if (is_lock_upgrade) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
    }
    return false;
  }
  auto lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  lock_request->granted_ = true;
  txn->LockTxn();
  InsertTableLockInTransaction(txn, lock_mode, oid);
  if (is_lock_upgrade) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;  //  锁升级完毕，状态重置
  }
  if (txn->GetState() != TransactionState::SHRINKING) {
    txn->SetState(TransactionState::GROWING);
  }
  txn->UnlockTxn();
  lock_request_queue->request_queue_.push_back(std::unique_ptr<LockRequest>(lock_request));
  return true;
}

void LockManager::DeleteTableLockInTransaction(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  PrintState(txn);
  PrintIsolationLevel(txn);
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      std::cout << txn->GetTransactionId() << "解意向读锁在表" << oid << std::endl;
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      std::cout << txn->GetTransactionId() << "解读锁在表" << oid << std::endl;
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      std::cout << txn->GetTransactionId() << "解意向读写锁在表" << oid << std::endl;
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      std::cout << txn->GetTransactionId() << "解写锁在表" << oid << std::endl;
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      std::cout << txn->GetTransactionId() << "解意向写锁在表" << oid << std::endl;
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
  PrintState(txn);
}

void LockManager::InsertTableLockInTransaction(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  PrintState(txn);
  PrintIsolationLevel(txn);
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:

      std::cout << "(状态)" << txn->GetTransactionId() << "加意向读锁在表" << oid << std::endl;
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED:
      std::cout << "(状态)" << txn->GetTransactionId() << "加读锁在表" << oid << std::endl;
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      std::cout << "(状态)" << txn->GetTransactionId() << "加意向读写锁在表" << oid << std::endl;
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      std::cout << "(状态)" << txn->GetTransactionId() << std::endl;
      break;
    case LockMode::EXCLUSIVE:
      std::cout << "(状态)" << txn->GetTransactionId() << "加写锁在表" << oid << std::endl;
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      std::cout << "(状态)" << txn->GetTransactionId() << "加意向写锁在表" << oid << std::endl;
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
  PrintState(txn);
}

auto LockManager::GrantLock(LockRequestQueue *lock_request_queue, LockMode lock_mode, txn_id_t txn_id) -> bool {
  //  第一步，判断队列中已授予锁的请求是否与当前请求的锁兼容
  for (auto &ele : lock_request_queue->request_queue_) {
    if (ele->granted_ && !compatable_lock_[static_cast<int>(lock_mode)][static_cast<int>(ele->lock_mode_)]) {
      return false;
    }
  }
  //  第二步，判断是否有锁升级，锁升级请求优先级最高，如果当前请求就是锁升级请求，直接通过，如果不是，返回false，优先级低于锁升级请求，得先让锁升级请求先过
  if (lock_request_queue->upgrading_ == txn_id) {
    return true;
  }
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    return false;
  }
  //  第三步，不存在锁升级，则根据优先级来决定是否授予锁,找到第一个waiting请求，如果是当前请求，返回true，如果不是当前请求，则看是否与第一个waiting的锁兼容，兼容则放行
  bool has_first_waiting = false;
  LockMode first_waiting_lock_mode = LockMode::INTENTION_SHARED;  //  随便初始化
  for (auto &ele : lock_request_queue->request_queue_) {
    //  找到第一个waiting请求
    if (!ele->granted_) {
      has_first_waiting = true;
      first_waiting_lock_mode = ele->lock_mode_;
    }
  }

  return !has_first_waiting ||
         (has_first_waiting &&
          compatable_lock_[static_cast<int>(lock_mode)][static_cast<int>(first_waiting_lock_mode)]);
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  //  如果行锁未全部释放，则异常
  // if (!txn->GetSharedLockSet()->empty() || !txn->GetExclusiveLockSet()->empty()) {
  if ((txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end() &&
       !txn->GetSharedRowLockSet()->find(oid)->second.empty()) ||
      (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end() &&
       !txn->GetExclusiveRowLockSet()->find(oid)->second.empty())) {
    std::cout << "该表上的行锁未全部释放" << std::endl;
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_latch_.unlock();
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  auto request_lock_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(request_lock_queue->latch_);
  table_lock_map_latch_.unlock();
  for (auto ele = request_lock_queue->request_queue_.begin(); ele != request_lock_queue->request_queue_.end(); ele++) {
    if ((*ele)->txn_id_ == txn->GetTransactionId()) {
      //  同时对事务的状态进行更新
      txn->LockTxn();
      DeleteTableLockInTransaction(txn, (*ele)->lock_mode_, oid);
      if (txn->GetState() == TransactionState::GROWING &&
          ((txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
            (*ele)->lock_mode_ == LockMode::EXCLUSIVE) ||
           (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && (*ele)->lock_mode_ == LockMode::EXCLUSIVE) ||
           (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
            ((*ele)->lock_mode_ == LockMode::EXCLUSIVE || (*ele)->lock_mode_ == LockMode::SHARED)))) {
        txn->SetState(TransactionState::SHRINKING);
      }
      txn->UnlockTxn();
      request_lock_queue->request_queue_.erase(ele);  //  将该请求移除队列
      request_lock_queue->cv_.notify_all();           //  锁释放，可以唤醒其他事务进行锁授予
      return true;
    }
  }
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return false;
  }
  txn->LockTxn();
  //  不能加意向锁
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    std::cout << " lockrow: ATTEMPTED_INTENTION_LOCK_ON_ROW" << std::endl;
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }
  //  如果没有对表加意向锁,异常
  if ((lock_mode == LockMode::SHARED && !txn->IsTableIntentionSharedLocked(oid) &&
       !txn->IsTableSharedIntentionExclusiveLocked(oid) && !txn->IsTableExclusiveLocked(oid) &&
       !txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid)) ||
      (lock_mode == LockMode::EXCLUSIVE && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
       !txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableExclusiveLocked(oid))) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    std::cout << " lockrow:TABLE_LOCK_NOT_PRESENT" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }
  //  若为收缩阶段
  if (txn->GetState() == TransactionState::SHRINKING) {
    //  第一种: 读未提交，读锁禁用，收缩阶段也不能上写锁（对应两种异常）   第二种: 读已提交,收缩阶段只能上读锁
    //  第三种: 可重复读, 收缩阶段不能上锁
    if ((txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::EXCLUSIVE) ||
        (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mode != LockMode::SHARED) ||
        (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      std::cout << " lockrow:LOCK_ON_SHRINKING" << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    //  读未提交上的读锁都是不被允许的
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      std::cout << " lockrow:LOCK_SHARED_ON_READ_UNCOMMITTED" << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }
  //  若为增长阶段，读未提交是不能进行加读锁的
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      lock_mode == LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    std::cout << " lockrow:LOCK_SHARED_ON_READ_UNCOMMITTED" << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  txn->UnlockTxn();
  //  对锁表上锁，找到该数据项对应的请求队列,找完立即释放锁表的锁
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = row_lock_map_[rid];
  //  将请求队列上锁
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  //  首先判断是否为一次事务锁升级，即如果请求队列中能找到该事务之前的上锁记录，则尝试升级锁
  bool is_lock_upgrade = false;
  for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end(); ele++) {
    if ((*ele)->txn_id_ == txn->GetTransactionId()) {
      if (lock_mode == (*ele)->lock_mode_) {  //  假设两次请求一样
        return true;
      }
      txn->LockTxn();
      //  不允许多个事务同时尝试锁升级
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        std::cout << " lockrow: UPGRADE_CONFLICT" << std::endl;
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      //  如果升级不兼容，抛出异常
      if (!update_lock_[static_cast<int>((*ele)->lock_mode_)][static_cast<int>(lock_mode)]) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        std::cout << " lockrow:INCOMPATIBLE_UPGRADE" << std::endl;
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        return false;
      }
      //  若以上情况均没有，则执行锁升级
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      if ((*ele)->lock_mode_ == LockMode::SHARED) {
        txn->GetSharedRowLockSet()->find(oid)->second.erase(rid);
        txn->GetSharedLockSet()->erase(rid);
        std::cout << txn->GetTransactionId() << "对记录" << rid.ToString() << "解读锁在表" << oid << std::endl;
      } else if ((*ele)->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->GetExclusiveRowLockSet()->find(oid)->second.erase(rid);
        std::cout << txn->GetTransactionId() << "对记录" << rid.ToString() << "解写锁在表" << oid << std::endl;
        txn->GetExclusiveLockSet()->erase(rid);
      }
      txn->UnlockTxn();
      //  尝试锁升级的事务肯定是上一个锁已经拿到了的，不然还会被阻塞在原队列，不可能跑来这里进行锁升级
      lock_request_queue->request_queue_.erase(ele);
      is_lock_upgrade = true;
      break;
    }
  }

  while (!GrantLock(lock_request_queue.get(), lock_mode, txn->GetTransactionId())) {
    lock_request_queue->cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    if (is_lock_upgrade) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
    }
    return false;
  }
  auto lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request->granted_ = true;
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {
    // std::cout << "insert row lock" << std::endl;
    txn->GetSharedLockSet()->insert(rid);
    if (txn->GetSharedRowLockSet()->find(oid) == txn->GetSharedRowLockSet()->end()) {
      std::unordered_set<RID> set;
      set.insert(rid);
      std::pair<table_oid_t, std::unordered_set<RID>> pair(oid, set);
      txn->GetSharedRowLockSet()->insert(pair);
    } else {
      txn->GetSharedRowLockSet()->find(oid)->second.insert(rid);
    }
    std::cout << txn->GetTransactionId() << "对记录" << rid.ToString() << "加读锁在表" << oid << std::endl;
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveLockSet()->insert(rid);
    if (txn->GetExclusiveRowLockSet()->find(oid) == txn->GetSharedRowLockSet()->end()) {
      std::unordered_set<RID> set;
      set.insert(rid);
      std::pair<table_oid_t, std::unordered_set<RID>> pair(oid, set);
      txn->GetExclusiveRowLockSet()->insert(pair);
    } else {
      txn->GetExclusiveRowLockSet()->find(oid)->second.insert(rid);
    }
    std::cout << txn->GetTransactionId() << "对记录" << rid.ToString() << "加写锁在表" << oid << std::endl;
  }
  if (txn->GetState() != TransactionState::SHRINKING) {
    txn->SetState(TransactionState::GROWING);
  }
  txn->UnlockTxn();
  lock_request_queue->request_queue_.push_back(std::unique_ptr<LockRequest>(lock_request));
  if (is_lock_upgrade) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;  //  锁升级完毕，状态重置
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_latch_.unlock();
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  auto request_lock_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(request_lock_queue->latch_);
  row_lock_map_latch_.unlock();
  for (auto ele = request_lock_queue->request_queue_.begin(); ele != request_lock_queue->request_queue_.end(); ele++) {
    if ((*ele)->txn_id_ == txn->GetTransactionId()) {
      //  同时对事务的状态进行更新
      txn->LockTxn();
      if ((*ele)->lock_mode_ == LockMode::SHARED) {
        txn->GetSharedLockSet()->erase(rid);
        txn->GetSharedRowLockSet()->find(oid)->second.erase(rid);

      } else if ((*ele)->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->GetExclusiveLockSet()->erase(rid);
        txn->GetExclusiveRowLockSet()->find(oid)->second.erase(rid);
      }

      //  根据事务隔离级别决定是否进入收缩状态
      if (txn->GetState() == TransactionState::GROWING &&
          ((txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
            (*ele)->lock_mode_ == LockMode::EXCLUSIVE) ||
           (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && (*ele)->lock_mode_ == LockMode::EXCLUSIVE) ||
           (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
            ((*ele)->lock_mode_ == LockMode::EXCLUSIVE || (*ele)->lock_mode_ == LockMode::SHARED)))) {
        txn->SetState(TransactionState::SHRINKING);
      }
      // if (txn->GetState() == TransactionState::GROWING &&
      //     ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) ||
      //      (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && (*ele)->lock_mode_ ==
      //      LockMode::EXCLUSIVE))) {
      //   txn->SetState(TransactionState::SHRINKING);
      // }
      if ((*ele)->lock_mode_ == LockMode::SHARED) {
        txn->GetSharedRowLockSet()->find(oid)->second.erase(rid);
        txn->GetSharedLockSet()->erase(rid);
      } else if ((*ele)->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->GetExclusiveRowLockSet()->find(oid)->second.erase(rid);
        txn->GetExclusiveLockSet()->erase(rid);
      }
      request_lock_queue->request_queue_.erase(ele);  //  将该请求移除队列
      txn->UnlockTxn();
      request_lock_queue->cv_.notify_all();  //  锁释放，可以唤醒其他事务进行锁授予
      return true;
    }
  }
  txn->LockTxn();
  txn->SetState(TransactionState::ABORTED);
  txn->UnlockTxn();
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

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
