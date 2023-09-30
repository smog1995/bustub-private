//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "common/config.h"
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // std::cout<<"seq_scan"<<std::endl;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_);
  // catalog->CreateTable(exec_ctx_->GetTransaction(), plan_->table_name_, GetOutputSchema());
  // 实际上不需要创建表，只是执行查询操作 GetTable 获取到TableInfo类，该类的tables_成员变量是TableHeap的智能指针，
  // 再用begin函数进行构造Iterator
  table_iterator_ =
      std::make_unique<TableIterator>(TableIterator(table_info_->table_->Begin(exec_ctx_->GetTransaction())));
  transaction_ = exec_ctx_->GetTransaction();
  if (transaction_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    exec_ctx_->GetLockManager()->LockTable(transaction_, LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
  }
}

void SeqScanExecutor::Init() {
  table_iterator_ =
      std::make_unique<TableIterator>(TableIterator(table_info_->table_->Begin(exec_ctx_->GetTransaction())));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "seqscan next" << std::endl;
  if (*table_iterator_ == table_info_->table_->End()) {
    return false;
  }
  if (transaction_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    bool lock_res = exec_ctx_->GetLockManager()->LockRow(transaction_, LockManager::LockMode::SHARED, table_info_->oid_,
                                                         (*table_iterator_)->GetRid());
    if (!lock_res) {
      transaction_->SetState(TransactionState::ABORTED);
      return false;
    }
  }
  *tuple = **table_iterator_;
  ++*table_iterator_;
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
