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
#include "common/config.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto catalog = exec_ctx_->GetCatalog();
  // catalog->CreateTable(exec_ctx_->GetTransaction(), plan_->table_name_, GetOutputSchema());
  // 实际上不需要创建表，只是执行查询操作 GetTable 获取到TableInfo类，该类的tables_成员变量是TableHeap的智能指针，
  // 再用begin函数进行构造Iterator
  table_iterator_ =
      new TableIterator(catalog->GetTable(plan_->table_name_)->table_->Begin(exec_ctx_->GetTransaction()));
}

SeqScanExecutor::~SeqScanExecutor() {
  delete table_iterator_;
  delete plan_;
}

// void SeqScanExecutor::Init() {

// }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  *tuple = **table_iterator_;
  if (tuple->GetRid().GetPageId() != INVALID_PAGE_ID) {
    *rid = tuple->GetRid();
    ++table_iterator_;
    return true;
  }
  return false;
}

}  // namespace bustub
