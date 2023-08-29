//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // std::cout<< "index_scan"<<std::endl;
  auto index_info = exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid());
  b_plus_tree_index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  //  make_unique函数可以简化动态分配unique_ptr对象的过程，并避免了手动释放资源的问题。
  index_iterator_ = std::make_unique<IndexIterator<IntegerKeyType, IntegerValueType, IntegerComparatorType>>(
      b_plus_tree_index_->GetBeginIterator());
  // table_info_ = exec_ctx->GetCatalog()->GetTable()->table_.get();
  table_info_ = exec_ctx->GetCatalog()->GetTable(index_info->table_name_);
}

void IndexScanExecutor::Init() {
  index_iterator_ = std::make_unique<IndexIterator<IntegerKeyType, IntegerValueType, IntegerComparatorType>>(
      b_plus_tree_index_->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_iterator_->IsEnd()) {
    return false;
  }
  std::pair<IntegerKeyType, IntegerValueType> pair = **index_iterator_;
  *rid = pair.second;
  ++*index_iterator_;
  table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  // std::cout << tuple->ToString(&table_info_->schema_) << std::endl;
  return true;
}

}  // namespace bustub
