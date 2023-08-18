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
    : AbstractExecutor(exec_ctx) {
  auto b_plus_tree_index = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
  exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())->index_.get());
  //  make_unique函数可以简化动态分配unique_ptr对象的过程，并避免了手动释放资源的问题。
  index_iterator_ = std::make_unique<IndexIterator<IntegerKeyType, IntegerValueType, IntegerComparatorType>>(b_plus_tree_index->GetBeginIterator());

}

void IndexScanExecutor::Init() { throw NotImplementedException("IndexScanExecutor is not implemented"); }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  **index_iterator
}

}  // namespace bustub
