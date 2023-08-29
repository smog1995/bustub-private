//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  // std::cout << "limit_init" << std::endl;
  child_executor_->Init();
  current_index_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (current_index_ >= plan_->limit_) {
    return false;
  }
  bool get_tuple = child_executor_->Next(tuple, rid);
  if (!get_tuple) {
    return false;
  }
  current_index_++;
  // std::cout << "limit index" << current_index_ << " ";
  return true;
}

}  // namespace bustub
