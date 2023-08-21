//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  RID temp;
  left_executor_->Next(&outer_table_tuple_, &temp);
}

void NestedLoopJoinExecutor::Init() {

}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID temp;
  if (join_finish_) {
    return false;
  }
  bool get_inner_tuple = right_executor_->Next(&inner_table_tuple_, &temp);
  if (!get_inner_tuple) {
    bool get_out_tuple = left_executor_->Next(&outer_table_tuple_, &temp);
    if (!get_out_tuple) {
      join_finish_ = true;
    }
  }
  while (get_inner_tuple) {
    plan_->Predicate().Evaluate(const Tuple *tuple, const Schema &schema)
  }
}

}  // namespace bustub
