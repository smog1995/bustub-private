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
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

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
  left_empty_flag_ = !left_executor_->Next(&outer_table_tuple_, &temp);
  right_empty_flag_ = !right_executor_->Next(&inner_table_tuple_, &temp);
}

void NestedLoopJoinExecutor::Init() {

}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID temp;
  //   如果已经执行完毕，或者左表为空，或者右表为空而且为内连接，执行结束
  if (left_empty_flag_ || join_finish_ || (plan_->GetJoinType() == JoinType::INNER && right_empty_flag_)) {
    return false;
  }
  bool get_outer_tuple = true;
  while (get_outer_tuple) {
    bool get_inner_tuple = ;
    while (get_inner_tuple) {
      //   我猜这个谓词是ccomparison_expression
      Value compare_res_value(plan_->Predicate().EvaluateJoin(&outer_table_tuple_, plan_->GetLeftPlan()->OutputSchema(), &inner_table_tuple_, plan_->GetRightPlan()->OutputSchema())); 
      Value true_value(TypeId::BOOLEAN,1);
      if (compare_res_value.CompareEquals(true_value) ==  CmpBool::CmpTrue) {
        // 非自然连接，会有重复值的属性列
        *tuple = GetOutputTuple();
        right_executor_->Next(&inner_table_tuple_, &temp);
        return true;
      }
      get_inner_tuple = right_executor_->Next(&inner_table_tuple_, &temp);
      
    }
    //  如果仍未返回，则获取外表下一个元组，并重置内表迭代器
    get_out_tuple = left_executor_->Next(&outer_table_tuple_, &temp);
    right_executor_->Init();
    get_inner_tuple = right_executor_->Next(&inner_table_tuple_, &temp);
    if (!get_inner_tuple) {
      //  说明右表为空表
      right_empty_flag_ = true;
      return false;
    }
  }
  return false;
}

auto NestedLoopJoinExecutor::GetOutputTuple() ->Tuple {
  std::vector<Value> res_vector;
  int left_column_count = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
  int right_column_count = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
  res_vector.reserve(left_column_count + right_column_count);
  for (int col_index = 0; col_index < left_column_count; col_index++) {
    res_vector.emplace_back(outer_table_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_index));
  }
  for (int col_index = 0; col_index < right_column_count; col_index++) {
    res_vector.emplace_back(outer_table_tuple_.GetValue(&plan_->GetRightPlan()->OutputSchema(), col_index));
  }
  return {res_vector, &GetOutputSchema()};
}

}  // namespace bustub
