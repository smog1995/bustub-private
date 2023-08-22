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
  get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
  // right_empty_flag_ = !right_executor_->Next(&inner_table_tuple_, &temp);
}

void NestedLoopJoinExecutor::Init() {

}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID temp;
  //   左表已经遍历完毕
  if (!get_outer_tuple_) {
    return false;
  }
  bool get_inner_tuple = right_executor_->Next(&inner_table_tuple_, &temp);
  while (get_inner_tuple) {  //  内表层
    //   这个谓词是comparison_expression
    Value compare_res_value(plan_->Predicate().EvaluateJoin(&outer_table_tuple_, plan_->GetLeftPlan()->OutputSchema(), &inner_table_tuple_, plan_->GetRightPlan()->OutputSchema())); 
    Value true_value(TypeId::BOOLEAN, 1);
    if (compare_res_value.CompareEquals(true_value) ==  CmpBool::CmpTrue) {
      // 非自然连接，会有重复值的属性列
      *tuple = GetOutputTuple();
      return true;
    }
    get_inner_tuple = right_executor_->Next(&inner_table_tuple_, &temp);
    if (get_inner_tuple) {  
      get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
    } else {  //  说明内表循环完毕,
      right_executor_->Init();
      get_inner_tuple = right_executor_->Next(&inner_table_tuple_, &temp);
      if (!get_inner_tuple) {  //  重置后仍获得不了内表元组说明为空表
        right_empty_flag_ = true;
        if (plan_->GetJoinType() == JoinType::INNER && right_empty_flag_) {  //  右表为空而且为内连接，执行结束
        return false;
      }
      *tuple = GetOutputTuple(true);  //  返回悬浮元组
      get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
      return true;
      }
      
    }
  }

    //  如果内表遍历结束还没有获取到，则重置内表迭代器
    right_executor_->Init();
    get_inner_tuple = right_executor_->Next(&inner_table_tuple_, &temp);
    if (!get_inner_tuple) {
      //  重置后发现仍获取不到内表元组，说明内表为空表
      right_empty_flag_ = true;
      if (plan_->GetJoinType() == JoinType::LEFT) {  //  左连接，需要保持悬浮元组
        *tuple = GetOutputTuple(true);
        get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
        return true;
      }
    }
    get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
  }
  return false;
  }

  auto NestedLoopJoinExecutor::GetOutputTuple(bool is_left_join) ->Tuple {
  std::vector<Value> res_vector;
  int left_column_count = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
  int right_column_count = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
  res_vector.reserve(left_column_count + right_column_count);
  for (int col_index = 0; col_index < left_column_count; col_index++) {
    res_vector.emplace_back(outer_table_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_index));
  }
  if (!is_left_join) {
    for (int col_index = 0; col_index < right_column_count; col_index++) {
      res_vector.emplace_back(outer_table_tuple_.GetValue(&plan_->GetRightPlan()->OutputSchema(), col_index));
    }
  } else {  // 为左值连接，右边的属性列均为空
    for (int col_index = 0; col_index < right_column_count; col_index++) {
      Value col_index_value(plan_->GetRightPlan()->OutputSchema().GetColumn(col_index).GetType()); //创建空值Value
      res_vector.emplace_back(col_index_value);
    }
  }
  
  return {res_vector, &GetOutputSchema()};
}

}  // namespace bustub
