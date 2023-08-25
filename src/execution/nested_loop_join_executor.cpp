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
#include <utility>
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
  std::cout<<"loopjoin"<<std::endl;
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  RID temp;
  get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
  get_inner_tuple_ = right_executor_->Next(&inner_table_tuple_, &temp);
  // std::cout<<"gouzao"<<std::endl;
  if (!get_inner_tuple_) {
    //  内表为空时
    std::cout << 1 << std::endl;
    right_empty_flag_ = true;
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  std::cout << "Init";
  RID temp;
  get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
  get_inner_tuple_ = right_executor_->Next(&inner_table_tuple_, &temp);
  // std::cout<<"gouzao"<<std::endl;
  if (!get_inner_tuple_) {
    //  内表为空时
    std::cout << 1 << std::endl;
    right_empty_flag_ = true;
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID temp;
  //  处理内表为空的情况 ----------------------------
  // std::cout<<"next"<<std::endl;
  if (!get_outer_tuple_) {  //   外表循环完毕
    std::cout<<"outer_table_recur_finish."<<std::endl;
    return false;
  }
  if (right_empty_flag_) {  //  内表为空表
    //  处理内表为空的内连接
    std::cout << "right_table is empty." << std::endl;
    if (plan_->GetJoinType() == JoinType::INNER) {
      return false;
    }
    // 处理内表为空的左外连接      内表为空但是不为内连接，即左外连接,返回悬浮元组
    *tuple = GetOutputTuple(true);
    get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
    return true;
  }
  //  -------------------------------------------
  if (!get_inner_tuple_) {    //  内表循环完毕
    std::cout<<"右表遍历完毕，外表开始执行下一个元组"<<std::endl;
    right_executor_->Init();  //  重置内表
    get_inner_tuple_ = right_executor_->Next(&inner_table_tuple_, &temp);
    /**  左外连接，而且该外表元组还未匹配到内表的任一元组，则需返回悬空元组
     *   在返回悬浮元组之前，记得获取下一个外表元组
     */
    if (plan_->GetJoinType() == JoinType::LEFT && !current_outer_tuple_has_join_) {  //  需要检查是否为悬浮元组
      std::cout << "悬浮元组" << std::endl;
      *tuple = GetOutputTuple(true);
      get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
      return true;
    }
    get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
    current_outer_tuple_has_join_ = false;
  }

  while (get_inner_tuple_ && get_outer_tuple_) {  //  内表层(内表不为空)
    //   这个谓词是可能是comparison_expression, 也可能是constant_value_expression(当无条件连接时)
    Value compare_res_value(plan_->Predicate().EvaluateJoin(&outer_table_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                                                            &inner_table_tuple_,
                                                            plan_->GetRightPlan()->OutputSchema()));
    std::cout<<"nest_loop谓词:"<<plan_->Predicate().ToString()<<std::endl;
    Value true_value(TypeId::BOOLEAN, 1);
    if (compare_res_value.CompareEquals(true_value) == CmpBool::CmpTrue) {
      // 非自然连接，会有重复值的属性列
      *tuple = GetOutputTuple();
      current_outer_tuple_has_join_ = true;  //  外表当前元组已经成功匹配，为非悬浮元组，标记为true
      get_inner_tuple_ = right_executor_->Next(&inner_table_tuple_, &temp);
      std::cout << "compare true" << std::endl;
      return true;
    }
    get_inner_tuple_ = right_executor_->Next(&inner_table_tuple_, &temp);
    // std::cout << get_inner_tuple_ << " ";
    if (!get_inner_tuple_) {  //  说明内表循环完毕,但仍未匹配到连接
      right_executor_->Init();
      get_inner_tuple_ = right_executor_->Next(&inner_table_tuple_, &temp);
      if (plan_->GetJoinType() == JoinType::LEFT && !current_outer_tuple_has_join_) {  //  需要检查是否为悬浮元组
        *tuple = GetOutputTuple(true);
        std::cout << "悬浮元组(while内)" << std::endl;
        get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
        return true;
      }
      // std::cout << get_outer_tuple_ << " ";
      get_outer_tuple_ = left_executor_->Next(&outer_table_tuple_, &temp);
      current_outer_tuple_has_join_ = false;
    }
  }
  std::cout << "end" << std::endl;
  return false;
}

auto NestedLoopJoinExecutor::GetOutputTuple(bool is_dangling_tuple) -> Tuple {
  std::vector<Value> res_vector;
  int left_column_count = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
  int right_column_count = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
  res_vector.reserve(left_column_count + right_column_count);
  for (int col_index = 0; col_index < left_column_count; col_index++) {
    res_vector.emplace_back(outer_table_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_index));
  }
  if (!is_dangling_tuple) {
    for (int col_index = 0; col_index < right_column_count; col_index++) {
      res_vector.emplace_back(inner_table_tuple_.GetValue(&plan_->GetRightPlan()->OutputSchema(), col_index));
    }
  } else {  // 为左值连接，右边的属性列均为空
    for (int col_index = 0; col_index < right_column_count; col_index++) {
      Value col_index_value(plan_->GetRightPlan()->OutputSchema().GetColumn(col_index).GetType(),
                            BUSTUB_INT32_NULL);  //创建空值Value
      res_vector.emplace_back(col_index_value);
    }
  }

  return {res_vector, &GetOutputSchema()};
}

}  // namespace bustub
