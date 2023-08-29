//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), outer_table_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  index_info_ = exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->inner_table_oid_);
  RID temp;
  get_outer_tuple_ = outer_table_executor_->Next(&outer_table_tuple_, &temp);
}

void NestIndexJoinExecutor::Init() { std::cout << "index_join init" << std::endl; }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!get_outer_tuple_) {
    return false;
  }
  RID temp;
  // std::cout<<"元组格式："<<outer_table_tuple_.KeyFromTuple(plan_->InnerTableSchema(),
  // *index_info_->index_->GetKeySchema(),
  //                                   index_info_->index_->GetKeyAttrs()).ToString(index_info_->index_->GetKeySchema())<<std::endl;
  index_info_->index_->ScanKey(
      GetPredicateTuple().KeyFromTuple(plan_->InnerTableSchema(), *index_info_->index_->GetKeySchema(),
                                       index_info_->index_->GetKeyAttrs()),
      &current_outer_tuple_match_rids_, GetExecutorContext()->GetTransaction());
  while (current_outer_tuple_match_rids_.empty() && get_outer_tuple_) {
    if (current_outer_tuple_match_rids_.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      //  重新填装左右表元组
      *tuple = GetOutputTuple(true);
      get_outer_tuple_ = outer_table_executor_->Next(&outer_table_tuple_, &temp);
      return true;
    }
    //  否则为内连接，同时无匹配元组，需要开始执行外表下一个元组
    get_outer_tuple_ = outer_table_executor_->Next(&outer_table_tuple_, &temp);
    if (!get_outer_tuple_) {
      return false;
    }
    index_info_->index_->ScanKey(
        GetPredicateTuple().KeyFromTuple(plan_->InnerTableSchema(), *index_info_->index_->GetKeySchema(),
                                         index_info_->index_->GetKeyAttrs()),
        &current_outer_tuple_match_rids_, GetExecutorContext()->GetTransaction());
  }

  // std::cout<<current_outer_tuple_match_rids_.back().ToString()<<std::endl;
  bool get_inner_tuple = table_info_->table_->GetTuple(current_outer_tuple_match_rids_.back(), &inner_table_tuple_,
                                                       GetExecutorContext()->GetTransaction());
  current_outer_tuple_match_rids_.pop_back();
  assert(get_inner_tuple == true);
  *tuple = GetOutputTuple();
  get_outer_tuple_ = outer_table_executor_->Next(&outer_table_tuple_, &temp);
  return true;
}

auto NestIndexJoinExecutor::GetOutputTuple(bool is_dangling_tuple) -> Tuple {
  std::vector<Value> res_vector;
  int left_column_count = plan_->GetChildPlan()->OutputSchema().GetColumnCount();
  int right_column_count = plan_->inner_table_schema_->GetColumnCount();
  res_vector.reserve(left_column_count + right_column_count);
  for (int col_index = 0; col_index < left_column_count; col_index++) {
    res_vector.emplace_back(outer_table_tuple_.GetValue(&plan_->GetChildPlan()->OutputSchema(), col_index));
  }
  if (!is_dangling_tuple) {
    for (int col_index = 0; col_index < right_column_count; col_index++) {
      res_vector.emplace_back(inner_table_tuple_.GetValue(plan_->inner_table_schema_.get(), col_index));
    }
  } else {  // 为左值连接，右边的属性列均为空
    for (int col_index = 0; col_index < right_column_count; col_index++) {
      Value col_index_value(plan_->inner_table_schema_->GetColumn(col_index).GetType(),
                            BUSTUB_INT32_NULL);  //  创建空值Value
      res_vector.emplace_back(col_index_value);
    }
  }

  return {res_vector, &GetOutputSchema()};
}

auto NestIndexJoinExecutor::GetPredicateTuple() -> Tuple {
  Value predicate_value(plan_->key_predicate_->Evaluate(&outer_table_tuple_, plan_->GetChildPlan()->OutputSchema()));
  std::vector<Value> predicate_vec;
  predicate_vec.emplace_back(predicate_value);
  return {predicate_vec, index_info_->index_->GetKeySchema()};
}

}  // namespace bustub
