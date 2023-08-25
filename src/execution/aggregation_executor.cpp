//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/varlen_type.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {
  std::cout << "init" << std::endl;
  Tuple child_tuple;
  RID child_rid;
  bool fetch_child_res = child_->Next(&child_tuple, &child_rid);
  while (fetch_child_res) {
    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
    fetch_child_res = child_->Next(&child_tuple, &child_rid);
    aht_iterator_ = aht_.Begin();
    // std::cout<<"insert"<<std::endl;
  }
}

void AggregationExecutor::Init() { aht_iterator_ = aht_.Begin(); }

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End() &&
      execute_flag_) {  //  execute_flag用来标记是否为空表,空表的话一次next都不执行则不会返回tuple
    std::cout << "end" << std::endl;
    return false;
  }
  execute_flag_ = true;
  if (aht_iterator_ == aht_.End()) {  //  说明为空表
    if (!plan_->GetGroupBys().empty()) {  //  如果group by属性不为空，则返回false，因为空表无法进行聚集
      // std::cout << "空表" << std::endl;

      return false;
    }
    std::vector<Value> aggregate_vals;
    for (auto &aggregate_type : plan_->GetAggregateTypes()) {
      if (aggregate_type == AggregationType::CountStarAggregate) {
        aggregate_vals.emplace_back(ValueFactory::GetIntegerValue(0));
      } else {
        aggregate_vals.emplace_back(ValueFactory::GetNullValueByType(INTEGER));
      }
    }
    Tuple res_tuple(aggregate_vals, &GetOutputSchema());
    *tuple = res_tuple;
    return true;
  }
  std::vector<Value> groupby_and_aggregation_vector(aht_iterator_.Key().group_bys_);
  for (auto &aggregate_value : aht_iterator_.Val().aggregates_) {
    groupby_and_aggregation_vector.emplace_back(aggregate_value);
  }
  Tuple res_tuple(groupby_and_aggregation_vector, &GetOutputSchema());
  *tuple = res_tuple;
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
