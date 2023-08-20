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

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),plan_(plan),aht_(plan->GetAggregates(),plan->GetAggregateTypes()), aht_iterator_(aht_.Begin()) {

    }

void AggregationExecutor::Init() {
    Tuple child_tuple;
    RID child_rid;
    bool fetch_child_res = child_->Next(&child_tuple, &child_rid);
    while (fetch_child_res) {
        aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
        fetch_child_res = child_->Next(&child_tuple, &child_rid);
    }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (aht_iterator_ == aht_.End()) {
        return false;
    }
    std::vector<Value> groupby_and_aggregation_vector(aht_iterator_.Key().group_bys_);
    for (auto& aggregate_value: aht_iterator_.Val().aggregates_) {
        groupby_and_aggregation_vector.emplace_back(aggregate_value);
    }
    Tuple res_tuple(groupby_and_aggregation_vector, &GetOutputSchema());
    *tuple = res_tuple;
    ++aht_iterator_;
    return true;
 }

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
