#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <vector>

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  Tuple temp_tuple;
  RID temp_rid;
  bool get_child_tuple = child_executor_->Next(&temp_tuple, &temp_rid);
  while (get_child_tuple) {
    int value_nums = plan_->output_schema_->GetColumnCount();
    std::vector<Value> values_vector(value_nums);
    for (int index = 0; index < value_nums; index++) {
        values_vector.emplace_back(temp_tuple.GetValue(plan_->output_schema_.get(), index));
    }
    container_.emplace_back(values_vector);
    get_child_tuple = child_executor_->Next(&temp_tuple, &temp_rid);
  }
    auto order_by = plan_->GetOrderBy();
    auto cmp = [&order_by, &plan](Tuple &a, Tuple &b) {
        for (auto &key : order_by) {
            if(key.first == OrderByType::DESC) {
                Value column_value_on_a = key.second->Evaluate(&a, plan->OutputSchema());
                Value column_value_on_b = key.second->Evaluate(&b, plan->OutputSchema());
                if (column_value_on_a.CompareEquals(column_value_on_b) == CmpBool::CmpTrue) {
                    continue;
                } else if (key.first )
            }
        }
    };
    std::sort(container_.begin(), container_.end(),cmp);
}

void SortExecutor::Init() {

}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
