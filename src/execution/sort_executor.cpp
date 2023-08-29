#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <vector>
#include "binder/bound_order_by.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  Tuple temp_tuple;
  RID temp_rid;
  bool get_child_tuple = child_executor_->Next(&temp_tuple, &temp_rid);
  while (get_child_tuple) {
    container_.emplace_back(temp_tuple);
    get_child_tuple = child_executor_->Next(&temp_tuple, &temp_rid);
  }
  auto order_by = plan_->GetOrderBy();
  auto cmp = [&order_by, &plan](Tuple &a, Tuple &b) -> bool {
    for (auto &key : order_by) {
      Value column_value_on_a = key.second->Evaluate(&a, plan->OutputSchema());
      Value column_value_on_b = key.second->Evaluate(&b, plan->OutputSchema());
      if (key.first == OrderByType::DESC) {
        if (column_value_on_a.CompareEquals(column_value_on_b) == CmpBool::CmpTrue) {
          continue;
        }
        bool cmp_res = false;
        if (column_value_on_a.CompareGreaterThan(column_value_on_b) == CmpBool::CmpTrue) {
          cmp_res = true;
        }
        return cmp_res;
      }
      if (key.first == OrderByType::ASC || key.first == OrderByType::DEFAULT) {
        if (column_value_on_a.CompareEquals(column_value_on_b) == CmpBool::CmpTrue) {
          continue;
        }
        bool cmp_res = true;
        if (column_value_on_a.CompareGreaterThan(column_value_on_b) == CmpBool::CmpTrue) {
          cmp_res = false;
        }
        return cmp_res;
      }
      if (key.first == OrderByType::INVALID) {
        return true;
      }
    }
    return true;
  };
  std::sort(container_.begin(), container_.end(), cmp);
}

void SortExecutor::Init() {
  // std::cout << "sort_init" << std::endl;
  current_index_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (current_index_ >= container_.size()) {
    return false;
  }
  *tuple = container_[current_index_++];
  // std::cout << "sort index" << current_index_ << " ";
  return true;
}

}  // namespace bustub
