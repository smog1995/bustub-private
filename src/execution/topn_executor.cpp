#include "execution/executors/topn_executor.h"
#include <iostream>
#include <memory>
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // std::cout<<"topnzhixq";
  Tuple temp_tuple;
  RID temp_rid;
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
  //   auto topn_heap = std::make_unique<std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)>>(cmp);
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> topn_heap(cmp);
  // bool get_tuple = false;
  // for (size_t index = plan_->GetN(); index > 0; index--) {
  //   get_tuple = child_executor_->Next(&temp_tuple, &temp_rid);
  //   if (!get_tuple) {
  //       break;
  //   }
  //   topn_heap.push(temp_tuple);
  // }
  while (child_executor_->Next(&temp_tuple, &temp_rid)) {
    if (topn_heap.size() < plan_->GetN()) {
      topn_heap.push(temp_tuple);
    } else {
      Tuple top_tuple = topn_heap.top();
      if (!cmp(top_tuple, temp_tuple)) {
        // std::cout<<"toptuple:"<<top_tuple.ToString(&plan_->OutputSchema())<<"
        // temp_tuple:"<<temp_tuple.ToString(&plan_->OutputSchema())<<std::endl;
        topn_heap.pop();
        topn_heap.push(temp_tuple);
      }
    }
  }
  topn_tuples_.resize(topn_heap.size());
  for (int index = topn_heap.size() - 1; index >= 0; index--) {
    topn_tuples_[index] = topn_heap.top();
    topn_heap.pop();
  }
  current_index_ = 0;
}

void TopNExecutor::Init() { current_index_ = 0; }

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (current_index_ >= topn_tuples_.size()) {
    return false;
  }
  std::cout << current_index_ << " ";
  *tuple = topn_tuples_[current_index_++];
  return true;
}

}  // namespace bustub
