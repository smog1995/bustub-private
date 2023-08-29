#include <memory>
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  // 先看底部子节点需要进行优化不,递归
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  //  无论是否执行优化，都需要把原先的子节点都移动过来
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  //  plan肯定是limit，从sort排完后的表一条条拿数据
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    const auto &child_plan = optimized_plan->children_[0];
    if (child_plan->GetType() != PlanType::Sort) {
      return optimized_plan;
    }
    size_t top_n = limit_plan.limit_;
    const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*optimized_plan->GetChildAt(0));
    auto order_bys = sort_plan.GetOrderBy();
    return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan.GetChildPlan(), order_bys,
                                          top_n);  //
  }
  return optimized_plan;
}

}  // namespace bustub
