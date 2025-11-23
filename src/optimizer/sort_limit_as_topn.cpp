#include "optimizer/optimizer.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const AbstractPlanNodeRef &exp_ref: plan->GetChildren()) {
    children.push_back(OptimizeSortLimitAsTopN(exp_ref));
  }
  std::unique_ptr<AbstractPlanNode> opt_plan = plan->CloneWithChildren(children);
  if (opt_plan->GetType() == PlanType::Limit) {
    const LimitPlanNode *limit_plan = dynamic_cast<const LimitPlanNode *>(opt_plan.get());
    auto child_plan = limit_plan->GetChildPlan();
    if (child_plan->GetType() == PlanType::Sort) {
       const SortPlanNode *sort_plan = dynamic_cast<const SortPlanNode *>(child_plan.get());
       return std::make_shared<TopNPlanNode>(limit_plan->output_schema_,  sort_plan->GetChildPlan(), sort_plan->GetOrderBy(), limit_plan->GetLimit());
    }
  }
  return opt_plan;
}

}  // namespace bustub
