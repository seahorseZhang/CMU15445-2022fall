#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), iterator_(0) {}

void SortExecutor::Init() {
    child_executor_->Init();
    sort_tuples_.clear();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
        sort_tuples_.push_back(tuple);
    }

    const Schema &schema = child_executor_->GetOutputSchema();
    const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &group_bys = plan_->GetOrderBy();
    std::sort(sort_tuples_.begin(), sort_tuples_.end(), [&](const Tuple &t1, const Tuple &t2) {
        for (const auto &[orderby_type, sort_expr]: group_bys) {
            Value key1 = sort_expr->Evaluate(&t1, schema);
            Value key2 = sort_expr->Evaluate(&t2, schema);

            if (key1.CompareEquals(key2) == CmpBool::CmpTrue) {
                continue;
            }

            if (orderby_type == OrderByType::ASC || orderby_type == OrderByType::DEFAULT) {
                return key1.CompareLessThan(key2) == CmpBool::CmpTrue;
            }

            return key1.CompareGreaterThan(key2) == CmpBool::CmpTrue;
        }
        return false;
    });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (iterator_ >= sort_tuples_.size()) {
        return false;
    }
    *tuple = sort_tuples_[iterator_];
    ++iterator_;
    return true;
}

}  // namespace bustub
