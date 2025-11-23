#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), iterator_(0) {}

void TopNExecutor::Init() {
    child_executor_->Init();
    const Schema &schema = child_executor_->GetOutputSchema();
    const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &group_bys = plan_->GetOrderBy();
    auto cmp = [&](const Tuple& t1, const Tuple& t2) {
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
    };

    std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> tuple_que(cmp);

    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
        if (tuple_que.empty() || tuple_que.size() < plan_->GetN()) {
            tuple_que.push(tuple);
        } else {
            if (cmp(tuple, tuple_que.top())) {
                tuple_que.pop();
                tuple_que.push(tuple);
            }
        }
    }
    while (!tuple_que.empty()) {
        Tuple tp = tuple_que.top();
        sort_tuples.push_back(tp);
        tuple_que.pop();
    }
    std::reverse(sort_tuples.begin(), sort_tuples.end());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (iterator_ >= plan_->GetN() || iterator_ >= sort_tuples.size()) {
        return false;
    }
    *tuple = sort_tuples[iterator_];
    *rid = tuple->GetRid();
    ++iterator_;
    return true;
}
}  // namespace bustub
