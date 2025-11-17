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

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_(plan->aggregates_, plan->agg_types_), aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
    child_->Init();
    Tuple tuple;
    RID rid;
    while (child_->Next(&tuple, &rid)) {
        AggregateKey key = MakeAggregateKey(&tuple);
        AggregateValue value = MakeAggregateValue(&tuple);
        aht_.InsertCombine(key, value);
    }
    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (aht_iterator_ == aht_.End()) {
        return false;
    }
    const AggregateKey &agg_key = aht_iterator_.Key();
    const AggregateValue &agg_value = aht_iterator_.Val();
    std::vector<Value> values;
    for (const Value& group_key: agg_key.group_bys_) {
        values.push_back(group_key);
    }
    for (const Value& value: agg_value.aggregates_) {
        values.push_back(value);
    }
    *tuple = Tuple(values, &plan_->OutputSchema());
    ++aht_iterator_;
    return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
