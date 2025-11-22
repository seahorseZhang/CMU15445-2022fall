//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)), 
    iterator_(0) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

Tuple NestedLoopJoinExecutor::MergeTuple(Tuple &left, const Schema *left_schema,  Tuple &right, const Schema *right_schema) {
  std::vector<Value> values;
  for (size_t i = 0; i < left_schema->GetColumnCount(); i++) {
    values.push_back(left.GetValue(left_schema, i));
  }

  for (size_t i = 0; i < right_schema->GetColumnCount(); i++) {
    values.push_back(right.GetValue(right_schema, i));
  }
  return Tuple(values, &plan_->OutputSchema());
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  cache_tuples_.clear();
  right_tuples_.clear();

  Tuple right_tup;
  RID right_r;
  while (right_executor_->Next(&right_tup, &right_r)) {
      right_tuples_.push_back(right_tup);
  }
  Tuple tuple;
  RID rid;
  const AbstractExpression &predict = plan_->Predicate();
  while (left_executor_->Next(&tuple, &rid)) {
    right_executor_->Init();
    bool has_match = false;
    for (Tuple &right_tuple: right_tuples_) {
      Value result = predict.EvaluateJoin(&tuple, left_executor_->GetOutputSchema(), &right_tuple, right_executor_->GetOutputSchema());
      if (!result.IsNull() && result.GetAs<bool>()) {
         cache_tuples_.push_back(MergeTuple(tuple, &left_executor_->GetOutputSchema(), right_tuple, &right_executor_->GetOutputSchema()));
         has_match = true;
      }
    }
    if (!has_match && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      const Schema *left_schema = &left_executor_->GetOutputSchema();
      for (size_t i = 0; i < left_schema->GetColumnCount(); i++) {
        values.push_back(tuple.GetValue(left_schema, i));
      }

      const Schema *right_schema = &right_executor_->GetOutputSchema();
      for (size_t i = 0; i < right_schema->GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_schema->GetColumn(i).GetType()));
      }
      cache_tuples_.emplace_back(values, &plan_->OutputSchema());
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ >= cache_tuples_.size()) {
    return false;
  }
  *tuple = cache_tuples_[iterator_];
  ++iterator_;
  *rid = tuple->GetRid();
  return true;
}
}  // namespace bustub
