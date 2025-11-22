//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), iterator_(0) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { 
  child_executor_->Init();
  Tuple out_tuple;
  RID out_rid;
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->GetInnerTableOid());
  Transaction *trx = exec_ctx_->GetTransaction();
  const Schema &out_schema = child_executor_->GetOutputSchema();
  const Schema &inner_schema = table_info->schema_;
  while (child_executor_->Next(&out_tuple, &out_rid)) {
    Value value = plan_->KeyPredicate()->Evaluate(&out_tuple, child_executor_->GetOutputSchema());
    IndexInfo *index_info = catalog->GetIndex(plan_->GetIndexOid());
    Tuple index_key = Tuple({value}, index_info->index_->GetKeySchema());
    std::vector<RID> result;
    index_info->index_->ScanKey(index_key, &result, trx);
    for (RID rid: result) {
      Tuple scan_tuple;
      bool ret = table_info->table_->GetTuple(rid, &scan_tuple, trx, true);
      if (!ret) {
        continue;
      }
      std::vector<Value> values;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(out_tuple.GetValue(&out_schema, i));
      }

      for (uint32_t i = 0; i < inner_schema.GetColumnCount(); i++) {
        values.push_back(scan_tuple.GetValue(&inner_schema, i));
      }
      cache_tuples_.emplace_back(values, &plan_->OutputSchema());
    }
    if (result.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(out_tuple.GetValue(&out_schema, i));
      }

      for (uint32_t i = 0; i < inner_schema.GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(inner_schema.GetColumn(i).GetType()));
      }
      cache_tuples_.emplace_back(values, &plan_->OutputSchema());
    }
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (iterator_ >= cache_tuples_.size()) {
    return false;
  }
  *tuple = cache_tuples_[iterator_];
  ++iterator_;
  if (rid != nullptr) {
    *rid = tuple->GetRid();
  }
  return true;
}
}  // namespace bustub
