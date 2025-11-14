//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor): AbstractExecutor(exec_ctx),
    plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
    child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (is_returned) {
        return false;
    }
    int32_t insert_count = 0;
    Tuple cur_tuple;
    RID cur_rid;
    while (child_executor_->Next(&cur_tuple, &cur_rid)) {
        Catalog *catalog =  exec_ctx_->GetCatalog();
        TableInfo *table_info = catalog->GetTable(plan_->TableOid());
        bool insertRet = table_info->table_->InsertTuple(cur_tuple, &cur_rid, exec_ctx_->GetTransaction());
        if (!insertRet) {
            return false;
        }
        ++insert_count;
        std::vector<IndexInfo *> indexes = catalog->GetTableIndexes(table_info->name_);
        for (IndexInfo* &index: indexes) {
            Tuple key = cur_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
            index->index_->InsertEntry(key, cur_rid, exec_ctx_->GetTransaction());
        }
    }
    std::vector<Value> values;
    values.emplace_back(TypeId::INTEGER, insert_count);
    *tuple = Tuple(values, &plan_->OutputSchema());
    is_returned = true;
    return true;
 }
    
}  // namespace bustub
