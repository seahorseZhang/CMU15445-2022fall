//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { 
    child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
     if (is_returned) {
        return false;
     }
     int32_t delete_count = 0;
     Tuple cur_tuple;
     RID cur_rid;
     while (child_executor_->Next(&cur_tuple, &cur_rid)) {
        ++delete_count;
        Catalog *catalog = exec_ctx_->GetCatalog();
        TableInfo *table_info = catalog->GetTable(plan_->TableOid());
        bool deleteRet = table_info->table_->MarkDelete(cur_rid, exec_ctx_->GetTransaction());
        if (!deleteRet) {
            return false;
        }
        std::vector<IndexInfo *> index_infos = catalog->GetTableIndexes(table_info->name_);
        for (IndexInfo *index_info: index_infos) {
            Tuple delete_key = cur_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
            index_info->index_->DeleteEntry(delete_key, cur_rid, exec_ctx_->GetTransaction());
        }
     }
     std::vector<Value> values;
     values.emplace_back(TypeId::INTEGER, delete_count);
     *tuple = Tuple(values, &plan_->OutputSchema());
     is_returned = true;
     return true;
}

}  // namespace bustub
