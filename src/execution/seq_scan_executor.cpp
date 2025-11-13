//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
AbstractExecutor(exec_ctx), plan_(plan), table_iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
    Catalog *catalog = exec_ctx_->GetCatalog();
    table_info_ = catalog->GetTable(plan_->table_name_);
    if (!table_info_) {
        return;
    }
    table_iter_ = (table_info_->table_->Begin(exec_ctx_->GetTransaction()));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (table_info_ == nullptr ||  table_iter_ == table_info_->table_->End()) {
        return false;
    }
    *tuple = *table_iter_;
    *rid = table_iter_->GetRid();
    ++table_iter_;
    return true;
}

}  // namespace bustub
