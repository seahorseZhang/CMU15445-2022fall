//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), index_iter_(nullptr, nullptr, 0) {}

void IndexScanExecutor::Init() {
    Catalog *catalog = exec_ctx_->GetCatalog();
    index_info_ = catalog->GetIndex(plan_->index_oid_);
    table_info_ = catalog->GetTable(index_info_->table_name_);
    tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
    index_iter_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (index_iter_ == tree_->GetEndIterator()) {
        return false;
    }
    *rid = (*index_iter_).second;
    bool get_ret = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction(), true);
    ++index_iter_;
    return get_ret;
}

}  // namespace bustub
