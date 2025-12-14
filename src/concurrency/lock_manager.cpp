//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::CheckUpgradeCompatible(LockMode request_lock_mode, LockMode current_lock_mode) -> bool {
  if (request_lock_mode == LockMode::INTENTION_SHARED &&
      (current_lock_mode == LockMode::SHARED || current_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
       current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    return true;
  }
  if (request_lock_mode == LockMode::SHARED &&
      (current_lock_mode == LockMode::EXCLUSIVE || current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    return true;
  }
  if (request_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && current_lock_mode == LockMode::EXCLUSIVE) {
    return true;
  }
  return false;
}

auto LockManager::GrantLock(std::shared_ptr<LockRequestQueue> request_queue, Transaction *txn,
                            LockMode lock_mode) -> bool {
  for (LockRequest *request : request_queue->request_queue_) {
    if (request->granted_ || request->txn_id_ == txn->GetTransactionId()) {
      continue;
    }
    LockMode request_mode = request->lock_mode_;
    switch (lock_mode) {
      case LockMode::EXCLUSIVE:
        return false;
      case LockMode::SHARED:
        if (request_mode != LockMode::SHARED && request_mode != LockMode::INTENTION_SHARED) {
          return false;
        }
        break;
      case LockMode::INTENTION_SHARED:
        if (request_mode == LockMode::EXCLUSIVE) {
          return false;
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (request_mode != LockMode::INTENTION_EXCLUSIVE && request_mode != LockMode::INTENTION_SHARED) {
          return false;
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (request_mode != LockMode::INTENTION_SHARED) {
          return false;
        }
        break;
      default:
        break;
    }
  }
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // check illegal txn lock operation
  TransactionState txn_state = txn->GetState();
  if (txn_state == TransactionState::ABORTED || txn_state == TransactionState::COMMITTED) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }
  if (txn_state == TransactionState::SHRINKING &&
      (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  IsolationLevel isolation_level = txn->GetIsolationLevel();
  if (isolation_level == IsolationLevel::REPEATABLE_READ && txn_state == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (isolation_level == IsolationLevel::READ_COMMITTED && txn_state == TransactionState::SHRINKING &&
      (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn_state != TransactionState::GROWING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  std::shared_ptr<LockRequestQueue> request_queue = table_lock_map_[oid];
  request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  LockRequest *lock_request = nullptr;
  for (LockRequest *request : request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      lock_request = request;
      break;
    }
  }
  request_queue->latch_.unlock();
  // upgrade lock
  if (lock_request != nullptr) {
    if (!lock_request->granted_) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    if (lock_request->lock_mode_ == lock_mode) {
      return true;
    }
    if (!CheckUpgradeCompatible(lock_request->lock_mode_, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    request_queue->latch_.lock();
    if (request_queue->upgrading_ != INVALID_TXN_ID && request_queue->upgrading_ != txn->GetTransactionId()) {
      request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    request_queue->upgrading_ = txn->GetTransactionId();
    request_queue->request_queue_.remove(lock_request);
    request_queue->latch_.unlock();

    // remove old lock_request
    switch (lock_request->lock_mode_) {
      case LockMode::SHARED:
        txn->GetSharedTableLockSet()->erase(oid);
        break;
      case LockMode::EXCLUSIVE:
        txn->GetExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_SHARED:
        txn->GetIntentionSharedTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
        break;
      default:
        LOG_ERROR("Unsupported lock mode in upgrade phase");
    }
    lock_request->lock_mode_ = lock_mode;
    lock_request->granted_ = false;
  }
  if (lock_request == nullptr) {
    lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  }
  std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
  request_queue->request_queue_.push_back(lock_request);
  while (!GrantLock(request_queue, txn, lock_mode)) {
    request_queue->cv_.wait(queue_lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      request_queue->request_queue_.remove(lock_request);
      delete lock_request;
      lock_request = nullptr;
      if (request_queue->upgrading_ == txn->GetTransactionId()) {
        request_queue->upgrading_ = INVALID_TXN_ID;
      }
      return false;
    }
  }
  request_queue->upgrading_ = INVALID_TXN_ID;
  lock_request->granted_ = true;
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->emplace(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->emplace(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->emplace(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->emplace(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->emplace(oid);
      break;
    default:
      LOG_ERROR("Unsupported lock mode in granted table lock");
  }
  return true;
}

auto LockManager::ChangeTxnState(Transaction *txn, LockMode lock_mode) -> void {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      txn->SetState(TransactionState::SHRINKING);
      break;
    case IsolationLevel::READ_COMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      txn->SetState(TransactionState::SHRINKING);
      break;
    default:
      LOG_ERROR("invalid isolation level when unlock table");
      break;
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (!(txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedLocked(oid) ||
        txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
        txn->IsTableSharedIntentionExclusiveLocked(oid))) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto share_iter = txn->GetSharedRowLockSet()->find(oid);
  auto ex_iter = txn->GetExclusiveRowLockSet()->find(oid);
  if ((share_iter != txn->GetSharedRowLockSet()->end() && share_iter->second.size() > 0) ||
      (ex_iter != txn->GetExclusiveRowLockSet()->end() && ex_iter->second.size() > 0)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  table_lock_map_latch_.lock();
  std::shared_ptr<LockRequestQueue> requet_queue = table_lock_map_[oid];
  requet_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  LockMode lock_mode;
  for (LockRequest *request : requet_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      lock_mode = request->lock_mode_;
      requet_queue->request_queue_.remove(request);
      break;
    }
  }
  requet_queue->latch_.unlock();
  requet_queue->cv_.notify_all();

  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    default:
      LOG_ERROR("Unsupported lock mode in unlock table");
  }

  if (txn->GetState() == TransactionState::GROWING) {
    ChangeTxnState(txn, lock_mode);
  }
  return true;
}

auto LockManager::GrantRowLock(std::shared_ptr<LockRequestQueue> request_queue, Transaction *txn) -> bool {
  for (LockRequest *request : request_queue->request_queue_) {
    if (!request->granted_ || request->txn_id_ == txn->GetTransactionId()) {
      continue;
    }
    if (request->lock_mode_ == LockMode::EXCLUSIVE) {
      return false;
    }
  }
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::INTENTION_SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  TransactionState state = txn->GetState();
  if (state == TransactionState::COMMITTED || state == TransactionState::ABORTED) {
    return false;
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn->GetState() != TransactionState::GROWING || lock_mode != LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!(txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
          txn->IsTableExclusiveLocked(oid))) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  if (lock_mode == LockMode::SHARED) {
    if (!(txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedLocked(oid) ||
          txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid))) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  std::shared_ptr<LockRequestQueue> request_queue = row_lock_map_[rid];
  request_queue->latch_.lock();
  row_lock_map_latch_.unlock();
  LockRequest *request = nullptr;
  for (LockRequest *request : request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      request = request;
      break;
    }
  }
  request_queue->latch_.unlock();
  // upgrade lock
  if (request != nullptr) {
    if (!request->granted_) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    if (request->lock_mode_ == lock_mode || request->lock_mode_ == LockMode::EXCLUSIVE) {
      return true;
    }
    if (request->lock_mode_ != LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    request_queue->latch_.lock();
    if (request_queue->upgrading_ != INVALID_TXN_ID) {
      request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    request_queue->upgrading_ = txn->GetTransactionId();
    request_queue->request_queue_.remove(request);
    request_queue->latch_.unlock();
    // upgrade的话只能是shared

    std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> shared_map = txn->GetSharedRowLockSet();
    auto iter = shared_map->find(oid);
    assert(iter != shared_map->end());
    iter->second.erase(rid);

    request->granted_ = false;
    request->lock_mode_ = lock_mode;
  }
  if (request == nullptr) {
    request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  }
  std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
  request_queue->request_queue_.push_back(request);
  while (!GrantRowLock(request_queue, txn)) {
    request_queue->cv_.wait(queue_lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      request_queue->request_queue_.remove(request);
      delete request;
      request = nullptr;
      return false;
    }
  }
  request->granted_ = true;
  request->lock_mode_ = lock_mode;
  request_queue->upgrading_ = INVALID_TXN_ID;
  if (lock_mode == LockMode::SHARED) {
    std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> shared_map = txn->GetSharedRowLockSet();
    shared_map->operator[](oid).emplace(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> exclusive_map =
        txn->GetExclusiveRowLockSet();
    exclusive_map->operator[](oid).emplace(rid);
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  auto shared_set = txn->GetSharedRowLockSet()->find(oid);
  auto exclusive_set = txn->GetExclusiveRowLockSet()->find(oid);
  if ((shared_set == txn->GetSharedRowLockSet()->end() || shared_set->second.count(rid) == 0) &&
      (exclusive_set == txn->GetExclusiveRowLockSet()->end() || exclusive_set->second.count(rid) == 0)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  row_lock_map_latch_.lock();
  std::shared_ptr<LockRequestQueue> request_queue = row_lock_map_[rid];
  LockRequest *target_request = nullptr;
  for (LockRequest *request : request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      target_request = request;
    }
  }
  BUSTUB_ASSERT(target_request != nullptr, "Unlock row");
  LockMode lock_mode = target_request->lock_mode_;
  request_queue->request_queue_.remove(target_request);
  row_lock_map_latch_.unlock();

  if (lock_mode == LockMode::SHARED) {
    auto shared_set = txn->GetSharedRowLockSet();
    auto shared_iter = shared_set->find(oid);
    BUSTUB_ASSERT(shared_iter != shared_set->end(), "not found lock from share row lock set");
    shared_iter->second.erase(rid);
  } else {
    BUSTUB_ASSERT(lock_mode == LockMode::EXCLUSIVE, "unlock rid, lock mode not share or exclusive");
    auto ex_set = txn->GetExclusiveRowLockSet();
    auto ex_iter = ex_set->find(oid);
    BUSTUB_ASSERT(ex_iter != ex_set->end(), "not found lock from ex row lock set");
    ex_iter->second.erase(rid);
  }

  if (txn->GetState() == TransactionState::GROWING) {
    ChangeTxnState(txn, lock_mode);
  }
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  txn_set_.emplace(t1);
  txn_set_.emplace(t2);
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::vector<txn_id_t> &t1_wait_list = waits_for_[t1];
  std::vector<txn_id_t>::iterator iter = std::find(t1_wait_list.begin(), t1_wait_list.end(), t2);
  if (iter != t1_wait_list.end()) {
    t1_wait_list.erase(iter);
  }
}

auto LockManager::DfsCycle(txn_id_t txn_id) -> bool {
  if (txn_path_.count(txn_id)) {
    return true;
  }
  txn_path_.emplace(txn_id);
  std::vector<txn_id_t> &wait_list = waits_for_[txn_id];
  for (auto iter = wait_list.begin(); iter != wait_list.end(); ++iter) {
    if (txn_path_.count(*iter)) {
      return true;
    }
    if (DfsCycle(*iter)) {
      return true;
    }
  }
  txn_path_.erase(txn_id);
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  txn_id_t max_txn_id = 0;
  for (const txn_id_t &id : txn_set_) {
    if (DfsCycle(id)) {
      for (const txn_id_t &node_id : txn_path_) {
        max_txn_id = std::max(max_txn_id, node_id);
      }
      *txn_id = max_txn_id;
      txn_path_.clear();
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto iter = txn_set_.begin(); iter != txn_set_.end(); ++iter) {
    std::vector<txn_id_t> &wait_list = waits_for_[*iter];
    for (txn_id_t txn_id : wait_list) {
      edges.emplace_back(*iter, txn_id);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
