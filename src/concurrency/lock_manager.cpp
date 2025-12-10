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

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (!(txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedLocked(oid) ||
        txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
        txn->IsTableSharedIntentionExclusiveLocked(oid))) {
    txn->SetState(TransactionState::ABORTED);
    throw new TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  if (txn->GetSharedRowLockSet()->count(oid) > 0 || txn->GetExclusiveRowLockSet()->count(oid) > 0) {
    txn->SetState(TransactionState::ABORTED);
    throw new TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
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
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
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
