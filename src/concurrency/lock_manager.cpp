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
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::CanGrantSharedLock(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);

  // (1) self checking: check if this txn has to be aborted.
  if (txn->GetState() == TransactionState::SHRINKING) {
    // violate 2PL rule: LOCK_ON_SHRINKING.
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  // (2) interaction checking: check if this txn collides with some other txns accessing the same data.
  /// FIXME(bayes): Shall I check for double locking?
  for (const auto &lock_request : lock_table_.at(rid).request_queue_) {
    Transaction *other_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
    assert(other_txn != nullptr);

    // skip the txn itself.
    if (other_txn->GetTransactionId() == txn->GetTransactionId()) {
      continue;
    }

    // check only those txns that is uncommitted.
    /// FIXME(bayes): Is this assertion always true?
    assert(other_txn->GetState() != TransactionState::ABORTED);
    if (other_txn->GetState() == TransactionState::COMMITTED) {
      continue;
    }
    // check only those txns that has a lock granted.
    if (!lock_request.granted_) {
      continue;
    }

    // interaction checking based on the requesting txn's isolation level.
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      // the requesting txn disallow updates by other txns.
      // to avoid unrepeatable reads, LM cannot grant a shared lock to the requesting txn currently.
      if (lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
    // else, the isolation level of the requesting txn is READ_UNCOMMITTED or READ_COMMITTED either of which permits
    // reads/updates by other txns.

    // interaction checking based on the other txns' isolation level.
    if (lock_request.lock_mode_ == LockMode::EXCLUSIVE &&
        other_txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      // found at least one other uncommited txn that is writing the data and its isolation level is not
      // READ_UNCOMMITTED which means it disallows interaction (2).
      // to avoid dirty reads, LM cannot grant a shared lock to the requesting txn currently.
      return false;
    }
  }

  // passed all checks, able to grant a shared lock to this txn on the data.
  return true;
}

/// @note: emplace vs. insert.
/// insert supports copy and move.
/// emplace support copy and move and in addition in-place construction.
/// Scott Meyers says: emplace would be more efficient than insert most of the time and would never be less efficient.
/// But emplace will dynamically allocate memory for the in-place constructed object and this allocation shall fail
/// which would result in problems.

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  //! use std::unique_lock to cooperate with std::condition_variable.
  std::unique_lock<std::mutex> lck{latch_};

  // create a new lock table entry if not exist.
  if (lock_table_.count(rid) == 0) {
    lock_table_.try_emplace(rid);
  }
  LockRequestQueue &lock_queue = lock_table_.at(rid);

  // add a new lock request on this data. Not granted initially.
  lock_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  LockRequest &lock_request = lock_queue.request_queue_.back();

  // check if this txn can acquire a shared lock on the data.
  // loop invariant: cannot grant a shared lock to this txn on this data.
  while (!CanGrantSharedLock(txn, rid)) {
    // block on wait if cannot acquire currently.
    lock_queue.cv_.wait(lck);
    // awake to check if the txn was aborted since the asleep.
    /// FIXME(bayes): Shall I check it at here?
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }
  // post condition: able to grant a shared lock to this txn on this data.

  // grant a shared lock.
  lock_request.granted_ = true;

  // add the data to the set of resouces shared with the txn.
  txn->GetSharedLockSet()->emplace(rid);

  return true;
}

bool LockManager::CanGrantExclusiveLock(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);

  // (1) self checking: check if this txn has to be aborted.
  if (txn->GetState() == TransactionState::SHRINKING) {
    // violate 2PL rule: LOCK_ON_SHRINKING.
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  // (2) interaction checking: check if this txn collides with some other txns accessing the same data.
  /// FIXME(bayes): Shall I check for double locking?
  /// FIXME(bayes): Shall I only check the txns before this txn?
  for (const auto &lock_request : lock_table_.at(rid).request_queue_) {
    Transaction *other_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
    assert(other_txn != nullptr);

    // skip the txn itself.
    if (other_txn->GetTransactionId() == txn->GetTransactionId()) {
      continue;
    }

    // check only those txns that is uncommitted.
    /// FIXME(bayes): Is this assertion always true?
    assert(other_txn->GetState() != TransactionState::ABORTED);
    if (other_txn->GetState() == TransactionState::COMMITTED) {
      continue;
    }
    // check only those txns that has a lock granted.
    if (!lock_request.granted_) {
      continue;
    }

    /// FIXME(bayes): How the incompatibility constrains involve in the checking?

    // interaction checking based on the requesting txn's isolation level.
    if ((txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_request.lock_mode_ == LockMode::SHARED) ||
        txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      /// FIXME(bayes): This would result in lost writes. Can we allow this to happen?
      // READ_COMMITTED: disallow other reads but allow other writes.
      // REPEATABLE_READ: disallow other reads and writes.
      return false;
    }

    // interaction checking based on the other txns' isolation level.
    if (other_txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      // to avoid unrepeatable reads, LM cannot grant an exclusive lock to the requesting txn currently.
      return false;
    }
  }

  // passed all checks, able to grant an exclusive lock to this txn on the data.
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  //! use std::unique_lock to cooperate with std::condition_variable.
  std::unique_lock<std::mutex> lck{latch_};

  // create a new lock table entry if not exist.
  if (lock_table_.count(rid) == 0) {
    lock_table_.try_emplace(rid);
  }
  LockRequestQueue &lock_queue = lock_table_.at(rid);

  // add a new lock request on this data. Not granted initially.
  lock_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  LockRequest &lock_request = lock_queue.request_queue_.back();

  // check if this txn can acquire an exclusive lock on the data.
  // loop invariant: cannot grant an exclusive lock to this txn on this data.
  while (!CanGrantExclusiveLock(txn, rid)) {
    // block on wait if cannot acquire currently.
    lock_queue.cv_.wait(lck);
    // awake to check if the txn was aborted since the asleep.
    /// FIXME(bayes): Shall I check it at here?
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }
  // post condition: able to grant an exclusive lock to this txn on this data.

  // grant an exclusive lock.
  lock_request.granted_ = true;

  // add the data to the set of resouces exclusive only to the txn.
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

bool LockManager::CanUpgradeLock(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);

  // (1) self checking: check if this txn has to be aborted.
  if (txn->GetState() == TransactionState::SHRINKING) {
    // violate 2PL rule: LOCK_ON_SHRINKING.
    // upgrading can only occur in growing phase.
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (lock_table_.at(rid).upgrading_ != INVALID_TXN_ID) {
    // if there's already a txn pending on upgrading, abort this txn.
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // (2) interaction checking: check if this txn collides with some other txns accessing the same data.
  /// FIXME(bayes): Shall I check for double locking?
  for (const auto &lock_request : lock_table_.at(rid).request_queue_) {
    Transaction *other_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
    assert(other_txn != nullptr);

    // skip the txn itself.
    if (other_txn->GetTransactionId() == txn->GetTransactionId()) {
      continue;
    }

    // check only those txns that is uncommitted.
    /// FIXME(bayes): Is this assertion always true?
    assert(other_txn->GetState() != TransactionState::ABORTED);
    if (other_txn->GetState() == TransactionState::COMMITTED) {
      continue;
    }
    // check only those txns that has a lock granted.
    if (!lock_request.granted_) {
      continue;
    }

    /// FIXME(bayes): How the incompatibility constrains involve in the checking?

    // interaction checking based on the requesting txn's isolation level.
    if ((txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_request.lock_mode_ == LockMode::SHARED) ||
        txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      /// FIXME(bayes): This would result in lost writes. Can we allow this to happen?
      // READ_COMMITTED: disallow other reads but allow other writes.
      // REPEATABLE_READ: disallow other reads and writes.
      return false;
    }

    // interaction checking based on the other txns' isolation level.
    if (other_txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      // to avoid unrepeatable reads, LM cannot upgrade the shared lock.
      return false;
    }
  }

  // passed all checks, able to upgrade the shared lock.
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  //! use std::unique_lock to cooperate with std::condition_variable.
  std::unique_lock<std::mutex> lck{latch_};

  // to upgrade a shared lock, the requesting txn must have been granted a shared lock on the data.
  bool shared_lock_granted{false};
  if (lock_table_.count(rid) == 0) {
    // no one is locking the data.
    return false;
  }

  LockRequestQueue &lock_queue = lock_table_.at(rid);
  auto iter = lock_queue.request_queue_.begin();
  auto iter_end = lock_queue.request_queue_.end();
  for (; iter != iter_end; ++iter) {
    LockRequest &lock_request = *iter;
    if (lock_request.txn_id_ == txn->GetTransactionId() && lock_request.lock_mode_ == LockMode::SHARED &&
        lock_request.granted_) {
      shared_lock_granted = true;
      break;
    }
  }
  if (!shared_lock_granted) {
    return false;
  }

  // check if this txn can upgrade the shared lock.
  // loop invariant: cannot upgrade currently.
  while (!CanUpgradeLock(txn, rid)) {
    // block on wait if cannot upgrade currently.
    lock_queue.cv_.wait(lck);
    // awake to check if the txn was aborted since the asleep.
    /// FIXME(bayes): Shall I check it at here?
    if (txn->GetState() == TransactionState::ABORTED) {
      /// FIXME(bayes): Shall I erase the data if this txn is aborted?
      // txn->GetSharedLockSet()->erase(rid);
      return false;
    }
  }
  // post condition: able to upgrade.

  // upgrade the shared lock.
  iter->lock_mode_ = LockMode::EXCLUSIVE;

  // update shared/exclusive set of resources accessed by this txn.
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  //! the txn manager may call Abort on aborted txns.

  std::unique_lock<std::mutex> lck{latch_};

  // an unlock request must follow a granted lock request.
  bool lock_granted{false};
  if (lock_table_.count(rid) == 0) {
    return false;
  }
  LockRequestQueue &lock_queue = lock_table_.at(rid);

  auto iter = lock_queue.request_queue_.begin();
  auto iter_end = lock_queue.request_queue_.end();
  for (; iter != iter_end; ++iter) {
    LockRequest &lock_request = *iter;
    if (lock_request.txn_id_ == txn->GetTransactionId() && lock_request.granted_) {
      lock_granted = true;
      break;
    }
  }
  if (!lock_granted) {
    return false;
  }

  // transition from GROWING to SHRINKING. This check is necessary.
  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // delete the lock request.
  lock_queue.request_queue_.erase(iter);
  // notify other txns blocking on wait.
  lock_queue.cv_.notify_all();

  //! erase is no-op if the key does not exist.
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  return true;
}

}  // namespace bustub
