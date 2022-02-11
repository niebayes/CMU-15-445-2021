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
#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

void LockManager::AbortTransaction(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);

  txn->SetState(TransactionState::ABORTED);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
}

bool LockManager::CanGrantSharedLock(Transaction *txn, const RID &rid) {
  // other thread has took the lock.
  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  LockRequestQueue &lock_queue = lock_table_.at(rid);

  // self checking: check if this txn is legal to get the lock.

  // locking is denied in SHRINKING phase. Since txn state is dynamic, so we must check it more than once.
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    AbortTransaction(txn, rid);
    lock_queue.cv_.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  bool oldest{true};

  // collision checking: check if granting a lock to this txn may collide other txns.
  auto iter = lock_queue.request_queue_.begin();
  auto iter_end = lock_queue.request_queue_.end();
  while (iter != iter_end) {
    const LockRequest &lock_request = *iter;
    Transaction *other_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
    assert(other_txn != nullptr);

    // txn id acts as timestamp, aka. priority.
    const txn_id_t txn_id = txn->GetTransactionId();
    const txn_id_t txn_id_other = other_txn->GetTransactionId();

    // skip this txn itself.
    if (txn_id == txn_id_other) {
      ++iter;
      continue;
    }

    // only consider uncommitted txns.
    if (other_txn->GetState() == TransactionState::COMMITTED || other_txn->GetState() == TransactionState::ABORTED) {
      ++iter;
      continue;
    }

    // only consider contending txns.
    if (lock_request.lock_mode_ == LockMode::SHARED) {
      ++iter;
      continue;
    }

    // by wound-wait scheme, younger waits for old and old kills younger.
    if (txn_id < txn_id_other) {
      // abort the younger.
      iter = lock_queue.request_queue_.erase(iter);
      AbortTransaction(other_txn, rid);
      lock_queue.cv_.notify_all();
    } else {
      oldest = false;
      ++iter;
    }
  }

  return oldest;
}

/// @note: emplace vs. insert.
/// insert supports copy and move.
/// emplace support copy and move and in addition in-place construction.
/// Scott Meyers says: emplace would be more efficient than insert most of the time and would never be less efficient.
/// But emplace will dynamically allocate memory for the in-place constructed object and this allocation shall fail
/// which would result in problems.

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);

  // aborted txn cannot acquire locks.
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // use unique_lock since we need to unlock it upon waiting.
  std::unique_lock<std::mutex> lck{latch_};

  // create a new lock table entry if not exist.
  if (lock_table_.count(rid) == 0) {
    // emplace instead of copy since LockRequestQueue has a copy-disabled field cv_.
    lock_table_.try_emplace(rid);
    // we may just grant lock right here since this txn is the first and the only one acquiring lock on the object.
  }
  LockRequestQueue &lock_queue = lock_table_.at(rid);

  // isolation level is static, so we check it only once. Actually, this checking should be lifted at the top. But we
  // need to notify other txns so we delay it at here.
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    AbortTransaction(txn, rid);
    lock_queue.cv_.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  // enqueue a new lock request on this object. Not granted initially.
  lock_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  LockRequest &lock_request = lock_queue.request_queue_.back();

  // block and wait until this txn is aborted or granted a lock.
  while (!CanGrantSharedLock(txn, rid)) {
    lock_queue.cv_.wait(lck);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  lock_request.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);

  return true;
}

bool LockManager::CanGrantExclusiveLock(Transaction *txn, const RID &rid) {
  // other thread has took the lock.
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  LockRequestQueue &lock_queue = lock_table_.at(rid);

  // self checking: check if this txn is legal to get the lock.

  // locking is denied in SHRINKING phase. Since txn state is dynamic, so we must check it more than once.
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    AbortTransaction(txn, rid);
    lock_queue.cv_.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  bool oldest{true};

  // collision checking: check if granting a lock to this txn may collide other txns.
  auto iter = lock_queue.request_queue_.begin();
  auto iter_end = lock_queue.request_queue_.end();
  while (iter != iter_end) {
    const LockRequest &lock_request = *iter;
    Transaction *other_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
    assert(other_txn != nullptr);

    // txn id acts as timestamp, aka. priority.
    const txn_id_t txn_id = txn->GetTransactionId();
    const txn_id_t txn_id_other = other_txn->GetTransactionId();

    // skip this txn itself.
    if (txn_id == txn_id_other) {
      ++iter;
      continue;
    }

    // only consider uncommitted txns.
    if (other_txn->GetState() == TransactionState::COMMITTED || other_txn->GetState() == TransactionState::ABORTED) {
      ++iter;
      continue;
    }

    // either of shared or exclusive locks contends with exclusive lock.

    // by wound-wait scheme, younger waits for old and old kills younger.
    if (txn_id < txn_id_other) {
      // abort the younger.
      iter = lock_queue.request_queue_.erase(iter);
      AbortTransaction(other_txn, rid);
      lock_queue.cv_.notify_all();
    } else {
      oldest = false;
      ++iter;
    }
  }

  return oldest;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);

  // aborted txn cannot acquire locks.
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // use unique_lock since we need to unlock it upon waiting.
  std::unique_lock<std::mutex> lck{latch_};

  // create a new lock table entry if not exist.
  if (lock_table_.count(rid) == 0) {
    // emplace instead of copy since LockRequestQueue has a copy-disabled field cv_.
    lock_table_.try_emplace(rid);
    // we may just grant lock right here since this txn is the first and the only one acquiring lock on the object.
  }
  LockRequestQueue &lock_queue = lock_table_.at(rid);

  // enqueue a new lock request on this object. Not granted initially.
  lock_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  LockRequest &lock_request = lock_queue.request_queue_.back();

  // block and wait until this txn is aborted or granted a lock.
  while (!CanGrantExclusiveLock(txn, rid)) {
    lock_queue.cv_.wait(lck);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  lock_request.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

bool LockManager::CanUpgradeLock(Transaction *txn, const RID &rid) {
  LockRequestQueue &lock_queue = lock_table_.at(rid);

  // self checking: check if this txn is legal to get the lock.

  // upgrade must be applied on an object that was shared locked.
  if (!txn->IsSharedLocked(rid)) {
    AbortTransaction(txn, rid);
    lock_queue.cv_.notify_all();
    // TODO(bayes): generate dedicated abort exception.
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // other thread has took the lock.
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  // locking is denied in SHRINKING phase. Since txn state is dynamic, so we must check it more than once.
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    AbortTransaction(txn, rid);
    lock_queue.cv_.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  bool oldest{true};

  // collision checking: check if granting a lock to this txn may collide other txns.
  auto iter = lock_queue.request_queue_.begin();
  auto iter_end = lock_queue.request_queue_.end();
  while (iter != iter_end) {
    const LockRequest &lock_request = *iter;
    Transaction *other_txn = TransactionManager::GetTransaction(lock_request.txn_id_);
    assert(other_txn != nullptr);

    // txn id acts as timestamp, aka. priority.
    const txn_id_t txn_id = txn->GetTransactionId();
    const txn_id_t txn_id_other = other_txn->GetTransactionId();

    // skip this txn itself.
    if (txn_id == txn_id_other) {
      ++iter;
      continue;
    }

    // only consider uncommitted txns.
    if (other_txn->GetState() == TransactionState::COMMITTED || other_txn->GetState() == TransactionState::ABORTED) {
      ++iter;
      continue;
    }

    // upgrade conflict. Abort txn by wound-wait.
    if (txn_id_other == lock_queue.upgrading_) {
      if (txn_id < txn_id_other) {
        // abort the younger even if it has not took the lock.
        iter = lock_queue.request_queue_.erase(iter);
        AbortTransaction(other_txn, rid);
        lock_queue.cv_.notify_all();
      } else {
        // abort the older since there cannot be two txns requesting for upgrading at a given time.
        AbortTransaction(txn, rid);
        lock_queue.cv_.notify_all();
        throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      }
      continue;
    }

    // either of shared or exclusive locks contends with exclusive lock.

    // by wound-wait scheme, younger waits for old and old kills younger.
    if (txn_id < txn_id_other) {
      // abort the younger.
      iter = lock_queue.request_queue_.erase(iter);
      AbortTransaction(other_txn, rid);
      lock_queue.cv_.notify_all();
    } else {
      oldest = false;
      ++iter;
    }
  }

  return oldest;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);

  // aborted txn cannot acquire locks.
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // use unique_lock since we need to unlock it upon waiting.
  std::unique_lock<std::mutex> lck{latch_};

  // create a new lock table entry if not exist.
  if (lock_table_.count(rid) == 0) {
    // emplace instead of copy since LockRequestQueue has a copy-disabled field cv_.
    lock_table_.try_emplace(rid);
    // we may just grant lock right here since this txn is the first and the only one acquiring lock on the object.
  }
  LockRequestQueue &lock_queue = lock_table_.at(rid);

  // enqueue a new lock request on this object. Not granted initially.
  lock_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  LockRequest &lock_request = lock_queue.request_queue_.back();

  // block and wait until this txn is aborted or granted a lock.
  while (!CanUpgradeLock(txn, rid)) {
    lock_queue.cv_.wait(lck);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  // upgrade the shared lock.
  // assert(lock_request.lock_mode_ == LockMode::SHARED);
  lock_request.lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  assert(txn != nullptr);

  std::unique_lock<std::mutex> lck{latch_};

  // unlock must follow lock.
  if (lock_table_.count(rid) == 0) {
    return false;
  }
  LockRequestQueue &lock_queue = lock_table_.at(rid);

  // transition from GROWING to SHRINKING.
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // remove all requests of this txn.
  auto iter = lock_queue.request_queue_.begin();
  auto iter_end = lock_queue.request_queue_.end();
  while (iter != iter_end) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      iter = lock_queue.request_queue_.erase(iter);
    } else {
      ++iter;
    }
  }

  lock_queue.cv_.notify_all();
  // erase is no-op if the set does contain the given object.
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  return true;
}

}  // namespace bustub
