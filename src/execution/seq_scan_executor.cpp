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

// niebayes, 2021-11-14, niebayes@gmail.com

#include <cassert>

#include "execution/executors/seq_scan_executor.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())},
      table_it_{table_info_->table_->End()} {
  if (table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception(ExceptionType::INVALID, "Table not found given the invalid table oid");
  }
}

void SeqScanExecutor::Init() {
  // init the table iterator to the beginning of the table to be scanned through.
  table_it_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

// true if the lock is granted.
static bool TryLockShared(LockManager *lock_mgr, Transaction *txn, const RID &rid) {
  // The lock manager is null in project 3.
  if (lock_mgr == nullptr) {
    return false;
  }
  //  READ_UNCOMMITTED only demands exclusive lock.
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    return false;
  }
  // do not acquire the same lock twice.
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }
  return lock_mgr->LockShared(txn, rid);
}

static void TryUnlockShared(LockManager *lock_mgr, Transaction *txn, const RID &rid) {
  // READ_UNCOMMITTED won't call this func. REPEATABLE_READ holds locks until commitment.
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    lock_mgr->Unlock(txn, rid);
  }
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  LockManager *lock_mgr = exec_ctx_->GetLockManager();
  Transaction *txn = exec_ctx_->GetTransaction();
  assert(txn != nullptr);

  // scan the table tuple by tuple and spit out the next tuple that satisfies the predicate.
  const AbstractExpression *predicate = plan_->GetPredicate();

  while (table_it_ != table_info_->table_->End()) {
    // lock the tuple.
    const bool locked = TryLockShared(lock_mgr, txn, table_it_->GetRid());

    bool flag{false};
    if (predicate == nullptr || predicate->Evaluate(&(*table_it_), &table_info_->schema_).GetAs<bool>()) {
      // found a tuple that satisfies the predicate.

      // retrieve values from the input table given the output schema.
      const Schema *output_schema = plan_->OutputSchema();
      std::vector<Value> values;
      values.reserve(output_schema->GetColumnCount());
      for (const Column &col : output_schema->GetColumns()) {
        // the value is obtained by evaluating the input tuple according to the expression of the given output schema.
        Value val = col.GetExpr()->Evaluate(&(*table_it_), &table_info_->schema_);
        values.emplace_back(std::move(val));
      }

      // create a new tuple given the values and the output schema.
      *tuple = Tuple(values, output_schema);
      //! the rid is the one associated with the input tuple.
      *rid = table_it_->GetRid();

      if (locked) {
        TryUnlockShared(lock_mgr, txn, table_it_->GetRid());
      }

      // increment the iterator to prepare for the next Next call.
      ++table_it_;

      flag = true;

    } else {
      if (locked) {
        TryUnlockShared(lock_mgr, txn, table_it_->GetRid());
      }

      // this tuple does not satisfy the predicate, proceed to the next tuple.
      ++table_it_;
    }
    if (flag) {
      return true;
    }
  }
  // if we've reached the end, return false to indicate the scanning is done.
  return false;
}

}  // namespace bustub
