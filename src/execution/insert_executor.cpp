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
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  assert(exec_ctx_ != nullptr);
  assert(plan_ != nullptr);

  // get catalog from context.
  Catalog *catalog = exec_ctx_->GetCatalog();
  assert(catalog != nullptr);

  // retrieve the table to insert into.
  table_info_ = catalog->GetTable(plan_->TableOid());
  if (table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception(ExceptionType::INVALID, "Table not found");
  }

  // get all indices corresponding to this table. Empty if the table has no corresponding indices.
  table_indices_ = catalog->GetTableIndexes(table_info_->name_);
  for (const IndexInfo *index_info : table_indices_) {
    if (index_info == Catalog::NULL_INDEX_INFO) {
      throw Exception(ExceptionType::INVALID, "Index not found");
    }
  }
}

void InsertExecutor::Init() {
  assert(plan_ != nullptr);
  if (!plan_->IsRawInsert()) {
    // init the child executor for non raw insert.
    assert(child_executor_ != nullptr);
    child_executor_->Init();
  }
}

void InsertExecutor::UpdateIndexes(Tuple *tuple, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  assert(txn != nullptr);

  for (IndexInfo *index_info : table_indices_) {
    assert(index_info != Catalog::NULL_INDEX_INFO);

    // generate a key tuple given the tuple schema and the index metadata.
    const Tuple key = tuple->KeyFromTuple(table_info_->schema_, *(index_info->index_->GetKeySchema()),
                                          index_info->index_->GetKeyAttrs());
    // insert a new entry to the index.
    index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());

    // update index write set.
    IndexWriteRecord record(*rid, table_info_->oid_, WType::INSERT, *tuple, index_info->index_oid_,
                            exec_ctx_->GetCatalog());
    // transaction manager would process each record according its WType.
    txn->GetIndexWriteSet()->emplace_back(record);
  }
}

// since Insert cannot modify result set, the insertion must be done in one run, i.e. it acts as a pipeline breaker.
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  assert(table_info_ != Catalog::NULL_TABLE_INFO);

  // since the lock granularity is tuple-level, no need to lock during insertion.

  if (plan_->IsRawInsert()) {
    // it's a raw insert plan, insert the embeded raw values.

    for (const std::vector<Value> &values : plan_->RawValues()) {
      // create a new tuple based on the values and the given schema.
      *tuple = Tuple(values, &table_info_->schema_);
      // FIXME(bayes): How the rid is allocated?
      *rid = tuple->GetRid();
      // insert the tuple to the table.
      const bool inserted = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
      // update indexes if the insertion succeeds.
      if (inserted) {
        UpdateIndexes(tuple, rid);
      }
    }

  } else {
    // it's not a raw insert plan, obtain values from the child.
    assert(child_executor_ != nullptr);

    // obtain stream of tuple from the child executor until it's exhausted.
    while (child_executor_->Next(tuple, rid)) {
      // insert the produced tuple to the table.
      const bool inserted = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
      // update indexes if the insertion succeeds.
      if (inserted) {
        UpdateIndexes(tuple, rid);
      }
    }
  }

  // always return false to not modify result set.
  return false;
}

}  // namespace bustub
