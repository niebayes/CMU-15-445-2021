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
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  assert(exec_ctx_ != nullptr);
  assert(plan_ != nullptr);

  // get catalog from context.
  Catalog *catalog = exec_ctx_->GetCatalog();
  assert(catalog != nullptr);

  // retrieve the table from which to delete tuples.
  table_info_ = catalog->GetTable(plan_->TableOid());
  if (table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception(ExceptionType::INVALID, "Table not found");
  }

  // get all indices corresponding to this table. Empty if the table has no corresponding indices.
  table_indexes_ = catalog->GetTableIndexes(table_info_->name_);
  for (const IndexInfo *index_info : table_indexes_) {
    if (index_info == Catalog::NULL_INDEX_INFO) {
      throw Exception(ExceptionType::INVALID, "Index not found");
    }
  }
}

void DeleteExecutor::Init() {
  // init the child executor.
  assert(child_executor_ != nullptr);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // get the rid of the tuple to be deleted.
  while (child_executor_->Next(tuple, rid)) {
    const bool deleted = table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    // if the delete marking succeeds, i.e. the tuple exists, also delete all corresponding indices.
    if (deleted) {
      DeleteIndexes(tuple, rid);
    }
  }

  // always return false to avoid not modify result set.
  return false;
}

void DeleteExecutor::DeleteIndexes(Tuple *tuple, RID *rid) {
  for (IndexInfo *index_info : table_indexes_) {
    /// FIXME(bayes): What does the FIXME below mean?
    /// FIXME(bayes): Will the child executor also spit out the tuple?
    assert(tuple != nullptr);
    const Tuple key =
        tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    //! the rid is the one associated with the deleted tuple.
    index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
  }
}

}  // namespace bustub
