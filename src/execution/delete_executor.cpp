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

  // retrieve the table to update.
  table_info_ = catalog->GetTable(plan_->TableOid());
  /// FIXME(bayes): Is it recommended to throw in ctor?
  if (table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception(ExceptionType::INVALID, "Table not found given the invalid table oid");
  }
}

void DeleteExecutor::Init() {
  // get all indices corresponding to this table. Empty if the table has no corresponding indices.
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_indices_ = catalog->GetTableIndexes(table_info_->name_);
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool delete_success{false};

  // get the rid of the tuple to be deleted.
  if (child_executor_->Next(tuple, rid)) {
    // mark the tuple as deleted.
    delete_success = table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());

  } else {
    // the child has no more tuples to delete.
    return false;
  }

  // if the delete marking succeeds, i.e. the tuple exists, also delete all corresponding indices.
  /// FIXME(bayes): Shall I delete the indices right away?
  if (delete_success) {
    for (IndexInfo *index_info : table_indices_) {
      /// FIXME(bayes): Will the child executor also spit out the tuple?
      assert(tuple != nullptr);
      Tuple key = tuple->KeyFromTuple(table_info_->schema_, *(index_info->index_->GetKeySchema()),
                                      index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
  }

  return true;
}

}  // namespace bustub
