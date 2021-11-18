//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
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

void UpdateExecutor::Init() {
  // get all indices corresponding to this table. Empty if the table has no corresponding indices.
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_indices_ = catalog->GetTableIndexes(table_info_->name_);
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tuple{};
  Tuple updated_tuple{};
  bool update_success{false};

  // pull values from the child executor.
  if (child_executor_->Next(tuple, rid)) {
    // record the old tuple.
    assert(tuple != nullptr);
    old_tuple = *tuple;
    // generate the updated tuple from the old tuple given the update attributes.
    updated_tuple = GenerateUpdatedTuple(old_tuple);
    /// FIXME(bayes): Shall I use std:move(updated_tuple) to move the tuple?
    // apply the update to the table.
    update_success = table_info_->table_->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction());

  } else {
    // the child has no more tuples to produce.
    return false;
  }

  // if the update succeeds, also update the corresponding indices.
  if (update_success) {
    for (IndexInfo *index_info : table_indices_) {
      // first, delete the old index entry.
      Tuple old_key = old_tuple.KeyFromTuple(table_info_->schema_, *(index_info->index_->GetKeySchema()),
                                             index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());

      // then, add the updated index entry.
      Tuple updated_key = updated_tuple.KeyFromTuple(table_info_->schema_, *(index_info->index_->GetKeySchema()),
                                                     index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(updated_key, *rid, exec_ctx_->GetTransaction());
    }
  }

  return true;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
