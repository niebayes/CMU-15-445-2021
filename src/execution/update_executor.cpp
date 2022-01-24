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

void UpdateExecutor::Init() {
  // init the child executor.
  assert(child_executor_ != nullptr);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // pull values from the child executor.
  while (child_executor_->Next(tuple, rid)) {
    assert(tuple != nullptr);
    Tuple old_tuple = *tuple;
    // generate the updated tuple from the old tuple given the update attributes.
    Tuple updated_tuple = GenerateUpdatedTuple(old_tuple);
    // apply the update to the table.
    const bool updated = table_info_->table_->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction());
    // if the update succeeds, also update the corresponding indexes.
    if (updated) {
      UpdateIndexes(std::move(old_tuple), std::move(updated_tuple), *rid);
    }
  }

  // always return false to not modify result set.
  return false;
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

void UpdateExecutor::UpdateIndexes(Tuple &&old_tuple, Tuple &&updated_tuple, const RID &rid) {
  for (IndexInfo *index_info : table_indexes_) {
    assert(index_info != Catalog::NULL_INDEX_INFO);

    // first, delete the old index entry.
    const Tuple old_key =
        old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->DeleteEntry(old_key, old_key.GetRid(), exec_ctx_->GetTransaction());

    // then, add the updated index entry.
    //! the updated key reuses the rid of the tuple to be updated.
    const Tuple updated_key =
        updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->InsertEntry(updated_key, rid, exec_ctx_->GetTransaction());
  }
}

}  // namespace bustub
