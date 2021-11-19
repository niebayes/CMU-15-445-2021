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
  table_indices_ = catalog->GetTableIndexes(table_info_->name_);
  for (const IndexInfo *index_info : table_indices_) {
    if (index_info == Catalog::NULL_INDEX_INFO) {
      throw Exception(ExceptionType::INVALID, "Index not found");
    }
  }
}

void UpdateExecutor::Init() {
  // init the child executor properly.
  assert(child_executor_ != nullptr);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tuple{};
  Tuple updated_tuple{};
  bool update_success{false};

  /// TODO(bayes): The only solution to pass the update_executor local test is to wrap the current Next in a while loop
  /// until there's no tuples produced from the child executor. And then we return false unconditionally.

  // pull values from the child executor.
  while (child_executor_->Next(tuple, rid)) {
    // record the old tuple.
    assert(tuple != nullptr);
    old_tuple = *tuple;
    // generate the updated tuple from the old tuple given the update attributes.
    updated_tuple = GenerateUpdatedTuple(old_tuple);
    // apply the update to the table.
    update_success = table_info_->table_->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction());

    // if the update succeeds, also update the corresponding indices.
    if (update_success) {
      for (IndexInfo *index_info : table_indices_) {
        // first, delete the old index entry.
        const Tuple old_key = old_tuple.KeyFromTuple(table_info_->schema_, *(index_info->index_->GetKeySchema()),
                                                     index_info->index_->GetKeyAttrs());
        //! you have to pass in the rid of the key but it's unused however.
        index_info->index_->DeleteEntry(old_key, old_key.GetRid(), exec_ctx_->GetTransaction());

        // then, add the updated index entry.
        const Tuple updated_key = updated_tuple.KeyFromTuple(
            table_info_->schema_, *(index_info->index_->GetKeySchema()), index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(updated_key, updated_tuple.GetRid(), exec_ctx_->GetTransaction());
      }
    } else {
      LOG_DEBUG("Update fail");
    }
  }

  // return false unconditionally to indicate the update is done.
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

}  // namespace bustub
