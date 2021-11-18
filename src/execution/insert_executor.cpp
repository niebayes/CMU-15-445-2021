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
  if (plan_->IsRawInsert()) {
    // it's the raw insert plan, get the number of tuples to be inserted.
    const auto &raw_values_vec = plan_->RawValues();
    tuple_cnt_ = raw_values_vec.size();

    // and check if the number of columns of the tuples to be inserted coincide with the schema.
    if (raw_values_vec.front().size() != table_info_->schema_.GetColumnCount()) {
      throw Exception(ExceptionType::INVALID, "Tuples does not fit into the table");
    }
  } else {
    // it's not a raw insert plan. Assert the child plan exist and init it properly.
    /// FIXME(bayes): Shall I throw or assert?
    assert(child_executor_ != nullptr);
    assert(plan_->GetChildAt(0) != nullptr);
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  assert(table_info_ != Catalog::NULL_TABLE_INFO);

  bool insert_success{false};

  if (plan_->IsRawInsert()) {
    // it's a raw insert plan, insert the embeded raw values.

    // check if we've inserted all the tuples.
    if (next_idx_ >= tuple_cnt_) {
      // return false to indicate the raw insertion is done.
      return false;
    }

    const std::vector<Value> &raw_values = plan_->RawValuesAt(next_idx_);
    // create a new tuple based on the values and the given schema.
    *tuple = Tuple(raw_values, &table_info_->schema_);
    // insert the tuple to the table.
    insert_success = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    /// FIXME(bayes): should I increment next_idx_ only if the insertion succeeds?
    // increment the index to prepare for the next Next call.
    next_idx_ += insert_success;

  } else {
    // it's not a raw insert plan, obtain values from the child.
    assert(child_executor_ != nullptr);

    // obtain a tuple from the child executor.
    if (child_executor_->Next(tuple, rid)) {
      // insert the produced tuple to the table.
      insert_success = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());

    } else {
      // the child has no more tuples to produce.
      return false;
    }
  }

  // if the insertion succeeds, also modify the corresponding indices.
  for (IndexInfo *index_info : table_indices_) {
    assert(index_info != Catalog::NULL_INDEX_INFO);

    // generate a key tuple given the tuple schema and the index metadata.
    const Tuple key = tuple->KeyFromTuple(table_info_->schema_, *(index_info->index_->GetKeySchema()),
                                          index_info->index_->GetKeyAttrs());
    // insert a new entry to the index.
    index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
  }

  return true;
}

}  // namespace bustub
