//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_child_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_child_executor_{std::move(left_child_executor)},
      right_child_executor_{std::move(right_child_executor)} {
  assert(exec_ctx_ != nullptr);
  assert(plan_ != nullptr);

  // get catalog from context.
  Catalog *catalog = exec_ctx_->GetCatalog();
  assert(catalog != nullptr);

  // retrieve the outer/inner tables.
  //! by convention, the outer table is the smaller table. It's the caller's duty to follow this convention.
  outer_table_info_ = catalog->GetTable(dynamic_cast<const SeqScanPlanNode *>(plan_->GetLeftPlan())->GetTableOid());
  if (outer_table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception(ExceptionType::INVALID, "Table not found given the invalid table oid");
  }
  inner_table_info_ = catalog->GetTable(dynamic_cast<const SeqScanPlanNode *>(plan_->GetRightPlan())->GetTableOid());
  if (inner_table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception(ExceptionType::INVALID, "Table not found given the invalid table oid");
  }
}

void NestedLoopJoinExecutor::Init() {
  // init child executors.
  assert(left_child_executor_ != nullptr);
  assert(right_child_executor_ != nullptr);
  left_child_executor_->Init();
  right_child_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple inner_tuple{};
  RID right_rid;

  // fetch the next inner tuple from the inner table, i.e. the right one.
  if (!right_child_executor_->Next(&inner_tuple, &right_rid)) {
    // the inner table is exhausted, check if the outer table is exhausted as well.
    RID left_rid;
    if (!left_child_executor_->Next(&outer_tuple_, &left_rid)) {
      // yes, the join is done.
      return false;
    }
    // no, the join proceeds.

    // reset the table iterator of the inner table to the beginning.
    right_child_executor_->Init();
    // and retry the fetching. This assertion fails if the inner table is empty.
    /// FIXME(bayes): Shall I throw instead?
    assert(right_child_executor_->Next(&inner_tuple, &right_rid));
  }

  // get the left and right schemae.
  const Schema *left_schema = &outer_table_info_->schema_;
  const Schema *right_schema = &inner_table_info_->schema_;
  assert(left_schema != nullptr);
  assert(right_schema != nullptr);

  // apply the predicate on the two tuples.
  const AbstractExpression *predicate = plan_->Predicate();

  //! the predicate might be null.
  if (predicate == nullptr ||
      predicate->EvaluateJoin(&outer_tuple_, left_schema, &inner_tuple, right_schema).GetAs<bool>()) {
    // this pair of outer/inner tuples match, emit the output tuple.

    // get the output schema.
    const Schema *output_schema = GetOutputSchema();
    assert(output_schema != nullptr);
    // what columns the output schema expects.
    const std::vector<Column> &output_cols = output_schema->GetColumns();
    assert(!output_cols.empty());
    // what columns the left table schema and thr right table schema have.
    const std::vector<Column> &left_cols = left_schema->GetColumns();
    const std::vector<Column> &right_cols = right_schema->GetColumns();
    assert(!left_cols.empty());
    assert(!right_cols.empty());

    // either of the left/right schema must be a superset of the output schema.
    assert(left_cols.size() >= output_cols.size() && right_cols.size() >= output_cols.size());

    // values of the output tuple given the output schema.
    std::vector<Value> values;

    // used to deduplicate columns of the same name.
    std::unordered_set<uint32_t> added_left_col_indices;
    std::unordered_set<uint32_t> added_right_col_indices;

    // collect values from the outer/inner tuples that the output schema requires.
    for (const Column &col : output_cols) {
      const std::string_view col_name{col.GetName()};
      // find the index of the column in the outer/inner schema given its name.
      bool found{false};

      // first, lookup the outer table.
      for (uint32_t col_idx = 0; col_idx < left_cols.size(); ++col_idx) {
        if (left_cols.at(col_idx).GetName() == col_name && added_left_col_indices.count(col_idx) == 0) {
          found = true;
          values.push_back(outer_tuple_.GetValue(left_schema, col_idx));
          added_left_col_indices.insert(col_idx);
          break;
        }
      }

      // if not found, lookup the inner table.
      if (!found) {
        for (uint32_t col_idx = 0; col_idx < right_cols.size(); ++col_idx) {
          if (right_cols.at(col_idx).GetName() == col_name && added_right_col_indices.count(col_idx) == 0) {
            found = true;
            values.push_back(inner_tuple.GetValue(right_schema, col_idx));
            added_right_col_indices.insert(col_idx);
            break;
          }
        }
      }

      // both're searched and we still found no matched column.
      if (!found) {
        throw Exception(ExceptionType::INVALID, "Matched column not found");
      }
    }
    // check if we've collected all required values.
    assert(values.size() == output_cols.size());

    // create a new tuple given the values and the schema.
    *tuple = Tuple(values, output_schema);
    *rid = tuple->GetRid();

  } else {
    // this pair of outer/inner tuples does not match.
    // retry to check the next pair.
    return Next(tuple, rid);
  }

  // succefully produce a joined tuple.
  return true;
}

}  // namespace bustub
