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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())},
      table_it_{table_info_->table_->End()} {
  /// FIXME(bayes): How can I properly init the table_it_?
  if (table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception(ExceptionType::INVALID, "Table not found given the invalid table oid");
  }
}

void SeqScanExecutor::Init() {
  // init the table iterator to the beginning of the table to be scanned through.
  table_it_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // scan the table tuple by tuple and spit out the next tuple that satisfies the predicate.
  const AbstractExpression *predicate = plan_->GetPredicate();

  //! TableIterator++ will call GetNextTupleId which would skip tuples marked deleted.
  //! So deleted tuples won't be spit out.
  while (table_it_ != table_info_->table_->End()) {
    bool flag{false};
    if (predicate == nullptr || predicate->Evaluate(&(*table_it_), &table_info_->schema_).GetAs<bool>()) {
      // found a tuple that satisfies the predicate.

      // what columns the output schema has.
      const Schema *output_schema = GetOutputSchema();
      assert(output_schema != nullptr);
      const std::vector<Column> &output_cols = output_schema->GetColumns();
      assert(!output_cols.empty());

      // what columns the input schema has.
      const Schema *input_schema = &table_info_->schema_;
      assert(input_schema != nullptr);
      const std::vector<Column> &input_cols = input_schema->GetColumns();
      assert(!input_cols.empty());

      /// FIXME(bayes): Is this always true? Won't there be some replica?
      // the output schema must be the superset of the input schema.
      assert(input_cols.size() >= output_cols.size());

      // values corresponding to the output schema.
      std::vector<Value> values;
      /// FIXME(bayes): Get values in this way?
      // const Value value = output_cols.front().GetExpr()->Evaluate(&(*table_it_), &table_info_->schema_);

      // use this to deduplicate columns of the same name.
      std::unordered_set<uint32_t> added_col_indices;

      // collect values from the input tuple that the output schema requires.
      for (const Column &col : output_cols) {
        const std::string &col_name{col.GetName()};
        // find the index of the column in the input schema given its name.
        bool found{false};
        for (uint32_t col_idx = 0; col_idx < input_cols.size(); ++col_idx) {
          if (input_cols.at(col_idx).GetName() == col_name && added_col_indices.count(col_idx) == 0) {
            found = true;
            values.push_back(table_it_->GetValue(input_schema, col_idx));
            added_col_indices.insert(col_idx);
            break;
          }
        }
        if (!found) {
          throw Exception(ExceptionType::INVALID, "Matched column not found");
        }
      }
      // check if we've collected all required values.
      assert(values.size() == output_cols.size());

      // create a new tuple given the values and the schema.
      *tuple = Tuple(values, output_schema);
      *rid = tuple->GetRid();

      // increment the iterator to prepare for the next Next call.
      ++table_it_;

      flag = true;

    } else {
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
