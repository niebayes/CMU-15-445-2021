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
#include <string_view>

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan} {
  assert(exec_ctx_ != nullptr);
  assert(plan_ != nullptr);

  // get catalog from context.
  Catalog *catalog = exec_ctx_->GetCatalog();
  assert(catalog != nullptr);

  // retrieve the table to be scaned.
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  if (table_info_ == Catalog::NULL_TABLE_INFO) {
    throw Exception(ExceptionType::INVALID, "Table not found given the invalid table oid");
  }
}

void SeqScanExecutor::Init() {
  // init iterators.
  table_it_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  table_end_ = table_info_->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // scan the table tuple by tuple and spit out the next tuple that satisfies the predicate.
  const AbstractExpression *predicate = plan_->GetPredicate();
  assert(predicate != nullptr);

  while (table_it_ != table_end_) {
    const Value res = predicate->Evaluate(&(*table_it_), &table_info_->schema_);
    if (res.GetAs<bool>()) {
      // found a tuple that satisfies the predicate.

      // create the output tuple given its schema from the original tuple with its schema.
      *tuple = Tuple::CreateOutputTuple(&table_info_->schema_, *table_it_, GetOutputSchema());
      *rid = table_it_->GetRid();

      // increment the iterator to prepare for the next Next call.
      ++table_it_;
      return true;

    } else {
      // this tuple does not satisfy the predicate, proceed to the next tuple.
      ++table_it_;
    }
  }
  // if we've reached the end, return false to indicate the scanning is done.
  return false;
}

}  // namespace bustub
