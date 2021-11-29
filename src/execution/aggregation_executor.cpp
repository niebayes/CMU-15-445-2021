//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_executor_{std::move(child_executor)},
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      //! init to End to indicate the Init has not yet been called.
      aht_iterator_{aht_.End()} {
  assert(exec_ctx_ != nullptr);
  assert(plan_ != nullptr);
}

void AggregationExecutor::Init() {
  // init the child executor.
  assert(child_executor_ != nullptr);
  child_executor_->Init();

  //! since aggregations are pipeline breakers, i.e. operators that produce the first output tuple only after all input
  //! tuples have been produced, we must finish the hash table build phase inside Init.

  // hash table build phase starts.

  // fetch all input tuples from the child executor.
  Tuple input_tuple{};
  RID rid;
  // loop invariant: there's still input tuples produced in the child.
  while (child_executor_->Next(&input_tuple, &rid)) {
    // make aggregate key-value pair.
    const AggregateKey agg_key = MakeAggregateKey(&input_tuple);
    const AggregateValue agg_val = MakeAggregateValue(&input_tuple);
    // insert it into the hash table to combine with the running value.
    /// FIXME(bayes): seems the move has no effect.
    aht_.InsertCombine(agg_key, agg_val);
  }
  // post invariant: no more input tuples to produce.

  // hash table build phase done.

  // set the current cursor to the beginning of the hash table.
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ != aht_.End()) {
    *tuple = Tuple(aht_iterator_.Val().aggregates_, GetOutputSchema());
    *rid = tuple->GetRid();

    // increment the iterator to prepare for the next Next call.
    ++aht_iterator_;

    // apply the having predicate to filter out tuples.
    //! the having clause is not mandatory.
    auto having_expr = plan_->GetHaving();
    if (having_expr == nullptr ||
        having_expr->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>()) {
      return true;
    }
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_executor_.get(); }

}  // namespace bustub
