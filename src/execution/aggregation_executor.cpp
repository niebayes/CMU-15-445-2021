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
      aht_iterator_{aht_.End()} {}

void AggregationExecutor::Init() {
  // init the child executor.
  assert(child_executor_ != nullptr);
  child_executor_->Init();

  //! since aggregations are pipeline breakers, i.e. operators that produce the first output tuple only after all input
  //! tuples have been produced, we must finish the hash table build phase inside Init.

  // build phase.
  Tuple input_tuple{};
  RID rid;
  while (child_executor_->Next(&input_tuple, &rid)) {
    // make aggregate key-value pair.
    const AggregateKey agg_key = MakeAggregateKey(&input_tuple);
    const AggregateValue agg_val = MakeAggregateValue(&input_tuple);
    // insert it into the hash table to combine with the running value.
    aht_.InsertCombine(agg_key, agg_val);
  }

  // set the current cursor to the beginning of the hash table.
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_.End()) {
    // all tuples are aggregated, the aggregation is done.
    return false;
  }

  // current cursor.
  AggregateKey agg_key = aht_iterator_.Key();
  AggregateValue agg_val = aht_iterator_.Val();

  // filtering.
  const AbstractExpression *having = plan_->GetHaving();
  while (having != nullptr && !having->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_).GetAs<bool>()) {
    ++aht_iterator_;
    if (aht_iterator_ == aht_.End()) {
      return false;
    }
    agg_key = aht_iterator_.Key();
    agg_val = aht_iterator_.Val();
  }

  // construct the output tuple from the aggregates.
  std::vector<Value> values;
  const Schema *output_schema = GetOutputSchema();
  values.reserve(output_schema->GetColumnCount());
  for (const Column &col : output_schema->GetColumns()) {
    Value val = col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_);
    values.emplace_back(std::move(val));
  }
  *tuple = Tuple(values, output_schema);
  //! no need to set rid.

  // advance iterator preparing for the next Next call.
  ++aht_iterator_;

  return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_executor_.get(); }

}  // namespace bustub
