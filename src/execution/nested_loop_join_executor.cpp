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
      right_child_executor_{std::move(right_child_executor)} {}

void NestedLoopJoinExecutor::Init() {
  // init child executors.
  assert(left_child_executor_ != nullptr);
  assert(right_child_executor_ != nullptr);
  left_child_executor_->Init();
  right_child_executor_->Init();

  // fetch the first left tuple.
  RID rid;
  if (!left_child_executor_->Next(&left_tuple_, &rid)) {
    // the left tuple is empty.
    done_ = true;
  }
}

// join algorithm:
// for each left tuple in left table
//   for each right tuple in right table
//      emit joined tuple.
//
// for each call on Next, we fetch the next right tuple for the current left tuple.
// if the right table is exhausted, we fetch the next left tuple.
// if the left table is exhausted, the join is done.

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // immediately return if the join is done.
  if (done_) {
    return false;
  }

  // get the left and right schemae.
  const Schema *left_schema = left_child_executor_->GetOutputSchema();
  const Schema *right_schema = right_child_executor_->GetOutputSchema();
  assert(left_schema != nullptr);
  assert(right_schema != nullptr);

  Tuple right_tuple{};
  RID right_rid;

  // fetch the next pair of left/right tuple until the predicate evaluates to true.
  const AbstractExpression *predicate = plan_->Predicate();
  do {
    // fetch the next right tuple from the right table.
    if (!right_child_executor_->Next(&right_tuple, &right_rid)) {
      // the right table is exhausted.
      RID left_rid;
      if (!left_child_executor_->Next(&left_tuple_, &left_rid)) {
        // when the left table is exhausted, the join is done.
        return false;
      }
      // otherwise, the join proceeds.

      // reset the table iterator of the right table to the beginning.
      right_child_executor_->Init();

      // fetch the first right tuple.
      if (!right_child_executor_->Next(&right_tuple, &right_rid)) {
        // this could only happen if the right tuple is empty.
        return false;
      }
    }
  } while (predicate != nullptr &&
           !predicate->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema).GetAs<bool>());
  // if the predicate is null, all pairs of tuples match.

  // construct the output tuple from the given output schema by retrieving values from the joined tuples.
  const Schema *output_schema = GetOutputSchema();
  std::vector<Value> values;
  values.reserve(output_schema->GetColumnCount());
  for (const Column &col : output_schema->GetColumns()) {
    Value val = col.GetExpr()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema);
    values.emplace_back(std::move(val));
  }

  // create a new tuple given the values and the schema.
  *tuple = Tuple(values, output_schema);
  //! no need to set rid.

  // succefully produce a joined tuple.
  return true;
}

}  // namespace bustub
