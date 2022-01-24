//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child_executor,
                                   std::unique_ptr<AbstractExecutor> &&right_child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_child_executor_{std::move(left_child_executor)},
      right_child_executor_{std::move(right_child_executor)} {}

// hash join algorithm:
// build phase:
//   for each tuple in left table
//     make the join key and insert into the hash table.
//
// probe phase:
//   for each tuple in right table
//     make the join key and probe the hash table.
//       if found a matched key, compare all tuples to see if they really match.
//         if trully match, emit out the joined tuple.

void HashJoinExecutor::Init() {
  // init child executors.
  assert(left_child_executor_ != nullptr);
  assert(right_child_executor_ != nullptr);
  left_child_executor_->Init();
  right_child_executor_->Init();

  // build phase.
  Tuple left_tuple{};
  RID rid;
  while (left_child_executor_->Next(&left_tuple, &rid)) {
    // make a join key from the tuple.
    const JoinKey left_join_key =
        MakeJoinKey(&left_tuple, left_child_executor_->GetOutputSchema(), plan_->LeftJoinKeyExpression());
    jht_[left_join_key].push_back(left_tuple);
  }

  // fetch the first right tuple.
  right_child_executor_->Next(&right_tuple_, &rid);
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // if the right table is empty, immediately return.
  if (right_tuple_.GetLength() == 0) {
    return false;
  }

  // make a join key from the right tuple.
  const JoinKey right_join_key =
      MakeJoinKey(&right_tuple_, right_child_executor_->GetOutputSchema(), plan_->RightJoinKeyExpression());

  // probe the hash table to see if there's a potential matching left tuple.
  if (jht_.count(right_join_key) == 1) {
    // match by comparing rids.
    const auto &left_tuples = jht_.at(right_join_key);
    for (int i = 0; i < static_cast<int>(left_tuples.size()); ++i) {
      // skip duplicate matched pairs.
      if (matched_left_tuples_.count(i) == 1) {
        continue;
      }

      const Tuple &left_tuple = left_tuples[i];

      if (left_tuple.GetRid() == right_tuple_.GetRid()) {
        // construct the output tuple from the given output schema by retrieving values from the joined tuples.
        const Schema *output_schema = GetOutputSchema();
        std::vector<Value> values;
        values.reserve(output_schema->GetColumnCount());
        for (const Column &col : output_schema->GetColumns()) {
          Value val = col.GetExpr()->EvaluateJoin(&left_tuple, left_child_executor_->GetOutputSchema(), &right_tuple_,
                                                  right_child_executor_->GetOutputSchema());
          values.emplace_back(std::move(val));
        }

        // create a new tuple given the values and the schema.
        *tuple = Tuple(values, output_schema);
        //! no need to set rid.

        // add to matched left tuples for this right tuple.
        matched_left_tuples_.insert(i);

        return true;
      }
    }
  }
  // if there's no matched one, fetch the next right tuple.
  if (!right_child_executor_->Next(&right_tuple_, rid)) {
    // the right table is exhausted, the join is done.
    return false;
  }
  // reset matched tuples.
  matched_left_tuples_ = std::unordered_set<int>{};

  // otherwise, proceed to check the next right tuple until found a matched one or the right table is exhausted.
  return Next(tuple, rid);
}

}  // namespace bustub
