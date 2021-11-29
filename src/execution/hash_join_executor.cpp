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
      right_child_executor_{std::move(right_child_executor)} {
  assert(exec_ctx_ != nullptr);
  assert(plan_ != nullptr);
}

void HashJoinExecutor::Init() {
  // init child executors.
  assert(left_child_executor_ != nullptr);
  assert(right_child_executor_ != nullptr);
  left_child_executor_->Init();
  right_child_executor_->Init();

  // hash table build phase starts.

  // fetch all tuples from the outer table.
  Tuple outer_tuple{};
  RID rid;
  // loop invariant: there's still outer tuples produced.
  while (left_child_executor_->Next(&outer_tuple, &rid)) {
    const JoinKey join_key =
        MakeJoinKey(&outer_tuple, left_child_executor_->GetOutputSchema(), plan_->LeftJoinKeyExpression());
    //! since the outer_tuple is an automatic var which gets recycled after leaving the function scope, we have to
    //! store Tuple in the hash table.
    jht_[join_key].emplace_back(std::move(outer_tuple));
  }
  // post invariant: no more outer tuples to produce.

  // hash table build phase done.
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // fetch a tuple from the inner table.
  if (right_child_executor_->Next(tuple, rid)) {
    // hash the tuple to a join key.
    const JoinKey join_key =
        MakeJoinKey(tuple, right_child_executor_->GetOutputSchema(), plan_->RightJoinKeyExpression());
    // probe the join hash table to see if there's a potential matching outer tuple.
    if (jht_.count(join_key) == 1) {
      // yes, but we have to examine the original values to determine whether the outer/inner tuples are truly matching.
      for (const Tuple &outer_tuple : jht_.at(join_key)) {
        /// TODO(bayes): how to check?
        outer_tuple.GetData();  //>! delete this.
        return true;
      }
      // no truly matched pair found.
      return false;
    }
    // no, retry to check the next inner tuple.
    return Next(tuple, rid);
  }
  // no more inner tuples to produce. The join is done.
  return false;
}

}  // namespace bustub
