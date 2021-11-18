//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  assert(exec_ctx_ != nullptr);
  assert(plan_ != nullptr);
}

void DistinctExecutor::Init() {}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  // build the hash table on the fly.

  // fetch a tuple from the child.
  if (child_executor_->Next(tuple, rid)) {
    // check if there exists a duplicate of this tuple.
    const DistinctKey dis_key = MakeDistinctKey(tuple);
    if (dht_.count(dis_key) == 1) {
      // found a duplicate. Retry to check the next tuple.
      return Next(tuple, rid);

    } else {
      // no duplicate found.

      // insert this key to hash table.
      dht_.insert(dis_key);

      return true;
    }

  } else {
    // the child has no more tuples to produce.
    return false;
  }
}

}  // namespace bustub
