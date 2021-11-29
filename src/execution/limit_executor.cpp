//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  assert(exec_ctx_ != nullptr);
  assert(plan_ != nullptr);
}

void LimitExecutor::Init() {
  // init the child executor.
  assert(child_executor_ != nullptr);
  child_executor_->Init();

  tuple_cnt_ = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  // check if we've reached the maximum number of tuples.
  if (tuple_cnt_ < plan_->GetLimit()) {
    // fetch a tuple from the child.
    if (child_executor_->Next(tuple, rid)) {
      // update the number of tuples emit so far.
      ++tuple_cnt_;
      return true;
    }
    // the child has no more tuples to produce.
    return false;
  }
  // reached the limit, stop emitting tuples.
  return false;
}

}  // namespace bustub
