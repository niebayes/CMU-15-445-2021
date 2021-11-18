//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct JoinKey {
  /// FIXME(bayes): Am I right?
  //! there will be a compiler-generated ctor which is able to accept an initilaizer list to init the members in order.

  /// FIXME(bayes): Shall be a vector of values as the AggregateKey.
  Value value_;

  // used in std::unordered_map to compare two JoinKeys for equality.
  bool operator==(const JoinKey &other) const {
    if (value_.CompareEquals(other.value_) != CmpBool::CmpTrue) {
      return false;
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

// implements std::hash on JoinKey.
template <>
struct hash<bustub::JoinKey> {
  std::size_t operator()(const bustub::JoinKey &join_key) const {
    return bustub::HashUtil::HashValue(&join_key.value_);
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a HASH JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child_executor The child executor that produces tuples for the left side of join
   * @param right_child_executor The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child_executor,
                   std::unique_ptr<AbstractExecutor> &&right_child_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /// FIXME(bayes): Shall I use left and right join expressions to make the join key?
  JoinKey MakeJoinKey(const Tuple *tuple, const Schema *schema, const AbstractExpression *join_expr) {
    assert(tuple != nullptr);
    assert(schema != nullptr);
    assert(join_expr != nullptr);

    const Value value = join_expr->Evaluate(tuple, schema);
    /// FIXME(bayes): Shall I call std::move at here?
    return {std::move(value)};
  }

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  // the child executor which produces tuples of the outer table.
  std::unique_ptr<AbstractExecutor> left_child_executor_;
  // the child executor which produces tuples of the inner table.
  std::unique_ptr<AbstractExecutor> right_child_executor_;
  /// FIXME(bayes): Shall I store Tuple* or Tuple?
  // hash table for hash join.
  //! using vector of tuples to handle the case where there exists multiple tuples having the same join key.
  std::unordered_map<JoinKey, std::vector<Tuple>> jht_{};
};

}  // namespace bustub
