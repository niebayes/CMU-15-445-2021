//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

// key used in distinct executor.
struct DistinctKey {
  // since there're no expressions, the values of key are just the values of tuple.
  std::vector<Value> values_;

  bool operator==(const DistinctKey &other) const {
    assert(values_.size() == other.values_.size());

    for (uint32_t i = 0; i < values_.size(); ++i) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

// implements std::hash on DistinctKey. Used for std::unordered_map on DistinctKey.
template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &dis_key) const {
    size_t curr_hash = 0;
    for (const auto &key : dis_key.values_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  DistinctKey MakeDistinctKey(const Tuple *tuple) {
    assert(tuple != nullptr);

    std::vector<Value> values;

    const uint32_t col_count = child_executor_->GetOutputSchema()->GetColumnCount();
    for (uint32_t i = 0; i < col_count; ++i) {
      values.emplace_back(tuple->GetValue(child_executor_->GetOutputSchema(), i));
    }

    return {values};
  }

  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  // hash table for deduplicating tuples.
  /// FIXME(bayes): Is it sufficient to use std::unordered_set? Shall we do further checking?
  std::unordered_set<DistinctKey> dht_{};
};
}  // namespace bustub
