//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_executor_benchmark_test.cpp
//
// Identification: test/execution/grading_executor_benchmark_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>  // NOLINT
#include <cstdio>
#include <future>  // NOLINT
#include <iostream>
#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <unordered_set>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "catalog/table_generator.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "gtest/gtest.h"
#include "test_util.h"  // NOLINT
#include "type/value_factory.h"

// Macro for time out mechanism
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

/** The benchmark timeout in seconds */
static constexpr const std::size_t TIMEOUT_SECONDS = 200;
/** The number of items in the benchmark table */
static constexpr const std::size_t BENCHMARK_SIZE = 1000;
/** The maximum VARCHAR size */
static constexpr const uint32_t MAX_VARCHAR_SIZE = 128;

namespace bustub {

static const AbstractExpression *MakeColumnValueExpression(
    const Schema &schema, uint32_t tuple_idx, const std::string &col_name,
    std::vector<std::unique_ptr<AbstractExpression>> *allocated_exprs) {
  uint32_t col_idx = schema.GetColIdx(col_name);
  auto col_type = schema.GetColumn(col_idx).GetType();
  allocated_exprs->emplace_back(std::make_unique<ColumnValueExpression>(tuple_idx, col_idx, col_type));
  return allocated_exprs->back().get();
}

static const AbstractExpression *MakeComparisonExpression(
    const AbstractExpression *lhs, const AbstractExpression *rhs, ComparisonType comp_type,
    std::vector<std::unique_ptr<AbstractExpression>> *allocated_exprs) {
  allocated_exprs->emplace_back(std::make_unique<ComparisonExpression>(lhs, rhs, comp_type));
  return allocated_exprs->back().get();
}

static const AbstractExpression *MakeAggregateValueExpression(
    bool is_group_by_term, uint32_t term_idx, std::vector<std::unique_ptr<AbstractExpression>> *allocated_exprs) {
  allocated_exprs->emplace_back(
      std::make_unique<AggregateValueExpression>(is_group_by_term, term_idx, TypeId::INTEGER));
  return allocated_exprs->back().get();
}

static const Schema *MakeOutputSchema(const std::vector<std::pair<std::string, const AbstractExpression *>> &exprs,
                                      std::vector<std::unique_ptr<Schema>> *allocated_output_schemas) {
  std::vector<Column> cols;
  cols.reserve(exprs.size());
  for (const auto &input : exprs) {
    if (input.second->GetReturnType() != TypeId::VARCHAR) {
      cols.emplace_back(input.first, input.second->GetReturnType(), input.second);
    } else {
      cols.emplace_back(input.first, input.second->GetReturnType(), MAX_VARCHAR_SIZE, input.second);
    }
  }
  allocated_output_schemas->emplace_back(std::make_unique<Schema>(cols));
  return allocated_output_schemas->back().get();
}

void ExecutorBenchmark() {
  std::vector<std::unique_ptr<AbstractExpression>> allocated_exprs{};
  std::vector<std::unique_ptr<Schema>> allocated_output_schemas{};

  auto disk_manager = std::make_unique<DiskManager>("executor_test.db");
  auto bpm = std::make_unique<BufferPoolManagerInstance>(256, disk_manager.get());
  auto lock_manager = std::make_unique<LockManager>();
  auto txn_mgr = std::make_unique<TransactionManager>(lock_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), lock_manager.get(), nullptr);

  // Begin a new transaction, along with its executor context
  Transaction *txn = txn_mgr->Begin();
  auto exec_ctx = std::make_unique<ExecutorContext>(txn, catalog.get(), bpm.get(), txn_mgr.get(), lock_manager.get());
  auto execution_engine = std::make_unique<ExecutionEngine>(bpm.get(), txn_mgr.get(), catalog.get());

  // Generate some test tables
  TableGenerator gen{exec_ctx.get()};
  gen.GenerateTestTables();

  // Insert into empty table
  std::vector<std::vector<Value>> raw_vals{};
  raw_vals.reserve(BENCHMARK_SIZE);
  for (std::size_t i = 0; i < BENCHMARK_SIZE; ++i) {
    std::vector<Value> value{ValueFactory::GetIntegerValue(static_cast<int32_t>(BENCHMARK_SIZE - i)),
                             ValueFactory::GetIntegerValue(i % 53)};
    raw_vals.emplace_back(value);
  }

  // Create insert plan node
  auto *table_info = exec_ctx->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};
  execution_engine->Execute(&insert_plan, nullptr, txn, exec_ctx.get());

  // Insert into test1
  raw_vals.clear();
  raw_vals.reserve(BENCHMARK_SIZE);
  for (std::size_t i = 4999; i < BENCHMARK_SIZE; ++i) {
    std::vector<Value> value{ValueFactory::GetIntegerValue(static_cast<int32_t>(i)),
                             ValueFactory::GetIntegerValue(static_cast<int32_t>(i % 53)),
                             ValueFactory::GetIntegerValue(static_cast<int32_t>(i)),
                             ValueFactory::GetIntegerValue(static_cast<int32_t>(i % 2))};
    raw_vals.emplace_back(value);
  }

  // Create insert plan node
  auto *table_info_1 = exec_ctx->GetCatalog()->GetTable("test_1");
  InsertPlanNode insert_plan1{std::move(raw_vals), table_info_1->oid_};
  execution_engine->Execute(&insert_plan1, nullptr, txn, exec_ctx.get());

  // Start the clock
  const auto time_start = std::chrono::high_resolution_clock::now();

  const Schema *outer_schema1;
  std::unique_ptr<AbstractPlanNode> scan_plan1;
  auto &schema_outer = exec_ctx->GetCatalog()->GetTable("test_1")->schema_;
  auto outer_col_a = MakeColumnValueExpression(schema_outer, 0, "colA", &allocated_exprs);
  auto outer_col_b = MakeColumnValueExpression(schema_outer, 0, "colB", &allocated_exprs);
  auto outer_col_c = MakeColumnValueExpression(schema_outer, 0, "colC", &allocated_exprs);
  auto outer_col_d = MakeColumnValueExpression(schema_outer, 0, "colD", &allocated_exprs);
  const Schema *outer_out_schema1 =
      MakeOutputSchema({{"colA", outer_col_a}, {"colB", outer_col_b}, {"colC", outer_col_c}, {"colD", outer_col_d}},
                       &allocated_output_schemas);

  {
    auto *table_info = exec_ctx->GetCatalog()->GetTable("test_1");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA", &allocated_exprs);
    auto col_b = MakeColumnValueExpression(schema, 0, "colB", &allocated_exprs);
    outer_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}}, &allocated_output_schemas);
    scan_plan1 = std::make_unique<SeqScanPlanNode>(outer_out_schema1, nullptr, table_info->oid_);
  }

  const Schema *out_schema2;
  std::unique_ptr<AbstractPlanNode> scan_plan2;
  {
    auto *table_info = exec_ctx->GetCatalog()->GetTable("empty_table2");
    auto &schema = table_info->schema_;
    auto col_a = MakeColumnValueExpression(schema, 0, "colA", &allocated_exprs);
    auto col_b = MakeColumnValueExpression(schema, 0, "colB", &allocated_exprs);
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}}, &allocated_output_schemas);
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  const Schema *out_final;
  std::unique_ptr<NestedLoopJoinPlanNode> join_plan;
  {
    // colA and colB have a tuple index of 0 because they are the left side of the join
    auto col_a = MakeColumnValueExpression(*outer_schema1, 0, "colA", &allocated_exprs);
    auto col_b = MakeColumnValueExpression(*outer_schema1, 0, "colB", &allocated_exprs);

    // col1 and col2 have a tuple index of 1 because they are the right side of the join
    auto col_a_empty = MakeColumnValueExpression(*out_schema2, 1, "colA", &allocated_exprs);
    auto col_b_empty = MakeColumnValueExpression(*out_schema2, 1, "colB", &allocated_exprs);
    auto predicate = MakeComparisonExpression(col_a, col_a_empty, ComparisonType::Equal, &allocated_exprs);
    out_final =
        MakeOutputSchema({{"colA", col_a}, {"colB", col_b}, {"colA_empty", col_a_empty}, {"colB_empty", col_b_empty}},
                         &allocated_output_schemas);
    join_plan = std::make_unique<NestedLoopJoinPlanNode>(
        out_final, std::vector<const AbstractPlanNode *>{scan_plan1.get(), scan_plan2.get()}, predicate);
  }

  // Aggregation plan
  const Schema *agg_schema;
  std::unique_ptr<AbstractPlanNode> agg_plan;
  {
    const AbstractExpression *state_id = MakeColumnValueExpression(*out_final, 0, "colB_empty", &allocated_exprs);
    const AbstractExpression *val = MakeColumnValueExpression(*out_final, 0, "colA_empty", &allocated_exprs);

    // Make group bys
    std::vector<const AbstractExpression *> group_by_cols{state_id};
    const AbstractExpression *groupby_state = MakeAggregateValueExpression(true, 0, &allocated_exprs);

    // Make aggregates
    std::vector<const AbstractExpression *> aggregate_cols{val, val, val, val};
    std::vector<AggregationType> agg_types{AggregationType::CountAggregate, AggregationType::MaxAggregate,
                                           AggregationType::MinAggregate, AggregationType::SumAggregate};
    const AbstractExpression *count_val = MakeAggregateValueExpression(false, 0, &allocated_exprs);
    const AbstractExpression *max_val = MakeAggregateValueExpression(false, 1, &allocated_exprs);
    const AbstractExpression *min_val = MakeAggregateValueExpression(false, 2, &allocated_exprs);
    const AbstractExpression *sum_val = MakeAggregateValueExpression(false, 3, &allocated_exprs);
    agg_schema = MakeOutputSchema({{"state_id", groupby_state},
                                   {"countVal", count_val},
                                   {"maxVal", max_val},
                                   {"minVal", min_val},
                                   {"sumVal", sum_val}},
                                  &allocated_output_schemas);
    agg_plan = std::make_unique<AggregationPlanNode>(agg_schema, join_plan.get(), nullptr, std::move(group_by_cols),
                                                     std::move(aggregate_cols), std::move(agg_types));
  }

  // Execute
  std::vector<Tuple> result_set;
  execution_engine->Execute(agg_plan.get(), &result_set, txn, exec_ctx.get());

  const auto time_end = std::chrono::high_resolution_clock::now();
  const auto elapsed_time = std::chrono::duration<double, std::milli>(time_end - time_start).count();

  // Verify results
  int total = 0;
  for (const auto &tuple : result_set) {
    total += tuple.GetValue(agg_schema, agg_schema->GetColIdx("countVal")).GetAs<int32_t>();
  }

  const bool success = (result_set.size() == 53) && (total == BENCHMARK_SIZE - 1);
  if (success) {
    std::cout << elapsed_time;
  } else {
    std::cout << "FAIL";
  }
  std::cout << std::endl;

  txn_mgr->Commit(txn);
  disk_manager->ShutDown();
  delete txn;
}

TEST(BenchmarkTest, ExecutorBenchmarkTest) {
  TEST_TIMEOUT_BEGIN
  ExecutorBenchmark();
  remove("executor_test.db");
  TEST_TIMEOUT_FAIL_END(1000 * TIMEOUT_SECONDS)
}

}  // namespace bustub
