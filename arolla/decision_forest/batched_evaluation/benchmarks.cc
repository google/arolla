// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// To run benchmarks on parameters that are typical for production
//   use:    --benchmarks="BM_Prod_"
// To run benchmarks on wider range of parameters
//   use:    --benchmarks="BM_Main_"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "arolla/decision_forest/batched_evaluation/batched_forest_evaluator.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/testing/test_util.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/threading.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

absl::Status RunBatchedBenchmark(
    size_t batch_size, const DecisionForest& forest,
    BatchedForestEvaluator::CompilationParams params, benchmark::State& state,
    int64_t split_count) {
  // Create memory layout and slots
  std::vector<TypedSlot> slots;
  FrameLayout::Builder layout_builder;
  RETURN_IF_ERROR(CreateArraySlotsForForest(forest, &layout_builder, &slots));

  std::vector<TypedSlot> output_slots;
  auto output_slot = layout_builder.AddSlot<DenseArray<float>>();
  output_slots.push_back(TypedSlot::FromSlot(output_slot));

  FrameLayout layout = std::move(layout_builder).Build();

  // Compile forest
  ASSIGN_OR_RETURN(auto evaluator, BatchedForestEvaluator::Compile(
                                       forest, {TreeFilter()}, params));

  // Prepare input data
  MemoryAllocation ctx(&layout);
  FramePtr frame = ctx.frame();
  absl::BitGen rnd;
  for (auto slot : slots) {
    RETURN_IF_ERROR(FillArrayWithRandomValues(batch_size, slot, frame, &rnd));
  }

  // Run
  for (auto _ : state) {
    RETURN_IF_ERROR(
        evaluator->EvalBatch(slots, output_slots, frame, nullptr, batch_size));
  }
  // The total number of evaluated split conditions
  state.SetItemsProcessed(state.iterations() * batch_size * split_count);
  return absl::OkStatus();
}

void BM_IntervalSplits(benchmark::State& state, size_t batch_size) {
  int64_t num_splits = state.range(0);
  int64_t num_trees = state.range(1);
  absl::BitGen rnd;
  auto forest = CreateRandomFloatForest(
      &rnd, /*num_features=*/10, /*interactions=*/true,
      /*min_num_splits=*/num_splits, /*max_num_splits=*/num_splits, num_trees);
  CHECK_OK(RunBatchedBenchmark(batch_size, *forest, {}, state,
                               num_splits * num_trees));
}

void BM_MixedSplits(benchmark::State& state, size_t batch_size) {
  int64_t num_splits = state.range(0);
  int64_t num_trees = state.range(1);
  absl::BitGen rnd;
  auto forest = CreateRandomForest(
      &rnd, /*num_features=*/10, /*interactions=*/true,
      /*min_num_splits=*/num_splits, /*max_num_splits=*/num_splits, num_trees);
  CHECK_OK(RunBatchedBenchmark(batch_size, *forest, {}, state,
                               num_splits * num_trees));
}

// MainPairs are used to compare different algorithm in wide range of params.
void RunMainPairs(benchmark::internal::Benchmark* b) {
  b->ArgPair(0, 100000)
      ->ArgPair(1, 1000)
      ->ArgPair(1, 10000)
      ->ArgPair(3, 1000)
      ->ArgPair(3, 10000)
      ->ArgPair(7, 100)
      ->ArgPair(7, 1000)
      ->ArgPair(7, 10000)
      ->ArgPair(15, 100)
      ->ArgPair(15, 1000)
      ->ArgPair(31, 100)
      ->ArgPair(31, 1000)
      ->ArgPair(1023, 100)
      ->ArgPair(3, 1)
      ->ArgPair(3, 5)
      ->ArgPair(3, 9)
      ->ArgPair(7, 1)
      ->ArgPair(7, 5)
      ->ArgPair(7, 9)
      ->ArgPair(15, 1)
      ->ArgPair(15, 5)
      ->ArgPair(15, 9);
}

// ProdPairs are parameters that a typical for prodaction usage.
void RunProdPairs(benchmark::internal::Benchmark* b) {
  b->ArgPair((1 << 5) - 1, 500)
      ->ArgPair((1 << 3) - 1, 1000)
      ->ArgPair((1 << 3) - 1, 100)
      ->ArgPair((1 << 6) - 1, 200)
      ->ArgPair(1, 4000)
      ->ArgPair((1 << 10) - 1, 200)
      ->ArgPair((1 << 15) - 1, 30);
}

void RunBigModelPairs(benchmark::internal::Benchmark* b) {
  b->ArgPair((1 << 6) - 1, 200)
      ->ArgPair((1 << 6) - 1, 2000)
      ->ArgPair((1 << 6) - 1, 20000)
      ->ArgPair((1 << 6) - 1, 200000);
}

#define BATCH_BENCHMARK(TYPE, SPLITS, BATCH)                     \
  void BM_##TYPE##_##SPLITS##_##BATCH(benchmark::State& state) { \
    BM_##SPLITS(state, BATCH);                                   \
  }                                                              \
  BENCHMARK(BM_##TYPE##_##SPLITS##_##BATCH)->Apply(&Run##TYPE##Pairs)

#define THREADED_BENCHMARK(TYPE, SPLITS, BATCH)                              \
  void BM_##TYPE##_##SPLITS##_##BATCH##_T4(benchmark::State& state) {        \
    BatchedForestEvaluator::SetThreading(std::make_unique<StdThreading>(4)); \
    BM_##SPLITS(state, BATCH);                                               \
    BatchedForestEvaluator::SetThreading(nullptr);                           \
  }                                                                          \
  BENCHMARK(BM_##TYPE##_##SPLITS##_##BATCH##_T4)->Apply(&Run##TYPE##Pairs)

void BM_HugeForest(benchmark::State& state) {
  BM_IntervalSplits(state, /*batch_size=*/1000);
}

BENCHMARK(BM_HugeForest)
    ->ArgPair(1000, 10)      // 10'000 split nodes
    ->ArgPair(1000, 30)      // 30'000 split nodes
    ->ArgPair(1000, 100)     // 100'000 split nodes
    ->ArgPair(1000, 300)     // 300'000 split nodes
    ->ArgPair(1000, 1000)    // 1000'000 split nodes
    ->ArgPair(1000, 3000)    // 3000'000 split nodes
    ->ArgPair(1000, 10000);  // 10'000'000 split nodes

BATCH_BENCHMARK(Main, IntervalSplits, 100000);
BATCH_BENCHMARK(Main, MixedSplits, 100000);

BATCH_BENCHMARK(Prod, IntervalSplits, 1);
BATCH_BENCHMARK(Prod, IntervalSplits, 30);
BATCH_BENCHMARK(Prod, IntervalSplits, 100);
BATCH_BENCHMARK(Prod, IntervalSplits, 300);
BATCH_BENCHMARK(Prod, IntervalSplits, 600);
BATCH_BENCHMARK(Prod, IntervalSplits, 1000);
BATCH_BENCHMARK(Prod, IntervalSplits, 10000);
BATCH_BENCHMARK(Prod, IntervalSplits, 100000);

BATCH_BENCHMARK(Prod, MixedSplits, 1);
BATCH_BENCHMARK(Prod, MixedSplits, 100);
BATCH_BENCHMARK(Prod, MixedSplits, 1000);
BATCH_BENCHMARK(Prod, MixedSplits, 100000);

THREADED_BENCHMARK(Prod, MixedSplits, 1);
THREADED_BENCHMARK(Prod, MixedSplits, 100);
THREADED_BENCHMARK(Prod, MixedSplits, 1000);
THREADED_BENCHMARK(Prod, MixedSplits, 100000);

BATCH_BENCHMARK(BigModel, IntervalSplits, 100);
BATCH_BENCHMARK(BigModel, IntervalSplits, 1000);

BATCH_BENCHMARK(BigModel, MixedSplits, 100);
BATCH_BENCHMARK(BigModel, MixedSplits, 1000);

}  // namespace
}  // namespace arolla
