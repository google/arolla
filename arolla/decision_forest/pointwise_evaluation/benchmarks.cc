// Copyright 2024 Google LLC
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
// To run benchmarks for low level library
//   use:    --benchmarks="BM_LowLevel_"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/pointwise_evaluation/forest_evaluator.h"
#include "arolla/decision_forest/pointwise_evaluation/pointwise.h"
#include "arolla/decision_forest/testing/test_util.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

absl::Status RunPointwiseBenchmark(const DecisionForest& forest,
                                   ForestEvaluator::CompilationParams params,
                                   benchmark::State& state) {
  // Create memory layout and slots
  std::vector<TypedSlot> slots;
  FrameLayout::Builder layout_builder;
  CreateSlotsForForest(forest, &layout_builder, &slots);
  FrameLayout layout = std::move(layout_builder).Build();

  // Compile forest
  auto evaluator =
      SimpleForestEvaluator::Compile(forest, slots, params).value();

  // Prepare input data
  const int frame_count = 10;
  std::vector<std::unique_ptr<MemoryAllocation>> frames;
  absl::BitGen rnd;
  for (int i = 0; i < frame_count; ++i) {
    frames.push_back(std::make_unique<MemoryAllocation>(&layout));
    for (auto slot : slots) {
      RETURN_IF_ERROR(FillWithRandomValue(slot, frames.back()->frame(), &rnd));
    }
  }

  // Run
  while (state.KeepRunningBatch(frame_count)) {
    for (int i = 0; i < frame_count; ++i) {
      auto x = evaluator.Eval(frames[i]->frame());
      benchmark::DoNotOptimize(x);
    }
  }
  state.SetItemsProcessed(state.iterations());
  return absl::OkStatus();
}

void BM_IntervalSplits_Pointwise(benchmark::State& state) {
  int num_splits = state.range(0);
  int num_trees = state.range(1);
  absl::BitGen rnd;
  auto forest = CreateRandomFloatForest(
      &rnd, /*num_features=*/10, /*interactions=*/true,
      /*min_num_splits=*/num_splits, /*max_num_splits=*/num_splits, num_trees);
  CHECK_OK(RunPointwiseBenchmark(*forest, {}, state));
}

void BM_MixedSplits_Pointwise(benchmark::State& state) {
  int num_splits = state.range(0);
  int num_trees = state.range(1);
  absl::BitGen rnd;
  auto forest =
      CreateRandomForest(&rnd, /*num_features=*/10, /*interactions=*/true,
                         /*min_num_splits=*/num_splits,
                         /*max_num_splits=*/num_splits, num_trees);
  CHECK_OK(RunPointwiseBenchmark(*forest, {}, state));
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

// Aliases to BM_IntervalSplits_Pointwise
void BM_Main_IntervalSplits_Pointwise(benchmark::State& state) {
  BM_IntervalSplits_Pointwise(state);
}
void BM_Prod_IntervalSplits_Pointwise(benchmark::State& state) {
  BM_IntervalSplits_Pointwise(state);
}
void BM_Main_MixedSplits_Pointwise(benchmark::State& state) {
  BM_MixedSplits_Pointwise(state);
}
void BM_Prod_MixedSplits_Pointwise(benchmark::State& state) {
  BM_MixedSplits_Pointwise(state);
}

BENCHMARK(BM_Main_IntervalSplits_Pointwise)->Apply(&RunMainPairs);
BENCHMARK(BM_Prod_IntervalSplits_Pointwise)->Apply(&RunProdPairs);
BENCHMARK(BM_Main_MixedSplits_Pointwise)->Apply(&RunMainPairs);
BENCHMARK(BM_Prod_MixedSplits_Pointwise)->Apply(&RunProdPairs);

// Low level pointwise.h benchmarks

template <class T>
struct LessTest {
  bool operator()(absl::Span<const T> values) const {
    return values[feature_id] < threshold;
  }
  int feature_id;
  T threshold;
};

template <class TreeCompiler>
void FillRandomBalanced(int depth, int num_features, absl::BitGen* rnd,
                        TreeCompiler* compiler) {
  const int num_splits = (1 << depth) - 1;
  for (int id = 0; id < num_splits; ++id) {
    CHECK_OK(compiler->SetNode(id, id * 2 + 1, id * 2 + 2,
                               {absl::Uniform<uint16_t>(*rnd) % num_features,
                                absl::Uniform<uint8_t>(*rnd) / 256.0f}));
  }
  for (int id = 0; id <= num_splits; ++id) {
    CHECK_OK(compiler->SetLeaf(num_splits + id,
                               absl::Uniform<uint8_t>(*rnd) / 256.0f));
  }
}

inline SinglePredictor<float, LessTest<float>> CompileRandomBalanced(
    int depth, int num_features, absl::BitGen* rnd) {
  const int num_splits = (1 << depth) - 1;
  PredictorCompiler<float, LessTest<float>> compiler(num_splits * 2 + 1);
  FillRandomBalanced(depth, num_features, rnd, &compiler);
  auto eval_or = compiler.Compile();
  return std::move(eval_or).value();
}

inline BoostedPredictor<float, LessTest<float>, std::plus<float>>
CompileManyRandomBalanced(int depth, int num_features, int num_trees,
                          absl::BitGen* rnd) {
  const int num_splits = (1 << depth) - 1;
  BoostedPredictorCompiler<float, LessTest<float>, std::plus<float>> compiler;
  for (int i = 0; i < num_trees; ++i) {
    auto tree_compiler = compiler.AddTree(num_splits * 2 + 1);
    FillRandomBalanced(depth, num_features, rnd, &tree_compiler);
  }
  auto eval_or = compiler.Compile();
  return std::move(eval_or).value();
}

SinglePredictor<float, LessTest<float>> CompileConstBalanced(int depth,
                                                             int num_features) {
  const int num_splits = (1 << depth) - 1;
  PredictorCompiler<float, LessTest<float>> compiler(num_splits * 2 + 1);
  for (int id = 0; id < num_splits; ++id) {
    CHECK_OK(compiler.SetNode(id, id * 2 + 1, id * 2 + 2,
                              {id % num_features, 0.5f}));
  }
  for (int id = 0; id <= num_splits; ++id) {
    CHECK_OK(compiler.SetLeaf(num_splits + id, 0.5f));
  }
  auto eval_or = compiler.Compile();
  CHECK_OK(eval_or.status());
  return std::move(eval_or).value();
}

void BM_LowLevel_Compile(benchmark::State& state) {
  int depth = state.range(0);
  for (auto _ : state) {
    auto x = CompileConstBalanced(depth, 10);
    ::benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_LowLevel_Compile)->Arg(3)->Arg(6)->Arg(9)->Arg(15)->Arg(20);

void BM_LowLevel_Evaluation(benchmark::State& state) {
  int depth = state.range(0);
  int num_trees = state.range(1);
  constexpr size_t kNumFeatures = 50;
  absl::BitGen rnd;
  std::vector<SinglePredictor<float, LessTest<float>>> trees;
  trees.reserve(num_trees);
  for (int i = 0; i < num_trees; ++i) {
    trees.push_back(CompileRandomBalanced(depth, kNumFeatures, &rnd));
  }
  for (auto _ : state) {
    state.PauseTiming();
    std::vector<float> values;
    values.reserve(kNumFeatures);
    for (int i = 0; i < kNumFeatures; ++i) {
      values.push_back(absl::Uniform<uint8_t>(rnd) / 256.0);
    }
    state.ResumeTiming();
    for (int q = 0; q < num_trees; ++q) {
      auto x = trees[q].Predict(values);
      ::benchmark::DoNotOptimize(x);
    }
  }
  state.SetItemsProcessed(state.iterations() * num_trees);
}

BENCHMARK(BM_LowLevel_Evaluation)
    ->ArgPair(3, 10)
    ->ArgPair(3, 20)
    ->ArgPair(3, 1000)
    ->ArgPair(6, 1000)
    ->ArgPair(9, 500)
    ->ArgPair(15, 1)
    ->ArgPair(15, 2)
    ->ArgPair(15, 10)
    ->ArgPair(15, 20)
    ->ArgPair(15, 50)
    ->ArgPair(20, 1)
    ->ArgPair(20, 2)
    ->ArgPair(20, 10)
    ->ArgPair(20, 20);

void BM_LowLevel_EvaluationBoosted(benchmark::State& state) {
  int depth = state.range(0);
  int num_trees = state.range(1);
  constexpr size_t kNumFeatures = 50;
  absl::BitGen rnd;
  auto eval = CompileManyRandomBalanced(depth, kNumFeatures, num_trees, &rnd);
  for (auto _ : state) {
    state.PauseTiming();
    std::vector<float> values;
    values.reserve(kNumFeatures);
    for (int i = 0; i < kNumFeatures; ++i) {
      values.push_back(absl::Uniform<uint8_t>(rnd) / 256.0);
    }
    state.ResumeTiming();
    auto x = eval.Predict(values);
    ::benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(state.iterations() * num_trees);
}

BENCHMARK(BM_LowLevel_EvaluationBoosted)
    ->ArgPair(3, 10)
    ->ArgPair(3, 20)
    ->ArgPair(3, 1000)
    ->ArgPair(6, 1000)
    ->ArgPair(9, 500)
    ->ArgPair(15, 1)
    ->ArgPair(15, 2)
    ->ArgPair(15, 10)
    ->ArgPair(15, 20)
    ->ArgPair(15, 50)
    ->ArgPair(20, 1)
    ->ArgPair(20, 2)
    ->ArgPair(20, 10)
    ->ArgPair(20, 20);

void BM_LowLevel_ProdBenchmarks(benchmark::State& state) {
  int depth = state.range(0);
  int num_trees = state.range(1);
  constexpr size_t kNumFeatures = 10;
  absl::BitGen rnd;
  auto eval = CompileManyRandomBalanced(depth, kNumFeatures, num_trees, &rnd);
  for (auto _ : state) {
    state.PauseTiming();
    std::vector<float> values;
    values.reserve(kNumFeatures);
    for (int i = 0; i < kNumFeatures; ++i) {
      values.push_back(absl::Uniform<uint8_t>(rnd) / 256.0);
    }
    state.ResumeTiming();
    auto x = eval.Predict(values);
    ::benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_LowLevel_ProdBenchmarks)
    ->ArgPair(5, 500)
    ->ArgPair(3, 100)
    ->ArgPair(3, 1000)
    ->ArgPair(1, 4000)
    ->ArgPair(10, 200)
    ->ArgPair(15, 30);

}  // namespace
}  // namespace arolla
