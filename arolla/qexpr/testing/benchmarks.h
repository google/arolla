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
#ifndef AROLLA_QEXPR_TESTING_BENCHMARKS_H_
#define AROLLA_QEXPR_TESTING_BENCHMARKS_H_

#include <vector>

#include "benchmark/benchmark.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::testing {

// Runs a benchmark on evaluation of a tree of binary operators.
//
// I.e.:
//   - Builds a tree of binary operators.
//   - Runs a benchmark on the tree evaluation.
//
// Args:
//  op: operator to benchmark
//  num_inputs: number of input leafs in the tree. There will be num_inputs - 1
//    nodes in the tree.
//  input_value: value to set the leafs to.
//  common_inputs: set of common inputs to all operators, will be passed in the
//    first slots.
//  shuffle: By default input slots are packed consequtively layer by layer. But
//    if shuffle is true they are shuffled within each layer.
//  state: benchchmark state.
//
void BenchmarkBinaryOperator(const QExprOperator& op, int num_inputs,
                             const TypedValue& input_value,
                             std::vector<TypedValue> common_inputs,
                             bool shuffle, benchmark::State& state,
                             bool use_arena = false);

// Set of presets for benchmarking tree of operators on arrays. The resulting
// benchmark will have two ranges: the first is number of elements in array and
// the second is number of inputs to the tree.
//
// Usage:
//   void BM_Something(benchmark::State& state) {
//     int array_size = state.range(0);
//     int num_inputs = state.range(1);
//     ...
//     state.SetItemsProcessed(
//         array_size * (num_inputs - 1) * state.iterations());
//   }
//   BENCHMARK(BM_Something)->Apply(&testing::RunArrayBenchmark);
//
void RunArrayBenchmark(benchmark::internal::Benchmark* b);

}  // namespace arolla::testing

#endif  // AROLLA_QEXPR_TESTING_BENCHMARKS_H_
