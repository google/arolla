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
#include <cstddef>
#include <memory>
#include <variant>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/input_loader.h"
#include "arolla/memory/optional_value.h"
#include "arolla/serving/expr_compiler.h"
#include "py/arolla/codegen/testing/float_benchmark_expr_1k_batch.h"

namespace arolla {
namespace {

template <class T>
std::unique_ptr<InputLoader<std::monostate>> CreateXYLoader(size_t batch_size) {
  return CreateAccessorsInputLoader<std::monostate>(
             "x",
             [=](auto in) {
               return CreateDenseArray<T>(std::vector<OptionalValue<T>>(
                   batch_size, static_cast<T>(3)));
             },  //
             "y",
             [=](auto in) {
               return CreateDenseArray<T>(std::vector<OptionalValue<T>>(
                   batch_size, static_cast<T>(7)));
             })
      .value();
}

template <class OutT, class GetCompiledFn, class CreateLoaderFn>
void RunInBatches(benchmark::State& state, GetCompiledFn get_compiled_fn,
                  CreateLoaderFn create_loader_fn, bool use_arena,
                  size_t batch_size, size_t op_count, OutT expected_value) {
  auto compiler =
      ExprCompiler<std::monostate, DenseArray<OutT>>().SetInputLoader(
          create_loader_fn(batch_size));
  if (use_arena) {
    compiler.SetExperimentalArenaAllocator();
  }
  auto executor = compiler.Compile(get_compiled_fn()).value();
  while (state.KeepRunningBatch(batch_size * op_count)) {
    auto value_or = executor({});
    CHECK_OK(value_or.status());
    CHECK_EQ(value_or.value()[0], expected_value);
  }
}

#define AROLLA_CODEGEN_BENCHMARK(name, OutT, get_compiled_fn,                  \
                                 create_loader_fn, op_count, expected_value)   \
  void BM_##name(benchmark::State& state) {                                    \
    RunInBatches<OutT>(state, get_compiled_fn, create_loader_fn,               \
                       /*use_arena=*/state.range(1), state.range(0), op_count, \
                       expected_value);                                        \
  }                                                                            \
  BENCHMARK(BM_##name)

// Actual benchmarks.

AROLLA_CODEGEN_BENCHMARK(AddDependantFloats, float,
                         ::test_namespace::GetCompiledFloat1KBatchBenchmark,
                         CreateXYLoader<float>, 1'000, 5010.0f)
    ->Args({10, true})
    ->Args({10, false})
    ->Args({20, true})
    ->Args({20, false})
    ->Args({50, true})
    ->Args({50, false})
    ->Args({100, true})
    ->Args({100, false})
    ->Args({1000, true})
    ->Args({1000, false});

}  // namespace
}  // namespace arolla
