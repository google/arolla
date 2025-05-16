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

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/input_loader.h"
#include "arolla/memory/optional_value.h"
#include "arolla/serving/expr_compiler.h"
#include "py/arolla/codegen/testing/scalars/double_benchmark_expr_10k.h"
#include "py/arolla/codegen/testing/scalars/double_two_parallel_benchmark_expr_1k.h"
#include "py/arolla/codegen/testing/scalars/float_benchmark_expr_10k.h"
#include "py/arolla/codegen/testing/scalars/optional_float_benchmark_expr_1k.h"

namespace arolla {
namespace {

template <class T>
std::unique_ptr<InputLoader<std::monostate>> CreateXYLoader() {
  return CreateAccessorsInputLoader<std::monostate>(
             "x", [](auto in) { return static_cast<T>(3); },  //
             "y", [](auto in) { return static_cast<T>(7); })
      .value();
}

template <class OutT, class GetCompiledFn, class CreateLoaderFn>
void RunInBatches(benchmark::State& state, GetCompiledFn get_compiled_fn,
                  CreateLoaderFn create_loader_fn, size_t batch_size,
                  OutT expected_value) {
  auto executor = ExprCompiler<std::monostate, OutT>()
                      .SetInputLoader(create_loader_fn())
                      .Compile(get_compiled_fn())
                      .value();
  while (state.KeepRunningBatch(batch_size)) {
    auto value_or = executor({});
    CHECK_OK(value_or.status());
    CHECK_EQ(value_or.value(), expected_value);
  }
}

#define AROLLA_CODEGEN_BENCHMARK(name, OutT, get_compiled_fn,                  \
                                 create_loader_fn, batch_size, expected_value) \
  void BM_##name(benchmark::State& state) {                                    \
    RunInBatches<OutT>(state, get_compiled_fn, create_loader_fn, batch_size,   \
                       expected_value);                                        \
  }                                                                            \
  BENCHMARK(BM_##name);

// Actual benchmarks.

AROLLA_CODEGEN_BENCHMARK(AddDependantFloats, float,
                         ::test_namespace::GetCompiledFloat10KBenchmark,
                         CreateXYLoader<float>, 10000, 50010.0f);

AROLLA_CODEGEN_BENCHMARK(AddDependantOptionalFloats, OptionalValue<float>,
                         ::test_namespace::GetCompiledOptionalFloat1KBenchmark,
                         CreateXYLoader<OptionalValue<float>>, 1000, 5010.0f);

AROLLA_CODEGEN_BENCHMARK(AddDependantDoubles, double,
                         ::test_namespace::GetCompiledDouble10KBenchmark,
                         CreateXYLoader<double>, 10000, 50010.0);

AROLLA_CODEGEN_BENCHMARK(
    AddTwoParallelComputeDoubles, double,
    ::test_namespace::GetCompiledDoubleTwoParallelComputes1KBenchmark,
    CreateXYLoader<double>, 1000, 0.0);

}  // namespace
}  // namespace arolla
