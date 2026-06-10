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
#include <cstddef>
#include <cstdint>
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/optimization/default/default_optimizer.h"
#include "arolla/io/wildcard_input_loader.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/init_arolla.h"

namespace {

using ::arolla::EvaluationOptions;
using ::arolla::InitArolla;
using ::arolla::WildcardInputLoader;
using ::arolla::expr::CallOp;
using ::arolla::expr::CompileModelExecutor;
using ::arolla::expr::DefaultOptimizer;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::ModelExecutorOptions;

constexpr int N = 10'000;

void Add_F32_F32_NTimes(int N, bool enable_expr_stack_trace,
                        benchmark::State& state) {
  InitArolla();
  auto leaf = Leaf("x");
  auto expr = Literal<float>(0.0f);
  for (int i = 0; i < N; ++i) {
    expr = *CallOp("math.add", {expr, leaf});
  }
  auto accessor = [](float x, absl::string_view) { return x; };
  for (auto _ : state) {
    auto input_loader = *WildcardInputLoader<float>::Build(accessor);
    auto model_executor = *CompileModelExecutor<float>(
        expr, *input_loader,
        ModelExecutorOptions{.eval_options = {.enable_expr_stack_trace =
                                                  enable_expr_stack_trace}});
    auto y = *model_executor.ExecuteOnStack<128>(EvaluationOptions{}, 1.0f);
    benchmark::DoNotOptimize(y);
  }
}

void BM_Add_F32_F32_NTimes_WithoutStacktrace(benchmark::State& state) {
  Add_F32_F32_NTimes(N, false, state);
}

void BM_Add_F32_F32_NTimes_WithStacktrace(benchmark::State& state) {
  Add_F32_F32_NTimes(N, true, state);
}

BENCHMARK(BM_Add_F32_F32_NTimes_WithStacktrace);
BENCHMARK(BM_Add_F32_F32_NTimes_WithoutStacktrace);

void Add_F64_F32_NTimes(int N, bool enable_expr_stack_trace,
                        benchmark::State& state) {
  InitArolla();
  auto leaf = Leaf("x");
  auto expr = Literal<double>(0.0f);
  for (int i = 0; i < N; ++i) {
    expr = *CallOp("math.add", {expr, leaf});
  }
  auto accessor = [](float x, absl::string_view) { return x; };
  for (auto _ : state) {
    auto input_loader = *WildcardInputLoader<float>::Build(accessor);
    auto model_executor = *CompileModelExecutor<double>(
        expr, *input_loader,
        ModelExecutorOptions{.eval_options = {.enable_expr_stack_trace =
                                                  enable_expr_stack_trace}});
    auto y = *model_executor.ExecuteOnStack<128>(EvaluationOptions{}, 1.0f);
    benchmark::DoNotOptimize(y);
  }
}

void BM_Add_F64_F32_NTimes_WithoutStacktrace(benchmark::State& state) {
  Add_F64_F32_NTimes(N, false, state);
}

void BM_Add_F64_F32_NTimes_WithStacktrace(benchmark::State& state) {
  Add_F64_F32_NTimes(N, true, state);
}

BENCHMARK(BM_Add_F64_F32_NTimes_WithStacktrace);
BENCHMARK(BM_Add_F64_F32_NTimes_WithoutStacktrace);

template <size_t N>
void BM_Fib(benchmark::State& state) {
  InitArolla();
  auto f0_expr = Literal<int64_t>(0);
  auto f1_expr = Leaf("x");
  for (int i = 2; i < N; ++i) {
    auto f2_expr = *CallOp("math.add", {f0_expr, f1_expr});
    f0_expr = std::move(f1_expr);
    f1_expr = std::move(f2_expr);
  }
  auto expr = std::move(f1_expr);
  auto input_loader = *WildcardInputLoader<int64_t>::Build(
      [](int64_t x, absl::string_view) { return x; });
  ModelExecutorOptions options;
  options.eval_options.enable_expr_stack_trace = true;
  options.eval_options.optimizer = *DefaultOptimizer();
  for (auto _ : state) {
    auto model_executor =
        *CompileModelExecutor<int64_t>(expr, *input_loader, options);
    auto y = *model_executor.Execute(1);
    benchmark::DoNotOptimize(y);
  }
}

BENCHMARK_TEMPLATE(BM_Fib, 10);
BENCHMARK_TEMPLATE(BM_Fib, 100);

}  // namespace
