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
#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/io/wildcard_input_loader.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/init_arolla.h"

namespace {

using ::arolla::InitArolla;
using ::arolla::WildcardInputLoader;
using ::arolla::expr::CallOp;
using ::arolla::expr::CompileModelExecutor;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::ModelEvaluationOptions;
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
    auto y =
        *model_executor.ExecuteOnStack<128>(ModelEvaluationOptions{}, 1.0f);
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
    auto y =
        *model_executor.ExecuteOnStack<128>(ModelEvaluationOptions{}, 1.0f);
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

}  // namespace
