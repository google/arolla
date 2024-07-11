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
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/strings/str_format.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr {
namespace {

void BM_AddN_Create(benchmark::State& state) {
  InitArolla();
  int64_t summand_count = state.range(0);
  DCHECK_GE(summand_count, 1);

  std::vector<std::string> leaf_names;
  leaf_names.reserve(summand_count);
  for (int64_t i = 0; i < summand_count; ++i) {
    leaf_names.push_back(absl::StrFormat("v%d", i));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(leaf_names);
    ExprNodePtr expr = Leaf(leaf_names[0]);
    for (int64_t i = 1; i < summand_count; ++i) {
      expr = *CallOp("math.add", {std::move(expr), Leaf(leaf_names[i])});
    }
    benchmark::DoNotOptimize(expr);
  }
}

BENCHMARK(BM_AddN_Create)->Range(1, 10000);

}  // namespace
}  // namespace arolla::expr
