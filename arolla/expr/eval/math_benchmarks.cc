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
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/dense_array/testing/util.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/default/default_optimizer.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr {

void BM_AddN(benchmark::State& state, bool balanced) {
  InitArolla();
  int64_t item_count = state.range(0);
  int64_t summand_count = state.range(1);
  DCHECK_GE(summand_count, 1);

  std::vector<std::string> leaf_names;
  FrameLayout::Builder layout_builder;
  absl::flat_hash_map<std::string, TypedSlot> input_slots;
  leaf_names.reserve(summand_count);
  for (int64_t i = 0; i < summand_count; ++i) {
    leaf_names.push_back(absl::StrFormat("v%d", i));
    input_slots.emplace(
        leaf_names[i],
        TypedSlot::FromSlot(layout_builder.AddSlot<DenseArray<float>>()));
  }

  ExprNodePtr expr;
  if (balanced) {
    std::vector<ExprNodePtr> nodes;
    nodes.reserve(summand_count);
    for (const std::string& name : leaf_names) {
      nodes.push_back(Leaf(name));
    }
    while (nodes.size() > 1) {
      for (int64_t i = 0; i < nodes.size() / 2; ++i) {
        nodes[i] = *CallOp("math.add", {nodes[i * 2], nodes[i * 2 + 1]});
      }
      if (nodes.size() % 2 == 1) {
        nodes[nodes.size() / 2] = nodes.back();
      }
      nodes.resize((nodes.size() + 1) / 2);
    }
    expr = nodes[0];
  } else {
    expr = Leaf(leaf_names[0]);
    for (int64_t i = 1; i < summand_count; ++i) {
      expr = *CallOp("math.add", {std::move(expr), Leaf(leaf_names[i])});
    }
  }

  DynamicEvaluationEngineOptions options{.optimizer = *DefaultOptimizer()};
  auto executable_expr = *CompileAndBindForDynamicEvaluation(
      options, &layout_builder, expr, input_slots);

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  CHECK_OK(executable_expr->InitializeLiterals(&ctx));
  absl::BitGen gen;
  for (const auto& [_, typed_slot] : input_slots) {
    FrameLayout::Slot<DenseArray<float>> slot =
        typed_slot.UnsafeToSlot<DenseArray<float>>();
    ctx.Set(slot, testing::RandomDenseArray<float>(item_count, false, 0, gen));
  }
  auto output_slot =
      executable_expr->output_slot().UnsafeToSlot<DenseArray<float>>();

  while (state.KeepRunningBatch(item_count * (summand_count - 1))) {
    CHECK_OK(executable_expr->Execute(&ctx));
    auto x = ctx.Get(output_slot);
    ::benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(state.iterations());
}

void BM_AddN_LinearExpr(benchmark::State& state) { BM_AddN(state, false); }

void BM_AddN_BalancedExpr(benchmark::State& state) { BM_AddN(state, true); }

BENCHMARK(BM_AddN_LinearExpr)
    ->ArgPair(10, 2)
    ->ArgPair(10, 3)
    ->ArgPair(10, 4)
    ->ArgPair(10, 8)
    ->ArgPair(10, 57)
    ->ArgPair(10, 64)
    ->ArgPair(10, 128)
    ->ArgPair(32, 2)
    ->ArgPair(32, 3)
    ->ArgPair(32, 4)
    ->ArgPair(32, 8)
    ->ArgPair(32, 57)
    ->ArgPair(32, 64)
    ->ArgPair(32, 128)
    ->ArgPair(320, 2)
    ->ArgPair(320, 3)
    ->ArgPair(320, 4)
    ->ArgPair(320, 8)
    ->ArgPair(320, 57)
    ->ArgPair(320, 64)
    ->ArgPair(320, 128);

BENCHMARK(BM_AddN_BalancedExpr)
    ->ArgPair(10, 2)
    ->ArgPair(10, 3)
    ->ArgPair(10, 4)
    ->ArgPair(10, 8)
    ->ArgPair(10, 57)
    ->ArgPair(10, 64)
    ->ArgPair(10, 128)
    ->ArgPair(32, 2)
    ->ArgPair(32, 3)
    ->ArgPair(32, 4)
    ->ArgPair(32, 8)
    ->ArgPair(32, 57)
    ->ArgPair(32, 64)
    ->ArgPair(32, 128)
    ->ArgPair(320, 2)
    ->ArgPair(320, 3)
    ->ArgPair(320, 4)
    ->ArgPair(320, 8)
    ->ArgPair(320, 57)
    ->ArgPair(320, 64)
    ->ArgPair(320, 128);

}  // namespace arolla::expr
