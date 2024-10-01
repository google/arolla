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
#include "arolla/qexpr/testing/benchmarks.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/base/attributes.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::testing {

// Builds a tree of operators on top of input_slots. If `shuffle` is true,
// shuffles inputs and operators on each tree level.
std::vector<std::unique_ptr<BoundOperator>> BuildOperatorTree(
    const std::vector<TypedSlot>& input_slots,
    const std::vector<TypedSlot>& common_slots, const QExprOperator& op,
    bool shuffle, FrameLayout::Builder* layout_builder) {
  CHECK(!input_slots.empty());
  QTypePtr input_type = input_slots[0].GetType();

  std::vector<TypedSlot> current_slots;
  current_slots.reserve(input_slots.size());
  for (const auto& slot : input_slots) {
    current_slots.push_back(slot);
  }

  std::mt19937 gen(1);
  std::vector<std::unique_ptr<BoundOperator>> operators;

  while (current_slots.size() != 1) {
    if (shuffle) {
      std::shuffle(current_slots.begin(), current_slots.end(), gen);
    }
    size_t previous_operators_size = operators.size();

    std::vector<TypedSlot> next_slots;
    next_slots.reserve(current_slots.size() / 2 + 1);
    for (size_t i = 0; i < current_slots.size(); i += 2) {
      if (i + 1 != current_slots.size()) {
        next_slots.push_back(AddSlot(input_type, layout_builder));
        auto operator_input_slots = common_slots;
        operator_input_slots.push_back(current_slots[i]);
        operator_input_slots.push_back(current_slots[i + 1]);
        operators.push_back(
            op.Bind(operator_input_slots, {next_slots.back()}).value());
      } else {
        next_slots.push_back(current_slots[i]);
      }
    }

    if (shuffle) {
      std::shuffle(operators.begin() + previous_operators_size, operators.end(),
                   gen);
    }

    current_slots = std::move(next_slots);
  }

  return operators;
}

namespace {

ABSL_ATTRIBUTE_NOINLINE
absl::Status NoInlineRunBoundOperators(
    absl::Span<const std::unique_ptr<BoundOperator>> ops,
    EvaluationContext* ctx, FramePtr frame) {
  RunBoundOperators(ops, ctx, frame);
  return ctx->status();
}

}  // namespace

void BenchmarkBinaryOperator(const QExprOperator& op, int num_inputs,
                             const TypedValue& input_value,
                             std::vector<TypedValue> common_inputs,
                             bool shuffle, benchmark::State& state,
                             bool use_arena) {
  FrameLayout::Builder layout_builder;

  std::vector<TypedSlot> common_slots;
  common_slots.reserve(common_inputs.size());
  for (const auto& ci : common_inputs) {
    common_slots.push_back(AddSlot(ci.GetType(), &layout_builder));
  }

  std::vector<TypedSlot> input_slots;
  input_slots.reserve(num_inputs);
  for (int i = 0; i < num_inputs; ++i) {
    input_slots.push_back(AddSlot(input_value.GetType(), &layout_builder));
  }

  auto bound_operators = BuildOperatorTree(input_slots, common_slots, op,
                                           shuffle, &layout_builder);

  FrameLayout layout = std::move(layout_builder).Build();

  UnsafeArenaBufferFactory arena(1024 * 1024 * 64);
  RootEvaluationContext root_ctx(&layout,
                                 use_arena ? &arena : GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  for (size_t i = 0; i < common_slots.size(); ++i) {
    CHECK_OK(common_inputs[i].CopyToSlot(common_slots[i], root_ctx.frame()));
  }
  for (const auto& slot : input_slots) {
    CHECK_OK(input_value.CopyToSlot(slot, root_ctx.frame()));
  }

  if (use_arena) {
    int64_t iter = 0;
    for (auto _ : state) {
      if (((++iter) & 0xff) == 0) {
        arena.Reset();
      }
      auto x =
          NoInlineRunBoundOperators(bound_operators, &ctx, root_ctx.frame());
      ::benchmark::DoNotOptimize(x);
    }
  } else {
    for (auto _ : state) {
      auto x =
          NoInlineRunBoundOperators(bound_operators, &ctx, root_ctx.frame());
      ::benchmark::DoNotOptimize(x);
    }
  }
}

void RunArrayBenchmark(benchmark::internal::Benchmark* b) {
  // Single element array, a typical array for serving, a big array.
  const std::array kArraySizes = {1, 100, 100 * 1024};
  // Single node expr, a typical NG3 expr, a typical advance project expr.
  const std::array kExprSizes = {2, 1024, 10 * 1024};

  for (size_t array_size : kArraySizes) {
    for (size_t expr_size : kExprSizes) {
      b->ArgPair(array_size, expr_size);
    }
  }
}

}  // namespace arolla::testing
