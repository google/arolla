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
#include "arolla/expr/eval/executable_builder.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/eval/test_utils.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::Eq;
using ::testing::HasSubstr;

class ExecutableBuilderTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

std::unique_ptr<BoundOperator> Noop() {
  return MakeBoundOperator([](EvaluationContext* ctx, FramePtr frame) {});
}

TEST_F(ExecutableBuilderTest, SetEvalOp) {
  FrameLayout::Builder layout_builder;
  auto output_slot = layout_builder.AddSlot<float>();
  ExecutableBuilder builder(&layout_builder, /*collect_op_descriptions=*/true);

  EXPECT_THAT(builder.SetEvalOp(0, Noop(), "noop", "noop"),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("illegal operator offset")));
  builder.SkipEvalOp();
  ASSERT_THAT(builder.SetEvalOp(0, Noop(), "noop", "noop"), IsOk());

  EXPECT_THAT(
      builder.SetEvalOp(0, Noop(), "noop", "noop"),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr("attempt to override existing operator at position 0")));

  EXPECT_THAT(std::move(builder).Build({}, TypedSlot::FromSlot(output_slot)),
              AllOf(InitOperationsAre(), EvalOperationsAre("noop")));
}

TEST_F(ExecutableBuilderTest, BindInitializeLiteralOp) {
  FrameLayout::Builder layout_builder;
  auto float_slot = layout_builder.AddSlot<float>();
  auto optional_int_slot = layout_builder.AddSlot<OptionalValue<int32_t>>();
  ExecutableBuilder builder(&layout_builder, /*collect_op_descriptions=*/true);

  EXPECT_THAT(
      builder.AddLiteralInitialization(TypedValue::FromValue(float{57.}),
                                       TypedSlot::FromSlot(float_slot)),
      IsOk());
  EXPECT_THAT(
      builder.AddLiteralInitialization(TypedValue::FromValue(int32_t{57}),
                                       TypedSlot::FromSlot(optional_int_slot)),
      StatusIs(absl::StatusCode::kInternal,
               "incompatible types for literal and its slot: INT32 vs "
               "OPTIONAL_INT32"));
  EXPECT_THAT(builder.AddLiteralInitialization(
                  TypedValue::FromValue(OptionalValue<int32_t>(57)),
                  TypedSlot::FromSlot(optional_int_slot)),
              IsOk());
  auto bound_expr =
      std::move(builder).Build({}, TypedSlot::FromSlot(float_slot));
  EXPECT_THAT(
      bound_expr,
      AllOf(InitOperationsAre("FLOAT32 [0x00] = 57.\n"
                              "OPTIONAL_INT32 [0x04] = optional_int32{57}"),
            EvalOperationsAre()));

  auto layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);
  EvaluationContext ctx;
  bound_expr->InitializeLiterals(&ctx, alloc.frame());
  EXPECT_THAT(alloc.frame().Get(float_slot), Eq(57.));
  EXPECT_THAT(alloc.frame().Get(optional_int_slot), Eq(57));
}

TEST_F(ExecutableBuilderTest, ExecuteOk) {
  FrameLayout::Builder layout_builder;
  FrameLayout::Slot<int32_t> x_slot = layout_builder.AddSlot<int32_t>();

  auto make_increment_operator = [x_slot](int32_t increment) {
    return MakeBoundOperator(
        [x_slot, increment](EvaluationContext* ctx, FramePtr frame) {
          frame.Set(x_slot, frame.Get(x_slot) + increment);
        });
  };

  ExecutableBuilder builder(&layout_builder, /*collect_op_descriptions=*/true);
  builder.AddEvalOp(make_increment_operator(1), "inc(1)", "inc(1)");
  builder.AddEvalOp(make_increment_operator(10), "inc(10)", "inc(10)");
  builder.AddEvalOp(make_increment_operator(100), "inc(100)", "inc(100)");
  builder.AddEvalOp(make_increment_operator(1000), "inc(1000)", "inc(1000)");

  auto dynamic_bound_expr =
      std::move(builder).Build({}, TypedSlot::FromSlot(x_slot));
  FrameLayout layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);
  EvaluationContext ctx;
  dynamic_bound_expr->Execute(&ctx, alloc.frame());

  EXPECT_OK(ctx.status());
  EXPECT_THAT(alloc.frame().Get(x_slot), Eq(1111));
}

TEST_F(ExecutableBuilderTest, ExecuteWithError) {
  FrameLayout::Builder layout_builder;
  FrameLayout::Slot<int32_t> x_slot = layout_builder.AddSlot<int32_t>();

  auto make_increment_operator = [x_slot](int32_t increment) {
    return MakeBoundOperator(
        [x_slot, increment](EvaluationContext* ctx, FramePtr frame) {
          frame.Set(x_slot, frame.Get(x_slot) + increment);
        });
  };

  ExecutableBuilder builder(&layout_builder, /*collect_op_descriptions=*/true);
  builder.AddEvalOp(make_increment_operator(1), "inc(1)", "inc(1)");
  builder.AddEvalOp(make_increment_operator(10), "inc(10)", "inc(10)");
  builder.AddEvalOp(make_increment_operator(100), "inc(100)", "inc(100)");
  builder.AddEvalOp(
      MakeBoundOperator([](EvaluationContext* ctx, FramePtr frame) {
        ctx->set_status(absl::InvalidArgumentError("foo"));
      }),
      "error_operator", "error_operator");
  builder.AddEvalOp(make_increment_operator(1000), "inc(1000)", "inc(1000)");

  auto dynamic_bound_expr =
      std::move(builder).Build({}, TypedSlot::FromSlot(x_slot));
  FrameLayout layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);
  EvaluationContext ctx;
  dynamic_bound_expr->Execute(&ctx, alloc.frame());

  EXPECT_THAT(
      ctx.status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("foo; during evaluation of operator error_operator")));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
