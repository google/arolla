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
#include "arolla/qexpr/bound_operators.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::Eq;

template <typename T>
using Slot = FrameLayout::Slot<T>;

// Creates a bound operator to add a mixture of optional and non-optional
// floats. This exercises both the Functor and WhereAll bound operator
// classes.
absl::StatusOr<std::unique_ptr<BoundOperator>> CreateAddFloatsBoundOp(
    absl::Span<const TypedSlot> input_slots,
    Slot<OptionalValue<float>> output_slot) {
  std::vector<Slot<bool>> input_cond_slots;
  std::vector<Slot<float>> input_value_slots;
  for (const auto& typed_input_slot : input_slots) {
    QTypePtr input_type = typed_input_slot.GetType();
    if (IsOptionalQType(input_type)) {
      ASSIGN_OR_RETURN(auto input_slot,
                       typed_input_slot.ToSlot<OptionalValue<float>>());
      input_cond_slots.push_back(GetPresenceSubslotFromOptional(input_slot));
      input_value_slots.push_back(GetValueSubslotFromOptional(input_slot));
    } else {
      ASSIGN_OR_RETURN(auto value_slot, typed_input_slot.ToSlot<float>());
      input_value_slots.push_back(value_slot);
    }
  }

  Slot<bool> output_presence_slot = output_slot.GetSubslot<0>();
  Slot<float> output_value_slot = output_slot.GetSubslot<1>();

  // Create operator to add values. Should only be called if all optionals
  // are present.
  auto add_op =
      FunctorBoundOperator([input_value_slots, output_value_slot](
                               EvaluationContext* ctx, FramePtr frame) {
        float result = 0.0f;
        for (auto input_value_slot : input_value_slots) {
          result += frame.Get(input_value_slot);
        }
        frame.Set(output_value_slot, result);
      });

  return std::unique_ptr<BoundOperator>(new WhereAllBoundOperator(
      input_cond_slots, output_presence_slot, add_op));
}

TEST(BoundOperators, RunBoundOperators) {
  FrameLayout::Builder layout_builder;
  Slot<int32_t> x_slot = layout_builder.AddSlot<int32_t>();
  FrameLayout layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);

  ASSERT_THAT(alloc.frame().Get(x_slot), Eq(0));

  auto make_increment_operator = [x_slot](int32_t increment) {
    return MakeBoundOperator(
        [x_slot, increment](EvaluationContext* ctx, FramePtr frame) {
          frame.Set(x_slot, frame.Get(x_slot) + increment);
        });
  };

  std::vector<std::unique_ptr<BoundOperator>> bound_operators;
  bound_operators.push_back(make_increment_operator(1));
  bound_operators.push_back(make_increment_operator(10));
  bound_operators.push_back(make_increment_operator(100));

  EvaluationContext ctx;
  EXPECT_EQ(RunBoundOperators(bound_operators, &ctx, alloc.frame()), 2);
  EXPECT_THAT(alloc.frame().Get(x_slot), Eq(111));
  EXPECT_THAT(ctx.status(), IsOk());
}

TEST(BoundOperators, RunBoundOperators_WithError) {
  FrameLayout::Builder layout_builder;
  Slot<int32_t> x_slot = layout_builder.AddSlot<int32_t>();
  FrameLayout layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);

  ASSERT_THAT(alloc.frame().Get(x_slot), Eq(0));

  auto make_increment_operator = [x_slot](int32_t increment) {
    return MakeBoundOperator(
        [x_slot, increment](EvaluationContext* ctx, FramePtr frame) {
          frame.Set(x_slot, frame.Get(x_slot) + increment);
        });
  };

  std::vector<std::unique_ptr<BoundOperator>> bound_operators;
  bound_operators.push_back(make_increment_operator(1));
  bound_operators.push_back(make_increment_operator(10));
  bound_operators.push_back(
      MakeBoundOperator([](EvaluationContext* ctx, FramePtr frame) {
        ctx->set_status(absl::InvalidArgumentError("foo"));
      }));
  bound_operators.push_back(make_increment_operator(100));

  EvaluationContext ctx;
  EXPECT_EQ(RunBoundOperators(bound_operators, &ctx, alloc.frame()), 2);
  EXPECT_THAT(alloc.frame().Get(x_slot), Eq(11));
  EXPECT_THAT(ctx.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "foo"));
}

TEST(BoundOperators, RunBoundOperators_WithJump) {
  FrameLayout::Builder layout_builder;
  Slot<int32_t> x_slot = layout_builder.AddSlot<int32_t>();
  FrameLayout layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);

  ASSERT_THAT(alloc.frame().Get(x_slot), Eq(0));

  auto make_increment_operator = [x_slot](int32_t increment) {
    return MakeBoundOperator(
        [x_slot, increment](EvaluationContext* ctx, FramePtr frame) {
          frame.Set(x_slot, frame.Get(x_slot) + increment);
        });
  };

  std::vector<std::unique_ptr<BoundOperator>> bound_operators;
  bound_operators.push_back(make_increment_operator(1));
  bound_operators.push_back(JumpBoundOperator(1));
  bound_operators.push_back(make_increment_operator(10));
  bound_operators.push_back(make_increment_operator(100));

  EvaluationContext ctx;
  EXPECT_EQ(RunBoundOperators(bound_operators, &ctx, alloc.frame()), 3);
  EXPECT_THAT(alloc.frame().Get(x_slot), Eq(101));
  EXPECT_THAT(ctx.status(), IsOk());
}

TEST(BoundOperators, RunBoundOperators_Cancellation) {
  FrameLayout::Builder layout_builder;
  Slot<int> x_slot = layout_builder.AddSlot<int32_t>();
  FrameLayout layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);

  std::vector<std::unique_ptr<BoundOperator>> bound_operators;
  for (int i = 1; i <= 16; ++i) {
    bound_operators.push_back(MakeBoundOperator([x_slot, i](EvaluationContext*,
                                                            FramePtr frame) {
      frame.Set(x_slot, i);
      if (auto* cancellation_context =
              CancellationContext::ScopeGuard::current_cancellation_context()) {
        cancellation_context->Cancel();
      }
    }));
  }
  {  // Without cancellation scope.
    EvaluationContext ctx;
    EXPECT_EQ(RunBoundOperators(bound_operators, &ctx, alloc.frame()), 15);
    EXPECT_EQ(alloc.frame().Get(x_slot), 16);
  }
  {  // With cancellation scope.
    CancellationContext::ScopeGuard cancellation_scope;
    EvaluationContext ctx;
    EXPECT_EQ(RunBoundOperators(bound_operators, &ctx, alloc.frame()), 0);
    EXPECT_EQ(alloc.frame().Get(x_slot), 1);
    EXPECT_THAT(CheckCancellation(),
                StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  }
}

// Exercise WhereAllBoundOperator by adding mixture of optional and
// non-optional floats.
TEST(BoundOperators, WhereAll) {
  FrameLayout::Builder layout_builder;

  // mixture of optional and required inputs.
  auto input1 = layout_builder.AddSlot<OptionalValue<float>>();
  auto input2 = layout_builder.AddSlot<OptionalValue<float>>();
  auto input3 = layout_builder.AddSlot<float>();
  auto input4 = layout_builder.AddSlot<float>();

  // result is always optional
  auto result = layout_builder.AddSlot<OptionalValue<float>>();

  ASSERT_OK_AND_ASSIGN(
      auto op1, CreateAddFloatsBoundOp(
                    ToTypedSlots(input1, input2, input3, input4), result));

  FrameLayout layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);
  alloc.frame().Set(input1, 1.0f);
  alloc.frame().Set(input2, 10.0f);
  alloc.frame().Set(input3, 100.0f);
  alloc.frame().Set(input4, 1000.0f);

  EvaluationContext ctx;

  // All optional inputs are present.
  op1->Run(&ctx, alloc.frame());
  EXPECT_OK(ctx.status());
  EXPECT_EQ(alloc.frame().Get(result), OptionalValue<float>{1111.0f});

  // Some optional inputs are missing.
  alloc.frame().Set(input2, std::nullopt);
  alloc.frame().Set(result, 0.0f);
  op1->Run(&ctx, alloc.frame());
  EXPECT_OK(ctx.status());

  // Optional output is marked as not present.
  EXPECT_EQ(alloc.frame().Get(result), OptionalValue<float>{});

  // Verify that the add operator was not invoked; value is unchanged from
  // previous operation.
  EXPECT_EQ(alloc.frame().Get(result).value, 0.0f);
}

TEST(BoundOperators, MakeBoundOperator) {
  auto bound_op = MakeBoundOperator(
      [](arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
        return absl::InvalidArgumentError("test error");
      });
  arolla::EvaluationContext ctx;
  arolla::FrameLayout memory_layout = arolla::FrameLayout::Builder().Build();
  arolla::MemoryAllocation alloc(&memory_layout);

  bound_op->Run(&ctx, alloc.frame());

  EXPECT_THAT(ctx.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "test error"));
}

}  // namespace
}  // namespace arolla
