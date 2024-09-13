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
#include "arolla/expr/eval/slot_allocator.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Ne;

TEST(SlotAllocatorTest, CompilerWorkflow) {
  auto zero = Literal(0.0f);
  ASSERT_OK_AND_ASSIGN(auto x1, CallOp("math.add", {zero, Leaf("x1")}));
  ASSERT_OK_AND_ASSIGN(auto x1_x1, CallOp("math.add", {x1, Leaf("x1")}));
  ASSERT_OK_AND_ASSIGN(auto x1_x1_x2, CallOp("math.add", {x1_x1, Leaf("x2")}));
  ASSERT_OK_AND_ASSIGN(auto x1_x1_x2_x3,
                       CallOp("math.add", {x1_x1_x2, Leaf("x3")}));
  FrameLayout::Builder layout_builder;
  absl::flat_hash_map<std::string, TypedSlot> input_slots{
      {"x1", TypedSlot::FromSlot(layout_builder.AddSlot<float>())},
      {"x2", TypedSlot::FromSlot(layout_builder.AddSlot<float>())},
      {"x3", TypedSlot::FromSlot(layout_builder.AddSlot<float>())},
  };
  SlotAllocator allocator(x1_x1_x2_x3, layout_builder, input_slots,
                          /*allow_reusing_leaves=*/false);

  TypedSlot zero_slot = allocator.AddSlotForNode(zero, GetQType<float>(),
                                                 /*allow_recycled=*/false);
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(zero), IsOk());

  TypedSlot x1_slot =
      allocator.AddSlotForNode(x1, GetQType<float>(), /*allow_recycled=*/true);
  EXPECT_THAT(x1_slot, Ne(zero_slot));
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(x1), IsOk());
  EXPECT_THAT(allocator.ReusableSlots(), IsEmpty());

  TypedSlot x1_x1_slot = allocator.AddSlotForNode(x1_x1, GetQType<float>(),
                                                  /*allow_recycled=*/true);
  // Slot for `zero` is not needed anymore, but not reused.
  EXPECT_THAT(x1_x1_slot, AllOf(Ne(zero_slot), Ne(x1_slot)));
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(x1_x1), IsOk());
  EXPECT_THAT(allocator.ReusableSlots(), ElementsAre(x1_slot));

  // Assume x1_x1_x2 reuses output slot of x1_x1.
  EXPECT_THAT(allocator.ExtendSlotLifetime(x1_x1, x1_x1_x2), IsOk());
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(x1_x1_x2), IsOk());
  // x1_x1_slot is not released because still used by x1_x1_x2 expression.
  EXPECT_THAT(allocator.ReusableSlots(), ElementsAre(x1_slot));

  TypedSlot x1_x1_x2_x3_slot = allocator.AddSlotForNode(
      x1_x1_x2_x3, GetQType<float>(), /*allow_recycled=*/true);
  // Slot for `x1` is not needed anymore and reused.
  EXPECT_THAT(x1_x1_x2_x3_slot, Eq(x1_slot));
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(x1_x1_x2_x3), IsOk());
  EXPECT_THAT(allocator.ReusableSlots(), ElementsAre(x1_x1_slot));

  // NOTE: These operations leave allocator in inconsistent state so placed at
  // the end of the test.
  // Cannot extend lifetime for already released slot.
  EXPECT_THAT(allocator.ExtendSlotLifetime(x1, x1_x1_x2),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("missing last usage for node")));
  // Cannot release slot twice.
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(x1_x1),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("missing last usage for node")));
}

TEST(SlotAllocatorTest, CompilerWorkflowWithReusedLeaves) {
  auto zero = Literal(0.0f);
  ASSERT_OK_AND_ASSIGN(auto x1, CallOp("math.add", {zero, Leaf("x1")}));
  ASSERT_OK_AND_ASSIGN(auto x1_x1, CallOp("math.add", {x1, Leaf("x1")}));
  ASSERT_OK_AND_ASSIGN(auto x1_x1_x2, CallOp("math.add", {x1_x1, Leaf("x2")}));
  ASSERT_OK_AND_ASSIGN(auto x1_x1_x2_x3,
                       CallOp("math.add", {x1_x1_x2, Leaf("x3")}));
  FrameLayout::Builder layout_builder;
  absl::flat_hash_map<std::string, TypedSlot> input_slots{
      {"x1", TypedSlot::FromSlot(layout_builder.AddSlot<float>())},
      {"x2", TypedSlot::FromSlot(layout_builder.AddSlot<float>())},
      {"x3", TypedSlot::FromSlot(layout_builder.AddSlot<float>())},
  };
  SlotAllocator allocator(x1_x1_x2_x3, layout_builder, input_slots,
                          /*allow_reusing_leaves=*/true);

  TypedSlot zero_slot = allocator.AddSlotForNode(zero, GetQType<float>(),
                                                 /*allow_recycled=*/false);
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(zero), IsOk());

  TypedSlot x1_slot =
      allocator.AddSlotForNode(x1, GetQType<float>(), /*allow_recycled=*/true);
  EXPECT_THAT(x1_slot, Ne(zero_slot));
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(x1), IsOk());
  EXPECT_THAT(allocator.ReusableSlots(), IsEmpty());

  TypedSlot x1_x1_slot = allocator.AddSlotForNode(x1_x1, GetQType<float>(),
                                                  /*allow_recycled=*/true);
  // Slot for `zero` is not needed anymore, but not reused. Slots for x1 and
  // L.x1 are released.
  EXPECT_THAT(x1_x1_slot, AllOf(Ne(zero_slot), Ne(x1_slot)));
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(x1_x1), IsOk());
  EXPECT_THAT(allocator.ReusableSlots(),
              ElementsAre(x1_slot, input_slots.at("x1")));

  // Assume x1_x1_x2 reuses output slot of x1_x1.
  EXPECT_THAT(allocator.ExtendSlotLifetime(x1_x1, x1_x1_x2), IsOk());
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(x1_x1_x2), IsOk());
  // x1_x1_slot is not released because still used by x1_x1_x2 expression. But
  // L.x2 slot got released.
  EXPECT_THAT(allocator.ReusableSlots(),
              ElementsAre(x1_slot, input_slots.at("x1"), input_slots.at("x2")));

  TypedSlot x1_x1_x2_x3_slot = allocator.AddSlotForNode(
      x1_x1_x2_x3, GetQType<float>(), /*allow_recycled=*/true);
  // Slot for L.x2 is not needed anymore and reused.
  EXPECT_THAT(x1_x1_x2_x3_slot, Eq(input_slots.at("x2")));
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(x1_x1_x2_x3), IsOk());
  // x1_x1_slot and for L.x3 got released.
  EXPECT_THAT(allocator.ReusableSlots(),
              ElementsAre(x1_slot, input_slots.at("x1"), x1_x1_slot,
                          input_slots.at("x3")));

  // NOTE: These operations leave allocator in inconsistent state so placed at
  // the end of the test.
  // Cannot extend lifetime for already released slot.
  EXPECT_THAT(allocator.ExtendSlotLifetime(x1, x1_x1_x2),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("missing last usage for node")));
  // Cannot release slot twice.
  EXPECT_THAT(allocator.ReleaseSlotsNotNeededAfter(x1_x1),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("missing last usage for node")));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
