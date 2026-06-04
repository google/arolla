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
#include "arolla/expr/eval/slot_allocator.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::expr::eval_internal {
namespace {

using ::testing::AllOf;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Ne;
using ::testing::UnorderedElementsAre;

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
  PostOrder post_order(x1_x1_x2_x3);
  SlotAllocator allocator(post_order, layout_builder, input_slots,
                          /*allow_reusing_leaves=*/false);

  TypedSlot zero_slot = allocator.AllocatePermanentSlot(
      *post_order.node_index(zero->fingerprint()), GetQType<float>());
  allocator.ReleaseSlotsNotNeededAfter(
      *post_order.node_index(zero->fingerprint()));

  TypedSlot x1_slot = allocator.AllocateSlot(
      *post_order.node_index(x1->fingerprint()), GetQType<float>());
  EXPECT_THAT(x1_slot, Ne(zero_slot));
  allocator.ReleaseSlotsNotNeededAfter(
      *post_order.node_index(x1->fingerprint()));
  EXPECT_THAT(allocator.ReusableSlots(), IsEmpty());

  TypedSlot x1_x1_slot = allocator.AllocateSlot(
      *post_order.node_index(x1_x1->fingerprint()), GetQType<float>());
  // Slot for `zero` is not needed anymore, but not reused.
  EXPECT_THAT(x1_x1_slot, AllOf(Ne(zero_slot), Ne(x1_slot)));
  allocator.ReleaseSlotsNotNeededAfter(
      *post_order.node_index(x1_x1->fingerprint()));
  EXPECT_THAT(allocator.ReusableSlots(), UnorderedElementsAre(x1_slot));

  // Assume x1_x1_x2 reuses output slot of x1_x1.
  allocator.InheritSlotFrom(*post_order.node_index(x1_x1_x2->fingerprint()),
                            *post_order.node_index(x1_x1->fingerprint()));
  allocator.ReleaseSlotsNotNeededAfter(
      *post_order.node_index(x1_x1_x2->fingerprint()));

  // x1_x1_slot is not released because still used by x1_x1_x2 expression.
  EXPECT_THAT(allocator.ReusableSlots(), UnorderedElementsAre(x1_slot));

  TypedSlot x1_x1_x2_x3_slot = allocator.AllocateSlot(
      *post_order.node_index(x1_x1_x2_x3->fingerprint()), GetQType<float>());
  // Slot for `x1` is not needed anymore and reused.
  EXPECT_THAT(x1_x1_x2_x3_slot, Eq(x1_slot));
  allocator.ReleaseSlotsNotNeededAfter(
      *post_order.node_index(x1_x1_x2_x3->fingerprint()));

  EXPECT_THAT(allocator.ReusableSlots(), UnorderedElementsAre(x1_x1_slot));
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
  PostOrder post_order(x1_x1_x2_x3);
  SlotAllocator allocator(post_order, layout_builder, input_slots,
                          /*allow_reusing_input_slots=*/true);

  TypedSlot zero_slot = allocator.AllocatePermanentSlot(
      *post_order.node_index(zero->fingerprint()), GetQType<float>());
  allocator.ReleaseSlotsNotNeededAfter(
      *post_order.node_index(zero->fingerprint()));

  TypedSlot x1_slot = allocator.AllocateSlot(
      *post_order.node_index(x1->fingerprint()), GetQType<float>());
  EXPECT_THAT(x1_slot, Ne(zero_slot));
  allocator.ReleaseSlotsNotNeededAfter(
      *post_order.node_index(x1->fingerprint()));

  EXPECT_THAT(allocator.ReusableSlots(), IsEmpty());

  TypedSlot x1_x1_slot = allocator.AllocateSlot(
      *post_order.node_index(x1_x1->fingerprint()), GetQType<float>());
  // Slot for `zero` is not needed anymore, but not reused. Slots for x1 and
  // L.x1 are released.
  EXPECT_THAT(x1_x1_slot, AllOf(Ne(zero_slot), Ne(x1_slot)));
  allocator.ReleaseSlotsNotNeededAfter(
      *post_order.node_index(x1_x1->fingerprint()));
  EXPECT_THAT(allocator.ReusableSlots(),
              UnorderedElementsAre(x1_slot, input_slots.at("x1")));

  // Assume x1_x1_x2 reuses output slot of x1_x1.
  allocator.InheritSlotFrom(*post_order.node_index(x1_x1_x2->fingerprint()),
                            *post_order.node_index(x1_x1->fingerprint()));

  allocator.ReleaseSlotsNotNeededAfter(
      *post_order.node_index(x1_x1_x2->fingerprint()));

  // x1_x1_slot is not released because still used by x1_x1_x2 expression. But
  // L.x2 slot got released.
  EXPECT_THAT(allocator.ReusableSlots(),
              UnorderedElementsAre(x1_slot, input_slots.at("x1"),
                                   input_slots.at("x2")));

  TypedSlot x1_x1_x2_x3_slot = allocator.AllocateSlot(
      *post_order.node_index(x1_x1_x2_x3->fingerprint()), GetQType<float>());
  // Slot for L.x2 is not needed anymore and reused.
  EXPECT_THAT(x1_x1_x2_x3_slot, Eq(input_slots.at("x2")));
  allocator.ReleaseSlotsNotNeededAfter(
      *post_order.node_index(x1_x1_x2_x3->fingerprint()));
  // x1_x1_slot and for L.x3 got released.
  EXPECT_THAT(allocator.ReusableSlots(),
              UnorderedElementsAre(x1_slot, input_slots.at("x1"), x1_x1_slot,
                                   input_slots.at("x3")));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
