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
#include "arolla/decision_forest/pointwise_evaluation/bound_split_conditions.h"

#include <cmath>
#include <cstdint>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/bytes.h"

namespace arolla::testing {
namespace {

TEST(BoundConditions, IntervalSplitCondition) {
  auto interval_split = IntervalSplit(0, 2, 3);

  FrameLayout::Builder bldr;
  auto slot = bldr.AddSlot<OptionalValue<float>>();

  auto layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);
  FramePtr context = alloc.frame();

  using BoundCondition =
      VariantBoundCondition<IntervalBoundCondition,
                            SetOfValuesBoundCondition<int64_t>>;

  std::vector<TypedSlot> typed_slots = {TypedSlot::FromSlot(slot)};
  ASSERT_OK_AND_ASSIGN(BoundCondition bound_interval,
                       BoundCondition::Create(interval_split, typed_slots));

  context.Set(slot, 3.5);
  EXPECT_EQ(bound_interval(context), false);

  context.Set(slot, NAN);
  EXPECT_EQ(bound_interval(context), false);

  context.Set(slot, 2.5);
  EXPECT_EQ(bound_interval(context), true);

  context.Set(slot, {});
  EXPECT_EQ(bound_interval(context), false);
}

TEST(BoundConditions, SetOfValuesSplitCondition) {
  auto set_of_values = SetOfValuesSplit<int64_t>(0, {2, 4, 3}, true);

  FrameLayout::Builder bldr;
  auto slot = bldr.AddSlot<OptionalValue<int64_t>>();
  std::vector<TypedSlot> typed_slots = {TypedSlot::FromSlot(slot)};

  auto layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);
  FramePtr context = alloc.frame();

  using BoundCondition =
      VariantBoundCondition<IntervalBoundCondition,
                            SetOfValuesBoundCondition<int64_t>>;

  ASSERT_OK_AND_ASSIGN(BoundCondition bound_set_of_values,
                       BoundCondition::Create(set_of_values, typed_slots));

  context.Set(slot, 3);
  EXPECT_EQ(bound_set_of_values(context), true);

  context.Set(slot, 5);
  EXPECT_EQ(bound_set_of_values(context), false);

  context.Set(slot, {});
  EXPECT_EQ(bound_set_of_values(context), true);
}

TEST(BoundConditions, VirtualBoundCondition) {
  auto set_of_values =
      SetOfValuesSplit<Bytes>(0, {Bytes("A"), Bytes("B"), Bytes("C")}, true);

  FrameLayout::Builder bldr;
  auto slot = bldr.AddSlot<OptionalValue<Bytes>>();
  std::vector<TypedSlot> typed_slots = {TypedSlot::FromSlot(slot)};

  auto layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);
  FramePtr context = alloc.frame();

  using BoundCondition =
      VariantBoundCondition<IntervalBoundCondition,
                            SetOfValuesBoundCondition<int64_t>,
                            VirtualBoundCondition>;

  ASSERT_OK_AND_ASSIGN(BoundCondition bound_set_of_values,
                       BoundCondition::Create(set_of_values, typed_slots));

  context.Set(slot, Bytes("B"));
  EXPECT_EQ(bound_set_of_values(context), true);

  context.Set(slot, Bytes("D"));
  EXPECT_EQ(bound_set_of_values(context), false);

  context.Set(slot, {});
  EXPECT_EQ(bound_set_of_values(context), true);
}

}  // namespace
}  // namespace arolla::testing
