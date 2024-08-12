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
#include "arolla/qtype/optional_qtype.h"

#include <cstdint>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::TypedValueWith;
using ::testing::FloatEq;
using ::testing::IsFalse;
using ::testing::IsTrue;

template <typename T>
using Slot = FrameLayout::Slot<T>;

TEST(OptionalQType, SplitOptionalUnitSlot) {
  FrameLayout::Builder layout_builder;
  auto slot = layout_builder.AddSlot<OptionalUnit>();
  auto layout = std::move(layout_builder).Build();

  // from Slot<OptionalUnit>.
  Slot<bool> presence_slot1 = GetPresenceSubslotFromOptional(slot);

  // from TypedSlot
  auto typed_slot = TypedSlot::FromSlot(slot);
  ASSERT_OK_AND_ASSIGN(Slot<bool> presence_slot2,
                       GetPresenceSubslotFromOptional(typed_slot));
  Slot<bool> presence_slot3 = UnsafePresenceSubslotFromOptional(typed_slot);
  EXPECT_THAT(GetValueSubslotFromOptional(typed_slot),
              StatusIs(absl::StatusCode::kInvalidArgument));

  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  frame.Set(slot, kPresent);
  EXPECT_TRUE(frame.Get(presence_slot1));
  EXPECT_TRUE(frame.Get(presence_slot2));
  EXPECT_TRUE(frame.Get(presence_slot3));

  frame.Set(slot, kMissing);
  EXPECT_FALSE(frame.Get(presence_slot1));
  EXPECT_FALSE(frame.Get(presence_slot2));
  EXPECT_FALSE(frame.Get(presence_slot3));
}

TEST(OptionalQType, SplitOptionalFloatSlot) {
  FrameLayout::Builder layout_builder;
  auto slot = layout_builder.AddSlot<OptionalValue<float>>();
  auto layout = std::move(layout_builder).Build();

  // from Slot<OptionalValue<float>>.
  Slot<bool> presence_slot1 = GetPresenceSubslotFromOptional(slot);
  Slot<float> value_slot1 = GetValueSubslotFromOptional(slot);

  // from TypedSlot
  auto typed_slot = TypedSlot::FromSlot(slot);
  ASSERT_OK_AND_ASSIGN(Slot<bool> presence_slot2,
                       GetPresenceSubslotFromOptional(typed_slot));
  ASSERT_OK_AND_ASSIGN(TypedSlot typed_value_slot2,
                       GetValueSubslotFromOptional(typed_slot));
  ASSERT_OK_AND_ASSIGN(Slot<float> value_slot2,
                       typed_value_slot2.ToSlot<float>());
  Slot<bool> presence_slot3 = UnsafePresenceSubslotFromOptional(typed_slot);
  Slot<float> value_slot3 =
      UnsafeValueSubslotFromOptional(typed_slot).UnsafeToSlot<float>();

  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  frame.Set(slot, 17.5f);
  EXPECT_TRUE(frame.Get(presence_slot1));
  EXPECT_TRUE(frame.Get(presence_slot2));
  EXPECT_TRUE(frame.Get(presence_slot3));
  EXPECT_THAT(frame.Get(value_slot1), FloatEq(17.5f));
  EXPECT_THAT(frame.Get(value_slot2), FloatEq(17.5f));
  EXPECT_THAT(frame.Get(value_slot3), FloatEq(17.5f));

  frame.Set(slot, std::nullopt);
  EXPECT_FALSE(frame.Get(presence_slot1));
  EXPECT_FALSE(frame.Get(presence_slot2));
  EXPECT_FALSE(frame.Get(presence_slot3));
}

TEST(OptionalQType, CreateMissingValue) {
  EXPECT_THAT(
      CreateMissingValue(GetOptionalQType<int64_t>()),
      IsOkAndHolds(TypedValueWith<OptionalValue<int64_t>>(std::nullopt)));
  EXPECT_THAT(
      CreateMissingValue(GetQType<int64_t>()),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot create a missing value for non-optional qtype `INT64`"));
}

TEST(OptionalQType, UnsafeIsPresent) {
  EXPECT_THAT(UnsafeIsPresent(TypedRef::FromValue(kPresent)), IsTrue());
  EXPECT_THAT(UnsafeIsPresent(TypedRef::FromValue(kMissing)), IsFalse());

  OptionalValue<float> present_float = 1;
  EXPECT_THAT(UnsafeIsPresent(TypedRef::FromValue(present_float)), IsTrue());

  OptionalValue<float> missing_float = std::nullopt;
  EXPECT_THAT(UnsafeIsPresent(TypedRef::FromValue(missing_float)), IsFalse());

  ASSERT_OK_AND_ASSIGN(TypedValue typed_missing_float,
                       CreateMissingValue(GetOptionalQType<float>()));
  EXPECT_THAT(UnsafeIsPresent(typed_missing_float.AsRef()), IsFalse());
}

}  // namespace
}  // namespace arolla
