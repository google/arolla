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
#include "arolla/qtype/typed_slot.h"

#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::MatchesRegex;
using ::testing::Pair;
using ::testing::StrEq;
using ::testing::UnorderedElementsAre;

TEST(TypedSlotTest, Copy) {
  FrameLayout::Builder layout_builder;
  auto slot = layout_builder.AddSlot<int>();
  auto slot2 = layout_builder.AddSlot<float>();
  auto typed_slot = TypedSlot::FromSlot(slot);
  auto typed_slot2 = TypedSlot::FromSlot(slot2);
  auto typed_slot_copy = typed_slot;  // copy constructor
  EXPECT_EQ(typed_slot.GetType(), typed_slot_copy.GetType());
  EXPECT_EQ(typed_slot, typed_slot_copy);
  typed_slot_copy = typed_slot2;  // copy assignment
  EXPECT_EQ(typed_slot2.GetType(), typed_slot_copy.GetType());
  EXPECT_EQ(typed_slot2, typed_slot_copy);
}

TEST(TypedSlotTest, PrimitiveTypes) {
  FrameLayout::Builder layout_builder;
  auto slot = layout_builder.AddSlot<int32_t>();
  auto typed_slot = TypedSlot::FromSlot(slot);
  EXPECT_EQ(typed_slot.GetType(), GetQType<int32_t>());
  FrameLayout::Slot<int32_t> new_slot = typed_slot.ToSlot<int32_t>().value();
  EXPECT_EQ(slot.byte_offset(), new_slot.byte_offset());

  EXPECT_THAT(typed_slot.ToSlot<int64_t>().status(),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(TypedSlotTest, SlotsToTypes) {
  FrameLayout::Builder layout_builder;
  auto slot1 = layout_builder.AddSlot<int32_t>();
  auto slot2 = layout_builder.AddSlot<float>();
  auto typed_slot1 = TypedSlot::FromSlot(slot1);
  auto typed_slot2 = TypedSlot::FromSlot(slot2);

  EXPECT_THAT(SlotsToTypes(std::vector<TypedSlot>{typed_slot1, typed_slot2}),
              ElementsAre(GetQType<int32_t>(), GetQType<float>()));

  EXPECT_THAT(SlotsToTypes(absl::flat_hash_map<std::string, TypedSlot>{
                  {"X", typed_slot1}, {"Y", typed_slot2}}),
              UnorderedElementsAre(Pair("X", GetQType<int32_t>()),
                                   Pair("Y", GetQType<float>())));
}

TEST(TypedSlotTest, UnsafeFromOffset) {
  const QType* i32 = GetQType<int32_t>();
  auto typed_slot = TypedSlot::UnsafeFromOffset(i32, 10);
  EXPECT_EQ(typed_slot.byte_offset(), 10);
  EXPECT_EQ(typed_slot.GetType(), i32);
}

TEST(TypedSlotTest, AddSlots) {
  FrameLayout::Builder layout_builder;
  const QType* i32 = GetQType<int32_t>();
  const QType* i64 = GetQType<int64_t>();
  std::vector<TypedSlot> slots = AddSlots({i32, i64}, &layout_builder);
  ASSERT_EQ(slots.size(), 2);
  EXPECT_EQ(i32, slots[0].GetType());
  EXPECT_EQ(i64, slots[1].GetType());
}

TEST(TypedSlotTest, AddNamedSlots) {
  FrameLayout::Builder layout_builder;
  const QType* i32 = GetQType<int32_t>();
  const QType* i64 = GetQType<int64_t>();
  std::vector<std::pair<std::string, TypedSlot>> slots =
      AddNamedSlots({{"c", i32}, {"b", i64}}, &layout_builder);
  ASSERT_EQ(slots.size(), 2);
  EXPECT_EQ("c", slots[0].first);
  EXPECT_EQ(i32, slots[0].second.GetType());
  EXPECT_EQ("b", slots[1].first);
  EXPECT_EQ(i64, slots[1].second.GetType());
}

TEST(TypedSlotTest, AddSlotsMap) {
  FrameLayout::Builder layout_builder;
  const QType* i32 = GetQType<int32_t>();
  const QType* i64 = GetQType<int64_t>();
  absl::flat_hash_map<std::string, TypedSlot> slots =
      AddSlotsMap({{"a", i32}, {"b", i64}}, &layout_builder);
  ASSERT_EQ(slots.size(), 2);
  EXPECT_EQ(i32, slots.at("a").GetType());
  EXPECT_EQ(i64, slots.at("b").GetType());
}

TEST(TypedSlotTest, RegisterUnsafeSlots) {
  FrameLayout::Builder layout_builder;
  layout_builder.AddSlot<int64_t>();
  const QType* i32 = GetQType<int32_t>();
  const QType* f32 = GetQType<float>();
  auto slot_i32 = TypedSlot::UnsafeFromOffset(i32, 0);
  auto slot_f32 = TypedSlot::UnsafeFromOffset(f32, 4);
  ASSERT_OK(RegisterUnsafeSlots({slot_i32, slot_f32}, &layout_builder));
#ifndef NDEBUG
  ASSERT_FALSE(RegisterUnsafeSlots({slot_i32}, &layout_builder).ok());
#endif
  auto layout = std::move(layout_builder).Build();
  layout.HasField(0, typeid(int32_t));
  layout.HasField(4, typeid(float));
}

TEST(TypedSlotTest, RegisterUnsafeSlotsMap) {
  FrameLayout::Builder layout_builder;
  layout_builder.AddSlot<int64_t>();
  const QType* i32 = GetQType<int32_t>();
  const QType* f32 = GetQType<float>();
  auto slot_i32 = TypedSlot::UnsafeFromOffset(i32, 0);
  auto slot_f32 = TypedSlot::UnsafeFromOffset(f32, 4);
  ASSERT_OK(RegisterUnsafeSlotsMap({{"a", slot_i32}, {"b", slot_f32}},
                                   &layout_builder));
#ifndef NDEBUG
  ASSERT_FALSE(RegisterUnsafeSlotsMap({{"a", slot_i32}}, &layout_builder).ok());
#endif
  auto layout = std::move(layout_builder).Build();
  layout.HasField(0, typeid(int32_t));
  layout.HasField(4, typeid(float));
}

TEST(TypedSlotTest, GetSubslots) {
  // Create a layout containing two OptionalValue slots and one numeric slot.
  FrameLayout::Builder layout_builder;
  auto opt_float_slot = layout_builder.AddSlot<OptionalValue<float>>();
  auto opt_int32_slot = layout_builder.AddSlot<OptionalValue<int32_t>>();
  auto float64_slot = layout_builder.AddSlot<double>();
  FrameLayout layout = std::move(layout_builder).Build();

  // Create corresponding TypedSlot objects.
  TypedSlot opt_float_tslot = TypedSlot::FromSlot(opt_float_slot);
  TypedSlot opt_int32_tslot = TypedSlot::FromSlot(opt_int32_slot);
  TypedSlot float64_tslot = TypedSlot::FromSlot(float64_slot);

  // Get the subslots via the TypedSlot interface.
  EXPECT_EQ(opt_float_tslot.SubSlotCount(), 2);
  EXPECT_EQ(opt_int32_tslot.SubSlotCount(), 2);
  EXPECT_EQ(float64_tslot.SubSlotCount(), 0);

  EXPECT_EQ(opt_float_tslot.SubSlot(0),
            TypedSlot::FromSlot(opt_float_slot.GetSubslot<0>()));
  EXPECT_EQ(opt_float_tslot.SubSlot(1),
            TypedSlot::FromSlot(opt_float_slot.GetSubslot<1>()));

  EXPECT_EQ(opt_int32_tslot.SubSlot(0),
            TypedSlot::FromSlot(opt_int32_slot.GetSubslot<0>()));
  EXPECT_EQ(opt_int32_tslot.SubSlot(1),
            TypedSlot::FromSlot(opt_int32_slot.GetSubslot<1>()));

  // Create an allocation for the layout.
  MemoryAllocation alloc_holder(&layout);
  FramePtr frame = alloc_holder.frame();

  // Load values into optional slots
  frame.Set(opt_float_slot, OptionalValue<float>(1.0));
  frame.Set(opt_int32_slot, OptionalValue<int32_t>());

  // Read presence bools using subslots
  auto float_present_slot = opt_float_tslot.SubSlot(0).ToSlot<bool>().value();
  auto int32_present_slot = opt_int32_tslot.SubSlot(0).ToSlot<bool>().value();
  EXPECT_EQ(frame.Get(float_present_slot), true);
  EXPECT_EQ(frame.Get(int32_present_slot), false);

  // Update opt_int32_slot via subslots.
  auto int32_value_slot = opt_int32_tslot.SubSlot(1).ToSlot<int32_t>().value();
  frame.Set(int32_present_slot, true);
  frame.Set(int32_value_slot, 2);

  // Read modified opt_int32_slot.
  EXPECT_EQ(frame.Get(opt_int32_slot), OptionalValue<int32_t>(2));
}

TEST(TypedSlotTest, DebugPrintTypedSlot) {
  FrameLayout::Builder layout_builder;
  auto slot1 = layout_builder.AddSlot<int32_t>();
  auto slot2 = layout_builder.AddSlot<float>();
  auto slot3 = layout_builder.AddSlot<Bytes>();
  auto typed_slot1 = TypedSlot::FromSlot(slot1);
  auto typed_slot2 = TypedSlot::FromSlot(slot2);
  auto typed_slot3 = TypedSlot::FromSlot(slot3);

  std::stringstream buffer;
  buffer << "typed_slot1 is: " << typed_slot1 << ", ";
  buffer << "typed_slot2 is: " << typed_slot2 << ", ";
  buffer << "typed_slot3 is: " << typed_slot3 << ".";
  EXPECT_THAT(buffer.str(), StrEq("typed_slot1 is: TypedSlot<INT32>@0, "
                                  "typed_slot2 is: TypedSlot<FLOAT32>@4, "
                                  "typed_slot3 is: TypedSlot<BYTES>@8."));
}

TEST(TypedSlotTest, ToSlots) {
  FrameLayout::Builder layout_builder;
  auto slot1 = layout_builder.AddSlot<int32_t>();
  auto slot2 = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      auto slots_tuple,
      (TypedSlot::ToSlots<int32_t, float>(
          {TypedSlot::FromSlot(slot1), TypedSlot::FromSlot(slot2)})));
  EXPECT_THAT(std::get<0>(slots_tuple).byte_offset(), Eq(slot1.byte_offset()));
  EXPECT_THAT(std::get<1>(slots_tuple).byte_offset(), Eq(slot2.byte_offset()));

  EXPECT_THAT(TypedSlot::ToSlots<float>({TypedSlot::FromSlot(slot1)}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("slot type does not match C++ type")));
  EXPECT_THAT(
      (TypedSlot::ToSlots<int32_t, float>({TypedSlot::FromSlot(slot1)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("wrong number of slots: expected 2, got 1")));
}

TEST(TypedSlotTest, MaybeFindSlotsAndVerifyTypesErrors) {
  FrameLayout::Builder layout_builder;
  auto float_slot = layout_builder.AddSlot<float>();
  EXPECT_THAT(
      MaybeFindSlotsAndVerifyTypes({{"a", GetQType<int>()}},
                                   {{"a", TypedSlot::FromSlot(float_slot)}}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex(".*slot types "
                            "mismatch.*a.*expected:INT32.*actual:FLOAT32.*")));
}

TEST(TypedSlotTest, MaybeFindSlotsAndVerifyTypes) {
  FrameLayout::Builder layout_builder;
  auto int_slot = layout_builder.AddSlot<int>();
  auto float_slot = layout_builder.AddSlot<float>();
  EXPECT_THAT(
      MaybeFindSlotsAndVerifyTypes(
          {{"a", GetQType<int>()}, {"c", GetQType<float>()}},
          {{"b", TypedSlot::FromSlot(float_slot)},
           {"a", TypedSlot::FromSlot(int_slot)}}),
      IsOkAndHolds(ElementsAre(TypedSlot::FromSlot(int_slot), std::nullopt)));
}

TEST(TypedSlotTest, FindSlotsAndVerifyTypesErrors) {
  FrameLayout::Builder layout_builder;
  auto float_slot = layout_builder.AddSlot<float>();
  EXPECT_THAT(
      FindSlotsAndVerifyTypes({{"NAME", GetQType<int>()}},
                              {{"NAME", TypedSlot::FromSlot(float_slot)}}),
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          MatchesRegex(".*slot types "
                       "mismatch.*NAME.*expected:INT32.*actual:FLOAT32.*")));
  EXPECT_THAT(FindSlotsAndVerifyTypes({{"FAKE", GetQType<int>()}},
                                      {{"b", TypedSlot::FromSlot(float_slot)}}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*missed slots:.*FAKE.*")));
  EXPECT_THAT(
      FindSlotsAndVerifyTypes(
          {{"NAME", GetQType<int>()}, {"FAKE", GetQType<int>()}},
          {{"NAME", TypedSlot::FromSlot(float_slot)}}),
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          MatchesRegex(".*missed slots:.*FAKE.*slot types mismatch:.*NAME.*")));
}

TEST(TypedSlotTest, FindSlotsAndVerifyTypes) {
  FrameLayout::Builder layout_builder;
  auto int_slot = layout_builder.AddSlot<int>();
  auto float_slot = layout_builder.AddSlot<float>();
  auto int8_slot = layout_builder.AddSlot<int32_t>();
  EXPECT_THAT(FindSlotsAndVerifyTypes(
                  {{"c", GetQType<float>()}, {"a", GetQType<int>()}},
                  {{"c", TypedSlot::FromSlot(float_slot)},
                   {"b", TypedSlot::FromSlot(int8_slot)},
                   {"a", TypedSlot::FromSlot(int_slot)}}),
              IsOkAndHolds(ElementsAre(TypedSlot::FromSlot(float_slot),
                                       TypedSlot::FromSlot(int_slot))));
}

TEST(TypedSlotTest, VerifySlotTypes) {
  FrameLayout::Builder layout_builder;
  auto int_slot = layout_builder.AddSlot<int>();
  auto float_slot = layout_builder.AddSlot<float>();
  EXPECT_OK(VerifySlotTypes({{"a", GetQType<int>()}, {"c", GetQType<float>()}},
                            {{"c", TypedSlot::FromSlot(float_slot)},
                             {"a", TypedSlot::FromSlot(int_slot)}}));
  EXPECT_OK(VerifySlotTypes({{"a", GetQType<int>()}, {"c", GetQType<float>()}},
                            {{"c", TypedSlot::FromSlot(float_slot)}},
                            /*verify_unwanted_slots=*/true,
                            /*verify_missed_slots=*/false));
  EXPECT_OK(VerifySlotTypes({{"a", GetQType<int>()}},
                            {{"c", TypedSlot::FromSlot(float_slot)},
                             {"a", TypedSlot::FromSlot(int_slot)}},
                            /*verify_unwanted_slots=*/false));
  EXPECT_THAT(
      VerifySlotTypes({{"a", GetQType<int>()}},
                      {{"a", TypedSlot::FromSlot(float_slot)}}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex(".*slot types "
                            "mismatch.*a.*expected:INT32.*actual:FLOAT32.*")));
  EXPECT_THAT(
      VerifySlotTypes({{"a", GetQType<int>()}, {"c", GetQType<float>()}},
                      {{"a", TypedSlot::FromSlot(float_slot)}}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex(".*missed slots:.*c.*slot types mismatch:.*a.*")));
  EXPECT_THAT(
      VerifySlotTypes({{"d", GetQType<int>()}},
                      {{"a", TypedSlot::FromSlot(float_slot)}}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex(".*missed slots:.*d.*unwanted slots:.*a.*")));
}

}  // namespace
}  // namespace arolla
