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
#include "arolla/io/inplace_slot_builder.h"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::testing::HasSubstr;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

struct TestStruct {
  int a;
  double b;
};

TEST(ImplaceSlotTest, InplaceSlotBuilder) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  FrameLayout::Builder layout_builder;
  layout_builder.AddSlot<double>();  // unused
  auto struct_slot = layout_builder.AddSlot<TestStruct>();

  InplaceSlotBuilder<TestStruct> builder;
  ASSERT_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, a, "a"));
  ASSERT_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, b, "name_for_b"));

  EXPECT_THAT(
      builder.OutputSlots(struct_slot),
      UnorderedElementsAre(
          Pair("a",
               TypedSlot::UnsafeFromOffset(
                   i32, offsetof(TestStruct, a) + struct_slot.byte_offset())),
          Pair("name_for_b",
               TypedSlot::UnsafeFromOffset(
                   f64, offsetof(TestStruct, b) + struct_slot.byte_offset()))));
}

TEST(InputLoaderTest, InplaceLoaderBuilderErrors) {
  FrameLayout::Builder layout_builder;
  InplaceSlotBuilder<TestStruct> builder;
  ASSERT_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, a, "a"));
  ASSERT_THAT(
      AROLLA_ADD_INPLACE_SLOT_FIELD(builder, b, "a"),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("InplaceLoaderBuilder: duplicated slot name: 'a'")));
}

struct TestStructWithStruct {
  int a;
  TestStruct b;
};

TEST(InputLoaderTest, InplaceLoaderBuilderNestedStruct) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  FrameLayout::Builder layout_builder;
  layout_builder.AddSlot<double>();  // unused
  auto struct_slot = layout_builder.AddSlot<TestStructWithStruct>();

  InplaceSlotBuilder<TestStructWithStruct> builder;
  ASSERT_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, a, "a"));
  ASSERT_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, b.a, "b.a"));
  ASSERT_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, b.b, "b.b"));

  EXPECT_THAT(builder.OutputSlots(struct_slot),
              UnorderedElementsAre(
                  Pair("a", TypedSlot::UnsafeFromOffset(
                                i32, offsetof(TestStructWithStruct, a) +
                                         struct_slot.byte_offset())),
                  Pair("b.a", TypedSlot::UnsafeFromOffset(
                                  i32, offsetof(TestStructWithStruct, b.a) +
                                           struct_slot.byte_offset())),
                  Pair("b.b", TypedSlot::UnsafeFromOffset(
                                  f64, offsetof(TestStructWithStruct, b.b) +
                                           struct_slot.byte_offset()))));
}

TEST(StructReadOperatorTest, Reader) {
  // Creation InplaceLoader.
  InplaceSlotBuilder<TestStructWithStruct> builder;
  ASSERT_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, a, "a"));
  ASSERT_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, b.b, "b.b"));

  // Initializing FrameLayout.
  FrameLayout::Builder layout_builder;
  layout_builder.AddSlot<double>();  // unused
  auto struct_slot = layout_builder.AddSlot<TestStructWithStruct>();
  auto slots_map = builder.OutputSlots(struct_slot);
  ASSERT_OK(RegisterUnsafeSlotsMap(slots_map, &layout_builder));
  ASSERT_OK_AND_ASSIGN(auto slot_a, slots_map.at("a").ToSlot<int>());
  ASSERT_OK_AND_ASSIGN(auto slot_b, slots_map.at("b.b").ToSlot<double>());

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  // Loading user data by copying struct into the Frame.
  alloc.frame().Set(struct_slot, {3, {0, 5.5}});
  // Validating slot values.
  EXPECT_EQ(alloc.frame().Get(slot_a), 3);
  EXPECT_EQ(alloc.frame().Get(slot_b), 5.5);

  // Alternatively we can fill structure right from the Frame.
  TestStructWithStruct* input = alloc.frame().GetMutable(struct_slot);
  input->a = 4;
  input->b.b = 6.5;

  // Validating slot values.
  EXPECT_EQ(alloc.frame().Get(slot_a), 4);
  EXPECT_EQ(alloc.frame().Get(slot_b), 6.5);

  // We are actually reading them directly from the same memory.
  // Note that below we are comparing pointers, not values.
  EXPECT_EQ(alloc.frame().GetMutable(slot_a), &input->a);
  EXPECT_EQ(alloc.frame().GetMutable(slot_b), &input->b.b);
}

}  // namespace
}  // namespace arolla

namespace other_namespace {
namespace {

struct TestStruct {
  int a;
  double b;
};

TEST(InputLoaderTest, InplaceLoaderMacroCompilesInOtherNamespace) {
  ::arolla::InplaceSlotBuilder<TestStruct> builder;
  ASSERT_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, a, "a"));
}

}  // namespace
}  // namespace other_namespace
