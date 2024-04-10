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
#include "arolla/io/struct_io.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/bytes.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace arolla {
namespace {

using ::arolla::testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::UnorderedElementsAreArray;

struct TestStruct {
  int32_t a = 1;
  bool b = true;
  float c = 3.0f;
  int32_t d = 4;
  int32_t j = 10;
  DenseArray<int32_t> e;
  Bytes f;
  double g = 7.0;
  int64_t h = 8;
  OptionalValue<int32_t> i = 9;
  OptionalValue<float> k = 11.0f;
};

#define STRUCT_SLOT(STRUCT, FIELD)                                         \
  {                                                                        \
    #FIELD, TypedSlot::UnsafeFromOffset(GetQType<typeof(STRUCT::FIELD)>(), \
                                        offsetof(STRUCT, FIELD))           \
  }

absl::flat_hash_map<std::string, TypedSlot> GetStructSlots() {
  return absl::flat_hash_map<std::string, TypedSlot>{
    STRUCT_SLOT(TestStruct, a),
    STRUCT_SLOT(TestStruct, b),
    STRUCT_SLOT(TestStruct, c),
    STRUCT_SLOT(TestStruct, d),
    STRUCT_SLOT(TestStruct, e),
    STRUCT_SLOT(TestStruct, f),
    STRUCT_SLOT(TestStruct, g),
    STRUCT_SLOT(TestStruct, h),
    STRUCT_SLOT(TestStruct, i),
    STRUCT_SLOT(TestStruct, j),
    STRUCT_SLOT(TestStruct, k),
  };
}

TEST(StructIO, GetNamesAndTypes) {
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       StructInputLoader<TestStruct>::Create(GetStructSlots()));
  ASSERT_OK_AND_ASSIGN(
      auto slot_listener,
      StructSlotListener<TestStruct>::Create(GetStructSlots()));
  std::vector<std::string> expected_names{"a", "b", "c", "d", "e", "f",
                                          "g", "h", "i", "j", "k"};

  EXPECT_THAT(input_loader->SuggestAvailableNames(),
              UnorderedElementsAreArray(expected_names));
  EXPECT_THAT(slot_listener->SuggestAvailableNames(),
              UnorderedElementsAreArray(expected_names));

  EXPECT_EQ(input_loader->GetQTypeOf("e"), GetDenseArrayQType<int32_t>());
  EXPECT_EQ(slot_listener->GetQTypeOf("g"), GetQType<double>());
}

TEST(StructIO, BasicTest) {
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       StructInputLoader<TestStruct>::Create(GetStructSlots()));
  ASSERT_OK_AND_ASSIGN(
      auto slot_listener,
      StructSlotListener<TestStruct>::Create(GetStructSlots()));

  FrameLayout::Builder bldr;
  auto a_slot = bldr.AddSlot<int32_t>();
  auto d_slot = bldr.AddSlot<int32_t>();
  auto j_slot = bldr.AddSlot<int32_t>();
  auto k_slot = bldr.AddSlot<OptionalValue<float>>();
  auto b_slot = bldr.AddSlot<bool>();
  auto c_slot = bldr.AddSlot<float>();
  auto i_slot = bldr.AddSlot<OptionalValue<int32_t>>();
  FrameLayout layout = std::move(bldr).Build();

  absl::flat_hash_map<std::string, TypedSlot> frame_slots{
    {"a", TypedSlot::FromSlot(a_slot)},
    {"d", TypedSlot::FromSlot(d_slot)},
    {"j", TypedSlot::FromSlot(j_slot)},
    {"b", TypedSlot::FromSlot(b_slot)},
    {"c", TypedSlot::FromSlot(c_slot)},
    {"i", TypedSlot::FromSlot(i_slot)},
    {"k", TypedSlot::FromSlot(k_slot)},
  };
  ASSERT_OK_AND_ASSIGN(auto bound_loader, input_loader->Bind(frame_slots));
  ASSERT_OK_AND_ASSIGN(auto bound_listener, slot_listener->Bind(frame_slots));

  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();
  TestStruct ts;

  ASSERT_OK(bound_loader(ts, frame));

  EXPECT_EQ(frame.Get(a_slot), 1);
  EXPECT_EQ(frame.Get(b_slot), true);
  EXPECT_EQ(frame.Get(c_slot), 3.0f);
  EXPECT_EQ(frame.Get(d_slot), 4);
  EXPECT_EQ(frame.Get(i_slot), 9);
  EXPECT_EQ(frame.Get(j_slot), 10);
  EXPECT_EQ(frame.Get(k_slot), 11.0f);

  frame.Set(a_slot, 100);
  frame.Set(b_slot, false);
  frame.Set(c_slot, 3.14f);
  frame.Set(d_slot, 57);
  frame.Set(i_slot, std::nullopt);
  frame.Set(j_slot, 19);
  frame.Set(k_slot, 0.5f);

  ASSERT_OK(bound_listener(frame, &ts));

  EXPECT_EQ(ts.a, 100);
  EXPECT_EQ(ts.b, false);
  EXPECT_EQ(ts.c, 3.14f);
  EXPECT_EQ(ts.d, 57);
  EXPECT_EQ(ts.i, std::nullopt);
  EXPECT_EQ(ts.j, 19);
  EXPECT_EQ(ts.k, 0.5f);
}

TEST(StructIO, ComplicatedQType) {
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       StructInputLoader<TestStruct>::Create(GetStructSlots()));
  ASSERT_OK_AND_ASSIGN(
      auto slot_listener,
      StructSlotListener<TestStruct>::Create(GetStructSlots()));

  FrameLayout::Builder bldr;
  auto f_slot = bldr.AddSlot<Bytes>();
  auto e_slot = bldr.AddSlot<DenseArray<int32_t>>();
  FrameLayout layout = std::move(bldr).Build();

  absl::flat_hash_map<std::string, TypedSlot> frame_slots{
    {"e", TypedSlot::FromSlot(e_slot)},
    {"f", TypedSlot::FromSlot(f_slot)},
  };
  ASSERT_OK_AND_ASSIGN(auto bound_loader, input_loader->Bind(frame_slots));
  ASSERT_OK_AND_ASSIGN(auto bound_listener, slot_listener->Bind(frame_slots));

  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();
  TestStruct ts;

  ts.e = CreateDenseArray<int32_t>({1, 2, 3});
  ts.f = Bytes("abacaba");

  ASSERT_OK(bound_loader(ts, frame));

  ts.e = DenseArray<int32_t>();
  ts.f = Bytes();

  EXPECT_THAT(frame.Get(e_slot), ElementsAre(1, 2, 3));
  EXPECT_EQ(frame.Get(f_slot), Bytes("abacaba"));

  ASSERT_OK(bound_listener(frame, &ts));

  EXPECT_THAT(ts.e, ElementsAre(1, 2, 3));
  EXPECT_EQ(ts.f, Bytes("abacaba"));
}

TEST(StructIO, Errors) {
  absl::flat_hash_map<std::string, TypedSlot> struct_slots1{
      {"a", TypedSlot::UnsafeFromOffset(GetQType<int32_t>(), 0)},
      {"b", TypedSlot::UnsafeFromOffset(GetQType<int32_t>(), 5)},
      {"c", TypedSlot::UnsafeFromOffset(GetQType<int32_t>(), 100500)},
  };
  EXPECT_THAT(StructInputLoader<TestStruct>::Create(struct_slots1),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "slot 'c' is not within the struct"));

  absl::flat_hash_map<std::string, TypedSlot> struct_slots2{
      {"a", TypedSlot::UnsafeFromOffset(GetQType<int32_t>(), 4)},
      {"b", TypedSlot::UnsafeFromOffset(GetQType<int32_t>(),
                                        sizeof(TestStruct) - 3)},
      {"c", TypedSlot::UnsafeFromOffset(GetQType<int32_t>(), 0)},
  };
  EXPECT_THAT(StructSlotListener<TestStruct>::Create(struct_slots2),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "slot 'b' is not within the struct"));
}

}  // namespace
}  // namespace arolla
