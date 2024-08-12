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
#include "arolla/array/qtype/types.h"

#include <cstdint>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/any_qtype.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/repr_token_eq.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::ReprTokenEq;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsNull;

TEST(ArrayTypesTest, FloatArrayTraits) {
  QTypePtr type = GetArrayQType<float>();
  EXPECT_EQ(type->name(), "ARRAY_FLOAT32");
  EXPECT_EQ(type->type_info(), typeid(Array<float>));
  EXPECT_TRUE(IsArrayQType(type));
  EXPECT_FALSE(IsArrayQType(GetQType<float>()));
  EXPECT_THAT(type->value_qtype(), GetQType<float>());
}

TEST(ArrayTypesTest, Int32ArrayTraits) {
  QTypePtr type = GetArrayQType<int32_t>();
  EXPECT_EQ(type->name(), "ARRAY_INT32");
  EXPECT_EQ(type->type_info(), typeid(Array<int32_t>));
  EXPECT_TRUE(IsArrayQType(type));
  EXPECT_FALSE(IsArrayQType(GetQType<int32_t>()));
  EXPECT_THAT(type->value_qtype(), GetQType<int32_t>());
}

TEST(ArrayTypesTest, AnyArrayTraits) {
  QTypePtr type = GetArrayQType<Any>();
  EXPECT_EQ(type->name(), "ARRAY_ANY");
  EXPECT_EQ(type->type_info(), typeid(Array<Any>));
  EXPECT_TRUE(IsArrayQType(type));
  EXPECT_FALSE(IsArrayQType(GetQType<Any>()));
  EXPECT_THAT(type->value_qtype(), GetQType<Any>());
}

TEST(ArrayTypesTest, ArrayUnitTraits) {
  QTypePtr type = GetQType<Array<Unit>>();
  EXPECT_EQ(type->name(), "ARRAY_UNIT");
  EXPECT_EQ(type->type_info(), typeid(Array<Unit>));
  EXPECT_TRUE(IsArrayLikeQType(type));
  EXPECT_TRUE(IsArrayQType(type));
  EXPECT_THAT(type->value_qtype(), GetQType<Unit>());
}

TEST(DenseArrayTypesTest, ArrayWeakFloat) {
  QTypePtr type = GetArrayWeakFloatQType();
  EXPECT_EQ(type->name(), "ARRAY_WEAK_FLOAT");
  EXPECT_EQ(type->type_info(), typeid(Array<double>));
  EXPECT_TRUE(IsArrayLikeQType(type));
  EXPECT_TRUE(IsArrayQType(type));
  EXPECT_THAT(type->value_qtype(), GetWeakFloatQType());
  EXPECT_THAT(GetArrayQTypeByValueQType(GetWeakFloatQType()),
              IsOkAndHolds(Eq(type)));
}

TEST(ArrayTypesTest, DeepCopy) {
  QTypePtr type = GetArrayQType<float>();
  Array<float> array{CreateDenseArray<float>({2.0, 3.0, 1.0})};

  FrameLayout::Builder layout_builder;
  TypedSlot tslot1 = AddSlot(type, &layout_builder);
  TypedSlot tslot2 = AddSlot(type, &layout_builder);

  using Slot = FrameLayout::Slot<Array<float>>;
  ASSERT_OK_AND_ASSIGN(Slot slot1, tslot1.ToSlot<Array<float>>());
  ASSERT_OK_AND_ASSIGN(Slot slot2, tslot2.ToSlot<Array<float>>());

  FrameLayout layout = std::move(layout_builder).Build();
  MemoryAllocation alloc_holder(&layout);
  FramePtr frame = alloc_holder.frame();
  frame.Set(slot1, std::move(array));

  tslot1.CopyTo(frame, tslot2, frame);
  EXPECT_EQ(frame.Get(slot2)[2], OptionalValue<float>(1.0));
}

TEST(ArrayTypesTest, CreateCopiers) {
  QTypePtr type = GetArrayQType<float>();

  ASSERT_OK_AND_ASSIGN(auto reader, CreateBatchToFramesCopier(type));
  const auto* reader_casted =
      dynamic_cast<const ArrayToFramesCopier<float>*>(reader.get());
  EXPECT_NE(reader_casted, nullptr);

  ASSERT_OK_AND_ASSIGN(auto writer, CreateBatchFromFramesCopier(type));
  const auto* writer_casted =
      dynamic_cast<const ArrayFromFramesCopier<float>*>(writer.get());
  EXPECT_NE(writer_casted, nullptr);
}

TEST(ArrayTypesTest, SliceToSlot) {
  QTypePtr type = GetArrayQType<int>();
  auto array_type = dynamic_cast<const ArrayQTypeBase*>(type);
  ASSERT_NE(array_type, nullptr);

  FrameLayout::Builder bldr;
  auto slot = bldr.AddSlot<Array<int>>();
  FrameLayout layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  Array<int> block = CreateArray<int>({1, std::nullopt, 4, 3, 5});

  array_type->UnsafeSlice(1, 3, &block, frame.GetMutable(slot));
  EXPECT_THAT(frame.Get(slot), ElementsAre(std::nullopt, 4, 3));
}

TEST(ArrayTypesTest, ArraySize) {
  EXPECT_THAT(GetArraySize(TypedRef::FromValue(
                  CreateArray<int>({1, std::nullopt, 4, 3, 5}))),
              IsOkAndHolds(Eq(5)));
}

TEST(ArrayTypesTest, ValueToArrayConversion) {
  QTypePtr value_type = GetQType<float>();
  QTypePtr array_qtype = GetArrayQType<float>();

  EXPECT_THAT(array_qtype->value_qtype(), Eq(value_type));
  EXPECT_THAT(value_type->value_qtype(), IsNull());

  EXPECT_THAT(GetArrayQTypeByValueQType(value_type),
              IsOkAndHolds(Eq(array_qtype)));
  EXPECT_THAT(GetArrayQTypeByValueQType(array_qtype),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Array type with elements of type "
                                 "ARRAY_FLOAT32 is not registered.")));
}

TEST(ArrayTypesTest, PresenceQType) {
  EXPECT_THAT(dynamic_cast<const ArrayLikeQType*>(GetArrayQType<float>())
                  ->presence_qtype(),
              Eq(GetQType<Array<Unit>>()));
  EXPECT_THAT(dynamic_cast<const ArrayLikeQType*>(GetArrayQType<int>())
                  ->presence_qtype(),
              Eq(GetQType<Array<Unit>>()));
}

TEST(ArrayTypesTest, EdgeQType) {
  EXPECT_THAT(
      dynamic_cast<const ArrayLikeQType*>(GetArrayQType<float>())->edge_qtype(),
      Eq(GetQType<ArrayEdge>()));
  EXPECT_THAT(dynamic_cast<const ArrayLikeQType*>(GetArrayQType<float>())
                  ->group_scalar_edge_qtype(),
              Eq(GetQType<ArrayGroupScalarEdge>()));
  EXPECT_THAT(
      dynamic_cast<const ArrayLikeQType*>(GetArrayQType<int>())->edge_qtype(),
      Eq(GetQType<ArrayEdge>()));
}

TEST(ArrayTypesTest, ShapeQTypeRepr) {
  EXPECT_THAT(TypedValue::FromValue(ArrayShape{5}).GenReprToken(),
              ReprTokenEq("array_shape{size=5}"));
}

TEST(ArrayTypesTest, ReprTraits) {
  {
    // Simple.
    EXPECT_THAT(GenReprToken(Array<float>(CreateDenseArray<float>({}))),
                ReprTokenEq("array([], value_qtype=FLOAT32)"));
    EXPECT_THAT(GenReprToken(Array<float>(CreateDenseArray<float>({1.}))),
                ReprTokenEq("array([1.])"));
    EXPECT_THAT(
        GenReprToken(Array<float>(CreateDenseArray<float>({1., std::nullopt}))),
        ReprTokenEq("array([1., NA])"));
    EXPECT_THAT(GenReprToken(Array<Bytes>(
                    CreateDenseArray<Bytes>({std::nullopt, Bytes("a")}))),
                ReprTokenEq("array([NA, b'a'])"));
    EXPECT_THAT(GenReprToken(
                    Array<int>(CreateDenseArray<int>({2, 3, std::nullopt, 7}))),
                ReprTokenEq("array([2, 3, NA, 7])"));
  }
  {
    // Truncation.
    auto arr =
        Array<int>(CreateDenseArray<int>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
    EXPECT_THAT(
        GenReprToken(arr),
        ReprTokenEq("array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, ...], size=11)"));
  }
  {
    // All missing.
    auto arr = Array<int>(CreateDenseArray<int>({std::nullopt, std::nullopt}));
    EXPECT_THAT(GenReprToken(arr),
                ReprTokenEq("array([NA, NA], value_qtype=INT32)"));
  }
  {
    // All values in repr missing.
    auto arr = Array<int>(CreateDenseArray<int>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt,
         std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt,
         1}));
    EXPECT_THAT(GenReprToken(arr), ReprTokenEq("array([NA, NA, NA, NA, NA, NA, "
                                               "NA, NA, NA, NA, ...], size=11, "
                                               "value_qtype=INT32)"));
  }
  {
    // Value reprs.
    EXPECT_THAT(GenReprToken(Array<int64_t>(
                    CreateDenseArray<int64_t>({2, 3, std::nullopt, 7}))),
                ReprTokenEq("array([int64{2}, int64{3}, NA, int64{7}])"));
    EXPECT_THAT(GenReprToken(Array<Unit>(
                    CreateDenseArray<Unit>({std::nullopt, Unit()}))),
                ReprTokenEq("array([NA, present])"));
  }
}

TEST(ArrayTypesTest, ReprTraits_WeakFloat) {
  auto weak_float_array = UnsafeDowncastDerivedQValue(
      GetArrayWeakFloatQType(),
      TypedValue::FromValue(
          Array<double>(CreateDenseArray<double>({1., std::nullopt}))));
  EXPECT_THAT(weak_float_array.GenReprToken(),
              ReprTokenEq("array([weak_float{1}, NA])"));
}

TEST(ArrayTypesTest, ReprTraits_ArrayEdge) {
  {
    // Split points.
    ASSERT_OK_AND_ASSIGN(auto array_edge, ArrayEdge::FromSplitPoints(
                                              CreateArray<int64_t>({0, 3, 5})));
    EXPECT_THAT(
        GenReprToken(array_edge),
        ReprTokenEq(
            "array_edge(split_points=array([int64{0}, int64{3}, int64{5}]))"));
  }
  {
    // Mapping.
    ASSERT_OK_AND_ASSIGN(
        auto array_edge,
        ArrayEdge::FromMapping(CreateArray<int64_t>({0, 0, 1}), 4));
    EXPECT_THAT(GenReprToken(array_edge),
                ReprTokenEq("array_edge(mapping=array([int64{0}, int64{0}, "
                            "int64{1}]), parent_size=4)"));
  }
}

TEST(ArrayTypesTest, ReprTraits_ArrayGroupScalarEdge) {
  EXPECT_THAT(GenReprToken(ArrayGroupScalarEdge(5)),
              ReprTokenEq("array_to_scalar_edge(child_size=5)"));
}

}  // namespace
}  // namespace arolla
