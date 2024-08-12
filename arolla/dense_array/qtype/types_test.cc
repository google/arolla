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
#include "arolla/dense_array/qtype/types.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <variant>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/buffer.h"
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
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsNull;

TEST(DenseArrayTypesTest, FloatDenseArrayTraits) {
  QTypePtr type = GetDenseArrayQType<float>();
  EXPECT_EQ(type->name(), "DENSE_ARRAY_FLOAT32");
  EXPECT_EQ(type->type_info(), typeid(DenseArray<float>));
  EXPECT_TRUE(IsDenseArrayQType(type));
  EXPECT_FALSE(IsDenseArrayQType(GetQType<float>()));
  EXPECT_THAT(type->value_qtype(), GetQType<float>());
}

TEST(DenseArrayTypesTest, Int32DenseArrayTraits) {
  QTypePtr type = GetDenseArrayQType<int32_t>();
  EXPECT_EQ(type->name(), "DENSE_ARRAY_INT32");
  EXPECT_EQ(type->type_info(), typeid(DenseArray<int32_t>));
  EXPECT_TRUE(IsDenseArrayQType(type));
  EXPECT_FALSE(IsDenseArrayQType(GetQType<int32_t>()));
  EXPECT_THAT(type->value_qtype(), GetQType<int32_t>());
}

TEST(DenseArrayTypesTest, AnyDenseArrayTraits) {
  QTypePtr type = GetDenseArrayQType<Any>();
  EXPECT_EQ(type->name(), "DENSE_ARRAY_ANY");
  EXPECT_EQ(type->type_info(), typeid(DenseArray<Any>));
  EXPECT_TRUE(IsDenseArrayQType(type));
  EXPECT_FALSE(IsDenseArrayQType(GetQType<Any>()));
  EXPECT_THAT(type->value_qtype(), GetQType<Any>());
}

TEST(DenseArrayTypesTest, DenseArrayUnitTraits) {
  QTypePtr type = GetQType<DenseArray<Unit>>();
  EXPECT_EQ(type->name(), "DENSE_ARRAY_UNIT");
  EXPECT_EQ(type->type_info(), typeid(DenseArray<Unit>));
  EXPECT_TRUE(IsArrayLikeQType(type));
  EXPECT_TRUE(IsDenseArrayQType(type));
  EXPECT_THAT(type->value_qtype(), GetQType<Unit>());
}

TEST(DenseArrayTypesTest, DenseArrayWeakFloat) {
  QTypePtr type = GetDenseArrayWeakFloatQType();
  EXPECT_EQ(type->name(), "DENSE_ARRAY_WEAK_FLOAT");
  EXPECT_EQ(type->type_info(), typeid(DenseArray<double>));
  EXPECT_TRUE(IsArrayLikeQType(type));
  EXPECT_TRUE(IsDenseArrayQType(type));
  EXPECT_THAT(type->value_qtype(), GetWeakFloatQType());
  EXPECT_THAT(GetDenseArrayQTypeByValueQType(GetWeakFloatQType()),
              IsOkAndHolds(Eq(type)));
}

TEST(DenseArrayTypesTest, DeepCopy) {
  QTypePtr type = GetDenseArrayQType<float>();
  DenseArray<float> array{CreateBuffer<float>({2.0, 3.0, 1.0})};

  FrameLayout::Builder layout_builder;
  TypedSlot tslot1 = AddSlot(type, &layout_builder);
  TypedSlot tslot2 = AddSlot(type, &layout_builder);

  using Slot = FrameLayout::Slot<DenseArray<float>>;
  ASSERT_OK_AND_ASSIGN(Slot slot1, tslot1.ToSlot<DenseArray<float>>());
  ASSERT_OK_AND_ASSIGN(Slot slot2, tslot2.ToSlot<DenseArray<float>>());

  FrameLayout layout = std::move(layout_builder).Build();
  MemoryAllocation alloc_holder(&layout);
  FramePtr frame = alloc_holder.frame();
  frame.Set(slot1, std::move(array));

  tslot1.CopyTo(frame, tslot2, frame);
  EXPECT_EQ(frame.Get(slot2)[2], OptionalValue<float>(1.0));
}

TEST(DenseArrayTypesTest, CreateCopiers) {
  QTypePtr type = GetDenseArrayQType<float>();

  ASSERT_OK_AND_ASSIGN(auto reader, CreateBatchToFramesCopier(type));
  const auto* reader_casted =
      dynamic_cast<const DenseArray2FramesCopier<float>*>(reader.get());
  EXPECT_NE(reader_casted, nullptr);

  ASSERT_OK_AND_ASSIGN(auto writer, CreateBatchFromFramesCopier(type));
  const auto* writer_casted =
      dynamic_cast<const Frames2DenseArrayCopier<float>*>(writer.get());
  EXPECT_NE(writer_casted, nullptr);
}

TEST(DenseArrayTypesTest, ArraySize) {
  EXPECT_THAT(GetArraySize(TypedRef::FromValue(
                  CreateDenseArray<int>({1, std::nullopt, 4, 3, 5}))),
              IsOkAndHolds(Eq(5)));
}

TEST(DenseArrayTypesTest, ValueToDenseArrayConversion) {
  QTypePtr value_type = GetQType<float>();
  QTypePtr array_qtype = GetDenseArrayQType<float>();

  EXPECT_THAT(array_qtype->value_qtype(), Eq(value_type));
  EXPECT_THAT(value_type->value_qtype(), IsNull());

  EXPECT_THAT(GetDenseArrayQTypeByValueQType(value_type),
              IsOkAndHolds(Eq(array_qtype)));
  EXPECT_THAT(GetDenseArrayQTypeByValueQType(array_qtype),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("DenseArray type with elements of type "
                                 "DENSE_ARRAY_FLOAT32 is not registered.")));
}

TEST(DenseArrayTypesTest, PresenceType) {
  EXPECT_THAT(dynamic_cast<const ArrayLikeQType*>(GetDenseArrayQType<float>())
                  ->presence_qtype(),
              Eq(GetQType<DenseArray<std::monostate>>()));
  EXPECT_THAT(dynamic_cast<const ArrayLikeQType*>(GetDenseArrayQType<int>())
                  ->presence_qtype(),
              Eq(GetQType<DenseArray<std::monostate>>()));
}

TEST(DenseArrayTypesTest, EdgeType) {
  EXPECT_THAT(dynamic_cast<const ArrayLikeQType*>(GetDenseArrayQType<float>())
                  ->edge_qtype(),
              Eq(GetQType<DenseArrayEdge>()));
  EXPECT_THAT(dynamic_cast<const ArrayLikeQType*>(GetDenseArrayQType<float>())
                  ->group_scalar_edge_qtype(),
              Eq(GetQType<DenseArrayGroupScalarEdge>()));
  EXPECT_THAT(dynamic_cast<const ArrayLikeQType*>(GetDenseArrayQType<int>())
                  ->edge_qtype(),
              Eq(GetQType<DenseArrayEdge>()));
}

TEST(DenseArrayTypesTest, ShapeTypeRepr) {
  EXPECT_THAT(TypedValue::FromValue(DenseArrayShape{5}).GenReprToken(),
              ReprTokenEq("dense_array_shape{size=5}"));
}

TEST(DenseArrayTypesTest, DenseArrayRepr) {
  {
    // Simple.
    EXPECT_THAT(GenReprToken(CreateDenseArray<float>({})),
                ReprTokenEq("dense_array([], value_qtype=FLOAT32)"));
    EXPECT_THAT(GenReprToken(CreateDenseArray<float>({1.})),
                ReprTokenEq("dense_array([1.])"));
    EXPECT_THAT(GenReprToken(CreateDenseArray<float>({1., std::nullopt})),
                ReprTokenEq("dense_array([1., NA])"));
    EXPECT_THAT(
        GenReprToken(CreateDenseArray<Bytes>({std::nullopt, Bytes("a")})),
        ReprTokenEq("dense_array([NA, b'a'])"));
    EXPECT_THAT(GenReprToken(CreateDenseArray<int>({2, 3, std::nullopt, 7})),
                ReprTokenEq("dense_array([2, 3, NA, 7])"));
  }
  {
    // Truncation.
    auto arr = CreateDenseArray<int>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    EXPECT_THAT(
        GenReprToken(arr),
        ReprTokenEq(
            "dense_array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, ...], size=11)"));
  }
  {
    // All missing.
    auto arr = CreateDenseArray<int>({std::nullopt, std::nullopt});
    EXPECT_THAT(GenReprToken(arr),
                ReprTokenEq("dense_array([NA, NA], value_qtype=INT32)"));
  }
  {
    // All values in repr missing.
    auto arr = CreateDenseArray<int>({std::nullopt, std::nullopt, std::nullopt,
                                      std::nullopt, std::nullopt, std::nullopt,
                                      std::nullopt, std::nullopt, std::nullopt,
                                      std::nullopt, 1});
    EXPECT_THAT(GenReprToken(arr),
                ReprTokenEq("dense_array([NA, NA, NA, NA, NA, NA, "
                            "NA, NA, NA, NA, ...], size=11, "
                            "value_qtype=INT32)"));
  }
  {
    // Value reprs.
    EXPECT_THAT(
        GenReprToken(CreateDenseArray<int64_t>({2, 3, std::nullopt, 7})),
        ReprTokenEq("dense_array([int64{2}, int64{3}, NA, int64{7}])"));
    EXPECT_THAT(GenReprToken(CreateDenseArray<Unit>({std::nullopt, Unit()})),
                ReprTokenEq("dense_array([NA, present])"));
  }
}

TEST(DenseArrayTypesTest, EdgeRepr) {
  const auto mapping = CreateDenseArray<int64_t>({0, 0, std::nullopt, 1, 1});
  const auto split_points = CreateDenseArray<int64_t>({0, 3, 5});
  ASSERT_OK_AND_ASSIGN(auto edge_from_split_points,
                       DenseArrayEdge::FromSplitPoints(split_points));
  EXPECT_THAT(
      GenReprToken(DenseArrayEdge::FromMapping(mapping, 2).value()),
      ReprTokenEq("dense_array_edge(mapping=dense_array([int64{0}, int64{0}, "
                  "NA, int64{1}, int64{1}]), parent_size=2)"));
  EXPECT_THAT(
      GenReprToken(DenseArrayEdge::FromMapping(mapping, 5).value()),
      ReprTokenEq("dense_array_edge(mapping=dense_array([int64{0}, int64{0}, "
                  "NA, int64{1}, int64{1}]), parent_size=5)"));
  EXPECT_THAT(
      GenReprToken(DenseArrayEdge::FromSplitPoints(split_points).value()),
      ReprTokenEq(
          "dense_array_edge(split_points=dense_array([int64{0}, int64{3}, "
          "int64{5}]))"));
}

TEST(DenseArrayTypesTest, GroupEdgeRepr) {
  EXPECT_THAT(GenReprToken(DenseArrayGroupScalarEdge(7)),
              ReprTokenEq("dense_array_to_scalar_edge(child_size=7)"));
}

TEST(DenseArrayTypesTest, DenseArrayShapeRepr) {
  EXPECT_THAT(GenReprToken(DenseArrayShape{5}),
              ReprTokenEq("dense_array_shape{size=5}"));
}

TEST(DenseArrayTypesTest, ReprTraits_WeakFloat) {
  auto weak_float_array = UnsafeDowncastDerivedQValue(
      GetDenseArrayWeakFloatQType(),
      TypedValue::FromValue(CreateDenseArray<double>({1., std::nullopt})));
  EXPECT_THAT(weak_float_array.GenReprToken(),
              ReprTokenEq("dense_array([weak_float{1}, NA])"));
}

}  // namespace
}  // namespace arolla
