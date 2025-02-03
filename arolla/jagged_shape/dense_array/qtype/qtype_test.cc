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
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"  // IWYU pragma: keep

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/array/jagged_shape.h"
#include "arolla/jagged_shape/array/qtype/qtype.h"  // IWYU pragma: keep
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/jagged_shape/qtype/qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::ReprTokenEq;

TEST(QTypeTest, TypedValueRepr) {
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 2})));
  ASSERT_OK_AND_ASSIGN(auto shape, JaggedDenseArrayShape::FromEdges({edge}));
  // TypedValue repr (-> python repr) works.
  auto tv = TypedValue::FromValue(shape);
  EXPECT_THAT(tv.GenReprToken(), ReprTokenEq("JaggedShape(2)"));
}

TEST(QTypeTest, JaggedDenseArrayShapeQType) {
  QTypePtr type = GetQType<JaggedDenseArrayShape>();
  EXPECT_NE(type, nullptr);
  EXPECT_EQ(type->name(), "JAGGED_DENSE_ARRAY_SHAPE");
  EXPECT_EQ(type->type_info(), typeid(JaggedDenseArrayShape));
  EXPECT_EQ(type->value_qtype(), nullptr);
  EXPECT_TRUE(IsJaggedShapeQType(type));
  EXPECT_EQ(type, GetQType<JaggedDenseArrayShape>());
  EXPECT_NE(type, GetQType<JaggedArrayShape>());
}

TEST(QTypeTest, JaggedDenseArrayShapeFingerprint) {
  ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 2})));
  ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 1, 3})));
  ASSERT_OK_AND_ASSIGN(auto shape1,
                       JaggedDenseArrayShape::FromEdges({edge1, edge2}));
  ASSERT_OK_AND_ASSIGN(auto shape2,
                       JaggedDenseArrayShape::FromEdges({edge1, edge2}));
  // Same shape -> same fingerprint.
  auto tv1 = TypedValue::FromValue(shape1);
  auto tv2 = TypedValue::FromValue(shape2);
  EXPECT_EQ(tv1.GetFingerprint(), tv2.GetFingerprint());
  // Different shape -> different fingerprint.
  ASSERT_OK_AND_ASSIGN(auto edge3, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 1, 4})));
  ASSERT_OK_AND_ASSIGN(auto shape3,
                       JaggedDenseArrayShape::FromEdges({edge1, edge3}));
  auto tv3 = TypedValue::FromValue(shape3);
  EXPECT_NE(tv1.GetFingerprint(), tv3.GetFingerprint());
}

TEST(QTypeTest, CopyTo) {
  ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 2})));
  ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 1, 3})));
  ASSERT_OK_AND_ASSIGN(auto shape,
                       JaggedDenseArrayShape::FromEdges({edge1, edge2}));
  auto tv = TypedValue::FromValue(shape);
  auto tv_copy = TypedValue(tv.AsRef());  // Copies the raw (void) data.
  EXPECT_EQ(tv.GetFingerprint(), tv_copy.GetFingerprint());
}

TEST(QTypeTest, JaggedShapeQTypeFromEdgeQType) {
  {
    // Registered type.
    ASSERT_OK_AND_ASSIGN(auto shape_qtype, GetJaggedShapeQTypeFromEdgeQType(
                                               GetQType<DenseArrayEdge>()));
    EXPECT_EQ(shape_qtype, GetQType<JaggedDenseArrayShape>());
  }
  {
    // Not registered type.
    EXPECT_THAT(
        GetJaggedShapeQTypeFromEdgeQType(GetQType<DenseArrayGroupScalarEdge>()),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "DENSE_ARRAY_TO_SCALAR_EDGE key is not registered"));
  }
  {
    // Re-registering is disallowed.
    EXPECT_THAT(
        SetEdgeQTypeToJaggedShapeQType(GetQType<DenseArrayEdge>(),
                                       GetQType<DenseArrayGroupScalarEdge>()),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "DENSE_ARRAY_EDGE key is already registered"));
  }
}

TEST(QTypeTest, EdgeQType) {
  auto type = GetQType<JaggedDenseArrayShape>();
  auto shape_qtype = dynamic_cast<const JaggedShapeQType*>(type);
  EXPECT_EQ(shape_qtype->edge_qtype(), GetQType<DenseArrayEdge>());
}

}  // namespace
}  // namespace arolla
