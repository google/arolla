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
#include "arolla/qtype/shape_qtype.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::ReprTokenEq;
using ::testing::Eq;
using ::testing::NotNull;

TEST(ShapeQType, ScalarShape) {
  auto scalar_shape = dynamic_cast<const ShapeQType*>(GetQType<ScalarShape>());
  ASSERT_THAT(scalar_shape, NotNull());

  EXPECT_THAT(scalar_shape->WithValueQType(GetQType<int64_t>()),
              IsOkAndHolds(Eq(GetQType<int64_t>())));
  EXPECT_THAT(scalar_shape->WithValueQType(GetQType<Bytes>()),
              IsOkAndHolds(Eq(GetQType<Bytes>())));

  // We allow sielent upgrade to optional types.
  EXPECT_THAT(scalar_shape->WithValueQType(GetOptionalQType<int64_t>()),
              IsOkAndHolds(Eq(GetOptionalQType<int64_t>())));
}

TEST(ShapeQType, OptionalScalarShape) {
  auto optional_shape =
      dynamic_cast<const ShapeQType*>(GetQType<OptionalScalarShape>());
  ASSERT_THAT(optional_shape, NotNull());

  EXPECT_THAT(optional_shape->WithValueQType(GetQType<int64_t>()),
              IsOkAndHolds(Eq(GetOptionalQType<int64_t>())));
  EXPECT_THAT(optional_shape->WithValueQType(GetQType<Bytes>()),
              IsOkAndHolds(Eq(GetOptionalQType<Bytes>())));
  EXPECT_THAT(optional_shape->WithValueQType(GetOptionalQType<int64_t>()),
              IsOkAndHolds(Eq(GetOptionalQType<int64_t>())));
}

TEST(ShapeQType, ScalarShapeRepr) {
  EXPECT_THAT(GenReprToken(ScalarShape{}), ReprTokenEq("scalar_shape"));
}

TEST(ShapeQType, OptionalScalarShapeRepr) {
  EXPECT_THAT(GenReprToken(OptionalScalarShape{}),
              ReprTokenEq("optional_scalar_shape"));
}

TEST(ShapeQType, TypedValuScalarShapeRepr) {
  EXPECT_THAT(TypedValue::FromValue(ScalarShape{}).GenReprToken(),
              ReprTokenEq("scalar_shape"));
}

TEST(ShapeQType, TypedValueOptionalScalarShapeRepr) {
  EXPECT_THAT(TypedValue::FromValue(OptionalScalarShape{}).GenReprToken(),
              ReprTokenEq("optional_scalar_shape"));
}

TEST(ShapeQType, ScalarShapeFingerprint) {
  EXPECT_THAT(TypedValue::FromValue(ScalarShape{}).GetFingerprint(),
              Eq(TypedValue::FromValue(ScalarShape{}).GetFingerprint()));
}

TEST(ShapeQType, OptionalScalarShapeFingerprint) {
  EXPECT_THAT(
      TypedValue::FromValue(OptionalScalarShape{}).GetFingerprint(),
      Eq(TypedValue::FromValue(OptionalScalarShape{}).GetFingerprint()));
}

TEST(ShapeQType, IsShapeQType) {
  EXPECT_TRUE(IsShapeQType(GetQType<OptionalScalarShape>()));
  EXPECT_FALSE(IsShapeQType(GetQType<int32_t>()));
}

}  // namespace
}  // namespace arolla
