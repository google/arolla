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
#include "arolla/qtype/weak_qtype.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/testing/repr_token_eq.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::ReprTokenEq;
using ::testing::MatchesRegex;

TEST(WeakQTypeTest, Smoke) {
  auto qtype = GetWeakFloatQType();
  EXPECT_EQ(qtype->name(), "WEAK_FLOAT");
  auto optional_qtype = GetOptionalWeakFloatQType();
  EXPECT_EQ(optional_qtype->name(), "OPTIONAL_WEAK_FLOAT");
}

TEST(WeakQTypeTest, Optional) {
  QTypePtr qtype = GetWeakFloatQType();
  QTypePtr optional_qtype = GetOptionalWeakFloatQType();

  EXPECT_EQ(optional_qtype->value_qtype(), qtype);
  EXPECT_TRUE(IsOptionalQType(optional_qtype));
  ASSERT_OK_AND_ASSIGN(QTypePtr to_optional_res, ToOptionalQType(qtype));
  EXPECT_EQ(to_optional_res, optional_qtype);
  EXPECT_EQ(DecayOptionalQType(optional_qtype), qtype);
  ASSERT_OK_AND_ASSIGN(TypedValue v, CreateMissingValue(optional_qtype));
  ASSERT_EQ(v.GetType(), optional_qtype);
}

TEST(WeakQTypeTest, IsScalarQType) {
  EXPECT_TRUE(IsScalarQType(GetWeakFloatQType()));
  EXPECT_FALSE(IsScalarQType(GetOptionalWeakFloatQType()));
}

TEST(WeakQTypeTest, GetScalarQType) {
  {
    ASSERT_OK_AND_ASSIGN(QTypePtr scalar_qtype,
                         GetScalarQType(GetWeakFloatQType()));
    EXPECT_EQ(scalar_qtype, GetWeakFloatQType());
  }
  {
    ASSERT_OK_AND_ASSIGN(QTypePtr scalar_qtype,
                         GetScalarQType(GetOptionalWeakFloatQType()));
    EXPECT_EQ(scalar_qtype, GetWeakFloatQType());
  }
}

TEST(WeakQTypeTest, WithScalarQType) {
  {
    ASSERT_OK_AND_ASSIGN(
        QTypePtr res_qtype,
        WithScalarQType(GetQType<float>(), GetWeakFloatQType()));
    EXPECT_EQ(res_qtype, GetWeakFloatQType());
  }
  {
    ASSERT_OK_AND_ASSIGN(
        QTypePtr res_qtype,
        WithScalarQType(GetOptionalQType<double>(), GetWeakFloatQType()));
    EXPECT_EQ(res_qtype, GetOptionalWeakFloatQType());
  }
  EXPECT_THAT(WithScalarQType(GetArrayQType<float>(), GetWeakFloatQType()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       MatchesRegex("Array type with elements of type "
                                    "WEAK_FLOAT is not registered.")));
  {
    ASSERT_OK_AND_ASSIGN(
        QTypePtr res_qtype,
        WithScalarQType(GetWeakFloatQType(), GetQType<double>()));
    EXPECT_EQ(res_qtype, GetQType<double>());
  }
  {
    ASSERT_OK_AND_ASSIGN(
        QTypePtr res_qtype,
        WithScalarQType(GetOptionalWeakFloatQType(), GetQType<float>()));
    EXPECT_EQ(res_qtype, GetOptionalQType<float>());
  }
}

TEST(WeakQTypeTest, DecayContainerQType) {
  EXPECT_EQ(DecayContainerQType(GetWeakFloatQType()), GetWeakFloatQType());
  EXPECT_EQ(DecayContainerQType(GetOptionalWeakFloatQType()),
            GetWeakFloatQType());
}

TEST(WeakQTypeTest, GetShapeQType) {
  {
    ASSERT_OK_AND_ASSIGN(QTypePtr shape_qtype,
                         GetShapeQType(GetWeakFloatQType()));
    EXPECT_EQ(shape_qtype, GetQType<ScalarShape>());
  }
  {
    ASSERT_OK_AND_ASSIGN(QTypePtr shape_qtype,
                         GetShapeQType(GetOptionalWeakFloatQType()));
    EXPECT_EQ(shape_qtype, GetQType<OptionalScalarShape>());
  }
}

TEST(WeakQTypeTest, GetPresenceQType) {
  {
    ASSERT_OK_AND_ASSIGN(QTypePtr presence_qtype,
                         GetPresenceQType(GetWeakFloatQType()));
    EXPECT_EQ(presence_qtype, GetQType<Unit>());
  }
  {
    ASSERT_OK_AND_ASSIGN(QTypePtr presence_qtype,
                         GetPresenceQType(GetOptionalWeakFloatQType()));
    EXPECT_EQ(presence_qtype, GetOptionalQType<Unit>());
  }
}

TEST(WeakQTypeTest, OptionalLike) {
  EXPECT_FALSE(IsOptionalLikeQType(GetWeakFloatQType()));
  EXPECT_TRUE(IsOptionalLikeQType(GetOptionalWeakFloatQType()));
  {
    ASSERT_OK_AND_ASSIGN(QTypePtr optional_like_qtype,
                         ToOptionalLikeQType(GetWeakFloatQType()));
    EXPECT_EQ(optional_like_qtype, GetOptionalWeakFloatQType());
  }
  {
    ASSERT_OK_AND_ASSIGN(QTypePtr optional_like_qtype,
                         ToOptionalLikeQType(GetOptionalWeakFloatQType()));
    EXPECT_EQ(optional_like_qtype, GetOptionalWeakFloatQType());
  }
}

TEST(WeakQTypeTest, WeakFloatFingerprint) {
  const double value_a = 1.5;
  const double value_b = 2.5;
  const auto float64_qvalue_a = TypedValue::FromValue(value_a);
  ASSERT_OK_AND_ASSIGN(
      const auto weak_float_qvalue_a1,
      TypedValue::FromValueWithQType(value_a, GetWeakFloatQType()));
  ASSERT_OK_AND_ASSIGN(
      const auto weak_float_qvalue_a2,
      TypedValue::FromValueWithQType(value_a, GetWeakFloatQType()));
  ASSERT_OK_AND_ASSIGN(
      const auto weak_float_qvalue_b,
      TypedValue::FromValueWithQType(value_b, GetWeakFloatQType()));
  EXPECT_NE(float64_qvalue_a.GetFingerprint(),
            weak_float_qvalue_a1.GetFingerprint());
  EXPECT_EQ(weak_float_qvalue_a1.GetFingerprint(),
            weak_float_qvalue_a2.GetFingerprint());
  EXPECT_NE(weak_float_qvalue_a1.GetFingerprint(),
            weak_float_qvalue_b.GetFingerprint());
}

TEST(WeakQTypeTest, OptionalWeakFloatFingerprint) {
  const OptionalValue<double> value_a(1.5);
  const OptionalValue<double> value_b(2.5);
  const auto optional_float64_qvalue_a = TypedValue::FromValue(value_a);
  ASSERT_OK_AND_ASSIGN(
      const auto optional_weak_float_qvalue_a1,
      TypedValue::FromValueWithQType(value_a, GetOptionalWeakFloatQType()));
  ASSERT_OK_AND_ASSIGN(
      const auto optional_weak_float_qvalue_a2,
      TypedValue::FromValueWithQType(value_a, GetOptionalWeakFloatQType()));
  ASSERT_OK_AND_ASSIGN(
      const auto optional_weak_float_qvalue_b,
      TypedValue::FromValueWithQType(value_b, GetOptionalWeakFloatQType()));
  EXPECT_NE(optional_float64_qvalue_a.GetFingerprint(),
            optional_weak_float_qvalue_a1.GetFingerprint());
  EXPECT_EQ(optional_weak_float_qvalue_a1.GetFingerprint(),
            optional_weak_float_qvalue_a2.GetFingerprint());
  EXPECT_NE(optional_weak_float_qvalue_a1.GetFingerprint(),
            optional_weak_float_qvalue_b.GetFingerprint());
}

TEST(WeakQTypeTest, WeakFloatRepr) {
  ASSERT_OK_AND_ASSIGN(auto qvalue, TypedValue::FromValueWithQType(
                                        double{1.5}, GetWeakFloatQType()));
  EXPECT_THAT(qvalue.GenReprToken(), ReprTokenEq("weak_float{1.5}"));
}

TEST(WeakQTypeTest, OptionalWeakFloatRepr) {
  ASSERT_OK_AND_ASSIGN(
      auto qvalue, TypedValue::FromValueWithQType(OptionalValue<double>(1.5),
                                                  GetOptionalWeakFloatQType()));
  EXPECT_THAT(qvalue.GenReprToken(), ReprTokenEq("optional_weak_float{1.5}"));
}

TEST(WeakQTypeTest, OptionalWeakFloatMissingValueRepr) {
  ASSERT_OK_AND_ASSIGN(
      auto qvalue, TypedValue::FromValueWithQType(OptionalValue<double>(),
                                                  GetOptionalWeakFloatQType()));
  EXPECT_THAT(qvalue.GenReprToken(), ReprTokenEq("optional_weak_float{NA}"));
}

}  // namespace
}  // namespace arolla
