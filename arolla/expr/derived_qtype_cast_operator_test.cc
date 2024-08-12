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
#include "arolla/expr/derived_qtype_cast_operator.h"

#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla::expr {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::InvokeExprOperator;
using ::arolla::testing::ReprTokenEq;
using ::testing::HasSubstr;

struct TimeQType final : BasicDerivedQType {
  TimeQType()
      : BasicDerivedQType(ConstructorArgs{
            .name = "TIME",
            .base_qtype = GetQType<float>(),
        }) {}

  ReprToken UnsafeReprToken(const void* source) const override {
    auto result = GetBaseQType()->UnsafeReprToken(source);
    result.str += "s";
    return result;
  }

  static QTypePtr get() {
    static const Indestructible<TimeQType> result;
    return result.get();
  }
};

struct DistanceQType final : BasicDerivedQType {
  DistanceQType()
      : BasicDerivedQType(ConstructorArgs{
            .name = "DISTANCE",
            .base_qtype = GetQType<float>(),
        }) {}

  ReprToken UnsafeReprToken(const void* source) const override {
    auto result = GetBaseQType()->UnsafeReprToken(source);
    result.str += "m";
    return result;
  }

  static QTypePtr get() {
    static const Indestructible<DistanceQType> result;
    return result.get();
  }
};

class DerivedQTypeCastOperatorTests : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

TEST_F(DerivedQTypeCastOperatorTests, UpcastDistance_WithDistanceInput) {
  ExprOperatorPtr upcast_distance =
      std::make_shared<DerivedQTypeUpcastOperator>(DistanceQType::get());
  ASSERT_OK_AND_ASSIGN(
      auto d, TypedValue::FromValueWithQType(6.28f, DistanceQType::get()));
  ASSERT_OK_AND_ASSIGN(auto f32,
                       InvokeExprOperator<TypedValue>(upcast_distance, d));
  EXPECT_EQ(f32.GetType(), GetQType<float>());
  EXPECT_THAT(f32.GenReprToken(),
              ReprTokenEq("6.28", ReprToken::kSafeForNegation));
}

TEST_F(DerivedQTypeCastOperatorTests, UpcastDistance_WithFloat32Input) {
  ExprOperatorPtr upcast_distance =
      std::make_shared<DerivedQTypeUpcastOperator>(DistanceQType::get());
  EXPECT_THAT(InvokeExprOperator<TypedValue>(upcast_distance, 6.28f),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected DISTANCE, got value: FLOAT32")));
}

TEST_F(DerivedQTypeCastOperatorTests, UpcastFloat32_WithDistanceInput) {
  ExprOperatorPtr upcast_float32 =
      std::make_shared<DerivedQTypeUpcastOperator>(GetQType<float>());
  ASSERT_OK_AND_ASSIGN(
      auto d, TypedValue::FromValueWithQType(6.28f, DistanceQType::get()));
  EXPECT_THAT(InvokeExprOperator<TypedValue>(upcast_float32, d),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected FLOAT32, got value: DISTANCE")));
}

TEST_F(DerivedQTypeCastOperatorTests, UpcastFloat32_WithFloat32Input) {
  ExprOperatorPtr upcast_float32 =
      std::make_shared<DerivedQTypeUpcastOperator>(GetQType<float>());
  ASSERT_OK_AND_ASSIGN(auto f32,
                       InvokeExprOperator<TypedValue>(upcast_float32, 6.28f));
  EXPECT_EQ(f32.GetType(), GetQType<float>());
  EXPECT_THAT(f32.GenReprToken(),
              ReprTokenEq("6.28", ReprToken::kSafeForNegation));
}

TEST_F(DerivedQTypeCastOperatorTests, DowncastDistance_WithDistanceInput) {
  ExprOperatorPtr downcast_distance =
      std::make_shared<DerivedQTypeDowncastOperator>(DistanceQType::get());
  ASSERT_OK_AND_ASSIGN(
      auto d, TypedValue::FromValueWithQType(6.28f, DistanceQType::get()));
  EXPECT_THAT(InvokeExprOperator<TypedValue>(downcast_distance, d),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected FLOAT32, got value: DISTANCE")));
}

TEST_F(DerivedQTypeCastOperatorTests, DowncastDistance_WithFloat32Input) {
  ExprOperatorPtr downcast_distance =
      std::make_shared<DerivedQTypeDowncastOperator>(DistanceQType::get());
  ASSERT_OK_AND_ASSIGN(
      auto d, InvokeExprOperator<TypedValue>(downcast_distance, 6.28f));
  EXPECT_EQ(d.GetType(), DistanceQType::get());
  EXPECT_THAT(d.GenReprToken(),
              ReprTokenEq("6.28m", ReprToken::kSafeForNegation));
}

TEST_F(DerivedQTypeCastOperatorTests, DowncastFloat32_WithDistanceInput) {
  ExprOperatorPtr downcast_float32 =
      std::make_shared<DerivedQTypeDowncastOperator>(GetQType<float>());
  ASSERT_OK_AND_ASSIGN(
      auto d, TypedValue::FromValueWithQType(6.28f, DistanceQType::get()));
  EXPECT_THAT(InvokeExprOperator<TypedValue>(downcast_float32, d),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected FLOAT32, got value: DISTANCE")));
}

TEST_F(DerivedQTypeCastOperatorTests, DowncastFloat32_WithFloat32Input) {
  ExprOperatorPtr downcast_float32 =
      std::make_shared<DerivedQTypeDowncastOperator>(GetQType<float>());
  ASSERT_OK_AND_ASSIGN(auto f32,
                       InvokeExprOperator<TypedValue>(downcast_float32, 6.28f));
  EXPECT_EQ(f32.GetType(), GetQType<float>());
  EXPECT_THAT(f32.GenReprToken(),
              ReprTokenEq("6.28", ReprToken::kSafeForNegation));
}

}  // namespace
}  // namespace arolla::expr
