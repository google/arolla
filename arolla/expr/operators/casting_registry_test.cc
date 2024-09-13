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
#include "arolla/expr/operators/casting_registry.h"

#include <cstdint>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/derived_qtype_cast_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/operators/bootstrap_operators.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/bytes.h"

namespace arolla::expr_operators {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::Leaf;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::HasSubstr;

TEST(CastingRegistryTest, CommonType) {
  const CastingRegistry* reg = CastingRegistry::GetInstance();
  EXPECT_THAT(reg->CommonType({GetQType<int32_t>(), GetQType<int32_t>()}),
              IsOkAndHolds(GetQType<int32_t>()));
  EXPECT_THAT(reg->CommonType({GetQType<uint64_t>(), GetQType<uint64_t>()}),
              IsOkAndHolds(GetQType<uint64_t>()));
  EXPECT_THAT(reg->CommonType({GetQType<int32_t>(), GetQType<int64_t>()}),
              IsOkAndHolds(GetQType<int64_t>()));
  EXPECT_THAT(
      reg->CommonType({GetQType<int32_t>(), GetOptionalQType<int32_t>()}),
      IsOkAndHolds(GetOptionalQType<int32_t>()));
  EXPECT_THAT(
      reg->CommonType({GetQType<uint64_t>(), GetOptionalQType<uint64_t>()}),
      IsOkAndHolds(GetOptionalQType<uint64_t>()));
  EXPECT_THAT(
      reg->CommonType({GetQType<int32_t>(), GetOptionalQType<int64_t>()}),
      IsOkAndHolds(GetOptionalQType<int64_t>()));
  EXPECT_THAT(reg->CommonType({GetQType<int32_t>(), GetQType<Bytes>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("no common QType for (INT32,BYTES)")));
  EXPECT_THAT(reg->CommonType({GetQType<int32_t>(), GetQType<uint64_t>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("no common QType for (INT32,UINT64)")));
  EXPECT_THAT(reg->CommonType({GetQType<int32_t>(), GetQType<int64_t>()}),
              IsOkAndHolds(GetQType<int64_t>()));
  EXPECT_THAT(
      reg->CommonType({GetQType<int32_t>(), GetQType<Bytes>()}).status(),
      StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(
      reg->CommonType({GetOptionalQType<int32_t>(), GetQType<int64_t>()}),
      IsOkAndHolds(GetOptionalQType<int64_t>()));
}

TEST(CastingRegistryTest, GetCast) {
  const CastingRegistry* reg = CastingRegistry::GetInstance();
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()));
  EXPECT_THAT(reg->GetCast(x, GetOptionalQType<int64_t>(),
                           /*implicit_only=*/true),
              IsOkAndHolds(EqualsExpr(
                  CallOp("core.to_optional", {CallOp("core.to_int64", {x})}))));
}

TEST(CastingRegistryTest, GetCastWithBroadcasting) {
  const CastingRegistry* reg = CastingRegistry::GetInstance();

  // Trigger DenseArray<int64_t> QType registration.
  GetDenseArrayQType<int64_t>();

  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()));
  ASSERT_OK_AND_ASSIGN(
      auto shape,
      WithQTypeAnnotation(Leaf("shape"), GetQType<DenseArrayShape>()));
  EXPECT_THAT(
      reg->GetCast(x, GetDenseArrayQType<int64_t>(),
                   /*implicit_only=*/true, shape),
      IsOkAndHolds(EqualsExpr(CallOp("core.const_with_shape",
                                     {shape, CallOp("core.to_int64", {x})}))));
}

TEST(CastingRegistryTest, GetCastFromWeakType) {
  const CastingRegistry* reg = CastingRegistry::GetInstance();
  expr::ExprOperatorPtr upcast_op =
      std::make_shared<expr::DerivedQTypeUpcastOperator>(GetWeakFloatQType());

  // WEAK_FLOAT -> OPTIONAL_FLOAT64
  {
    ASSERT_OK_AND_ASSIGN(auto x,
                         WithQTypeAnnotation(Leaf("x"), GetWeakFloatQType()));
    EXPECT_THAT(reg->GetCast(x, GetOptionalQType<double>(),
                             /*implicit_only=*/true),
                IsOkAndHolds(EqualsExpr(
                    CallOp("core.to_optional", {CallOp(upcast_op, {x})}))));
  }

  // OPTIONAL_WEAK_FLOAT -> OPTIONAL_FLOAT64
  {
    expr::ExprOperatorPtr opt_upcast_op =
        std::make_shared<expr::DerivedQTypeUpcastOperator>(
            GetOptionalWeakFloatQType());
    ASSERT_OK_AND_ASSIGN(
        auto x, WithQTypeAnnotation(Leaf("x"), GetOptionalWeakFloatQType()));
    EXPECT_THAT(reg->GetCast(x, GetOptionalQType<double>(),
                             /*implicit_only=*/true),
                IsOkAndHolds(EqualsExpr(CallOp(opt_upcast_op, {x}))));
  }

  // WEAK_FLOAT -> OPTIONAL_WEAK_FLOAT
  {
    ASSERT_OK_AND_ASSIGN(auto x,
                         WithQTypeAnnotation(Leaf("x"), GetWeakFloatQType()));
    EXPECT_THAT(reg->GetCast(x, GetOptionalWeakFloatQType(),
                             /*implicit_only=*/true),
                IsOkAndHolds(EqualsExpr(CallOp("core.to_optional", {x}))));
  }

  // WEAK_FLOAT -> DENSE_ARRAY_FLOAT32
  {
    ASSERT_OK_AND_ASSIGN(auto x,
                         WithQTypeAnnotation(Leaf("x"), GetWeakFloatQType()));
    ASSERT_OK_AND_ASSIGN(
        auto shape,
        WithQTypeAnnotation(Leaf("shape"), GetQType<DenseArrayShape>()));

    // Trigger DenseArray<float> QType registration.
    GetDenseArrayQType<float>();

    EXPECT_THAT(
        reg->GetCast(x, GetDenseArrayQType<float>(),
                     /*implicit_only=*/true, shape),
        IsOkAndHolds(EqualsExpr(CallOp(
            "core.const_with_shape",
            {shape, CallOp("core.to_float32", {CallOp(upcast_op, {x})})}))));
  }
}

TEST(CastingRegistryTest, GetCastToWeakType) {
  const CastingRegistry* reg = CastingRegistry::GetInstance();
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));

  // FLOAT32 -> WEAK_FLOAT
  {
    ASSERT_OK_AND_ASSIGN(auto x,
                         WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
    EXPECT_THAT(reg->GetCast(x, GetWeakFloatQType(),
                             /*implicit_only=*/false),
                IsOkAndHolds(EqualsExpr(CoreToWeakFloat(x))));
  }

  // OPTIONAL_FLOAT32 -> OPTIONAL_WEAK_FLOAT
  {
    ASSERT_OK_AND_ASSIGN(
        auto x, WithQTypeAnnotation(Leaf("x"), GetOptionalQType<float>()));
    EXPECT_THAT(reg->GetCast(x, GetOptionalWeakFloatQType(),
                             /*implicit_only=*/false),
                IsOkAndHolds(EqualsExpr(CoreToWeakFloat(x))));
  }

  // FLOAT32 -> OPTIONAL_WEAK_FLOAT
  {
    ASSERT_OK_AND_ASSIGN(auto x,
                         WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
    EXPECT_THAT(reg->GetCast(x, GetOptionalWeakFloatQType(),
                             /*implicit_only=*/false),
                IsOkAndHolds(EqualsExpr(
                    CallOp("core.to_optional", {CoreToWeakFloat(x)}))));
  }

  // DENSE_ARRAY_FLOAT32 -> DENSE_ARRAY_WEAK_FLOAT
  {
    // Trigger array QTypes registration.
    GetDenseArrayQType<float>();
    GetDenseArrayWeakFloatQType();

    ASSERT_OK_AND_ASSIGN(
        auto x, WithQTypeAnnotation(Leaf("x"), GetDenseArrayQType<float>()));
    EXPECT_THAT(reg->GetCast(x, GetDenseArrayWeakFloatQType(),
                             /*implicit_only=*/false),
                IsOkAndHolds(EqualsExpr(CoreToWeakFloat(x))));
  }

  // FLOAT32 -> WEAK_FLOAT, implicit only
  {
    ASSERT_OK_AND_ASSIGN(auto x,
                         WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
    EXPECT_THAT(
        reg->GetCast(x, GetWeakFloatQType(),
                     /*implicit_only=*/true),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr(
                "implicit casting from FLOAT32 to WEAK_FLOAT is not allowed")));
  }
}

}  // namespace
}  // namespace arolla::expr_operators
