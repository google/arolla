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
#include "arolla/expr/eval/casting.h"

#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/derived_qtype_cast_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qexpr/operator_factory.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::Test;

template <typename T>
absl::Status AddFakeAddOperator(OperatorRegistry& registry) {
  ASSIGN_OR_RETURN(
      auto op,
      OperatorFactory().WithName("math.add").BuildFromFunction([](T x, T) {
        return x;
      }));
  return registry.RegisterOperator(std::move(op));
}

absl::Status AddFakeLowerOperator(OperatorRegistry& registry) {
  ASSIGN_OR_RETURN(
      auto op,
      OperatorFactory().WithName("strings.lower").BuildFromFunction([](Text x) {
        return x;
      }));
  return registry.RegisterOperator(std::move(op));
}

class CastingTest : public Test {
 protected:
  void SetUp() override {
    InitArolla();

    backend_directory_ = std::make_shared<OperatorRegistry>();
    ASSERT_OK(AddFakeAddOperator<float>(*backend_directory_));
    ASSERT_OK(AddFakeAddOperator<double>(*backend_directory_));
    ASSERT_OK(AddFakeAddOperator<DenseArray<float>>(*backend_directory_));
    ASSERT_OK(AddFakeAddOperator<DenseArray<double>>(*backend_directory_));
    ASSERT_OK(AddFakeLowerOperator(*backend_directory_));
    options_ = DynamicEvaluationEngineOptions{.operator_directory =
                                                  backend_directory_.get()};
  }

  const QTypePtr f32_ = GetQType<float>();
  const QTypePtr f64_ = GetQType<double>();
  const QTypePtr of64_ = GetOptionalQType<double>();
  const QTypePtr text_ = GetQType<Text>();

  std::shared_ptr<OperatorRegistry> backend_directory_;
  DynamicEvaluationEngineOptions options_;
};

TEST_F(CastingTest, Basic) {
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {Literal<double>(2.),
                                                      Literal<float>(1.f)}));
  ASSERT_OK_AND_ASSIGN(
      auto cast_expr,
      CallOp("math.add", {Literal<double>(2.),
                          CallOp("core.to_float64", {Literal<float>(1.f)})}));
  ASSERT_OK_AND_ASSIGN(auto actual_expr, CastingTransformation(options_, expr));
  EXPECT_THAT(actual_expr, EqualsExpr(cast_expr));
}

TEST_F(CastingTest, WithOutputCasting_WeakFloat) {
  ASSERT_OK_AND_ASSIGN(auto weak_1,
                       TypedValue::FromValueWithQType(1., GetWeakFloatQType()));
  ASSERT_OK_AND_ASSIGN(auto weak_2,
                       TypedValue::FromValueWithQType(2., GetWeakFloatQType()));
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("math.add", {Literal(weak_1), Literal(weak_2)}));
  const auto upcast_op =
      std::make_shared<expr::DerivedQTypeUpcastOperator>(GetWeakFloatQType());
  const auto downcast_op =
      std::make_shared<expr::DerivedQTypeDowncastOperator>(GetWeakFloatQType());
  ASSERT_OK_AND_ASSIGN(
      auto cast_expr,
      CallOp(downcast_op,
             {CallOp("math.add", {CallOp(upcast_op, {Literal(weak_1)}),
                                  CallOp(upcast_op, {Literal(weak_2)})})}));
  ASSERT_OK_AND_ASSIGN(auto actual_expr, CastingTransformation(options_, expr));
  EXPECT_THAT(actual_expr, EqualsExpr(cast_expr));
}

TEST_F(CastingTest, WithOutputCasting_WeakFloatArray) {
  auto x = WithQTypeAnnotation(Leaf("x"), GetDenseArrayWeakFloatQType());
  auto y = WithQTypeAnnotation(Leaf("y"), GetDenseArrayWeakFloatQType());
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x, y}));
  const auto upcast_op = std::make_shared<expr::DerivedQTypeUpcastOperator>(
      GetDenseArrayWeakFloatQType());
  const auto downcast_op = std::make_shared<expr::DerivedQTypeDowncastOperator>(
      GetDenseArrayWeakFloatQType());
  ASSERT_OK_AND_ASSIGN(
      auto cast_expr,
      CallOp(downcast_op, {CallOp("math.add", {CallOp(upcast_op, {x}),
                                               CallOp(upcast_op, {y})})}));
  ASSERT_OK_AND_ASSIGN(auto actual_expr, CastingTransformation(options_, expr));
  EXPECT_THAT(actual_expr, EqualsExpr(cast_expr));
}

TEST_F(CastingTest, PassThroughSupportedOperator) {
  auto x = WithQTypeAnnotation(Leaf("x"), GetDenseArrayQType<double>());
  auto y = WithQTypeAnnotation(Leaf("x"), GetDenseArrayQType<double>());
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x, y}));

  // Operator is unchanged because backend has native support for adding
  // dense arrays of doubles.
  EXPECT_THAT(CastingTransformation(options_, expr),
              IsOkAndHolds(EqualsExpr(expr)));
}

TEST_F(CastingTest, CastDenseArrayToDoubleOperator) {
  auto x = WithQTypeAnnotation(Leaf("x"), GetDenseArrayQType<float>());
  auto y = WithQTypeAnnotation(Leaf("x"), GetDenseArrayQType<double>());
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x, y}));

  // Narrower (float) dense array type will be cast to wider (double) type.
  ASSERT_OK_AND_ASSIGN(auto expected_expr,
                       CallOp("math.add", {CallOp("core.to_float64", {x}), y}));
  EXPECT_THAT(CastingTransformation(options_, expr),
              IsOkAndHolds(EqualsExpr(expected_expr)));
}

TEST_F(CastingTest, Broadcasting) {
  auto x = WithQTypeAnnotation(Leaf("x"), f64_);
  auto y = WithQTypeAnnotation(Leaf("x"), GetDenseArrayQType<double>());
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x, y}));
  EXPECT_THAT(CastingTransformation(options_, expr),
              IsOkAndHolds(EqualsExpr(
                  CallOp("math.add", {CallOp("core.const_with_shape",
                                             {CallOp("core.shape_of", {y}), x}),
                                      y}))));
}

TEST_F(CastingTest, BroadcastingWithCasting) {
  auto x = WithQTypeAnnotation(Leaf("x"), f32_);
  auto y = WithQTypeAnnotation(Leaf("x"), GetDenseArrayQType<double>());
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x, y}));
  EXPECT_THAT(CastingTransformation(options_, expr),
              IsOkAndHolds(EqualsExpr(
                  CallOp("math.add", {CallOp("core.const_with_shape",
                                             {CallOp("core.shape_of", {y}),
                                              CallOp("core.to_float64", {x})}),
                                      y}))));
}

TEST_F(CastingTest, BroadcastingWithCastingToOptional) {
  auto x = WithQTypeAnnotation(Leaf("x"), of64_);
  auto y = WithQTypeAnnotation(Leaf("x"), GetDenseArrayQType<double>());
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x, y}));
  EXPECT_THAT(CastingTransformation(options_, expr),
              IsOkAndHolds(EqualsExpr(
                  CallOp("math.add", {CallOp("core.const_with_shape",
                                             {CallOp("core.shape_of", {y}), x}),
                                      y}))));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
