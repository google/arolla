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
#include "arolla/expr/optimization/peephole_optimizations/const_with_shape.h"

#include <memory>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;

class ConstWithShapeOptimizationsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitArolla();
    ASSERT_OK_AND_ASSIGN(
        optimizer_, CreatePeepholeOptimizer({ConstWithShapeOptimizations}));
    GetDenseArrayQType<float>();  // Trigger the registration of
    GetDenseArrayQType<Unit>();   // DENSE_ARRAY_{UNIT,FLOAT32}.
  }

  absl::StatusOr<ExprNodePtr> ApplyOptimizer(
      absl::StatusOr<ExprNodePtr> status_or_expr) const {
    ASSIGN_OR_RETURN(auto expr, ToLowest(status_or_expr));
    return ToLowest(optimizer_->ApplyToNode(expr));
  }

  absl::StatusOr<ExprNodePtr> ToLowest(
      const absl::StatusOr<ExprNodePtr>& status_or_expr) const {
    if (!status_or_expr.ok()) {
      return std::move(status_or_expr).status();
    }
    return ::arolla::expr::ToLowest(*status_or_expr);
  }

  std::unique_ptr<PeepholeOptimizer> optimizer_;
};

TEST_F(ConstWithShapeOptimizationsTest, UnaryPointwiseOpOptimizations) {
  ASSERT_OK_AND_ASSIGN(
      auto shape,
      WithQTypeAnnotation(Leaf("shape"), GetQType<DenseArrayShape>()));
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  {
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp(
            "math.exp", {CallOp("core.const_with_shape", {shape, x_plus_y})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.const_with_shape",
                        {shape, CallOp("math.exp", {x_plus_y})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp(
            "core.has", {CallOp("core.const_with_shape", {shape, x_plus_y})})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.const_with_shape", {shape, Literal(Unit{})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(ConstWithShapeOptimizationsTest, BinaryPointwiseOpOptimizations) {
  ASSERT_OK_AND_ASSIGN(
      auto shape,
      WithQTypeAnnotation(Leaf("shape"), GetQType<DenseArrayShape>()));
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto x_minus_y, CallOp("math.subtract", {x, y}));
  ASSERT_OK_AND_ASSIGN(
      auto actual_expr,
      ApplyOptimizer(
          CallOp("core.equal",
                 {CallOp("core.const_with_shape", {shape, x_plus_y}),
                  CallOp("core.const_with_shape", {shape, x_minus_y})})));
  ASSERT_OK_AND_ASSIGN(
      auto expected_expr,
      ToLowest(CallOp("core.const_with_shape",
                      {shape, CallOp("core.equal", {x_plus_y, x_minus_y})})));
  EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
}

TEST_F(ConstWithShapeOptimizationsTest, BinaryOpWithConstantOptimizations) {
  ASSERT_OK_AND_ASSIGN(
      auto shape,
      WithQTypeAnnotation(Leaf("shape"), GetQType<DenseArrayShape>()));
  ASSERT_OK_AND_ASSIGN(
      auto x, WithQTypeAnnotation(Leaf("x"), GetOptionalQType<float>()));
  ASSERT_OK_AND_ASSIGN(
      auto y, WithQTypeAnnotation(Leaf("y"), GetOptionalQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto x_minus_y, CallOp("math.subtract", {x, y}));
  ASSERT_OK_AND_ASSIGN(
      auto expected_expr,
      ToLowest(
          CallOp("core.const_with_shape",
                 {shape, CallOp("core.presence_or", {x_plus_y, x_minus_y})})));
  {
    SCOPED_TRACE("left expanded, right is not expanded");
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp(
            "core.presence_or",
            {CallOp("core.const_with_shape", {shape, x_plus_y}), x_minus_y})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    SCOPED_TRACE("left is not expanded, right is expanded");
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp(
            "core.presence_or",
            {x_plus_y, CallOp("core.const_with_shape", {shape, x_minus_y})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(ConstWithShapeOptimizationsTest, ArrayShapeOptimizations) {
  ASSERT_OK_AND_ASSIGN(
      auto shape,
      WithQTypeAnnotation(Leaf("shape"), GetQType<DenseArrayShape>()));
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(
      auto actual_expr,
      ApplyOptimizer(CallOp(
          "core.shape_of", {CallOp("core.has", {CallOp("core.const_with_shape",
                                                       {shape, x_plus_y})})})));
  EXPECT_THAT(actual_expr, EqualsExpr(shape));
}

TEST_F(ConstWithShapeOptimizationsTest, ArrayShapeOptimizationsForPresence) {
  ASSERT_OK_AND_ASSIGN(
      auto shape,
      WithQTypeAnnotation(Leaf("shape"), GetQType<DenseArrayShape>()));
  ASSERT_OK_AND_ASSIGN(
      auto actual_expr,
      ApplyOptimizer(
          CallOp("core.shape_of",
                 {CallOp("core.const_with_shape",
                         {shape, Literal<OptionalUnit>(std::nullopt)})})));
  EXPECT_THAT(actual_expr, EqualsExpr(shape));
}

}  // namespace
}  // namespace arolla::expr
