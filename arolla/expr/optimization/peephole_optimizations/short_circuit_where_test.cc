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
#include "arolla/expr/optimization/peephole_optimizations/short_circuit_where.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/optimization/peephole_optimizations/bool.h"
#include "arolla/expr/optimization/peephole_optimizations/const_with_shape.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;

class ShortCircuitWhereOptimizationsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(
        optimizer_, CreatePeepholeOptimizer({ShortCircuitWhereOptimizations}));
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

TEST_F(ShortCircuitWhereOptimizationsTest, ScalarCondition) {
  ASSERT_OK_AND_ASSIGN(
      auto shape,
      WithQTypeAnnotation(Leaf("shape"), GetQType<DenseArrayShape>()));
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto x_mul_y, CallOp("math.multiply", {x, y}));
  ASSERT_OK_AND_ASSIGN(
      auto scalar_cond,
      WithQTypeAnnotation(Leaf("cond"), GetQType<OptionalUnit>()));

  {
    // `core.where` transformed into `core._short_circuit_where` for scalar
    // conditions
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.where", {scalar_cond, x_plus_y, x_mul_y})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core._short_circuit_where",
                                         {scalar_cond, x_plus_y, x_mul_y})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // `core.where(core.const_with_shape(...), ...)` transformed
    // into `core._short_circuit_where`.
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core.where", {CallOp("core.const_with_shape",
                                                    {shape, scalar_cond}),
                                             x_plus_y, x_mul_y})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("core._short_circuit_where",
                                         {scalar_cond, x_plus_y, x_mul_y})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(ShortCircuitWhereOptimizationsTest, ArrayCondition) {
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto x_mul_y, CallOp("math.multiply", {x, y}));
  ASSERT_OK_AND_ASSIGN(
      auto array_cond,
      WithQTypeAnnotation(Leaf("cond"), GetDenseArrayQType<Unit>()));
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("core.where", {array_cond, x_plus_y, x_mul_y}));
  ASSERT_OK_AND_ASSIGN(auto actual_expr, ApplyOptimizer(expr));
  ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(expr));
  EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
}

TEST_F(ShortCircuitWhereOptimizationsTest, UntypedCondition) {
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto x_mul_y, CallOp("math.multiply", {x, y}));
  auto untyped_cond = Leaf("cond");
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("core.where", {untyped_cond, x_plus_y, x_mul_y}));
  ASSERT_OK_AND_ASSIGN(auto actual_expr, ApplyOptimizer(expr));
  ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(expr));
  EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
}

TEST_F(ShortCircuitWhereOptimizationsTest, ConstScalarCondition) {
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto x_mul_y, CallOp("math.multiply", {x, y}));
  {
    // `present ? x + y : x * y` transformed into `x * y`
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core._short_circuit_where",
                              {Literal(kPresent), x_plus_y, x_mul_y})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(x_plus_y));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    // `missing ? x + y : x * y` transformed into `x * y`
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(CallOp("core._short_circuit_where",
                              {Literal(kMissing), x_plus_y, x_mul_y})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(x_mul_y));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(ShortCircuitWhereOptimizationsTest, EndToEndWithBroadcastedCondition) {
  ASSERT_OK_AND_ASSIGN(auto peephole_optimizer,
                       CreatePeepholeOptimizer({ShortCircuitWhereOptimizations,
                                                BoolOptimizations,
                                                ConstWithShapeOptimizations}));
  auto optimize = [&](const ExprNodePtr& node) -> absl::StatusOr<ExprNodePtr> {
    ASSIGN_OR_RETURN(auto new_node, ToLowerNode(node));
    if (new_node->fingerprint() != node->fingerprint()) {
      return new_node;
    }
    return peephole_optimizer->ApplyToNode(node);
  };

  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<int32_t>()));

  auto true_case = WithQTypeAnnotation(Leaf("true_case"), GetQType<float>());
  auto false_or_missing_case =
      WithQTypeAnnotation(Leaf("false_or_missing_case"), GetQType<float>());
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto x_mul_y, CallOp("math.multiply", {x, y}));

  ASSERT_OK_AND_ASSIGN(
      auto shape, CallOp("core.shape_of", {CallOp("core.has", {true_case})}));
  // Two expressions computing the same shape, but in different ways.
  ASSERT_OK_AND_ASSIGN(
      auto relative_shape,
      CallOp("core.shape_of",
             {CallOp("core.has",
                     {CallOp("core.const_with_shape", {shape, true_case})})}));

  // A complicated condition, constructed from several expressions, each
  // broadcasted into a different (although relative) shape.
  ASSERT_OK_AND_ASSIGN(
      auto broadcasted_condition,
      CallOp(
          "bool.logical_or",
          {CallOp(
               "bool.equal",
               {CallOp("core.const_with_shape", {relative_shape, x_plus_y}),
                CallOp("core.const_with_shape", {relative_shape, Literal(1)})}),
           CallOp("bool.equal",
                  {CallOp("core.const_with_shape", {relative_shape, x_mul_y}),
                   CallOp("core.const_with_shape",
                          {relative_shape, Literal(1)})})}));
  // A logical_if operator with a complex condition and identical "false" and
  // "missing" cases.
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("bool.logical_if",
                              {broadcasted_condition, true_case,
                               false_or_missing_case, false_or_missing_case}));

  // We expect the optimization engine to automatically remove theunnecessary
  // broadcasting.
  ASSERT_OK_AND_ASSIGN(
      auto unbroadcasted_condition,
      CallOp("bool.logical_or", {CallOp("bool.equal", {x_plus_y, Literal(1)}),
                                 CallOp("bool.equal", {x_mul_y, Literal(1)})}));

  // Verify that the parts of the `expr` are optimized correctly.
  EXPECT_THAT(DeepTransform(relative_shape, optimize),
              IsOkAndHolds(EqualsExpr(ToLowest(shape))));
  EXPECT_THAT(
      DeepTransform(broadcasted_condition, optimize),
      IsOkAndHolds(EqualsExpr(ToLowest(
          CallOp("core.const_with_shape", {shape, unbroadcasted_condition})))));

  auto optimize_and_broadcast =
      [&](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
    ASSIGN_OR_RETURN(node, optimize(std::move(node)));
    auto true_literal = Literal(true);
    // Simulate broadcasting for the core.equal operator.
    if (node->is_op() && node->op()->display_name() == "core.equal" &&
        node->node_deps()[1]->fingerprint() == true_literal->fingerprint()) {
      return CallOp("core.equal",
                    {node->node_deps()[0],
                     CallOp("core.const_with_shape", {shape, true_literal})});
    }
    return node;
  };
  // Verify that the `expr` altogether is short circuited.
  EXPECT_THAT(DeepTransform(expr, optimize_and_broadcast),
              IsOkAndHolds(EqualsExpr(ToLowest(
                  CallOp("core._short_circuit_where",
                         // Note that another optimization replaced bool.* with
                         // core.* operators.
                         {CallOp("core.presence_or",
                                 {CallOp("core.equal", {x_plus_y, Literal(1)}),
                                  CallOp("core.equal", {x_mul_y, Literal(1)})}),
                          true_case, false_or_missing_case})))));
}

}  // namespace
}  // namespace arolla::expr
