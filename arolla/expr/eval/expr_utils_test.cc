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
#include "arolla/expr/eval/expr_utils.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsExpr;
using ::testing::ElementsAre;
using ::testing::Pointee;
using ::testing::Property;
using ::testing::WhenDynamicCastTo;

class ExptUtilsTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(ExptUtilsTest, ExtractLambda) {
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("math.add", {CallOp("math.add", {Leaf("x"), Leaf("y")}),
                                     Literal(1.0)}));

  auto is_op = [](const ExprNodePtr& node) -> absl::StatusOr<bool> {
    return node->is_op();
  };

  ASSERT_OK_AND_ASSIGN(auto expr_as_lambda, ExtractLambda(expr, is_op));
  EXPECT_THAT(expr_as_lambda->node_deps(),
              ElementsAre(EqualsExpr(Leaf("x")), EqualsExpr(Leaf("y")),
                          EqualsExpr(Literal(1.0))));
  EXPECT_THAT(
      expr_as_lambda->op().get(),
      WhenDynamicCastTo<const LambdaOperator*>(Pointee(Property(
          &LambdaOperator::lambda_body,
          EqualsExpr(CallOp(
              "math.add",
              {CallOp("math.add", {Placeholder("_0"), Placeholder("_1")}),
               Placeholder("_2")}))))));
}

TEST_F(ExptUtilsTest, ExtractLambda_WithSameSubnodes) {
  ASSERT_OK_AND_ASSIGN(
      auto to_keep_out,
      CallOp("math.add",
             {CallOp("math.add", {Leaf("x"), Leaf("y")}), Literal(1.0)}));
  // NOTE: nodes that we keep inside cannot contain leaves because of the lambda
  // restriction.
  ASSERT_OK_AND_ASSIGN(auto to_keep_in,
                       CallOp("math.add", {Literal(2.0), Literal(1.0)}));
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add",
             {CallOp("math.add", {to_keep_out, to_keep_in}), to_keep_out}));

  auto keep_in = [=](const ExprNodePtr& node) -> absl::StatusOr<bool> {
    return node->fingerprint() != to_keep_out->fingerprint();
  };

  ASSERT_OK_AND_ASSIGN(auto expr_as_lambda, ExtractLambda(expr, keep_in));
  EXPECT_THAT(expr_as_lambda->node_deps(),
              ElementsAre(EqualsExpr(to_keep_out)));
  EXPECT_THAT(
      expr_as_lambda->op().get(),
      WhenDynamicCastTo<const LambdaOperator*>(Pointee(Property(
          &LambdaOperator::lambda_body,
          EqualsExpr(CallOp(
              "math.add", {CallOp("math.add", {Placeholder("_0"), to_keep_in}),
                           Placeholder("_0")}))))));
}

TEST_F(ExptUtilsTest, ExtractLambda_AllFalse) {
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("math.add", {CallOp("math.add", {Leaf("x"), Leaf("y")}),
                                     Literal(1.0)}));
  auto all_false = [](const ExprNodePtr& node) -> absl::StatusOr<bool> {
    return false;
  };
  ASSERT_OK_AND_ASSIGN(auto expr_as_lambda, ExtractLambda(expr, all_false));
  EXPECT_THAT(expr_as_lambda->node_deps(), ElementsAre(EqualsExpr(expr)));
  EXPECT_THAT(
      expr_as_lambda->op().get(),
      WhenDynamicCastTo<const LambdaOperator*>(Pointee(Property(
          &LambdaOperator::lambda_body, EqualsExpr(Placeholder("_0"))))));
}

TEST_F(ExptUtilsTest, ExtractLambda_FilterFails) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add",
             {CallOp("math.subtract", {Leaf("x"), Leaf("y")}), Literal(1.0)}));
  auto returns_error = [](const ExprNodePtr& node) -> absl::StatusOr<bool> {
    if (node->is_op() && node->op()->display_name() == "math.subtract") {
      return absl::InvalidArgumentError("foo");
    }
    return true;
  };
  EXPECT_THAT(ExtractLambda(expr, returns_error),
              StatusIs(absl::StatusCode::kInvalidArgument, "foo"));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
