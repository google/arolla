// Copyright 2025 Google LLC
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

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/expr/eval/expr_stack_trace.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/testing/testing.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsExpr;
using ::testing::ElementsAre;
using ::testing::Pointee;
using ::testing::Property;
using ::testing::WhenDynamicCastTo;

// ExprStackTrace implementation that records all the calls.
struct RecordingExprStackTrace : public ExprStackTrace {
  void AddTrace(const ExprNodePtr& transformed_node,
                const ExprNodePtr& original_node) final {
    calls.push_back(absl::StrCat(GetDebugSnippet(original_node), " -> ",
                                 GetDebugSnippet(transformed_node)));
  }
  void InitNode(const ExprNodePtr& node) final {}
  absl::Status AnnotateWithNodeSourceLocations(
      absl::Status status, const ExprNodePtr& failed_node) const final {
    return status;
  }
  BoundExprStackTraceFactory StartBinding() const final { return {}; }

  std::vector<std::string> calls;
};

TEST(ExptUtilsTest, ExtractLambda) {
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

TEST(ExptUtilsTest, ExtractLambda_WithSameSubnodes) {
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

TEST(ExptUtilsTest, ExtractLambda_AllFalse) {
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

TEST(ExptUtilsTest, ExtractLambda_FilterFails) {
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

TEST(ExptUtilsTest, ExtractLambda_StackTrace) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.subtract",
             {CallOp("math.add", {Leaf("x"), Leaf("y")}), Literal(1.0)}));

  auto is_op = [](const ExprNodePtr& node) -> absl::StatusOr<bool> {
    return node->is_op();
  };

  RecordingExprStackTrace stack_trace;
  ASSERT_OK_AND_ASSIGN(auto expr_as_lambda,
                       ExtractLambda(expr, is_op, &stack_trace));

  EXPECT_THAT(stack_trace.calls,
              ElementsAre("M.math.add(L.x, L.y) -> M.math.add(P._0, P._1)",
                          "M.math.subtract(M.math.add(..., ...), float64{1})"
                          " -> M.math.subtract(M.math.add(..., ...), P._2)",
                          "M.math.subtract(M.math.add(..., ...), float64{1})"
                          " -> anonymous.lambda(L.x, L.y, float64{1})"));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
