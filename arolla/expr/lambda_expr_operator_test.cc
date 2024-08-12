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
#include "arolla/expr/lambda_expr_operator.h"

#include <cstdint>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/unit.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsAttr;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::InvokeExprOperator;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

using Attr = ExprAttributes;

class LambdaOperatorTest : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

TEST_F(LambdaOperatorTest, NoParameters) {
  auto foobar = Literal<int32_t>(0xf00baa);
  ASSERT_OK_AND_ASSIGN(
      const auto lambda_op,
      LambdaOperator::Make("foo.bar", ExprOperatorSignature{}, foobar));
  EXPECT_EQ(lambda_op->display_name(), "foo.bar");
  {  // Too many arguments
    EXPECT_THAT(CallOp(lambda_op, {Leaf("x")}),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
  ASSERT_OK_AND_ASSIGN(const auto folded_expr, CallOp(lambda_op, {}));
  ASSERT_OK_AND_ASSIGN(const auto expected_folded_expr,
                       MakeOpNode(lambda_op, {}));
  EXPECT_THAT(folded_expr, EqualsExpr(expected_folded_expr));
  ASSERT_OK_AND_ASSIGN(const auto unfolded_expr, ToLowerNode(folded_expr));
  EXPECT_THAT(unfolded_expr, EqualsExpr(foobar));
  EXPECT_EQ(lambda_op->doc(), "");
  EXPECT_THAT(lambda_op->GetDoc(), IsOkAndHolds(""));
}

TEST_F(LambdaOperatorTest, SingleArgument) {
  auto f1 = Literal<float>(1.0);
  auto p0 = Placeholder("p0");
  auto p1 = Placeholder("p1");
  {  // no arguments should raise an error
    EXPECT_THAT(LambdaOperator::Make(f1),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
  {  // two arguments should raise an error
    ASSERT_OK_AND_ASSIGN(auto lambda_body, CallOp("math.add", {p0, p1}));
    EXPECT_THAT(LambdaOperator::Make(lambda_body),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
  {  // valid case
    ASSERT_OK_AND_ASSIGN(auto lambda_body, CallOp("math.add", {p0, f1}));
    ASSERT_OK_AND_ASSIGN(auto lambda_op, LambdaOperator::Make(lambda_body));
    ASSERT_OK_AND_ASSIGN(
        const auto expected_lambda_op,
        LambdaOperator::Make(ExprOperatorSignature{{"p0"}}, lambda_body));
    EXPECT_EQ(lambda_op->fingerprint(), expected_lambda_op->fingerprint());
  }
  {  // valid case with name
    ASSERT_OK_AND_ASSIGN(auto lambda_body, CallOp("math.add", {p0, f1}));
    ASSERT_OK_AND_ASSIGN(auto lambda_op,
                         LambdaOperator::Make("op.name", lambda_body));
    ASSERT_OK_AND_ASSIGN(
        const auto expected_lambda_op,
        LambdaOperator::Make("op.name", ExprOperatorSignature{{"p0"}},
                             lambda_body));
    EXPECT_EQ(lambda_op->fingerprint(), expected_lambda_op->fingerprint());
    EXPECT_EQ(lambda_op->display_name(), "op.name");
  }
}

TEST_F(LambdaOperatorTest, General) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto u = Literal(kUnit);
  auto p0 = Placeholder("p0");
  auto p1 = Placeholder("p1");

  ASSERT_OK_AND_ASSIGN(auto lambda_signature,
                       ExprOperatorSignature::Make("p0, p1=", kUnit));
  ASSERT_OK_AND_ASSIGN(auto lambda_body, CallOp("math.add", {p0, p1}));
  ASSERT_OK_AND_ASSIGN(auto lambda_op,
                       LambdaOperator::Make(lambda_signature, lambda_body));
  EXPECT_EQ(lambda_op->display_name(), "anonymous.lambda");
  EXPECT_THAT(lambda_op->lambda_body(), EqualsExpr(lambda_body));
  {  // Missing a required parameter.
    EXPECT_THAT(CallOp(lambda_op, {}),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
  {  // Too many arguments.
    EXPECT_THAT(CallOp(lambda_op, {x, x, x}),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
  {  // Default value for second parameter.
    ASSERT_OK_AND_ASSIGN(auto folded_expr, CallOp(lambda_op, {x}));
    ASSERT_OK_AND_ASSIGN(auto expected_folded_expr,
                         MakeOpNode(lambda_op, {x, u}));
    EXPECT_THAT(folded_expr, EqualsExpr(expected_folded_expr));
    ASSERT_OK_AND_ASSIGN(auto unfolded_expr, ToLowerNode(folded_expr));
    ASSERT_OK_AND_ASSIGN(auto expected_unfolded_expr,
                         CallOp("math.add", {x, u}));
    EXPECT_THAT(unfolded_expr, EqualsExpr(expected_unfolded_expr));
  }
  {  // All parameters are explicit.
    ASSERT_OK_AND_ASSIGN(auto folded_expr, CallOp(lambda_op, {x, y}));
    ASSERT_OK_AND_ASSIGN(auto expected_folded_expr,
                         MakeOpNode(lambda_op, {x, y}));
    EXPECT_THAT(folded_expr, EqualsExpr(expected_folded_expr));
    ASSERT_OK_AND_ASSIGN(auto unfolded_expr, ToLowerNode(folded_expr));
    ASSERT_OK_AND_ASSIGN(auto expected_unfolded_expr,
                         CallOp("math.add", {x, y}));
    EXPECT_THAT(unfolded_expr, EqualsExpr(expected_unfolded_expr));
  }
}

TEST_F(LambdaOperatorTest, MakeLambdaOperator) {
  ASSERT_OK_AND_ASSIGN(
      auto lambda_op,
      MakeLambdaOperator(
          ExprOperatorSignature::Make("x, y"),
          CallOp("math.add", {Placeholder("x"), Placeholder("y")})));
  EXPECT_EQ(lambda_op->display_name(), "anonymous.lambda");
  EXPECT_THAT(
      MakeLambdaOperator(
          absl::StatusOr<ExprOperatorSignature>(
              absl::FailedPreconditionError("~~~")),
          CallOp("math.add", {Placeholder("x"), Placeholder("y")})),
      StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("~~~")));
}

TEST_F(LambdaOperatorTest, QTypePropagation) {
  ASSERT_OK_AND_ASSIGN(auto lambda_signature,
                       ExprOperatorSignature::Make("x, y"));
  ASSERT_OK_AND_ASSIGN(
      auto lambda_body,
      CallOp("math.add", {Placeholder("x"), Placeholder("y")}));
  ASSERT_OK_AND_ASSIGN(lambda_body,
                       CallOp("math.add", {lambda_body, Placeholder("y")}));
  ASSERT_OK_AND_ASSIGN(
      auto lambda_op,
      LambdaOperator::Make("test.lambda", lambda_signature, lambda_body));

  ASSERT_OK_AND_ASSIGN(
      const auto called_lambda,
      CallOp(lambda_op, {Literal<int64_t>(57), Literal<int64_t>(57)}));
  EXPECT_THAT(called_lambda->qtype(), GetQType<int64_t>());

  EXPECT_THAT(
      CallOp(lambda_op, {Literal(Bytes{""}), Literal<int64_t>(57)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr(
                   "while deducing output type for M.math.add(P.x, P.y); while "
                   "calling test.lambda with args {b'', int64{57}}")));
}

TEST_F(LambdaOperatorTest, QValuePropagation) {
  ASSERT_OK_AND_ASSIGN(auto op,
                       MakeLambdaOperator("test.lambda", Placeholder("x")));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Literal(1)}));
  EXPECT_THAT(expr->attr(), EqualsAttr(TypedRef::FromValue(1)));
}

TEST_F(LambdaOperatorTest, BadLambdaBody) {
  const ExprOperatorSignature lambda_signature{{"p"}};
  EXPECT_OK(LambdaOperator::Make(lambda_signature, Placeholder("p")));

  EXPECT_THAT(
      LambdaOperator::Make(lambda_signature, Placeholder("missing_parameter")),
      StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(LambdaOperator::Make(lambda_signature, Leaf("p")),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(LambdaOperatorTest, VariadicArg) {
  ASSERT_OK_AND_ASSIGN(
      auto head_op,
      MakeLambdaOperator(ExprOperatorSignature::Make("head, *_tail"),
                         Placeholder("head")));
  ASSERT_OK_AND_ASSIGN(
      auto tail_op,
      MakeLambdaOperator(ExprOperatorSignature::Make("_head, *tail"),
                         Placeholder("tail")));
  ASSERT_OK_AND_ASSIGN(auto h,
                       InvokeExprOperator<TypedValue>(head_op, 0.f, 1.f, 2.f));
  ASSERT_OK_AND_ASSIGN(auto t,
                       InvokeExprOperator<TypedValue>(tail_op, 0.f, 1.f, 2.f));
  EXPECT_THAT(h.As<float>(), IsOkAndHolds(0.f));
  EXPECT_EQ(t.GetType(),
            MakeTupleQType({GetQType<float>(), GetQType<float>()}));
  EXPECT_EQ(t.GetFieldCount(), 2);
  EXPECT_THAT(t.GetField(0).As<float>(), IsOkAndHolds(1.f));
  EXPECT_THAT(t.GetField(1).As<float>(), IsOkAndHolds(2.f));

  // Check that not fully defined QTypes and missing literal values for varargs
  // work as expected.

  EXPECT_THAT(
      head_op->InferAttributes(
          {Attr(GetQType<float>()), Attr(TypedValue::FromValue(1.f)), Attr{}}),
      IsOkAndHolds(EqualsAttr(GetQType<float>())));

  EXPECT_THAT(
      tail_op->InferAttributes(
          {Attr(GetQType<float>()), Attr(TypedValue::FromValue(1.f)), Attr{}}),
      IsOkAndHolds(EqualsAttr(nullptr)));
}

TEST_F(LambdaOperatorTest, VariadicArgInferAttributes) {
  ASSERT_OK_AND_ASSIGN(auto op,
                       MakeLambdaOperator(ExprOperatorSignature::Make("*args"),
                                          Placeholder("args")));
  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {}));
    ASSERT_OK_AND_ASSIGN(auto lowered_expr, ToLowest(expr));
    ASSERT_THAT(expr->attr(), EqualsAttr(lowered_expr->attr()));
  }
  {
    auto v0 = Placeholder("x");
    ASSERT_OK_AND_ASSIGN(
        auto v1, WithQTypeAnnotation(Placeholder("x"), GetQType<int>()));
    auto v2 = Literal(1.5f);
    for (const auto& a0 : {v0, v1, v2}) {
      for (const auto& a1 : {v0, v1, v2}) {
        ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {a0, a1}));
        ASSERT_OK_AND_ASSIGN(auto lowered_expr, ToLowest(expr));
        ASSERT_THAT(expr->attr(), EqualsAttr(lowered_expr->attr()));
      }
    }
  }
}

TEST_F(LambdaOperatorTest, OutputQTypeRequiresLiteral) {
  {
    ASSERT_OK_AND_ASSIGN(auto lambda_signature,
                         ExprOperatorSignature::Make("x, y"));
    ASSERT_OK_AND_ASSIGN(
        auto lambda_body,
        CallOp(QTypeAnnotation::Make(), {Placeholder("x"), Placeholder("y")}));
    ASSERT_OK_AND_ASSIGN(auto lambda_op,
                         LambdaOperator::Make(lambda_signature, lambda_body));

    ASSERT_OK_AND_ASSIGN(
        const auto called_lambda,
        CallOp(lambda_op, {Leaf("a"), Literal<QTypePtr>(GetQType<int64_t>())}));
    EXPECT_EQ(called_lambda->qtype(), GetQType<int64_t>());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto lambda_signature,
                         ExprOperatorSignature::Make("x"));
    ASSERT_OK_AND_ASSIGN(
        auto lambda_body,
        WithQTypeAnnotation(Placeholder("x"), GetQType<int64_t>()));
    ASSERT_OK_AND_ASSIGN(auto lambda_op,
                         LambdaOperator::Make(lambda_signature, lambda_body));

    EXPECT_THAT(lambda_op->InferAttributes({Attr{}}),
                IsOkAndHolds(EqualsAttr(GetQType<int64_t>())));
  }
}

TEST_F(LambdaOperatorTest, GetDoc) {
  auto lambda_body = Placeholder("x");
  ASSERT_OK_AND_ASSIGN(
      auto op, LambdaOperator::Make("lambda_op_with_docstring",
                                    ExprOperatorSignature{{"x"}}, lambda_body,
                                    "doc-string"));
  ASSERT_EQ(op->doc(), "doc-string");
  ASSERT_THAT(op->GetDoc(), IsOkAndHolds("doc-string"));
}

TEST_F(LambdaOperatorTest, SuppressUnusedWarning) {
  {
    ASSERT_OK_AND_ASSIGN(
        auto expr, CallOp("math.add", {Placeholder("x"), Placeholder("y")}));
    ASSERT_OK_AND_ASSIGN(auto wrapped_expr, SuppressUnusedWarning("", expr));
    EXPECT_THAT(GetPlaceholderKeys(wrapped_expr), ElementsAre("x", "y"));
    EXPECT_THAT(ToLowerNode(wrapped_expr), IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto expr, CallOp("math.add", {Placeholder("x"), Placeholder("y")}));
    ASSERT_OK_AND_ASSIGN(auto wrapped_expr,
                         SuppressUnusedWarning("a, b, c", expr));
    EXPECT_THAT(GetPlaceholderKeys(wrapped_expr),
                ElementsAre("a", "b", "c", "x", "y"));
    EXPECT_THAT(ToLowest(wrapped_expr), IsOkAndHolds(EqualsExpr(expr)));
  }
}

}  // namespace
}  // namespace arolla::expr
