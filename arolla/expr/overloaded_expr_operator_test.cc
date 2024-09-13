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
#include "arolla/expr/overloaded_expr_operator.h"

#include <cstdint>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::testing::DummyOp;
using ::arolla::testing::EqualsAttr;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::InvokeExprOperator;
using ::testing::HasSubstr;

using Attr = ExprAttributes;

TEST(OverloadedOperatorTest, SmokeTest) {
  ASSERT_OK_AND_ASSIGN(
      auto double_op,
      MakeOverloadedOperator(
          "Double",
          MakeLambdaOperator(
              CallOp("math.add", {Placeholder("x"), Placeholder("x")})),
          MakeLambdaOperator(
              CallOp("strings.join", {Placeholder("x"), Placeholder("x")}))));
  EXPECT_THAT(InvokeExprOperator<int>(double_op, 1), IsOkAndHolds(2));
  EXPECT_THAT(InvokeExprOperator<double>(double_op, 1.5), IsOkAndHolds(3.));
  EXPECT_THAT(InvokeExprOperator<Bytes>(double_op, Bytes("abc")),
              IsOkAndHolds(Bytes("abcabc")));
  EXPECT_THAT(double_op->InferAttributes({Attr(GetQType<bool>())}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unsupported argument type BOOLEAN")));
  EXPECT_THAT(double_op->InferAttributes(
                  {Attr(GetQType<int32_t>()), Attr(GetQType<int64_t>())}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unsupported argument types (INT32,INT64)")));
}

TEST(OverloadedOperatorTest, UsingLiteralValues) {
  ASSERT_OK_AND_ASSIGN(auto lambda_signature,
                       ExprOperatorSignature::Make("x, y"));
  ASSERT_OK_AND_ASSIGN(
      auto with_qtype_op,
      MakeOverloadedOperator(
          "WithQType",
          MakeLambdaOperator(lambda_signature,
                             CallOp(QTypeAnnotation::Make(),
                                    {Placeholder("x"), Placeholder("y")})),
          MakeLambdaOperator(
              lambda_signature,
              CallOp("strings.join", {Placeholder("x"), Placeholder("y")}))));

  EXPECT_THAT(with_qtype_op->InferAttributes(
                  {Attr{}, Attr(TypedValue::FromValue(GetQType<int32_t>()))}),
              IsOkAndHolds(EqualsAttr(GetQType<int>())));
  EXPECT_THAT(with_qtype_op->InferAttributes(
                  {Attr(GetQType<Bytes>()), Attr(GetQType<Bytes>())}),
              IsOkAndHolds(EqualsAttr(GetQType<Bytes>())));
  EXPECT_THAT(with_qtype_op->InferAttributes({Attr{}, Attr{}}),
              IsOkAndHolds(EqualsAttr(nullptr)));
  EXPECT_THAT(
      with_qtype_op->InferAttributes({Attr(GetQType<Bytes>()), Attr{}, Attr{}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("unsupported argument types (BYTES,*,*)")));
}

TEST(OverloadedOperatorTest, GetDoc) {
  auto op_1 = std::make_shared<testing::DummyOp>(
      "dummy_op_1", ExprOperatorSignature::MakeVariadicArgs(),
      "dummy_docstring_1");

  auto op_2 = std::make_shared<testing::DummyOp>(
      "dummy_op_2", ExprOperatorSignature::MakeVariadicArgs(),
      "dummy_docstring_2");
  OverloadedOperator op("overloaded_op", {op_1, op_2});
  ASSERT_THAT(op.GetDoc(), IsOkAndHolds("dummy_docstring_1"));
}

TEST(OverloadedOperatorTest, Empty) {
  OverloadedOperator op("empty", {});
  ASSERT_THAT(op.GetSignature(), StatusIs(absl::StatusCode::kInvalidArgument,
                                          HasSubstr("no base operators")));
  ASSERT_THAT(op.GetDoc(), StatusIs(absl::StatusCode::kInvalidArgument,
                                    HasSubstr("no base operators")));
}

TEST(OverloadedOperatorTest, ResolutionOrder) {
  // Consider an unary operator:
  //
  //   op = arolla.optools.dispatch[M.core.identity, lambda('_', 1)],
  //
  // Its returning qtype must always be the same as the qtype of the argument.
  // In particular, if the argument qtype is unspecified, like in `op(L.x)`,
  // the resulting qtype must also be unspecified.
  ASSERT_OK_AND_ASSIGN(
      auto op,
      MakeOverloadedOperator(
          "dispatch", LookupOperator("core.identity"),
          MakeLambdaOperator(ExprOperatorSignature::Make("_"), Literal(1))));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Placeholder("x")}));
  EXPECT_EQ(expr->qtype(), nullptr);
}

TEST(OverloadedOperatorTest, Lowering) {
  ASSERT_OK_AND_ASSIGN(auto double_add_op,
                       MakeLambdaOperator(CallOp(
                           "math.add", {Placeholder("x"), Placeholder("x")})));
  ASSERT_OK_AND_ASSIGN(
      auto double_op,
      MakeOverloadedOperator(
          "Double", double_add_op,
          MakeLambdaOperator(
              CallOp("strings.join", {Placeholder("x"), Placeholder("x")}))));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(double_op, {Literal(1.0)}));
  EXPECT_THAT(ToLowerNode(expr),
              IsOkAndHolds(EqualsExpr(CallOp(double_add_op, {Literal(1.0)}))));
}

}  // namespace
}  // namespace arolla::expr
