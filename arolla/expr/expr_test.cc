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
#include "arolla/expr/expr.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/unit.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsAttr;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::MockExprOperator;
using ::arolla::testing::WithNameAnnotation;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Not;
using ::testing::Return;

using Attr = ::arolla::expr::ExprAttributes;

TEST(ExprTest, CallOp) {
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core.equal", {Leaf("a"), Leaf("b")}));
  EXPECT_TRUE(IsRegisteredOperator(expr->op()));
  EXPECT_EQ(expr->op()->display_name(), "core.equal");
  EXPECT_THAT(expr->node_deps(),
              ElementsAre(EqualsExpr(Leaf("a")), EqualsExpr(Leaf("b"))));

  ASSERT_OK_AND_ASSIGN(auto op, LookupOperator("core.equal"));
  EXPECT_THAT(CallOp(op, {Leaf("a"), Leaf("b")}),
              IsOkAndHolds(EqualsExpr(expr)));
}

TEST(ExprTest, AdvancedCallOp) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto z = Leaf("z");
  auto w = Leaf("w");
  auto def = Literal(kUnit);
  absl::StatusOr<ExprNodePtr> x_or(x);
  absl::StatusOr<ExprNodePtr> y_or(y);
  absl::StatusOr<ExprNodePtr> z_or(z);
  absl::StatusOr<ExprNodePtr> w_or(w);

  ASSERT_OK_AND_ASSIGN(const auto sig,
                       ExprOperatorSignature::Make("p0, p1=, *tail", kUnit));
  const auto op = MockExprOperator::MakeNice({.signature = sig});

  EXPECT_THAT(
      CallOp(op, {}),
      StatusIs(absl::StatusCode::kInvalidArgument));  // missing argument
  {
    ASSERT_OK_AND_ASSIGN(auto expected_expr, MakeOpNode(op, {x, def}));
    EXPECT_THAT(CallOp(op, {x_or}), IsOkAndHolds(EqualsExpr(expected_expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expected_expr, MakeOpNode(op, {x, y}));
    EXPECT_THAT(CallOp(op, {x_or, y_or}),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expected_expr, MakeOpNode(op, {x, y, z}));
    EXPECT_THAT(CallOp(op, {x_or, y_or, z_or}),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expected_expr, MakeOpNode(op, {x, y, z, w}));
    EXPECT_THAT(CallOp(op, {x_or, y_or, z_or, w_or}),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expected_expr, MakeOpNode(op, {x, y}));
    EXPECT_THAT(CallOp(op, {x_or}, {{"p1", y_or}}),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
}

TEST(ExprTest, LiftStatus) {
  auto op = MockExprOperator::MakeNice();
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp(op, {Leaf("x"), absl::StatusOr<ExprNodePtr>(Leaf("y"))}));
  EXPECT_EQ(expr->op(), op);
  EXPECT_THAT(expr->node_deps(),
              ElementsAre(EqualsExpr(Leaf("x")), EqualsExpr(Leaf("y"))));

  EXPECT_THAT(
      CallOp("core.equal", {Leaf("x"), absl::InvalidArgumentError("Boom!")}),
      StatusIs(absl::StatusCode::kInvalidArgument, "Boom!"));
}

TEST(ExprTest, Literal) {
  const Bytes bytes("a long string literal to ensure memory allocation");
  const TypedValue qvalue = TypedValue::FromValue(bytes);
  {
    auto x = Literal(bytes);
    ASSERT_OK_AND_ASSIGN(Bytes x_bytes, x->qvalue()->As<Bytes>());
    EXPECT_THAT(x_bytes, Eq(bytes));
  }
  {
    auto x = Literal<Bytes>(bytes);
    ASSERT_OK_AND_ASSIGN(Bytes x_bytes, x->qvalue()->As<Bytes>());
    EXPECT_THAT(x_bytes, Eq(bytes));
  }
  {
    auto copy = bytes;
    auto* data_raw_ptr = absl::string_view(copy).data();
    auto x = Literal(std::move(copy));
    EXPECT_EQ(absl::string_view(x->qvalue()->UnsafeAs<Bytes>()).data(),
              data_raw_ptr);
  }
  {
    auto copy = bytes;
    auto* data_raw_ptr = absl::string_view(copy).data();
    auto x = Literal<Bytes>(std::move(copy));
    EXPECT_EQ(absl::string_view(x->qvalue()->UnsafeAs<Bytes>()).data(),
              data_raw_ptr);
  }
  {
    auto x = Literal(qvalue);
    EXPECT_EQ(x->qvalue()->GetType(), qvalue.GetType());
    EXPECT_EQ(x->qvalue()->GetRawPointer(), qvalue.GetRawPointer());
  }
  {
    auto fn = [&]() { return qvalue; };
    auto x = Literal(fn());
    EXPECT_EQ(x->qvalue()->GetType(), qvalue.GetType());
    EXPECT_EQ(x->qvalue()->GetRawPointer(), qvalue.GetRawPointer());
  }
  {
    auto x = Literal(TypedValue(qvalue));
    EXPECT_EQ(x->qvalue()->GetType(), qvalue.GetType());
    EXPECT_EQ(x->qvalue()->GetRawPointer(), qvalue.GetRawPointer());
  }
}

TEST(ExprTest, LiteralHash) {
  auto x = Literal(1.0);
  auto x1 = Literal(1.0);
  auto y = Literal(2.0);
  auto z = Literal(1);
  EXPECT_THAT(x, EqualsExpr(x1));
  EXPECT_THAT(x, Not(EqualsExpr(y)));  // Different value.
  EXPECT_THAT(x, Not(EqualsExpr(z)));  // Different QType.
}

TEST(ExprTest, WithNewOperator) {
  ASSERT_OK_AND_ASSIGN(auto op1, LookupOperator("core.equal"));
  ASSERT_OK_AND_ASSIGN(auto op2, LookupOperator("core.not_equal"));
  ASSERT_OK_AND_ASSIGN(auto actual_value, CallOp(op1, {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(actual_value, WithNewOperator(actual_value, op2));
  EXPECT_THAT(actual_value, EqualsExpr(CallOp(op2, {Leaf("x"), Leaf("y")})));
}

TEST(ExprTest, WithName) {
  ASSERT_OK_AND_ASSIGN(auto named_literal,
                       WithNameAnnotation(Literal(1.0), "a"));
  EXPECT_EQ(ReadNameAnnotation(named_literal), "a");

  ASSERT_OK_AND_ASSIGN(auto named_leaf, WithNameAnnotation(Leaf("x"), "a"));
  EXPECT_EQ(ReadNameAnnotation(named_leaf), "a");
  EXPECT_EQ(named_leaf->node_deps()[0]->leaf_key(), "x");

  ASSERT_OK_AND_ASSIGN(auto named_placeholder,
                       WithNameAnnotation(Placeholder("x"), "a"));
  EXPECT_EQ(ReadNameAnnotation(named_placeholder), "a");
  EXPECT_EQ(named_placeholder->node_deps()[0]->placeholder_key(), "x");
}

TEST(ExprTest, LeafHash) {
  auto x = Leaf("x");
  auto x1 = Leaf("x");
  auto y = Leaf("y");

  ASSERT_OK_AND_ASSIGN(auto float_x, WithQTypeAnnotation(x, GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto float_x1,
                       WithQTypeAnnotation(x1, GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto int_x, WithQTypeAnnotation(x, GetQType<int32_t>()));
  EXPECT_THAT(x, EqualsExpr(x1));
  EXPECT_THAT(float_x, EqualsExpr(float_x1));
  EXPECT_THAT(x, Not(EqualsExpr(y)));
  EXPECT_THAT(x, Not(EqualsExpr(float_x)));      // With vs. Without QType.
  EXPECT_THAT(int_x, Not(EqualsExpr(float_x)));  // Different QType.
}

TEST(ExprTest, PlaceholderHash) {
  auto x = Placeholder("x");
  auto x1 = Placeholder("x");
  auto y = Placeholder("y");

  EXPECT_THAT(x, EqualsExpr(x1));
  EXPECT_THAT(x, Not(EqualsExpr(y)));
}

TEST(ExprTest, GetLeafKeys) {
  auto l_a = Leaf("a");
  auto l_b = Leaf("b");
  auto p_a = Placeholder("a");
  auto p_b = Placeholder("b");
  {
    ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {p_a, p_b}));
    EXPECT_THAT(GetLeafKeys(expr), ElementsAre());
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {l_a, p_b}));
    EXPECT_THAT(GetLeafKeys(expr), ElementsAre("a"));
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {p_a, l_b}));
    EXPECT_THAT(GetLeafKeys(expr), ElementsAre("b"));
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {l_a, l_b}));
    EXPECT_THAT(GetLeafKeys(expr), ElementsAre("a", "b"));
  }
}

TEST(ExprTest, GetPlaceholderKeys) {
  auto l_a = Leaf("a");
  auto l_b = Leaf("b");
  auto p_a = Placeholder("a");
  auto p_b = Placeholder("b");
  {
    ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {p_a, p_b}));
    EXPECT_THAT(GetPlaceholderKeys(expr), ElementsAre("a", "b"));
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {l_a, p_b}));
    EXPECT_THAT(GetPlaceholderKeys(expr), ElementsAre("b"));
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {p_a, l_b}));
    EXPECT_THAT(GetPlaceholderKeys(expr), ElementsAre("a"));
  }
  {
    ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {l_a, l_b}));
    EXPECT_THAT(GetPlaceholderKeys(expr), ElementsAre());
  }
}

TEST(ExprTest, WithNewDependencies) {
  auto l_a = Leaf("a");
  auto p_b = Placeholder("b");
  auto lit = Literal(3.14);
  ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {l_a, p_b}));
  EXPECT_THAT(WithNewDependencies(l_a, {}), IsOkAndHolds(EqualsExpr(l_a)));
  EXPECT_THAT(WithNewDependencies(p_b, {}), IsOkAndHolds(EqualsExpr(p_b)));
  EXPECT_THAT(WithNewDependencies(lit, {}), IsOkAndHolds(EqualsExpr(lit)));
  ASSERT_OK_AND_ASSIGN(const auto actual_expr,
                       WithNewDependencies(expr, {p_b, l_a}));
  ASSERT_OK_AND_ASSIGN(const auto expected_expr,
                       CallOp("core.equal", {p_b, l_a}));
  EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
}

TEST(ExprTest, WithNewDependenciesOptimizations) {
  auto l_a = Leaf("a");
  auto l_b = Leaf("b");
  auto l_a2 = Leaf("a");
  ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {l_a, l_a}));
  ASSERT_OK_AND_ASSIGN(const auto expr2,
                       WithNewDependencies(expr, {l_a2, l_a2}));
  EXPECT_EQ(expr.get(), expr2.get());
  ASSERT_OK_AND_ASSIGN(const auto expr3, WithNewDependencies(expr, {l_b, l_a}));
  EXPECT_NE(expr.get(), expr3.get());
}

TEST(ExprTest, WithNewDependenciesAttr) {
  auto l_a = Leaf("a");
  ASSERT_OK_AND_ASSIGN(
      const auto l_a_int,
      CallOp("annotation.qtype", {l_a, Literal(GetQType<int>())}));
  ASSERT_OK_AND_ASSIGN(const auto expr, CallOp("core.equal", {l_a, l_a}));
  EXPECT_TRUE(expr->attr().IsIdenticalTo(Attr{}));
  ASSERT_OK_AND_ASSIGN(const auto expr_int,
                       WithNewDependencies(expr, {l_a_int, l_a_int}));
  EXPECT_TRUE(expr_int->attr().IsIdenticalTo(Attr(GetQType<OptionalUnit>())));
  ASSERT_OK_AND_ASSIGN(const auto expr2,
                       WithNewDependencies(expr_int, {l_a_int, l_a}));
  EXPECT_TRUE(expr2->attr().IsIdenticalTo(Attr{}));
}

TEST(ExprTest, RegisterOperatorAlias) {
  auto op = MockExprOperator::MakeStrict({
      .signature = ExprOperatorSignature{{"u"}, {"v"}},
      .doc = "original docstring",
  });
  ASSERT_OK_AND_ASSIGN(auto reg_op, RegisterOperator("alias_test.op", op));
  ASSERT_OK_AND_ASSIGN(
      auto alias_op,
      RegisterOperatorAlias("alias_test.alias_op", "alias_test.op"));

  // GetDoc
  EXPECT_THAT(alias_op->GetDoc(), IsOkAndHolds("original docstring"));

  // GetSignature
  ASSERT_OK_AND_ASSIGN(auto alias_sig, alias_op->GetSignature());
  ASSERT_EQ(alias_sig->parameters.size(), 2);
  EXPECT_EQ(alias_sig->parameters[0].name, "u");
  EXPECT_EQ(alias_sig->parameters[1].name, "v");

  // InferAttributes
  EXPECT_CALL(
      *op, InferAttributes(ElementsAre(EqualsAttr(TypedValue::FromValue(1.0)),
                                       EqualsAttr(TypedValue::FromValue(2)))))
      .WillOnce(Return(ExprAttributes(TypedValue::FromValue(3.0))));
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("alias_test.alias_op", {Literal(1.0), Literal(2)}));
  EXPECT_THAT(expr->attr(), EqualsAttr(TypedValue::FromValue(3.0)));

  // ToLowerLevel
  EXPECT_CALL(*op, ToLowerLevel(EqualsExpr(expr)))
      .WillOnce(Return(Literal(3.0)));
  EXPECT_THAT(ToLowerNode(expr), IsOkAndHolds(EqualsExpr(Literal(3.0))));
}

TEST(ExprTest, ToLowerNode) {
  auto op1 = MockExprOperator::MakeStrict();
  auto op2 = MockExprOperator::MakeStrict();

  EXPECT_CALL(*op1, InferAttributes(_)).WillOnce(Return(ExprAttributes()));
  EXPECT_CALL(*op2, InferAttributes(_)).WillOnce(Return(ExprAttributes()));
  ASSERT_OK_AND_ASSIGN(auto expr1, CallOp(op1, {Leaf("a"), Leaf("b")}));
  ASSERT_OK_AND_ASSIGN(auto expr2, CallOp(op2, {Leaf("c"), Leaf("d")}));

  EXPECT_CALL(*op1, ToLowerLevel(EqualsExpr(expr1))).WillOnce(Return(expr2));
  EXPECT_CALL(*op2, ToLowerLevel(_)).Times(0);
  EXPECT_THAT(ToLowerNode(expr1), IsOkAndHolds(EqualsExpr(expr2)));
}

TEST(ExprTest, ToLowest) {
  auto op1 = MockExprOperator::MakeStrict();
  auto op2 = MockExprOperator::MakeStrict();

  EXPECT_CALL(*op1, InferAttributes(_)).WillOnce(Return(ExprAttributes()));
  EXPECT_CALL(*op2, InferAttributes(_)).WillOnce(Return(ExprAttributes()));
  ASSERT_OK_AND_ASSIGN(auto expr1, CallOp(op1, {Leaf("a"), Leaf("b")}));
  ASSERT_OK_AND_ASSIGN(auto expr2, CallOp(op2, {Leaf("c"), Leaf("d")}));

  EXPECT_CALL(*op1, ToLowerLevel(EqualsExpr(expr1))).WillOnce(Return(expr2));
  EXPECT_CALL(*op2, ToLowerLevel(_)).WillOnce(Return(Leaf("e")));
  EXPECT_THAT(ToLowest(expr1), IsOkAndHolds(EqualsExpr(Leaf("e"))));
}

}  // namespace
}  // namespace arolla::expr
