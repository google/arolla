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
#include "arolla/expr/expr_debug_string.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_repr_functions.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/dummy_types.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"

namespace arolla::expr {
namespace {

using ::arolla::testing::WithNameAnnotation;
using ::arolla::testing::WithQTypeAnnotation;

class ExprDebugStringTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }

  ExprNodePtr Pos(ExprNodePtr x) { return CallOp("math.pos", {x}).value(); }
  ExprNodePtr Neg(ExprNodePtr x) { return CallOp("math.neg", {x}).value(); }
  ExprNodePtr Invert(ExprNodePtr x) {
    return CallOp("core.presence_not", {x}).value();
  }

  ExprNodePtr Pow(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("math.pow", {lhs, rhs}).value();
  }

  ExprNodePtr Mul(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("math.multiply", {lhs, rhs}).value();
  }
  ExprNodePtr TrueDiv(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("math.divide", {lhs, rhs}).value();
  }
  ExprNodePtr FloorDiv(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("math.floordiv", {lhs, rhs}).value();
  }
  ExprNodePtr Mod(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("math.mod", {lhs, rhs}).value();
  }

  ExprNodePtr Add(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("math.add", {lhs, rhs}).value();
  }
  ExprNodePtr Sub(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("math.subtract", {lhs, rhs}).value();
  }

  ExprNodePtr And(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("core.presence_and", {lhs, rhs}).value();
  }

  ExprNodePtr Or(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("core.presence_or", {lhs, rhs}).value();
  }

  ExprNodePtr Lt(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("core.less", {lhs, rhs}).value();
  }
  ExprNodePtr Le(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("core.less_equal", {lhs, rhs}).value();
  }
  ExprNodePtr Eq(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("core.equal", {lhs, rhs}).value();
  }
  ExprNodePtr Neq(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("core.not_equal", {lhs, rhs}).value();
  }
  ExprNodePtr Ge(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("core.greater_equal", {lhs, rhs}).value();
  }
  ExprNodePtr Gt(ExprNodePtr lhs, ExprNodePtr rhs) {
    return CallOp("core.greater", {lhs, rhs}).value();
  }

  ExprNodePtr GetAttr(ExprNodePtr lhs, ExprNodePtr rhs) {
    return ExprNode::UnsafeMakeOperatorNode(
        std::make_shared<RegisteredOperator>("core.getattr"), {lhs, rhs},
        ExprAttributes());
  }

  ExprNodePtr GetItem(ExprNodePtr lhs, ExprNodePtr rhs) {
    return ExprNode::UnsafeMakeOperatorNode(
        std::make_shared<RegisteredOperator>("core.getitem"), {lhs, rhs},
        ExprAttributes());
  }

  ExprNodePtr MakeSlice(ExprNodePtr a, ExprNodePtr b, ExprNodePtr c) {
    return ExprNode::UnsafeMakeOperatorNode(
        std::make_shared<RegisteredOperator>("core.make_slice"), {a, b, c},
        ExprAttributes());
  }

  ExprNodePtr Dummy(ExprNodePtr lhs, ExprNodePtr rhs) {
    return ExprNode::UnsafeMakeOperatorNode(
        std::make_shared<testing::DummyOp>(
            "custom.add", ExprOperatorSignature({{"x"}, {"y"}})),
        {lhs, rhs}, ExprAttributes());
  }
};

TEST_F(ExprDebugStringTest, Literal) {
  {
    auto expr = Literal(int32_t{271828182});
    EXPECT_EQ("271828182", ToDebugString(expr));
  }
  {
    auto expr = Literal(int64_t{3417201710});
    EXPECT_EQ("int64{3417201710}", ToDebugString(expr));
  }
  {
    auto expr = Literal(Bytes("Hello, World!"));
    EXPECT_EQ("b'Hello, World!'", ToDebugString(expr));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr,
                         WithNameAnnotation(Literal(Bytes("Foo")), "Bar"));
    EXPECT_EQ("Bar = b'Foo'\nBar", ToDebugString(expr));
  }
}

TEST_F(ExprDebugStringTest, Leaf) {
  EXPECT_THAT(ToDebugString(Leaf("")), "L['']");
  EXPECT_THAT(ToDebugString(Leaf("x")), "L.x");
  EXPECT_THAT(ToDebugString(Leaf("'Hello, World!'")),
              "L['\\'Hello, World!\\'']");

  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<double>()));
  EXPECT_THAT(ToDebugString(y), "M.annotation.qtype(L.y, FLOAT64)");
  EXPECT_THAT(ToDebugString(y, /*verbose=*/true),
              "M.annotation.qtype(L.y, FLOAT64)");
}

TEST_F(ExprDebugStringTest, Placeholder) {
  EXPECT_EQ("P['']", ToDebugString(Placeholder("")));
  EXPECT_EQ("P.foo", ToDebugString(Placeholder("foo")));
  EXPECT_EQ("P[':)']", ToDebugString(Placeholder(":)")));
}

TEST_F(ExprDebugStringTest, Operator) {
  EXPECT_EQ(ToDebugString(CallOp("math.max", {Leaf("x"), Leaf("y")}).value()),
            "M.math.max(L.x, L.y)");
  EXPECT_EQ(ToDebugString(Add(Leaf("x"), Leaf("y"))), "L.x + L.y");
}

TEST_F(ExprDebugStringTest, Trivial) {
  ASSERT_OK_AND_ASSIGN(
      auto abc,
      CallOp("test.add3", {Literal(0.f), Literal(2.7182f), Literal(3.1415f)}));
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("test.add3", {abc, Leaf("x"), Leaf("y")}));
  EXPECT_EQ("M.test.add3(M.test.add3(0., 2.7182, 3.1415), L.x, L.y)",
            ToDebugString(expr));
}

TEST_F(ExprDebugStringTest, UniqueStatements) {
  auto a = Leaf("a");
  auto b = Leaf("b");
  auto c = Leaf("c");
  ASSERT_OK_AND_ASSIGN(
      auto d,
      WithNameAnnotation(
          Pow(Sub(Mul(b, b), Mul(Literal(4.f), Mul(a, c))), Literal(0.5f)),
          "D"));

  ASSERT_OK_AND_ASSIGN(
      auto x0,
      WithNameAnnotation(TrueDiv(TrueDiv(Add(b, d), Literal(-2.f)), a), "x0"));
  ASSERT_OK_AND_ASSIGN(auto x1,
                       WithNameAnnotation(TrueDiv(TrueDiv(c, a), x0), "x1"));
  EXPECT_EQ(("D = (L.b * L.b - 4. * (L.a * L.c)) ** 0.5\n"
             "x0 = (L.b + D) / -2. / L.a\n"
             "x1 = L.c / L.a / x0\n"
             "x0 * x1"),
            ToDebugString(Mul(x0, x1)));
}

TEST_F(ExprDebugStringTest, LeafKeyNameCollisions) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       WithNameAnnotation(Add(Leaf("a"), Leaf("a")), "a"));
  EXPECT_EQ(ToDebugString(expr), "a = L.a + L.a\na");
}

TEST_F(ExprDebugStringTest, PlaceholderKeyNameCollisions) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      WithNameAnnotation(
          CallOp("math.min", {Placeholder("a"), Placeholder("a")}), "a"));
  EXPECT_EQ(ToDebugString(expr), "a = M.math.min(P.a, P.a)\na");
}

TEST_F(ExprDebugStringTest, UnsafeStatements) {
  auto expr = Leaf("a");
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "_"));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), ""));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "_1"));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "_X"));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "_Y"));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "_Y"));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "quick' fox"));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "foo.bar"));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "abc."));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), ".def"));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "fake..name"));
  ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "a.1"));
  EXPECT_EQ(ToDebugString(expr),
            "_ = L.a + L.a\n"
            "_1 = M.annotation.name(_ + _, '')\n"
            "_2 = M.annotation.name(_1 + _1, '_1')\n"
            "_X = _2 + _2\n"
            "_Y._1 = _X + _X\n"
            "_Y._2 = _Y._1 + _Y._1\n"
            "_3 = M.annotation.name(_Y._2 + _Y._2, 'quick\\' fox')\n"
            "foo.bar = _3 + _3\n"
            "_4 = M.annotation.name(foo.bar + foo.bar, 'abc.')\n"
            "_5 = M.annotation.name(_4 + _4, '.def')\n"
            "_6 = M.annotation.name(_5 + _5, 'fake..name')\n"
            "_7 = M.annotation.name(_6 + _6, 'a.1')\n"
            "_7");
}

TEST_F(ExprDebugStringTest, UnnamedStatements) {
  auto expr = Leaf("a");
  for (int i = 0; i < 10; ++i) {
    expr = Add(expr, expr);
  }
  EXPECT_EQ(ToDebugString(expr),
            "_1 = L.a + L.a + (L.a + L.a)\n"
            "_2 = _1 + _1 + (_1 + _1)\n"
            "_3 = _2 + _2 + (_2 + _2)\n"
            "_4 = _3 + _3 + (_3 + _3)\n"
            "_4 + _4 + (_4 + _4)");
}

TEST_F(ExprDebugStringTest, NonUniqueStatements) {
  auto expr = Leaf("a");
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK_AND_ASSIGN(expr, WithNameAnnotation(Add(expr, expr), "a"));
  }
  EXPECT_EQ(ToDebugString(expr),
            "a._1 = L.a + L.a\n"
            "a._2 = a._1 + a._1\n"
            "a._3 = a._2 + a._2\n"
            "a._4 = a._3 + a._3\n"
            "a._5 = a._4 + a._4\n"
            "a._5");
}

TEST_F(ExprDebugStringTest, ExponentionalBlow) {
  auto expr = Leaf("a");
  for (int i = 0; i < 100; ++i) {
    expr = Add(expr, expr);
  }
  EXPECT_LT(ToDebugString(expr).size(), 10000);
}

TEST_F(ExprDebugStringTest, Infix_Brackets) {
  EXPECT_EQ(ToDebugString(Neg(Add(Leaf("u"), Leaf("v")))), "-(L.u + L.v)");
  EXPECT_EQ(ToDebugString(Neg(Leaf("u"))), "-L.u");
  EXPECT_EQ(ToDebugString(Mul(Leaf("u"), Leaf("x"))), "L.u * L.x");
  EXPECT_EQ(ToDebugString(Mul(Add(Leaf("u"), Leaf("v")), Leaf("x"))),
            "(L.u + L.v) * L.x");
  EXPECT_EQ(ToDebugString(Mul(Leaf("u"), Add(Leaf("x"), Leaf("y")))),
            "L.u * (L.x + L.y)");
  EXPECT_EQ(
      ToDebugString(Mul(Add(Leaf("u"), Leaf("v")), Add(Leaf("x"), Leaf("y")))),
      "(L.u + L.v) * (L.x + L.y)");
}

TEST_F(ExprDebugStringTest, Infix_Unary_IncorrectArity) {
  auto x = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto op, LookupOperator("math.pos"));
  EXPECT_EQ(ToDebugString(ExprNode::UnsafeMakeOperatorNode(op, {x}, {})),
            "+L.x");
  EXPECT_EQ(ToDebugString(ExprNode::UnsafeMakeOperatorNode(op, {x, x}, {})),
            "M.math.pos(L.x, L.x)");
}

TEST_F(ExprDebugStringTest, Infix_Binary_IncorrectArity) {
  auto x = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto op, LookupOperator("math.add"));
  EXPECT_EQ(ToDebugString(ExprNode::UnsafeMakeOperatorNode(op, {x, x}, {})),
            "L.x + L.x");
  EXPECT_EQ(ToDebugString(ExprNode::UnsafeMakeOperatorNode(op, {x, x, x}, {})),
            "M.math.add(L.x, L.x, L.x)");
}

TEST_F(ExprDebugStringTest, Infix_NonRegisteredOperator) {
  auto x = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto op, LookupOperator("math.add"));
  ASSERT_OK_AND_ASSIGN(auto op_impl, DecayRegisteredOperator(op));
  EXPECT_EQ(ToDebugString(
                ExprNode::UnsafeMakeOperatorNode(std::move(op), {x, x}, {})),
            "L.x + L.x");
  EXPECT_EQ(ToDebugString(ExprNode::UnsafeMakeOperatorNode(std::move(op_impl),
                                                           {x, x}, {})),
            "math.add(L.x, L.x)");
}

TEST_F(ExprDebugStringTest, Infix_Unary_NegGroup) {
  auto x = Leaf("x");
  // Pos
  EXPECT_EQ(ToDebugString(Pos(x)), "+L.x");
  EXPECT_EQ(ToDebugString(Pos(Pos(x))), "+(+L.x)");
  // Neg
  EXPECT_EQ(ToDebugString(Neg(x)), "-L.x");
  EXPECT_EQ(ToDebugString(Neg(Neg(x))), "-(-L.x)");
  // Invert
  EXPECT_EQ(ToDebugString(Invert(x)), "~L.x");
  EXPECT_EQ(ToDebugString(Invert(Invert(x))), "~(~L.x)");
  // Pos, Neg, Invert
  EXPECT_EQ(ToDebugString(Pos(Neg(Invert(x)))), "+(-(~L.x))");
  EXPECT_EQ(ToDebugString(Pos(Neg(Invert(Pos(Neg(Invert(x))))))),
            "+(-(~(+(-(~L.x)))))");
}

TEST_F(ExprDebugStringTest, Infix_Binary_Pow) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto z = Leaf("z");
  // Pow
  EXPECT_EQ(ToDebugString(Pow(x, y)), "L.x ** L.y");
  EXPECT_EQ(ToDebugString(Pow(Pow(x, y), z)), "(L.x ** L.y) ** L.z");
  EXPECT_EQ(ToDebugString(Pow(x, Pow(y, z))), "L.x ** L.y ** L.z");
  // Pow, Neg
  EXPECT_EQ(ToDebugString(Neg(Pow(x, y))), "-(L.x ** L.y)");
  EXPECT_EQ(ToDebugString(Pow(Neg(x), y)), "(-L.x) ** L.y");
  EXPECT_EQ(ToDebugString(Pow(x, Neg(y))), "L.x ** -L.y");
}

TEST_F(ExprDebugStringTest, Infix_Binary_MulGroup) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto z = Leaf("z");
  // Mul
  EXPECT_EQ(ToDebugString(Mul(x, y)), "L.x * L.y");
  EXPECT_EQ(ToDebugString(Mul(Mul(x, y), z)), "L.x * L.y * L.z");
  EXPECT_EQ(ToDebugString(Mul(x, Mul(y, z))), "L.x * (L.y * L.z)");
  // TrueDiv
  EXPECT_EQ(ToDebugString(TrueDiv(x, y)), "L.x / L.y");
  EXPECT_EQ(ToDebugString(TrueDiv(TrueDiv(x, y), z)), "L.x / L.y / L.z");
  EXPECT_EQ(ToDebugString(TrueDiv(x, TrueDiv(y, z))), "L.x / (L.y / L.z)");
  // FloorDiv
  EXPECT_EQ(ToDebugString(FloorDiv(x, y)), "L.x // L.y");
  EXPECT_EQ(ToDebugString(FloorDiv(FloorDiv(x, y), z)), "L.x // L.y // L.z");
  EXPECT_EQ(ToDebugString(FloorDiv(x, FloorDiv(y, z))), "L.x // (L.y // L.z)");
  // Mod
  EXPECT_EQ(ToDebugString(Mod(x, y)), "L.x % L.y");
  EXPECT_EQ(ToDebugString(Mod(Mod(x, y), z)), "L.x % L.y % L.z");
  EXPECT_EQ(ToDebugString(Mod(x, Mod(y, z))), "L.x % (L.y % L.z)");
  // Mul, TrueDiv
  EXPECT_EQ(ToDebugString(TrueDiv(Mul(x, y), z)), "L.x * L.y / L.z");
  EXPECT_EQ(ToDebugString(Mul(x, TrueDiv(y, z))), "L.x * (L.y / L.z)");
  EXPECT_EQ(ToDebugString(Mul(TrueDiv(x, y), z)), "L.x / L.y * L.z");
  EXPECT_EQ(ToDebugString(TrueDiv(x, Mul(y, z))), "L.x / (L.y * L.z)");
  // Mul, FloorDiv
  EXPECT_EQ(ToDebugString(FloorDiv(Mul(x, y), z)), "L.x * L.y // L.z");
  EXPECT_EQ(ToDebugString(Mul(x, FloorDiv(y, z))), "L.x * (L.y // L.z)");
  EXPECT_EQ(ToDebugString(Mul(FloorDiv(x, y), z)), "L.x // L.y * L.z");
  EXPECT_EQ(ToDebugString(FloorDiv(x, Mul(y, z))), "L.x // (L.y * L.z)");
  // Mul, Mod
  EXPECT_EQ(ToDebugString(Mod(Mul(x, y), z)), "L.x * L.y % L.z");
  EXPECT_EQ(ToDebugString(Mul(x, Mod(y, z))), "L.x * (L.y % L.z)");
  EXPECT_EQ(ToDebugString(Mul(Mod(x, y), z)), "L.x % L.y * L.z");
  EXPECT_EQ(ToDebugString(Mod(x, Mul(y, z))), "L.x % (L.y * L.z)");
  // Mul, Pow
  EXPECT_EQ(ToDebugString(Pow(Mul(x, y), z)), "(L.x * L.y) ** L.z");
  EXPECT_EQ(ToDebugString(Mul(x, Pow(y, z))), "L.x * L.y ** L.z");
  EXPECT_EQ(ToDebugString(Mul(Pow(x, y), z)), "L.x ** L.y * L.z");
  EXPECT_EQ(ToDebugString(Pow(x, Mul(y, z))), "L.x ** (L.y * L.z)");
  // Mul, Neg
  EXPECT_EQ(ToDebugString(Neg(Mul(x, y))), "-(L.x * L.y)");
  EXPECT_EQ(ToDebugString(Mul(Neg(x), y)), "-L.x * L.y");
  EXPECT_EQ(ToDebugString(Mul(x, Neg(y))), "L.x * -L.y");
}

TEST_F(ExprDebugStringTest, Infix_Binary_AddGroup) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto z = Leaf("z");
  // Add
  EXPECT_EQ(ToDebugString(Add(x, y)), "L.x + L.y");
  EXPECT_EQ(ToDebugString(Add(Add(x, y), z)), "L.x + L.y + L.z");
  EXPECT_EQ(ToDebugString(Add(x, Add(y, z))), "L.x + (L.y + L.z)");
  // Sub
  EXPECT_EQ(ToDebugString(Sub(x, y)), "L.x - L.y");
  EXPECT_EQ(ToDebugString(Sub(Sub(x, y), z)), "L.x - L.y - L.z");
  EXPECT_EQ(ToDebugString(Sub(x, Sub(y, z))), "L.x - (L.y - L.z)");
  // Add, Sub
  EXPECT_EQ(ToDebugString(Sub(Add(x, y), z)), "L.x + L.y - L.z");
  EXPECT_EQ(ToDebugString(Add(x, Sub(y, z))), "L.x + (L.y - L.z)");
  EXPECT_EQ(ToDebugString(Add(Sub(x, y), z)), "L.x - L.y + L.z");
  EXPECT_EQ(ToDebugString(Sub(x, Add(y, z))), "L.x - (L.y + L.z)");
  // Add, Mul
  EXPECT_EQ(ToDebugString(Mul(Add(x, y), z)), "(L.x + L.y) * L.z");
  EXPECT_EQ(ToDebugString(Add(x, Mul(y, z))), "L.x + L.y * L.z");
  EXPECT_EQ(ToDebugString(Add(Mul(x, y), z)), "L.x * L.y + L.z");
  EXPECT_EQ(ToDebugString(Mul(x, Add(y, z))), "L.x * (L.y + L.z)");
  // Add, Pow
  EXPECT_EQ(ToDebugString(Pow(Add(x, y), z)), "(L.x + L.y) ** L.z");
  EXPECT_EQ(ToDebugString(Add(x, Pow(y, z))), "L.x + L.y ** L.z");
  EXPECT_EQ(ToDebugString(Add(Pow(x, y), z)), "L.x ** L.y + L.z");
  EXPECT_EQ(ToDebugString(Pow(x, Add(y, z))), "L.x ** (L.y + L.z)");
  // Add, Neg
  EXPECT_EQ(ToDebugString(Neg(Add(x, y))), "-(L.x + L.y)");
  EXPECT_EQ(ToDebugString(Add(Neg(x), y)), "-L.x + L.y");
  EXPECT_EQ(ToDebugString(Add(x, Neg(y))), "L.x + -L.y");
}

TEST_F(ExprDebugStringTest, Infix_Binary_And) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto z = Leaf("z");
  // And
  EXPECT_EQ(ToDebugString(And(x, y)), "L.x & L.y");
  EXPECT_EQ(ToDebugString(And(And(x, y), z)), "L.x & L.y & L.z");
  EXPECT_EQ(ToDebugString(And(x, And(y, z))), "L.x & (L.y & L.z)");
  // And, Add
  EXPECT_EQ(ToDebugString(Add(And(x, y), z)), "(L.x & L.y) + L.z");
  EXPECT_EQ(ToDebugString(And(x, Add(y, z))), "L.x & L.y + L.z");
  EXPECT_EQ(ToDebugString(And(Add(x, y), z)), "L.x + L.y & L.z");
  EXPECT_EQ(ToDebugString(Add(x, And(y, z))), "L.x + (L.y & L.z)");
  // And, Mul
  EXPECT_EQ(ToDebugString(Mul(And(x, y), z)), "(L.x & L.y) * L.z");
  EXPECT_EQ(ToDebugString(And(x, Mul(y, z))), "L.x & L.y * L.z");
  EXPECT_EQ(ToDebugString(And(Mul(x, y), z)), "L.x * L.y & L.z");
  EXPECT_EQ(ToDebugString(Mul(x, And(y, z))), "L.x * (L.y & L.z)");
  // And, Pow
  EXPECT_EQ(ToDebugString(Pow(And(x, y), z)), "(L.x & L.y) ** L.z");
  EXPECT_EQ(ToDebugString(And(x, Pow(y, z))), "L.x & L.y ** L.z");
  EXPECT_EQ(ToDebugString(And(Pow(x, y), z)), "L.x ** L.y & L.z");
  EXPECT_EQ(ToDebugString(Pow(x, And(y, z))), "L.x ** (L.y & L.z)");
  // And, Neg
  EXPECT_EQ(ToDebugString(Neg(And(x, y))), "-(L.x & L.y)");
  EXPECT_EQ(ToDebugString(And(Neg(x), y)), "-L.x & L.y");
  EXPECT_EQ(ToDebugString(And(x, Neg(y))), "L.x & -L.y");
}

TEST_F(ExprDebugStringTest, Infix_Binary_Or) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto z = Leaf("z");
  // Or
  EXPECT_EQ(ToDebugString(Or(x, y)), "L.x | L.y");
  EXPECT_EQ(ToDebugString(Or(Or(x, y), z)), "L.x | L.y | L.z");
  EXPECT_EQ(ToDebugString(Or(x, Or(y, z))), "L.x | (L.y | L.z)");
  // Or, And
  EXPECT_EQ(ToDebugString(And(Or(x, y), z)), "(L.x | L.y) & L.z");
  EXPECT_EQ(ToDebugString(Or(x, And(y, z))), "L.x | L.y & L.z");
  EXPECT_EQ(ToDebugString(Or(And(x, y), z)), "L.x & L.y | L.z");
  EXPECT_EQ(ToDebugString(And(x, Or(y, z))), "L.x & (L.y | L.z)");
  // Or, Add
  EXPECT_EQ(ToDebugString(Add(Or(x, y), z)), "(L.x | L.y) + L.z");
  EXPECT_EQ(ToDebugString(Or(x, Add(y, z))), "L.x | L.y + L.z");
  EXPECT_EQ(ToDebugString(Or(Add(x, y), z)), "L.x + L.y | L.z");
  EXPECT_EQ(ToDebugString(Add(x, Or(y, z))), "L.x + (L.y | L.z)");
  // Or, Mul
  EXPECT_EQ(ToDebugString(Mul(Or(x, y), z)), "(L.x | L.y) * L.z");
  EXPECT_EQ(ToDebugString(Or(x, Mul(y, z))), "L.x | L.y * L.z");
  EXPECT_EQ(ToDebugString(Or(Mul(x, y), z)), "L.x * L.y | L.z");
  EXPECT_EQ(ToDebugString(Mul(x, Or(y, z))), "L.x * (L.y | L.z)");
  // Or, Pow
  EXPECT_EQ(ToDebugString(Pow(Or(x, y), z)), "(L.x | L.y) ** L.z");
  EXPECT_EQ(ToDebugString(Or(x, Pow(y, z))), "L.x | L.y ** L.z");
  EXPECT_EQ(ToDebugString(Or(Pow(x, y), z)), "L.x ** L.y | L.z");
  EXPECT_EQ(ToDebugString(Pow(x, Or(y, z))), "L.x ** (L.y | L.z)");
  // Or, Neg
  EXPECT_EQ(ToDebugString(Neg(Or(x, y))), "-(L.x | L.y)");
  EXPECT_EQ(ToDebugString(Or(Neg(x), y)), "-L.x | L.y");
  EXPECT_EQ(ToDebugString(Or(x, Neg(y))), "L.x | -L.y");
}

TEST_F(ExprDebugStringTest, Infix_Binary_LtGroup) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto z = Leaf("z");
  // Lt
  EXPECT_EQ(ToDebugString(Lt(x, y)), "L.x < L.y");
  EXPECT_EQ(ToDebugString(Lt(Lt(x, y), z)), "(L.x < L.y) < L.z");
  EXPECT_EQ(ToDebugString(Lt(x, Lt(y, z))), "L.x < (L.y < L.z)");
  // Le
  EXPECT_EQ(ToDebugString(Le(x, y)), "L.x <= L.y");
  EXPECT_EQ(ToDebugString(Le(Le(x, y), z)), "(L.x <= L.y) <= L.z");
  EXPECT_EQ(ToDebugString(Le(x, Le(y, z))), "L.x <= (L.y <= L.z)");
  // Eq
  EXPECT_EQ(ToDebugString(Eq(x, y)), "L.x == L.y");
  EXPECT_EQ(ToDebugString(Eq(Eq(x, y), z)), "(L.x == L.y) == L.z");
  EXPECT_EQ(ToDebugString(Eq(x, Eq(y, z))), "L.x == (L.y == L.z)");
  // Ne
  EXPECT_EQ(ToDebugString(Neq(x, y)), "L.x != L.y");
  EXPECT_EQ(ToDebugString(Neq(Neq(x, y), z)), "(L.x != L.y) != L.z");
  EXPECT_EQ(ToDebugString(Neq(x, Neq(y, z))), "L.x != (L.y != L.z)");
  // Ge
  EXPECT_EQ(ToDebugString(Ge(x, y)), "L.x >= L.y");
  EXPECT_EQ(ToDebugString(Ge(Ge(x, y), z)), "(L.x >= L.y) >= L.z");
  EXPECT_EQ(ToDebugString(Ge(x, Ge(y, z))), "L.x >= (L.y >= L.z)");
  // Gt
  EXPECT_EQ(ToDebugString(Gt(x, y)), "L.x > L.y");
  EXPECT_EQ(ToDebugString(Gt(Gt(x, y), z)), "(L.x > L.y) > L.z");
  EXPECT_EQ(ToDebugString(Gt(x, Gt(y, z))), "L.x > (L.y > L.z)");
  // Lt, Le
  EXPECT_EQ(ToDebugString(Le(Lt(x, y), z)), "(L.x < L.y) <= L.z");
  EXPECT_EQ(ToDebugString(Lt(x, Le(y, z))), "L.x < (L.y <= L.z)");
  EXPECT_EQ(ToDebugString(Lt(Le(x, y), z)), "(L.x <= L.y) < L.z");
  EXPECT_EQ(ToDebugString(Le(x, Lt(y, z))), "L.x <= (L.y < L.z)");
  // Lt, Eq
  EXPECT_EQ(ToDebugString(Eq(Lt(x, y), z)), "(L.x < L.y) == L.z");
  EXPECT_EQ(ToDebugString(Lt(x, Eq(y, z))), "L.x < (L.y == L.z)");
  EXPECT_EQ(ToDebugString(Lt(Eq(x, y), z)), "(L.x == L.y) < L.z");
  EXPECT_EQ(ToDebugString(Eq(x, Lt(y, z))), "L.x == (L.y < L.z)");
  // Lt, Neq
  EXPECT_EQ(ToDebugString(Neq(Lt(x, y), z)), "(L.x < L.y) != L.z");
  EXPECT_EQ(ToDebugString(Lt(x, Neq(y, z))), "L.x < (L.y != L.z)");
  EXPECT_EQ(ToDebugString(Lt(Neq(x, y), z)), "(L.x != L.y) < L.z");
  EXPECT_EQ(ToDebugString(Neq(x, Lt(y, z))), "L.x != (L.y < L.z)");
  // Lt, Ge
  EXPECT_EQ(ToDebugString(Ge(Lt(x, y), z)), "(L.x < L.y) >= L.z");
  EXPECT_EQ(ToDebugString(Lt(x, Ge(y, z))), "L.x < (L.y >= L.z)");
  EXPECT_EQ(ToDebugString(Lt(Ge(x, y), z)), "(L.x >= L.y) < L.z");
  EXPECT_EQ(ToDebugString(Ge(x, Lt(y, z))), "L.x >= (L.y < L.z)");
  // Lt, Gt
  EXPECT_EQ(ToDebugString(Gt(Lt(x, y), z)), "(L.x < L.y) > L.z");
  EXPECT_EQ(ToDebugString(Lt(x, Gt(y, z))), "L.x < (L.y > L.z)");
  EXPECT_EQ(ToDebugString(Lt(Gt(x, y), z)), "(L.x > L.y) < L.z");
  EXPECT_EQ(ToDebugString(Gt(x, Lt(y, z))), "L.x > (L.y < L.z)");
  // Lt, Or
  EXPECT_EQ(ToDebugString(Or(Lt(x, y), z)), "(L.x < L.y) | L.z");
  EXPECT_EQ(ToDebugString(Lt(x, Or(y, z))), "L.x < L.y | L.z");
  EXPECT_EQ(ToDebugString(Lt(Or(x, y), z)), "L.x | L.y < L.z");
  EXPECT_EQ(ToDebugString(Or(x, Lt(y, z))), "L.x | (L.y < L.z)");
  // Lt, And
  EXPECT_EQ(ToDebugString(And(Lt(x, y), z)), "(L.x < L.y) & L.z");
  EXPECT_EQ(ToDebugString(Lt(x, And(y, z))), "L.x < L.y & L.z");
  EXPECT_EQ(ToDebugString(Lt(And(x, y), z)), "L.x & L.y < L.z");
  EXPECT_EQ(ToDebugString(And(x, Lt(y, z))), "L.x & (L.y < L.z)");
  // Lt, Add
  EXPECT_EQ(ToDebugString(Add(Lt(x, y), z)), "(L.x < L.y) + L.z");
  EXPECT_EQ(ToDebugString(Lt(x, Add(y, z))), "L.x < L.y + L.z");
  EXPECT_EQ(ToDebugString(Lt(Add(x, y), z)), "L.x + L.y < L.z");
  EXPECT_EQ(ToDebugString(Add(x, Lt(y, z))), "L.x + (L.y < L.z)");
  // Lt, Mul
  EXPECT_EQ(ToDebugString(Mul(Lt(x, y), z)), "(L.x < L.y) * L.z");
  EXPECT_EQ(ToDebugString(Lt(x, Mul(y, z))), "L.x < L.y * L.z");
  EXPECT_EQ(ToDebugString(Lt(Mul(x, y), z)), "L.x * L.y < L.z");
  EXPECT_EQ(ToDebugString(Mul(x, Lt(y, z))), "L.x * (L.y < L.z)");
  // Lt, Pow
  EXPECT_EQ(ToDebugString(Pow(Lt(x, y), z)), "(L.x < L.y) ** L.z");
  EXPECT_EQ(ToDebugString(Lt(x, Pow(y, z))), "L.x < L.y ** L.z");
  EXPECT_EQ(ToDebugString(Lt(Pow(x, y), z)), "L.x ** L.y < L.z");
  EXPECT_EQ(ToDebugString(Pow(x, Lt(y, z))), "L.x ** (L.y < L.z)");
  // Lt, Neg
  EXPECT_EQ(ToDebugString(Neg(Lt(x, y))), "-(L.x < L.y)");
  EXPECT_EQ(ToDebugString(Lt(Neg(x), y)), "-L.x < L.y");
  EXPECT_EQ(ToDebugString(Lt(x, Neg(y))), "L.x < -L.y");
}

TEST_F(ExprDebugStringTest, Infix_GetAttr) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto one = Literal<int>(1);
  auto foo = Literal(Text("foo"));
  auto bar = Literal(Text("bar"));
  // GetAttr
  EXPECT_EQ(ToDebugString(GetAttr(x, foo)), "L.x.foo");
  EXPECT_EQ(ToDebugString(GetAttr(GetAttr(x, foo), bar)), "L.x.foo.bar");
  EXPECT_EQ(ToDebugString(GetAttr(one, foo)), "(1).foo");
  EXPECT_EQ(ToDebugString(GetAttr(foo, bar)), "'foo'.bar");
  // GetAttr, Lt
  EXPECT_EQ(ToDebugString(Lt(GetAttr(x, foo), y)), "L.x.foo < L.y");
  EXPECT_EQ(ToDebugString(Lt(x, GetAttr(y, bar))), "L.x < L.y.bar");
  EXPECT_EQ(ToDebugString(GetAttr(Lt(x, y), foo)), "(L.x < L.y).foo");
  // GetAttr, Or
  EXPECT_EQ(ToDebugString(Or(GetAttr(x, foo), y)), "L.x.foo | L.y");
  EXPECT_EQ(ToDebugString(Or(x, GetAttr(y, bar))), "L.x | L.y.bar");
  EXPECT_EQ(ToDebugString(GetAttr(Or(x, y), foo)), "(L.x | L.y).foo");
  // GetAttr, And
  EXPECT_EQ(ToDebugString(And(GetAttr(x, foo), y)), "L.x.foo & L.y");
  EXPECT_EQ(ToDebugString(And(x, GetAttr(y, bar))), "L.x & L.y.bar");
  EXPECT_EQ(ToDebugString(GetAttr(And(x, y), foo)), "(L.x & L.y).foo");
  // GetAttr, Add
  EXPECT_EQ(ToDebugString(Add(GetAttr(x, foo), y)), "L.x.foo + L.y");
  EXPECT_EQ(ToDebugString(Add(x, GetAttr(y, bar))), "L.x + L.y.bar");
  EXPECT_EQ(ToDebugString(GetAttr(Add(x, y), foo)), "(L.x + L.y).foo");
  // GetAttr, Mul
  EXPECT_EQ(ToDebugString(Mul(GetAttr(x, foo), y)), "L.x.foo * L.y");
  EXPECT_EQ(ToDebugString(Mul(x, GetAttr(y, bar))), "L.x * L.y.bar");
  EXPECT_EQ(ToDebugString(GetAttr(Mul(x, y), foo)), "(L.x * L.y).foo");
  // GetAttr, Pow
  EXPECT_EQ(ToDebugString(Pow(GetAttr(x, foo), y)), "L.x.foo ** L.y");
  EXPECT_EQ(ToDebugString(Pow(x, GetAttr(y, bar))), "L.x ** L.y.bar");
  EXPECT_EQ(ToDebugString(GetAttr(Pow(x, y), foo)), "(L.x ** L.y).foo");
  // GetAttr, Neg
  EXPECT_EQ(ToDebugString(Neg(GetAttr(x, foo))), "-L.x.foo");
  EXPECT_EQ(ToDebugString(GetAttr(Neg(x), foo)), "(-L.x).foo");
}

TEST_F(ExprDebugStringTest, Infix_GetItem) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto one = Literal<int>(1);
  auto foo = Literal(Text("foo"));
  auto bar = Literal(Text("bar"));
  // GetItem
  EXPECT_EQ(ToDebugString(GetItem(x, foo)), "L.x['foo']");
  EXPECT_EQ(ToDebugString(GetItem(x, y)), "L.x[L.y]");
  EXPECT_EQ(ToDebugString(GetItem(GetItem(x, foo), bar)), "L.x['foo']['bar']");
  EXPECT_EQ(ToDebugString(GetItem(one, foo)), "(1)['foo']");
  EXPECT_EQ(ToDebugString(GetItem(foo, bar)), "'foo'['bar']");
  EXPECT_EQ(ToDebugString(GetItem(CallOp("math.max", {x, y}).value(), bar)),
            "M.math.max(L.x, L.y)['bar']");
  // GetItem, Lt
  EXPECT_EQ(ToDebugString(Lt(GetItem(x, foo), y)), "L.x['foo'] < L.y");
  EXPECT_EQ(ToDebugString(Lt(x, GetItem(y, bar))), "L.x < L.y['bar']");
  EXPECT_EQ(ToDebugString(GetItem(Lt(x, y), foo)), "(L.x < L.y)['foo']");
  // GetItem, Or
  EXPECT_EQ(ToDebugString(Or(GetItem(x, foo), y)), "L.x['foo'] | L.y");
  EXPECT_EQ(ToDebugString(Or(x, GetItem(y, bar))), "L.x | L.y['bar']");
  EXPECT_EQ(ToDebugString(GetItem(Or(x, y), foo)), "(L.x | L.y)['foo']");
  // GetItem, And
  EXPECT_EQ(ToDebugString(And(GetItem(x, foo), y)), "L.x['foo'] & L.y");
  EXPECT_EQ(ToDebugString(And(x, GetItem(y, bar))), "L.x & L.y['bar']");
  EXPECT_EQ(ToDebugString(GetItem(And(x, y), foo)), "(L.x & L.y)['foo']");
  // GetItem, Add
  EXPECT_EQ(ToDebugString(Add(GetItem(x, foo), y)), "L.x['foo'] + L.y");
  EXPECT_EQ(ToDebugString(Add(x, GetItem(y, bar))), "L.x + L.y['bar']");
  EXPECT_EQ(ToDebugString(GetItem(Add(x, y), foo)), "(L.x + L.y)['foo']");
  // GetItem, Mul
  EXPECT_EQ(ToDebugString(Mul(GetItem(x, foo), y)), "L.x['foo'] * L.y");
  EXPECT_EQ(ToDebugString(Mul(x, GetItem(y, bar))), "L.x * L.y['bar']");
  EXPECT_EQ(ToDebugString(GetItem(Mul(x, y), foo)), "(L.x * L.y)['foo']");
  // GetItem, Pow
  EXPECT_EQ(ToDebugString(Pow(GetItem(x, foo), y)), "L.x['foo'] ** L.y");
  EXPECT_EQ(ToDebugString(Pow(x, GetItem(y, bar))), "L.x ** L.y['bar']");
  EXPECT_EQ(ToDebugString(GetItem(Pow(x, y), foo)), "(L.x ** L.y)['foo']");
  // GetItem, Neg
  EXPECT_EQ(ToDebugString(Neg(GetItem(x, foo))), "-L.x['foo']");
  EXPECT_EQ(ToDebugString(GetItem(Neg(x), foo)), "(-L.x)['foo']");
  // GetItem, GetAttr
  EXPECT_EQ(ToDebugString(GetAttr(GetItem(x, foo), bar)), "L.x['foo'].bar");
  EXPECT_EQ(ToDebugString(GetItem(x, GetAttr(y, foo))), "L.x[L.y.foo]");
  EXPECT_EQ(ToDebugString(GetItem(GetAttr(x, foo), bar)), "L.x.foo['bar']");
  // GetItem, MakeSlice. See Infix_MakeSlice for more extensive tests.
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(one, foo, bar))),
            "L.x[1:'foo':'bar']");
}

TEST_F(ExprDebugStringTest, Infix_MakeSlice) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto u = Literal(GetUnspecifiedQValue());
  auto one = Literal<int>(1);
  auto two = Literal<int>(2);
  auto three = Literal<int>(3);
  // MakeSlice, Standalone
  EXPECT_EQ(ToDebugString(MakeSlice(u, u, u)),
            "M.core.make_slice(unspecified, unspecified, unspecified)");
  EXPECT_EQ(ToDebugString(MakeSlice(one, two, three)),
            "M.core.make_slice(1, 2, 3)");

  // Remaining tests are within the context of GetItem (which includes special
  // printing.
  // MakeSlice
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(u, u, u))), "L.x[:]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(one, u, u))), "L.x[1:]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(u, one, u))), "L.x[:1]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(u, u, one))), "L.x[::1]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(one, two, u))), "L.x[1:2]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(one, u, two))), "L.x[1::2]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(u, one, two))), "L.x[:1:2]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(one, two, three))),
            "L.x[1:2:3]");
  // MakeSlice, Add
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(Add(one, x), two, three))),
            "L.x[1 + L.x:2:3]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(one, Add(two, x), three))),
            "L.x[1:2 + L.x:3]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(one, two, Add(three, x)))),
            "L.x[1:2:3 + L.x]");
  // MakeSlice, Gt
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(Gt(one, x), two, three))),
            "L.x[1 > L.x:2:3]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(one, Gt(two, x), three))),
            "L.x[1:2 > L.x:3]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(one, two, Gt(three, x)))),
            "L.x[1:2:3 > L.x]");
  // // MakeSlice, DummyWithPrecedence (to test bracket rules).

  // No brackets by default.
  auto d = Literal(DummyWithPrecedence{});
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(d, u, u))),
            "L.x[dummy-with-precedence:]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(u, d, u))),
            "L.x[:dummy-with-precedence]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(u, u, d))),
            "L.x[::dummy-with-precedence]");

  // With brackets when l/r precedence is 11.
  auto d11 =
      Literal(DummyWithPrecedence{.precedence = ReprToken::Precedence{11, 11}});
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(d11, u, u))),
            "L.x[(dummy-with-precedence):]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(u, d11, u))),
            "L.x[:(dummy-with-precedence)]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(u, u, d11))),
            "L.x[::(dummy-with-precedence)]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(d11, d11, u))),
            "L.x[(dummy-with-precedence):(dummy-with-precedence)]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(d11, u, d11))),
            "L.x[(dummy-with-precedence)::(dummy-with-precedence)]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(u, d11, d11))),
            "L.x[:(dummy-with-precedence):(dummy-with-precedence)]");
  EXPECT_EQ(ToDebugString(GetItem(x, MakeSlice(d11, d11, d11))),
            "L.x[(dummy-with-precedence):(dummy-with-precedence):(dummy-with-"
            "precedence)]");
}

TEST_F(ExprDebugStringTest, Infix_Binary_NonInfix) {
  auto x = Leaf("x");
  auto foo = Literal(Text("foo"));
  ASSERT_OK_AND_ASSIGN(auto op, LookupOperator("core.getattr"));
  // Not a literal.
  EXPECT_EQ(ToDebugString(ExprNode::UnsafeMakeOperatorNode(op, {x, x}, {})),
            "M.core.getattr(L.x, L.x)");
  // Not a text attribute.
  EXPECT_EQ(ToDebugString(ExprNode::UnsafeMakeOperatorNode(
                op, {x, Literal(Bytes("bar"))}, {})),
            "M.core.getattr(L.x, b'bar')");
  // Wrong arity
  EXPECT_EQ(ToDebugString(ExprNode::UnsafeMakeOperatorNode(op, {}, {})),
            "M.core.getattr()");
  EXPECT_EQ(
      ToDebugString(ExprNode::UnsafeMakeOperatorNode(op, {foo, foo, foo}, {})),
      "M.core.getattr('foo', 'foo', 'foo')");
}

TEST_F(ExprDebugStringTest, Infix_NegativeLiteralRegression) {
  auto x = Leaf("x");
  // 2 ** x
  EXPECT_EQ(ToDebugString(Pow(Literal<int>(2), x)), "2 ** L.x");
  EXPECT_EQ(ToDebugString(Pow(Literal<float>(2.), x)), "2. ** L.x");
  EXPECT_EQ(ToDebugString(Pow(Literal<double>(2.), x)), "float64{2} ** L.x");
  // (-1) ** x
  EXPECT_EQ(ToDebugString(Pow(Literal<int>(-1), x)), "(-1) ** L.x");
  EXPECT_EQ(ToDebugString(Pow(Literal<float>(-1.), x)), "(-1.) ** L.x");
  EXPECT_EQ(ToDebugString(Pow(Literal<double>(-1.), x)), "float64{-1} ** L.x");
  // x ** -1
  EXPECT_EQ(ToDebugString(Pow(x, Literal<int>(-1))), "L.x ** -1");
  EXPECT_EQ(ToDebugString(Pow(x, Literal<float>(-1.))), "L.x ** -1.");
  EXPECT_EQ(ToDebugString(Pow(x, Literal<double>(-1.))), "L.x ** float64{-1}");
  // x ** 2
  EXPECT_EQ(ToDebugString(Pow(x, Literal<int>(2))), "L.x ** 2");
  EXPECT_EQ(ToDebugString(Pow(x, Literal<float>(2.))), "L.x ** 2.");
  EXPECT_EQ(ToDebugString(Pow(x, Literal<double>(2.))), "L.x ** float64{2}");
  // -(-1)
  EXPECT_EQ(ToDebugString(Neg(Literal<int>(-1))), "-(-1)");
  EXPECT_EQ(ToDebugString(Neg(Literal<float>(-1))), "-(-1.)");
  EXPECT_EQ(ToDebugString(Neg(Literal<double>(-1))), "-float64{-1}");
  // -2
  EXPECT_EQ(ToDebugString(Neg(Literal<int>(2))), "-2");
  EXPECT_EQ(ToDebugString(Neg(Literal<float>(2))), "-2.");
  EXPECT_EQ(ToDebugString(Neg(Literal<double>(2))), "-float64{2}");
}

TEST_F(ExprDebugStringTest, CustomOpRepr) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto expr = Dummy(x, y);
  {
    // Unregistered.
    EXPECT_EQ(ToDebugString(expr), "custom.add(L.x, L.y)");
  }

  {
    // Registered.
    auto repr_fn =
        [](const ExprNodePtr& node,
           const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens)
        -> std::optional<ReprToken> {
      const auto& lhs_str =
          node_tokens.at(node->node_deps()[0]->fingerprint()).str;
      const auto& rhs_str =
          node_tokens.at(node->node_deps()[1]->fingerprint()).str;
      auto res = absl::StrFormat("%s + %s", lhs_str, rhs_str);
      return ReprToken{.str = std::move(res)};
    };

    RegisterOpReprFnByQValueSpecializationKey(
        std::string(expr->op()->py_qvalue_specialization_key()), repr_fn);
    EXPECT_EQ(ToDebugString(expr), "L.x + L.y");
  }

  {
    // Fallback if empty.
    auto repr_fn =
        [](ExprNodePtr node,
           const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens)
        -> std::optional<ReprToken> { return std::nullopt; };

    RegisterOpReprFnByQValueSpecializationKey(
        std::string(expr->op()->py_qvalue_specialization_key()), repr_fn);
    EXPECT_EQ(ToDebugString(expr), "custom.add(L.x, L.y)");
  }
}

TEST_F(ExprDebugStringTest, GetDebugSnippet) {
  auto expr = Leaf("x");
  EXPECT_EQ(GetDebugSnippet(expr), "L.x");

  ASSERT_OK_AND_ASSIGN(auto typed_expr,
                       WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()));
  EXPECT_EQ(GetDebugSnippet(typed_expr), "M.annotation.qtype(L.x, INT32)");

  ASSERT_OK_AND_ASSIGN(auto named_expr, WithNameAnnotation(expr, "xxx"));
  EXPECT_EQ(GetDebugSnippet(named_expr), "M.annotation.name(L.x, 'xxx')");

  auto big_expr = Leaf("x");
  for (int i = 0; i < 100; ++i) {
    big_expr = Add(big_expr, big_expr);
  }
  EXPECT_EQ(GetDebugSnippet(big_expr),
            "M.math.add(M.math.add(..., ...), M.math.add(..., ...))");

  ASSERT_OK_AND_ASSIGN(auto big_typed_expr,
                       WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()));
  for (int i = 0; i < 100; ++i) {
    big_typed_expr = Add(big_typed_expr, big_typed_expr);
  }
  EXPECT_EQ(GetDebugSnippet(big_typed_expr),
            ("M.math.add(M.math.add(..., ...):INT32, M.math.add(..., "
             "...):INT32):INT32"));
}

void BM_GetDebugSnippet_Leaf(benchmark::State& state) {
  InitArolla();
  auto expr = Leaf("x");
  for (auto s : state) {
    auto x = GetDebugSnippet(expr);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_GetDebugSnippet_Leaf);

void BM_GetDebugSnippet_Literal(benchmark::State& state) {
  InitArolla();
  auto expr = Literal(57);
  for (auto s : state) {
    auto x = GetDebugSnippet(expr);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_GetDebugSnippet_Literal);

void BM_GetDebugSnippet_Small(benchmark::State& state) {
  InitArolla();
  auto expr = WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()).value();
  expr = CallOp("math.add", {Literal(57), Leaf("x")}).value();
  for (auto s : state) {
    auto x = GetDebugSnippet(expr);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_GetDebugSnippet_Small);

void BM_GetDebugSnippet_Big(benchmark::State& state) {
  InitArolla();
  auto expr = WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()).value();
  for (int i = 0; i < 100; ++i) {
    expr = CallOp("math.add", {expr, Leaf("x")}).value();
    expr = CallOp("math.add", {expr, Literal(57)}).value();
    expr = CallOp("math.add", {expr, expr}).value();
  }
  for (auto s : state) {
    auto x = GetDebugSnippet(expr);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_GetDebugSnippet_Big);

void BM_ToDebugString_Leaf(benchmark::State& state) {
  InitArolla();
  auto expr = Leaf("x");
  for (auto s : state) {
    auto x = ToDebugString(expr);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_ToDebugString_Leaf);

void BM_ToDebugString_Literal(benchmark::State& state) {
  InitArolla();
  auto expr = Literal(57);
  for (auto s : state) {
    auto x = ToDebugString(expr);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_ToDebugString_Literal);

void BM_ToDebugString_Small(benchmark::State& state) {
  InitArolla();
  auto expr = WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()).value();
  expr = CallOp("math.maximum", {Literal(57), Leaf("x")}).value();
  for (auto s : state) {
    auto x = ToDebugString(expr);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_ToDebugString_Small);

void BM_ToDebugString_Big(benchmark::State& state) {
  InitArolla();
  auto expr = WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()).value();
  for (int i = 0; i < 100; ++i) {
    expr = CallOp("math.maximum", {expr, Leaf("x")}).value();
    expr = CallOp("math.maximum", {expr, Literal(57)}).value();
    expr = CallOp("math.maximum", {expr, expr}).value();
  }
  for (auto s : state) {
    auto x = ToDebugString(expr);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_ToDebugString_Big);

void BM_ToDebugString_Small_Verbose(benchmark::State& state) {
  InitArolla();
  auto expr = WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()).value();
  expr = CallOp("math.maximum", {Literal(57), Leaf("x")}).value();
  for (auto s : state) {
    auto x = ToDebugString(expr, /*verbose=*/true);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_ToDebugString_Small_Verbose);

void BM_ToDebugString_Big_Verbose(benchmark::State& state) {
  InitArolla();
  auto expr = WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()).value();
  for (int i = 0; i < 100; ++i) {
    expr = CallOp("math.maximum", {expr, Leaf("x")}).value();
    expr = CallOp("math.maximum", {expr, Literal(57)}).value();
    expr = CallOp("math.maximum", {expr, expr}).value();
  }
  for (auto s : state) {
    auto x = ToDebugString(expr, /*verbose=*/true);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_ToDebugString_Big_Verbose);

void BM_ToDebugString_Big_Infix(benchmark::State& state) {
  InitArolla();
  auto expr = WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()).value();
  for (int i = 0; i < 100; ++i) {
    expr = CallOp("math.add", {expr, Leaf("x")}).value();
    expr = CallOp("math.add", {expr, Literal(57)}).value();
    expr = CallOp("math.add", {expr, expr}).value();
  }
  for (auto s : state) {
    auto x = ToDebugString(expr);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_ToDebugString_Big_Infix);

void BM_ToDebugString_CustomReprBig(benchmark::State& state) {
  InitArolla();
  auto x = WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>()).value();
  auto foo_bar = std::make_shared<testing::DummyOp>(
      "foo.bar", ExprOperatorSignature({{"x"}, {"y"}}));
  auto expr =
      ExprNode::UnsafeMakeOperatorNode(foo_bar, {x, x}, ExprAttributes());

  auto repr_fn =
      [](const ExprNodePtr& node,
         const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens)
      -> std::optional<ReprToken> {
    const auto& lhs_str =
        node_tokens.at(node->node_deps()[0]->fingerprint()).str;
    const auto& rhs_str =
        node_tokens.at(node->node_deps()[1]->fingerprint()).str;
    auto res = absl::StrFormat("foo.bar(%s, %s)", lhs_str, rhs_str);
    return ReprToken{.str = std::move(res)};
  };

  RegisterOpReprFnByQValueSpecializationKey(
      std::string(expr->op()->py_qvalue_specialization_key()), repr_fn);

  for (int i = 0; i < 100; ++i) {
    expr = ExprNode::UnsafeMakeOperatorNode(foo_bar, {expr, Leaf("x")},
                                            ExprAttributes());
    expr = ExprNode::UnsafeMakeOperatorNode(foo_bar, {expr, Literal(57)},
                                            ExprAttributes());
    expr = ExprNode::UnsafeMakeOperatorNode(foo_bar, {expr, expr},
                                            ExprAttributes());
  }
  for (auto s : state) {
    auto x = ToDebugString(expr);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_ToDebugString_CustomReprBig);

}  // namespace
}  // namespace arolla::expr
