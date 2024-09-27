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
#include "arolla/expr/optimization/peephole_optimizer.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash_testing.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsExpr;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::Ne;

TEST(Optimization, Errors) {
  ExprNodePtr leaf = Leaf("x");
  ExprNodePtr px = Placeholder("x");
  ASSERT_OK_AND_ASSIGN(ExprNodePtr opx, CallOp("math.add", {px, px}));
  ExprNodePtr py = Placeholder("y");
  ASSERT_OK_AND_ASSIGN(ExprNodePtr opy, CallOp("math.add", {py, py}));
  EXPECT_THAT(PeepholeOptimization::CreatePatternOptimization(opx, leaf),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("leaves are not allowed")));
  EXPECT_THAT(PeepholeOptimization::CreatePatternOptimization(leaf, opx),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("leaves are not allowed")));
  EXPECT_THAT(PeepholeOptimization::CreatePatternOptimization(opy, opx),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("unknown placeholder keys")));
  EXPECT_THAT(PeepholeOptimization::CreatePatternOptimization(px, opx),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("match everything")));
  EXPECT_THAT(PeepholeOptimization::CreatePatternOptimization(
                  opx, opx, {{"y", [](auto) { return true; }}}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("unknown placeholder matcher keys")));
}

// Returns "optimization" converting `a + b` to `a - b`.
absl::StatusOr<std::unique_ptr<PeepholeOptimization>> Plus2MinusOptimization() {
  ASSIGN_OR_RETURN(
      ExprNodePtr apb,
      CallOpReference("math.add", {Placeholder("a"), Placeholder("b")}));
  ASSIGN_OR_RETURN(
      ExprNodePtr amb,
      CallOpReference("math.subtract", {Placeholder("a"), Placeholder("b")}));
  return PeepholeOptimization::CreatePatternOptimization(apb, amb);
}

// Returns "optimization" converting `core.make_tuple(a, b)` to `a`.
absl::StatusOr<std::unique_ptr<PeepholeOptimization>> Pair2FirstOptimization() {
  ASSIGN_OR_RETURN(
      ExprNodePtr from,
      CallOpReference("core.make_tuple", {Placeholder("a"), Placeholder("b")}));
  ExprNodePtr to = Placeholder("a");
  return PeepholeOptimization::CreatePatternOptimization(from, to);
}

TEST(Optimization, NoOptimizations) {
  ASSERT_OK_AND_ASSIGN(auto optimization, Plus2MinusOptimization());
  {
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp("math.multiply", {Leaf("a"), Leaf("b")}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp("math.subtract", {Leaf("a"), Leaf("b")}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ExprNodePtr expr = Placeholder("x");
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ExprNodePtr expr = Placeholder("x");
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ExprNodePtr expr = Literal(1.);
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
  ASSERT_OK_AND_ASSIGN(auto pair_optimization, Pair2FirstOptimization());
  {  // different number of args
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp("core.make_tuple", {Leaf("x")}));
    EXPECT_THAT(pair_optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
}

TEST(Optimization, Key) {
  ASSERT_OK_AND_ASSIGN(auto optimization, Plus2MinusOptimization());
  ASSERT_OK_AND_ASSIGN(ExprNodePtr plus,
                       CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr minus,
                       CallOp("math.subtract", {Leaf("x"), Leaf("y")}));
  ExprNodePtr leaf = Leaf("x");
  ExprNodePtr leaf2 = Leaf("y");
  ExprNodePtr placeholder = Placeholder("x");
  ExprNodePtr placeholder2 = Placeholder("y");
  ExprNodePtr literal = Literal(1.0);
  ExprNodePtr literal2 = Literal(1.0f);
  EXPECT_THAT(optimization->GetKey(),
              Eq(PeepholeOptimization::PatternKey(plus)));
  EXPECT_THAT(optimization->GetKey(),
              Ne(PeepholeOptimization::PatternKey(minus)));
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly({
      PeepholeOptimization::PatternKey(plus),
      PeepholeOptimization::PatternKey(minus),
      PeepholeOptimization::PatternKey(leaf),
      PeepholeOptimization::PatternKey(leaf2),
      PeepholeOptimization::PatternKey(literal),
      PeepholeOptimization::PatternKey(literal2),
      PeepholeOptimization::PatternKey(placeholder),
      PeepholeOptimization::PatternKey(placeholder2),
  }));
}

TEST(Optimization, SimpleOptimizations) {
  ASSERT_OK_AND_ASSIGN(auto optimization, Plus2MinusOptimization());
  {
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp("math.add", {Leaf("x"), Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr,
                         CallOp("math.subtract", {Leaf("x"), Leaf("y")}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
  {  // same nodes are allowed for different placeholders
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp("math.add", {Leaf("x"), Leaf("x")}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr,
                         CallOp("math.subtract", {Leaf("x"), Leaf("x")}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
  {  // placeholder can match placeholder and operator nodes
    ASSERT_OK_AND_ASSIGN(ExprNodePtr subexpr,
                         CallOp("math.multiply", {Leaf("a"), Leaf("b")}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp("math.add", {Placeholder("x"), subexpr}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr,
                         CallOp("math.subtract", {Placeholder("x"), subexpr}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
  {  // placeholder can match literals
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp("math.add", {Literal(1.f), Literal(2.f)}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr,
                         CallOp("math.subtract", {Literal(1.f), Literal(2.f)}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
}

TEST(Optimization, BackendWrapperOperatorOptimizations) {
  ASSERT_OK_AND_ASSIGN(auto optimization, Plus2MinusOptimization());
  {
    ASSERT_OK_AND_ASSIGN(
        auto add_backend,
        DecayRegisteredOperator(LookupOperator("math.add").value()));
    ASSERT_TRUE(HasBackendExprOperatorTag(add_backend));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp(add_backend, {Leaf("x"), Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr,
                         CallOp("math.subtract", {Leaf("x"), Leaf("y")}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
}

constexpr auto kIsLiteral = [](const ExprNodePtr& expr) {
  return expr->is_literal();
};

// Returns "optimization" converting
// `core.has(a | is_literal(b))` to `core.has(a) | core.has(b)`.
absl::StatusOr<std::unique_ptr<PeepholeOptimization>> HasLiteralOptimization() {
  ASSIGN_OR_RETURN(
      ExprNodePtr from,
      CallOpReference("core.has._array",
                      {CallOpReference("core.presence_or",
                                       {Placeholder("a"), Placeholder("b")})}));
  ASSIGN_OR_RETURN(
      ExprNodePtr to,
      CallOpReference("core.presence_or",
                      {CallOpReference("core.has", {Placeholder("a")}),
                       CallOpReference("core.has", {Placeholder("b")})}));
  return PeepholeOptimization::CreatePatternOptimization(from, to,
                                                         {{"b", kIsLiteral}});
}

TEST(Optimization, RestrictedOptimizations) {
  ASSERT_OK_AND_ASSIGN(auto optimization, HasLiteralOptimization());
  OptionalValue<float> opt1 = 1.0f;
  {
    // Second argument must be literal to match optimization requirements.
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expr,
        CallOp("core.has._array",
               {CallOp("core.presence_or", {Leaf("x"), Leaf("y")})}));
    ASSERT_OK_AND_ASSIGN(expr, ToLowest(expr));  // Decay operator overloads.
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expr,
        CallOp("core.has._array",
               {CallOp("core.presence_or", {Leaf("x"), Literal(opt1)})}));
    ASSERT_OK_AND_ASSIGN(expr, ToLowest(expr));  // Decay operator overloads.
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expected_expr,
        CallOp("core.presence_or", {CallOp("core.has", {Leaf("x")}),
                                    CallOp("core.has", {Literal(opt1)})}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
}

// Returns optimization converting `a ** 2` to `a * a`.
absl::StatusOr<std::unique_ptr<PeepholeOptimization>>
SquareA2AxAOptimization() {
  ASSIGN_OR_RETURN(
      ExprNodePtr square_a,
      CallOpReference("math.pow", {Placeholder("a"), Literal(2.f)}));
  ASSIGN_OR_RETURN(
      ExprNodePtr axa,
      CallOpReference("math.multiply", {Placeholder("a"), Placeholder("a")}));
  return PeepholeOptimization::CreatePatternOptimization(square_a, axa);
}

TEST(Optimization, LiteralOptimizations) {
  ASSERT_OK_AND_ASSIGN(auto optimization, SquareA2AxAOptimization());
  {
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp("math.pow", {Leaf("x"), Literal(2.f)}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr,
                         CallOp("math.multiply", {Leaf("x"), Leaf("x")}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
  {  // no optimization on literal mismatch
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp("math.pow", {Leaf("x"), Literal(3.f)}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
  {  // literal includes type information
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp("math.pow", {Leaf("x"), Literal(2.)}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
}

// Returns optimization converting `(a + b) * (a - b)` to `a * a - b * b`.
absl::StatusOr<std::unique_ptr<PeepholeOptimization>> ApBxAmBOptimization() {
  ASSIGN_OR_RETURN(
      ExprNodePtr from,
      CallOpReference(
          "math.multiply",
          {CallOpReference("math.add", {Placeholder("a"), Placeholder("b")}),
           CallOpReference("math.subtract",
                           {Placeholder("a"), Placeholder("b")})}));
  ASSIGN_OR_RETURN(
      ExprNodePtr to,
      CallOpReference("math.subtract",
                      {CallOpReference("math.multiply",
                                       {Placeholder("a"), Placeholder("a")}),
                       CallOpReference("math.multiply",
                                       {Placeholder("b"), Placeholder("b")})}));
  return PeepholeOptimization::CreatePatternOptimization(from, to);
}

TEST(Optimization, SamePartsInOptimization) {
  ASSERT_OK_AND_ASSIGN(auto optimization, ApBxAmBOptimization());
  {
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expr,
        CallOp("math.multiply",
               {CallOp("math.add", {Leaf("x"), Leaf("y")}),
                CallOp("math.subtract", {Leaf("x"), Leaf("y")})}));
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expected_expr,
        CallOp("math.subtract",
               {CallOp("math.multiply", {Leaf("x"), Leaf("x")}),
                CallOp("math.multiply", {Leaf("y"), Leaf("y")})}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
  // no optimization on different node corresponding to the same placeholder.
  {
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expr,
        CallOp("math.multiply",
               {CallOp("math.add", {Leaf("x"), Leaf("y")}),
                CallOp("math.subtract", {Leaf("x"), Leaf("c")})}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expr,
        CallOp("math.multiply",
               {CallOp("math.add", {Leaf("x"), Leaf("y")}),
                CallOp("math.subtract", {Leaf("x"), Leaf("x")})}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
}

// Returns optimization converting `(a + b) * ... * (a + b)` to `(a + b) ** n`.
absl::StatusOr<std::unique_ptr<PeepholeOptimization>> ApBPowerNOptimization(
    int64_t n) {
  ASSIGN_OR_RETURN(
      ExprNodePtr apb,
      CallOpReference("math.add", {Placeholder("a"), Placeholder("b")}));
  ExprNodePtr from = apb;
  for (int64_t i = 1; i != n; ++i) {
    ASSIGN_OR_RETURN(from, CallOpReference("math.multiply", {from, apb}));
  }
  ASSIGN_OR_RETURN(ExprNodePtr to,
                   CallOpReference("math.pow", {apb, Literal<int64_t>(n)}));
  return PeepholeOptimization::CreatePatternOptimization(from, to);
}

TEST(Optimization, ManySimilarNodes) {
  constexpr int64_t n = 25;
  ASSERT_OK_AND_ASSIGN(auto optimization, ApBPowerNOptimization(n));
  {
    ASSERT_OK_AND_ASSIGN(ExprNodePtr xpy,
                         CallOp("math.add", {Leaf("x"), Leaf("y")}));
    ExprNodePtr expr = xpy;
    for (int64_t i = 1; i != n; ++i) {
      ASSERT_OK_AND_ASSIGN(expr, CallOp("math.multiply", {expr, xpy}));
    }
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr,
                         CallOp("math.pow", {xpy, Literal<int64_t>(n)}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expected_expr)));
  }
  // no optimization on different node corresponding to the same placeholder.
  {
    ASSERT_OK_AND_ASSIGN(ExprNodePtr xpy,
                         CallOp("math.add", {Leaf("x"), Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr ypx,
                         CallOp("math.add", {Leaf("y"), Leaf("x")}));
    ExprNodePtr expr = xpy;
    for (int64_t i = 1; i != n - 2; ++i) {
      ASSERT_OK_AND_ASSIGN(expr, CallOp("math.multiply", {expr, xpy}));
    }
    // ypx vs xpy
    ASSERT_OK_AND_ASSIGN(expr, CallOp("math.multiply", {expr, ypx}));
    ASSERT_OK_AND_ASSIGN(expr, CallOp("math.multiply", {expr, xpy}));
    EXPECT_THAT(optimization->ApplyToRoot(expr),
                IsOkAndHolds(EqualsExpr(expr)));
  }
}

// Returns big random expression with `placeholder_count` placeholders and
// at least `op_count` operations.
absl::StatusOr<ExprNodePtr> BigRandomExpr(int64_t placeholder_count,
                                          int64_t op_count) {
  std::vector<ExprNodePtr> exprs;
  for (int64_t i = 0; i != placeholder_count; ++i) {
    exprs.push_back(Placeholder(std::to_string(i)));
  }
  absl::BitGen gen;
  auto binary_op = [&]() -> std::string {
    std::vector<std::string> names = {"math.add", "math.multiply", "math.pow"};
    return names[absl::Uniform(gen, 0u, names.size())];
  };
  for (int64_t i = 0; i != op_count; ++i) {
    auto x = exprs[absl::Uniform(gen, 0u, exprs.size())];
    auto y = exprs[absl::Uniform(gen, 0u, exprs.size())];
    ASSIGN_OR_RETURN(ExprNodePtr op, CallOp(binary_op(), {x, y}));
  }

  auto unary_op = [&]() -> std::string {
    std::vector<std::string> names = {"math.neg", "math.log", "math.log1p"};
    return names[absl::Uniform(gen, 0u, names.size())];
  };

  // reduce to one expression
  ExprNodePtr res = exprs.back();
  for (const ExprNodePtr& expr : exprs) {
    ASSIGN_OR_RETURN(res, CallOp(binary_op(), {CallOp(unary_op(), {res}),
                                               CallOp(unary_op(), {expr})}));
  }
  return res;
}

// Test on big expressions
TEST(Optimization, StressTest) {
  for (int64_t placeholder_count = 1; placeholder_count <= 64;
       placeholder_count *= 4) {
    for (int64_t op_count = 1; op_count <= 256; op_count *= 4) {
      ASSERT_OK_AND_ASSIGN(ExprNodePtr from,
                           BigRandomExpr(placeholder_count, op_count));
      ASSERT_OK_AND_ASSIGN(ExprNodePtr to,
                           BigRandomExpr(placeholder_count, op_count));
      ASSERT_OK_AND_ASSIGN(
          auto optimization,
          PeepholeOptimization::CreatePatternOptimization(from, to));
      absl::flat_hash_map<std::string, ExprNodePtr> subs;
      for (int i = 0; i != placeholder_count; ++i) {
        ASSERT_OK_AND_ASSIGN(ExprNodePtr sub, BigRandomExpr(i + 1, i * 2 + 1));
        subs.emplace(std::to_string(i), sub);
      }
      ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                           SubstitutePlaceholders(from, subs));
      ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr,
                           SubstitutePlaceholders(to, subs));
      EXPECT_THAT(optimization->ApplyToRoot(expr),
                  IsOkAndHolds(EqualsExpr(expected_expr)));
    }
  }
}

TEST(Optimization, TwoOptimizations) {
  std::vector<std::unique_ptr<PeepholeOptimization>> optimizations;
  ASSERT_OK_AND_ASSIGN(auto a2_opt, SquareA2AxAOptimization());
  optimizations.push_back(std::move(a2_opt));
  ASSERT_OK_AND_ASSIGN(auto a3_opt, ApBPowerNOptimization(3));
  optimizations.push_back(std::move(a3_opt));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr square,  // x ** 2
                       CallOp("math.pow", {Leaf("x"), Literal(2.f)}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr square2,  // x ** 2 + x ** 2
                       CallOp("math.add", {square, square}));
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr cubic_square2,  // square2 * square2 * square2
      CallOp("math.multiply",
             {CallOp("math.multiply", {square2, square2}), square2}));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr x2,  // x * x
                       CallOp("math.multiply", {Leaf("x"), Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expected_cubic_square2_optimized,  // (x * x + x * x) ** 3
      CallOp("math.pow", {CallOp("math.add", {x2, x2}), Literal(int64_t{3})}));
  ASSERT_OK_AND_ASSIGN(auto optimizer,
                       PeepholeOptimizer::Create(std::move(optimizations)));
  // Both optimizations applied.
  EXPECT_THAT(optimizer->Apply(cubic_square2),
              IsOkAndHolds(EqualsExpr(expected_cubic_square2_optimized)));
  // No more optimizations
  EXPECT_THAT(optimizer->Apply(expected_cubic_square2_optimized),
              IsOkAndHolds(EqualsExpr(expected_cubic_square2_optimized)));
}

// Returns transformation converting `a + b` and `a * b` to `a`.
absl::StatusOr<std::unique_ptr<PeepholeOptimization>>
RemoveArithmeticOptimization() {
  return PeepholeOptimization::CreateTransformOptimization(
      [](ExprNodePtr expr) {
        if (!expr->is_op()) {
          return expr;
        }
        if (expr->op()->display_name() == "math.add" ||
            expr->op()->display_name() == "math.multiply") {
          return expr->node_deps().empty() ? expr : expr->node_deps()[0];
        }
        return expr;
      });
}

TEST(Optimization, TransformOptimization) {
  std::vector<std::unique_ptr<PeepholeOptimization>> optimizations;
  ASSERT_OK_AND_ASSIGN(auto opt, RemoveArithmeticOptimization());
  optimizations.push_back(std::move(opt));
  ASSERT_OK_AND_ASSIGN(auto optimizer,
                       PeepholeOptimizer::Create(std::move(optimizations)));

  ExprNodePtr z = Leaf("z");
  ASSERT_OK_AND_ASSIGN(ExprNodePtr zx1,  // x * 1
                       CallOp("math.multiply", {z, Literal(1.f)}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr zx1p0,  // x * 1 + 0
                       CallOp("math.add", {zx1, Literal(0.f)}));

  EXPECT_THAT(optimizer->Apply(zx1), IsOkAndHolds(EqualsExpr(z)));
  EXPECT_THAT(optimizer->Apply(zx1p0), IsOkAndHolds(EqualsExpr(z)));
}

}  // namespace
}  // namespace arolla::expr
