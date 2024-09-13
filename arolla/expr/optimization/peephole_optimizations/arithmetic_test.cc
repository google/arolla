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
#include "arolla/expr/optimization/peephole_optimizations/arithmetic.h"

#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/optimization/optimizer.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;

class ArithmeticOptimizationsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(auto peephole_optimizer,
                         CreatePeepholeOptimizer({ArithmeticOptimizations}));
    optimizer_ = MakeOptimizer(std::move(peephole_optimizer));
  }

  absl::StatusOr<ExprNodePtr> ApplyOptimizer(ExprNodePtr expr) const {
    return DeepTransform(expr, optimizer_);
  }

  Optimizer optimizer_;
};

TEST_F(ArithmeticOptimizationsTest, AddZeroRemoval) {
  ExprNodePtr x_no_type = Leaf("x");
  ASSERT_OK_AND_ASSIGN(ExprNodePtr x_float,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ExprNodePtr zero_float = Literal(0.0f);
  ASSERT_OK_AND_ASSIGN(ExprNodePtr x_int,
                       WithQTypeAnnotation(Leaf("x"), GetQType<int>()));
  ExprNodePtr zero_int = Literal(0);
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr x_opt_double,
      WithQTypeAnnotation(Leaf("x"), GetOptionalQType<double>()));
  ExprNodePtr zero_opt_double = Literal(OptionalValue<double>(0.0));

  for (const auto& [a, b] : std::vector<std::pair<ExprNodePtr, ExprNodePtr>>{
           {x_no_type, zero_float},
           {x_opt_double, zero_float},
       }) {  // test no type match
    ASSERT_OK_AND_ASSIGN(ExprNodePtr add_zero, CallOp("math.add", {a, b}));
    EXPECT_THAT(ApplyOptimizer(add_zero), IsOkAndHolds(EqualsExpr(add_zero)));
  }
  for (const auto& [a, zero] : std::vector<std::pair<ExprNodePtr, ExprNodePtr>>{
           {x_float, zero_float},
           {x_int, zero_int},
           {x_opt_double, zero_opt_double},
       }) {
    ASSERT_OK_AND_ASSIGN(ExprNodePtr add_zero, CallOp("math.add", {a, zero}));
    EXPECT_THAT(ApplyOptimizer(add_zero), IsOkAndHolds(EqualsExpr(a)));
    ASSERT_OK_AND_ASSIGN(add_zero, CallOp("math.add", {zero, a}));
    EXPECT_THAT(ApplyOptimizer(add_zero), IsOkAndHolds(EqualsExpr(a)));
  }
}

TEST_F(ArithmeticOptimizationsTest, MulOneRemoval) {
  ExprNodePtr x_no_type = Leaf("x");
  ASSERT_OK_AND_ASSIGN(ExprNodePtr x_float,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ExprNodePtr one_float = Literal(1.0f);
  ASSERT_OK_AND_ASSIGN(ExprNodePtr x_int,
                       WithQTypeAnnotation(Leaf("x"), GetQType<int>()));
  ExprNodePtr one_int = Literal(1);
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr x_opt_double,
      WithQTypeAnnotation(Leaf("x"), GetOptionalQType<double>()));
  ExprNodePtr one_opt_double = Literal(OptionalValue<double>(1.0));

  for (const auto& [a, b] : std::vector<std::pair<ExprNodePtr, ExprNodePtr>>{
           {x_no_type, one_float},
           {x_opt_double, one_float},
       }) {  // test no type match
    ASSERT_OK_AND_ASSIGN(ExprNodePtr mul_one, CallOp("math.multiply", {a, b}));
    EXPECT_THAT(ApplyOptimizer(mul_one), IsOkAndHolds(EqualsExpr(mul_one)));
  }
  for (const auto& [a, one] : std::vector<std::pair<ExprNodePtr, ExprNodePtr>>{
           {x_float, one_float},
           {x_int, one_int},
           {x_opt_double, one_opt_double},
       }) {
    ASSERT_OK_AND_ASSIGN(ExprNodePtr mul_one,
                         CallOp("math.multiply", {a, one}));
    EXPECT_THAT(ApplyOptimizer(mul_one), IsOkAndHolds(EqualsExpr(a)));
    ASSERT_OK_AND_ASSIGN(mul_one, CallOp("math.multiply", {one, a}));
    EXPECT_THAT(ApplyOptimizer(mul_one), IsOkAndHolds(EqualsExpr(a)));
  }
}

}  // namespace
}  // namespace arolla::expr
