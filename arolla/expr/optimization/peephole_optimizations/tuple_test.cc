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
#include "arolla/expr/optimization/peephole_optimizations/tuple.h"

#include <cstdint>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;

class TupleOptimizationsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitArolla();

    ASSERT_OK_AND_ASSIGN(optimizer_,
                         CreatePeepholeOptimizer({TupleOptimizations}));
  }

  std::unique_ptr<PeepholeOptimizer> optimizer_;
};

TEST_F(TupleOptimizationsTest, SingleSubstitution) {
  auto a = Leaf("l1");
  auto b = Leaf("l2");
  auto c = Leaf("l3");
  auto d = Leaf("l4");
  ASSERT_OK_AND_ASSIGN(auto tuple, CallOp("core.make_tuple", {a, b, c, d}));
  {
    ASSERT_OK_AND_ASSIGN(auto get0, CallOp(GetNthOperator::Make(0), {tuple}));
    EXPECT_THAT(optimizer_->Apply(get0), IsOkAndHolds(EqualsExpr(a)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto get1, CallOp(GetNthOperator::Make(1), {tuple}));
    EXPECT_THAT(optimizer_->Apply(get1), IsOkAndHolds(EqualsExpr(b)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto get2, CallOp(GetNthOperator::Make(2), {tuple}));
    EXPECT_THAT(optimizer_->Apply(get2), IsOkAndHolds(EqualsExpr(c)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto get3, CallOp(GetNthOperator::Make(3), {tuple}));
    EXPECT_THAT(optimizer_->Apply(get3), IsOkAndHolds(EqualsExpr(d)));
  }
  // Doesn't apply and do not crash
  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(GetNthOperator::Make(0), {a}));
    EXPECT_THAT(optimizer_->Apply(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
}

TEST_F(TupleOptimizationsTest, WorksWithConcatTuples) {
  ASSERT_OK_AND_ASSIGN(auto a,
                       WithQTypeAnnotation(Leaf("a"), GetQType<int32_t>()));
  ASSERT_OK_AND_ASSIGN(auto b,
                       WithQTypeAnnotation(Leaf("b"), GetQType<int64_t>()));

  ASSERT_OK_AND_ASSIGN(
      auto concat_tuples,
      CallOp("core.concat_tuples",
             {CallOp("core.make_tuple", {a, b}), CallOp("core.make_tuple", {b}),
              CallOp("core.make_tuple", {a})}));
  ASSERT_OK_AND_ASSIGN(auto lowest_concat_tuples, ToLowest(concat_tuples));
  EXPECT_THAT(
      optimizer_->Apply(lowest_concat_tuples),
      IsOkAndHolds(EqualsExpr(CallOp("core.make_tuple", {a, b, b, a}))));
  ASSERT_OK_AND_ASSIGN(auto get_2,
                       CallOp(GetNthOperator::Make(2), {concat_tuples}));
}

}  // namespace
}  // namespace arolla::expr
