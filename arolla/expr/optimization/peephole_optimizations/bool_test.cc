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
#include "arolla/expr/optimization/peephole_optimizations/bool.h"

#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;

class BoolOptimizationsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(optimizer_,
                         CreatePeepholeOptimizer({BoolOptimizations}));
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

TEST_F(BoolOptimizationsTest, LogicalNotRemoval) {
  ExprNodePtr x = Leaf("x");
  ExprNodePtr y = Leaf("y");

  {  // test double negation removal
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("bool.logical_not", {CallOp("bool.logical_not", {x})})));
    EXPECT_THAT(actual_expr, EqualsExpr(x));
  }
  {  // test `equal <-> not_equal`
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_eq, CallOp("bool.equal", {x, y}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_not_eq,
                         CallOp("bool.not_equal", {x, y}));
    {
      ASSERT_OK_AND_ASSIGN(
          auto actual_expr,
          ApplyOptimizer(CallOp("bool.logical_not", {bool_eq})));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(bool_not_eq));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
    {
      ASSERT_OK_AND_ASSIGN(
          auto actual_expr,
          ApplyOptimizer(CallOp("bool.logical_not", {bool_not_eq})));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(bool_eq));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
  }
  {  // test `less <-> greater_equal`
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(CallOp("bool.logical_not",
                                               {CallOp("bool.less", {x, y})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("bool.greater_equal", {x, y})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // test `greater <-> less_equal`
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("bool.logical_not", {CallOp("bool.less_equal", {x, y})})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(CallOp("bool.greater", {x, y})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(BoolOptimizationsTest, BoolToCore) {
  ASSERT_OK_AND_ASSIGN(auto x, WithQTypeAnnotation(Leaf("x"), GetQType<int>()));
  ASSERT_OK_AND_ASSIGN(auto y, WithQTypeAnnotation(Leaf("y"), GetQType<int>()));
  ExprNodePtr w = Leaf("w");
  ExprNodePtr q = Leaf("q");
  ExprNodePtr true_opt = Literal(MakeOptionalValue(true));
  {  // test `core.equal(True, bool.equal(a, b))` -> `core.equal(a, b)`
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_cmp, CallOp("bool.equal", {x, y}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr core_cmp, CallOp("core.equal", {x, y}));
    {
      ASSERT_OK_AND_ASSIGN(
          auto actual_expr,
          ApplyOptimizer(CallOp("core.equal", {bool_cmp, true_opt})));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_cmp));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
    {
      ASSERT_OK_AND_ASSIGN(
          auto actual_expr,
          ApplyOptimizer(CallOp("core.equal", {true_opt, bool_cmp})));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_cmp));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
  }
  {  // Test optimizer does not fall into inifnite loop on `core.equal(True,
     // optional{True})`.
    ASSERT_OK_AND_ASSIGN(auto core_cmp,
                         CallOp("core.equal", {Literal(true), true_opt}));
    ASSERT_OK_AND_ASSIGN(auto actual_expr, ApplyOptimizer(core_cmp));
    ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_cmp));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // test `core.equal(True, bool.less(a, b))` -> `core.less(a, b)`
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_cmp, CallOp("bool.less", {x, y}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr core_cmp, CallOp("core.less", {x, y}));
    {
      ASSERT_OK_AND_ASSIGN(
          auto actual_expr,
          ApplyOptimizer(CallOp("core.equal", {bool_cmp, true_opt})));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_cmp));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
    {
      ASSERT_OK_AND_ASSIGN(
          auto actual_expr,
          ApplyOptimizer(CallOp("core.equal", {true_opt, bool_cmp})));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_cmp));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
  }
  {  // test `core.equal(True, core._to_optional(bool.less(a, b)))` ->
     // `core.less(a, b)`
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_cmp, CallOp("bool.less", {x, y}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr core_cmp, CallOp("core.less", {x, y}));
    {
      ASSERT_OK_AND_ASSIGN(
          auto actual_expr,
          ApplyOptimizer(
              CallOp("core.equal",
                     {CallOp("core.to_optional", {bool_cmp}), true_opt})));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_cmp));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
    {
      ASSERT_OK_AND_ASSIGN(
          auto actual_expr,
          ApplyOptimizer(CallOp("core.equal", {true_opt, bool_cmp})));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_cmp));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
  }
  {  // test
    // core.equal(True, bool.logical_and(
    //    bool.less(a, b), bool.less_equal(d, c)))` ->
    // core.presence_and(core.less(a, b), core.less_equal(d, c))`
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_cmp1, CallOp("bool.less", {x, y}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_cmp2,
                         CallOp("bool.less_equal", {w, q}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_and,
                         CallOp("bool.logical_and", {bool_cmp1, bool_cmp2}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr core_cmp1, CallOp("core.less", {x, y}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr core_cmp2,
                         CallOp("core.less_equal", {w, q}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr core_and,
                         CallOp("core.presence_and", {core_cmp1, core_cmp2}));
    {
      ASSERT_OK_AND_ASSIGN(
          auto actual_expr,
          ToLowest(CallOp("core.equal", {bool_and, true_opt})));
      ASSERT_OK_AND_ASSIGN(actual_expr,  // apply multiple times
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(actual_expr,
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_and));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
    {
      ASSERT_OK_AND_ASSIGN(
          auto actual_expr,
          ToLowest(CallOp("core.equal", {true_opt, bool_and})));
      ASSERT_OK_AND_ASSIGN(actual_expr,  // apply multiple times
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(actual_expr,
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_and));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
  }
  {  // test
    // core.equal(True, bool.logical_or(
    //    bool.less(a, b), bool.less_equal(d, c)))` ->
    // core.presence_or(core.less(a, b), core.greater(c, d))`
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_cmp1, CallOp("bool.less", {x, y}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_cmp2,
                         CallOp("bool.less_equal", {w, q}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_or,
                         CallOp("bool.logical_or", {bool_cmp1, bool_cmp2}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr core_cmp1, CallOp("core.less", {x, y}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr core_cmp2,
                         CallOp("core.less_equal", {w, q}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr core_or,
                         CallOp("core.presence_or", {core_cmp1, core_cmp2}));
    {
      ASSERT_OK_AND_ASSIGN(auto actual_expr,
                           ToLowest(CallOp("core.equal", {bool_or, true_opt})));
      ASSERT_OK_AND_ASSIGN(actual_expr,  // apply multiple times
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(actual_expr,
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_or));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
    {
      ASSERT_OK_AND_ASSIGN(auto actual_expr,
                           ToLowest(CallOp("core.equal", {true_opt, bool_or})));
      ASSERT_OK_AND_ASSIGN(actual_expr,  // apply multiple times
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(actual_expr,
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_or));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
  }
  {  // test that one comparison is enough to apply optimization
    // core.equal(True, bool.logical_or(bool.less(a, b), c))` ->
    // core.presence_or(core.less(a, b), core.equal(c, True))`
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_cmp1, CallOp("bool.less", {x, y}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_or,
                         CallOp("bool.logical_or", {bool_cmp1, q}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr core_cmp1, CallOp("core.less", {x, y}));
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr core_or,
        CallOp("core.presence_or",
               {core_cmp1, CallOp("core.equal", {q, true_opt})}));
    {
      ASSERT_OK_AND_ASSIGN(auto actual_expr,
                           ToLowest(CallOp("core.equal", {bool_or, true_opt})));
      ASSERT_OK_AND_ASSIGN(actual_expr,  // apply multiple times
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(actual_expr,
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_or));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
    {
      ASSERT_OK_AND_ASSIGN(auto actual_expr,
                           ToLowest(CallOp("core.equal", {true_opt, bool_or})));
      ASSERT_OK_AND_ASSIGN(actual_expr,  // apply multiple times
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(actual_expr,
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_or));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
  }
  {  // test that literal also trigger optimization
    // core.equal(True, bool.logical_or(True, a))` ->
    // core.presence_or(core.equal(True, True), core.equal(a, True))`
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_or,
                         CallOp("bool.logical_or", {true_opt, q}));
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr core_or,
        CallOp("core.presence_or", {CallOp("core.equal", {true_opt, true_opt}),
                                    CallOp("core.equal", {q, true_opt})}));
    {
      ASSERT_OK_AND_ASSIGN(auto actual_expr,
                           ToLowest(CallOp("core.equal", {bool_or, true_opt})));
      ASSERT_OK_AND_ASSIGN(actual_expr,  // apply multiple times
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(actual_expr,
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_or));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
    {
      ASSERT_OK_AND_ASSIGN(auto actual_expr,
                           ToLowest(CallOp("core.equal", {true_opt, bool_or})));
      ASSERT_OK_AND_ASSIGN(actual_expr,  // apply multiple times
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(actual_expr,
                           ToLowest(optimizer_->Apply(actual_expr)));
      ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(core_or));
      EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
    }
  }
  {  // core.equal(bool.logical_or(a, b), True)` is not optimized
    ASSERT_OK_AND_ASSIGN(ExprNodePtr bool_or,
                         CallOp("bool.logical_or", {w, q}));
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core.equal", {bool_or, true_opt}));
    ASSERT_OK_AND_ASSIGN(auto actual_expr, ApplyOptimizer(expr));
    ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(expr));
    EXPECT_THAT(ApplyOptimizer(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
}

TEST_F(BoolOptimizationsTest, LogicalIf) {
  ExprNodePtr a = Leaf("a");
  ExprNodePtr b = Leaf("b");
  ExprNodePtr c = Leaf("c");
  ExprNodePtr d = Leaf("d");
  ASSERT_OK_AND_ASSIGN(ExprNodePtr cond_full,
                       WithQTypeAnnotation(Leaf("cond"), GetQType<bool>()));
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr cond_optional,
      WithQTypeAnnotation(Leaf("cond"), GetOptionalQType<bool>()));
  ExprNodePtr cond_unknown = Leaf("cond");

  {  // test
     // bool.logical_if(core.to_optional(cond), a, b, c)
     // ->
     // core.where(cond == True, a, b)
    for (const auto& [cond, do_optimize] :
         {std::pair{cond_full, true}, std::pair{cond_unknown, false}}) {
      ASSERT_OK_AND_ASSIGN(
          ExprNodePtr from,
          CallOp("bool.logical_if",
                 {CallOp("core.to_optional", {cond}), a, b, c}));
      ASSERT_OK_AND_ASSIGN(
          ExprNodePtr to,
          CallOp("core.where",
                 {CallOp("core.equal", {cond, Literal(true)}), a, b}));
      auto result = do_optimize ? to : from;
      EXPECT_THAT(ApplyOptimizer(from),
                  IsOkAndHolds(EqualsExpr(ToLowest(result))));
    }
    {  // test
       // bool.logical_if(core.presence_or(cond, False), a, b, c)
       // ->
       // core.where(cond == True, a, b)
      ASSERT_OK_AND_ASSIGN(
          ExprNodePtr from1,
          CallOp("bool.logical_if",
                 {CallOp("core.presence_or", {cond_unknown, Literal(false)}), a,
                  b, c}));
      ASSERT_OK_AND_ASSIGN(
          ExprNodePtr from2,
          CallOp("bool.logical_if",
                 {CallOp("core.presence_or",
                         {cond_unknown, Literal(MakeOptionalValue(false))}),
                  a, b, c}));
      ASSERT_OK_AND_ASSIGN(
          ExprNodePtr to,
          CallOp("core.where",
                 {CallOp("core.equal", {cond_unknown, Literal(true)}), a, b}));
      EXPECT_THAT(ApplyOptimizer(from1),
                  IsOkAndHolds(EqualsExpr(ToLowest(to))));
      EXPECT_THAT(ApplyOptimizer(from2),
                  IsOkAndHolds(EqualsExpr(ToLowest(to))));
      ASSERT_OK_AND_ASSIGN(
          ExprNodePtr no_optimization,
          CallOp("bool.logical_if",
                 {CallOp("core.presence_or", {cond_unknown, d}), a, b, c}));
      EXPECT_THAT(ApplyOptimizer(no_optimization),
                  IsOkAndHolds(EqualsExpr(ToLowest(no_optimization))));
    }
  }
  {  // test
     // bool.logical_if(bool.equal(a, 1), b, c, c)
     // ->
     // core.where(x == 1, b, c)
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ToLowest(CallOp("bool.logical_if",
                        {CallOp("bool.equal", {a, Literal(1)}), b, c, c})));
    ASSERT_OK_AND_ASSIGN(actual_expr,  // apply multiple times
                         ToLowest(optimizer_->Apply(actual_expr)));
    ASSERT_OK_AND_ASSIGN(actual_expr, ToLowest(optimizer_->Apply(actual_expr)));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("core.where",
                        {CallOp("core.equal", {a, Literal(1)}), b, c})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {  // test
    // Case when first 2 arguments after cond are boolean.
    // bool.logical_if(cond, true, false, a)
    // ->
    // core.presence_or(cond, a)
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr from,
        CallOp("bool.logical_if", {a, Literal(true), Literal(false), b}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr to, CallOp("core.presence_or", {a, b}));
    EXPECT_THAT(ApplyOptimizer(from), IsOkAndHolds(EqualsExpr(to)));
  }
  {  // test
     // bool.logical_if(core.presence_or(cond, True), a, b, c)
     // ->
     // core.where(cond == False, b, a)
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr1,
        ApplyOptimizer(
            CallOp("bool.logical_if",
                   {CallOp("core.presence_or", {cond_unknown, Literal(true)}),
                    a, b, c})));
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr2,
        ApplyOptimizer(
            CallOp("bool.logical_if",
                   {CallOp("core.presence_or",
                           {cond_unknown, Literal(MakeOptionalValue(true))}),
                    a, b, c})));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp(
            "core.where",
            {CallOp("core.equal", {cond_unknown, Literal(false)}), b, a})));
    EXPECT_THAT(actual_expr1, EqualsExpr(expected_expr));
    EXPECT_THAT(actual_expr2, EqualsExpr(expected_expr));
  }
  {  // no optimization
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr no_optimization,
        CallOp("bool.logical_if",
               {CallOp("core.presence_or", {Literal(false), cond_unknown}), a,
                b, c}));
    EXPECT_THAT(ApplyOptimizer(no_optimization),
                IsOkAndHolds(EqualsExpr(no_optimization)));
  }
}

}  // namespace
}  // namespace arolla::expr
