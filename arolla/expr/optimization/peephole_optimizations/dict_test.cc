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
#include "arolla/expr/optimization/peephole_optimizations/dict.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;

class DictOptimizationsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(optimizer_,
                         CreatePeepholeOptimizer({DictOptimizations}));
    GetDenseArrayQType<int>();   // Trigger the registration of
    GetDenseArrayQType<Unit>();  // DENSE_ARRAY_{UNIT,INT32}.
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

TEST_F(DictOptimizationsTest, Bool) {
  auto values = CreateDenseArray<float>({57.0, 1543.0});
  auto p = Leaf("cond");
  auto dict = Leaf("dict");
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expr,
      CallOp("array.at",
             {Literal(values), CallOp("dict._get_row", {dict, p})}));

  {
    // Nothing happens to non-literal dicts.
    ASSERT_OK_AND_ASSIGN(auto actual_expr, ApplyOptimizer(expr));
    ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(expr));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }

  {
    // Nothing happens to non-bool dicts.
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expr_with_literal_int_dict,
        SubstituteByFingerprint(
            expr, {{dict->fingerprint(),
                    Literal(KeyToRowDict<int64_t>{{1, 1}, {0, 0}})}}));
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(expr_with_literal_int_dict));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         ToLowest(expr_with_literal_int_dict));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }

  {
    // Bool literal dict lookups are replaced with logical_if.
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expr_with_literal_bool_dict,
        SubstituteByFingerprint(
            expr, {{dict->fingerprint(),
                    Literal(KeyToRowDict<bool>{{false, 1}, {true, 0}})}}));
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expected_true_value,
        SubstituteByFingerprint(expr_with_literal_bool_dict,
                                {{p->fingerprint(), Literal(true)}}));
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expected_false_value,
        SubstituteByFingerprint(expr_with_literal_bool_dict,
                                {{p->fingerprint(), Literal(false)}}));
    ASSERT_OK_AND_ASSIGN(auto actual_expr,
                         ApplyOptimizer(expr_with_literal_bool_dict));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        ToLowest(CallOp("bool.logical_if",
                        {p, expected_true_value, expected_false_value,
                         CallOp("core.empty_like", {expected_true_value})})));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

TEST_F(DictOptimizationsTest, Contains) {
  auto key = WithQTypeAnnotation(Leaf("key"), GetDenseArrayQType<int>());
  auto dict = Leaf("dict");
  ASSERT_OK_AND_ASSIGN(auto key_exists, CallOp("core.has", {key}));
  ASSERT_OK_AND_ASSIGN(auto dict_contains_key,
                       CallOp("dict._contains", {dict, key}));
  {
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.presence_and", {key_exists, dict_contains_key})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(dict_contains_key));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto actual_expr,
        ApplyOptimizer(
            CallOp("core.presence_and", {dict_contains_key, key_exists})));
    ASSERT_OK_AND_ASSIGN(auto expected_expr, ToLowest(dict_contains_key));
    EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
  }
}

}  // namespace
}  // namespace arolla::expr
