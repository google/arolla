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
#include "arolla/expr/operators/dynamic_lifting.h"

#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/backend_wrapping_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla::expr_operators {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::expr::BackendWrappingOperator;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr_operators::type_meta::CallableStrategy;
using ::arolla::expr_operators::type_meta::Chain;
using ::arolla::expr_operators::type_meta::CommonType;
using ::arolla::expr_operators::type_meta::Ternary;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Field;

TEST(DynamicLiftingTest, LiftDynamically) {
  ASSERT_OK_AND_ASSIGN(auto scalar_signature,
                       ExprOperatorSignature::Make("a, b, c"));
  auto scalar_operator = std::make_shared<BackendWrappingOperator>(
      "test.scalar_operator", scalar_signature,
      CallableStrategy(Chain(Ternary, CommonType)));
  ASSERT_OK_AND_ASSIGN(auto lifted_operator, LiftDynamically(scalar_operator));

  EXPECT_THAT(lifted_operator->display_name(), Eq("test.scalar_operator"));
  EXPECT_THAT(
      lifted_operator->GetSignature(),
      IsOkAndHolds(Field(
          &ExprOperatorSignature::parameters,
          ElementsAre(Field(&ExprOperatorSignature::Parameter::name, "a"),
                      Field(&ExprOperatorSignature::Parameter::name, "b"),
                      Field(&ExprOperatorSignature::Parameter::name, "c")))));
  {
    auto scalar_args = {
        WithQTypeAnnotation(Leaf("a"), GetQType<float>()),
        WithQTypeAnnotation(Leaf("b"), GetOptionalQType<float>()),
        WithQTypeAnnotation(Leaf("c"), GetQType<double>())};

    ASSERT_OK_AND_ASSIGN(auto scalar_expr,
                         CallOp(lifted_operator, scalar_args));
    EXPECT_THAT(scalar_expr->qtype(), Eq(GetOptionalQType<double>()));
    ASSERT_OK_AND_ASSIGN(auto expected_expr,
                         CallOp(scalar_operator, scalar_args));
    EXPECT_THAT(ToLowest(scalar_expr),
                IsOkAndHolds(EqualsExpr(ToLowest(expected_expr))));
  }
  {
    std::vector<absl::StatusOr<ExprNodePtr>> array_args = {
        WithQTypeAnnotation(Leaf("a"), GetQType<float>()),
        WithQTypeAnnotation(Leaf("b"), GetDenseArrayQType<float>()),
        WithQTypeAnnotation(Leaf("c"), GetOptionalQType<double>())};

    ASSERT_OK_AND_ASSIGN(auto array_expr, CallOp(lifted_operator, array_args));
    EXPECT_THAT(array_expr->qtype(), Eq(GetDenseArrayQType<double>()));
    ASSERT_OK_AND_ASSIGN(ExprOperatorPtr map_op,
                         expr::LookupOperator("core.map"));
    ASSERT_OK_AND_ASSIGN(
        auto expected_expr,
        CallOp("core.apply_varargs",
               {Literal(map_op), Literal<ExprOperatorPtr>(scalar_operator),
                CallOp(expr::MakeTupleOperator::Make(), array_args)}));
    EXPECT_THAT(ToLowest(array_expr),
                IsOkAndHolds(EqualsExpr(ToLowest(expected_expr))));
  }
}

}  // namespace
}  // namespace arolla::expr_operators
