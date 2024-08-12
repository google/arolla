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
#include "arolla/expr/operators/restricted_operator.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/expr/overloaded_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr_operators {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::Literal;
using ::arolla::expr_operators::type_meta::Floating;
using ::arolla::expr_operators::type_meta::Integral;
using ::arolla::testing::InvokeExprOperator;
using ::arolla::testing::TypedValueWith;
using ::testing::Eq;
using ::testing::HasSubstr;

class RestrictedOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(RestrictedOperatorTest, RestrictSimpleOperator) {
  ASSERT_OK_AND_ASSIGN(
      auto add_ints_op,
      RestrictOperator(expr::LookupOperator("math.add"), Integral));

  // Works fine with ints
  ASSERT_OK_AND_ASSIGN(
      auto add_ints,
      CallOp(add_ints_op, {Literal<int64_t>(50), Literal<int64_t>(7)}));
  EXPECT_THAT(add_ints->qtype(), Eq(GetQType<int64_t>()));
  EXPECT_THAT(expr::Invoke(add_ints, {}),
              IsOkAndHolds(TypedValueWith<int64_t>(57)));

  // Returns an error with floats
  EXPECT_THAT(
      CallOp(add_ints_op, {Literal<float>(50), Literal<float>(7)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr(
                   "expected all arguments to be integral, but got FLOAT32 for "
                   "0-th argument; in restriction for math.add operator")));
}

TEST_F(RestrictedOperatorTest, WorksWithOverloadedOperator) {
  ASSERT_OK_AND_ASSIGN(
      auto add_or_mul,
      expr::MakeOverloadedOperator(
          "test.add_or_mul",
          RestrictOperator(expr::LookupOperator("math.add"), Integral),
          RestrictOperator(expr::LookupOperator("math.multiply"), Floating)));

  // Adds integers.
  EXPECT_THAT(InvokeExprOperator<int32_t>(add_or_mul, 50, 7), IsOkAndHolds(57));
  // But multiplies floats.
  EXPECT_THAT(InvokeExprOperator<float>(add_or_mul, 3.f, 19.f),
              IsOkAndHolds(57.f));
}

}  // namespace
}  // namespace arolla::expr_operators
