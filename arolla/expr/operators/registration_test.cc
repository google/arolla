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
#include "arolla/expr/operators/registration.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/expr/registered_expr_operator.h"

namespace arolla::expr_operators {
namespace {
using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeLambdaOperator;
using ::arolla::expr::Placeholder;
using ::arolla::expr::RegisterOperator;
using ::arolla::expr_operators::RegisterBackendOperator;
using ::arolla::expr_operators::type_meta::Unary;
using ::testing::HasSubstr;

AROLLA_DECLARE_EXPR_OPERATOR(TestBackendOp);
AROLLA_DEFINE_EXPR_OPERATOR(TestBackendOp,
                            RegisterBackendOperator("test.backend_op", Unary));
AROLLA_DECLARE_EXPR_OPERATOR(TestHigherLevelOp);
AROLLA_DEFINE_EXPR_OPERATOR(
    TestHigherLevelOp,
    RegisterOperator("test.higher_level_op",
                     MakeLambdaOperator(CallOp("test.backend_op",
                                               {Placeholder("x")}))));
AROLLA_DECLARE_EXPR_OPERATOR(TestBrokenOp);
AROLLA_DEFINE_EXPR_OPERATOR(
    TestBrokenOp,
    RegisterOperator("test.broken_op",
                     MakeLambdaOperator(CallOp("test.unregistered_op",
                                               {Placeholder("x")}))));

TEST(RegistrationTest, RegistrationMacros) {
  // Unable to call an unregistered operator.
  EXPECT_THAT(CallOp("test.higher_level_op", {Literal(1.0)}),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("operator 'test.higher_level_op' not found")));

  // Registration succeeds if operators are initialized in order.
  ASSERT_THAT(RegisterTestBackendOp(), IsOk());
  ASSERT_THAT(RegisterTestHigherLevelOp(), IsOk());
  EXPECT_THAT(CallOp("test.higher_level_op", {Literal(1.0)}), IsOk());

  // Registration fails if a referenced operator is not yet registered.
  EXPECT_THAT(RegisterTestBrokenOp(),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("operator 'test.unregistered_op' not found")));
}

}  // namespace

}  // namespace arolla::expr_operators
