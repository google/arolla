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
#include "arolla/optools/optools.h"

#include <tuple>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::testing {
namespace {

using absl_testing::StatusIs;
using ::testing::HasSubstr;

int Add(int a, int b) { return a + b; }

template <typename T>
T Mul(T a, T b) {
  return a * b;
}

TEST(OpTools, RegisterFunctionAsOperator) {
  ASSERT_OK(optools::RegisterFunctionAsOperator(Add, "optools_test.add"));
  ASSERT_OK(optools::RegisterFunctionAsOperator(
      std::make_tuple(Mul<float>, Mul<int>), "optools_test.mul"));

  {
    ASSERT_OK_AND_ASSIGN(TypedValue res,
                         InvokeExprOperator("optools_test.add",
                                            {TypedValue::FromValue<int>(3),
                                             TypedValue::FromValue<int>(4)}));
    ASSERT_OK_AND_ASSIGN(int r, res.As<int>());
    EXPECT_EQ(r, 7);
  }
  {
    ASSERT_OK_AND_ASSIGN(TypedValue res,
                         InvokeExprOperator("optools_test.mul",
                                            {TypedValue::FromValue<int>(3),
                                             TypedValue::FromValue<int>(4)}));
    ASSERT_OK_AND_ASSIGN(int r, res.As<int>());
    EXPECT_EQ(r, 12);
  }
  {
    ASSERT_OK_AND_ASSIGN(TypedValue res,
                         InvokeExprOperator("optools_test.mul",
                                            {TypedValue::FromValue<float>(3),
                                             TypedValue::FromValue<float>(2)}));
    ASSERT_OK_AND_ASSIGN(float r, res.As<float>());
    EXPECT_FLOAT_EQ(r, 6.0);
  }

  EXPECT_THAT(
      InvokeExprOperator("optools_test.add", {TypedValue::FromValue<float>(3),
                                              TypedValue::FromValue<int>(4)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("no such overload")));

  EXPECT_THAT(
      InvokeExprOperator("optools_test.mul", {TypedValue::FromValue<float>(3),
                                              TypedValue::FromValue<int>(4)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("no such overload")));

  EXPECT_OK(optools::RegisterFunctionAsOperator(
      Add, "optools_test.add_with_description",
      expr::ExprOperatorSignature::Make("a, b"), "Sum A and B"));
  EXPECT_THAT(optools::RegisterFunctionAsOperator(
                  Add, "optools_test.add_with_wrong_signature",
                  expr::ExprOperatorSignature::Make("a, b, c"), "Sum A and B"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "operator signature doesn't match the function"));
}

}  // namespace
}  // namespace arolla::testing
