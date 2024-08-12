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
#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::InvokeExprOperator;
using ::arolla::testing::KeywordArg;
using ::arolla::testing::TypedValueWith;
using ::testing::Eq;
using ::testing::HasSubstr;

namespace arolla {
namespace {

class InvokeTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(InvokeTest, InvokeExprOperator) {
  EXPECT_THAT(InvokeExprOperator("math.multiply", {TypedValue::FromValue(3),
                                                   TypedValue::FromValue(19)}),
              IsOkAndHolds(TypedValueWith<int32_t>(57)));
  EXPECT_THAT(InvokeExprOperator<int32_t>("math.multiply", 3, 19),
              IsOkAndHolds(Eq(57)));
  EXPECT_THAT(InvokeExprOperator<int32_t>("math.subtract", 57,
                                          TypedValue::FromValue(17)),
              IsOkAndHolds(Eq(40)));
  EXPECT_THAT(InvokeExprOperator<TypedValue>("math.floordiv", 40,
                                             TypedValue::FromValue(34)),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(1))));
  ASSERT_OK_AND_ASSIGN(auto op, expr::LookupOperator("math.add"));
  EXPECT_THAT(InvokeExprOperator<TypedValue>(op, 1, TypedValue::FromValue(10)),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(11))));

  EXPECT_THAT(InvokeExprOperator<int64_t>("strings.find", Bytes("abcabcabc"),
                                          Bytes("abc"), KeywordArg("start", 1),
                                          KeywordArg("failure_value", -1)),
              IsOkAndHolds(3l));

  // Keyword arguments must follow positional arguments.
  EXPECT_THAT(
      InvokeExprOperator<int32_t>("strings.find", Bytes("abc"),
                                  KeywordArg("start", 1), Bytes("abcabcabc"),
                                  KeywordArg("failure_value", -1)),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Keyword arguments must follow positional arguments")));
}

}  // namespace
}  // namespace arolla
