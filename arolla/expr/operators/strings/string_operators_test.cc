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
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/status_matchers_backport.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::ExprOperatorPtr;
using ::arolla::testing::InvokeExprOperator;
using ::arolla::testing::IsOkAndHolds;
using ::arolla::testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

class StringOperatorsTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() { InitArolla(); }
};

TEST_F(StringOperatorsTest, ContainsRegex) {
  EXPECT_THAT(InvokeExprOperator<OptionalUnit>("strings.contains_regex",
                                               Text{"aaabccc"}, Text{"a.c"}),
              IsOkAndHolds(kPresent));
  EXPECT_THAT(InvokeExprOperator<OptionalUnit>("strings.contains_regex",
                                               Text{"cccbaaa"}, Text{"a.c"}),
              IsOkAndHolds(kMissing));
  EXPECT_THAT(InvokeExprOperator<OptionalUnit>("strings.contains_regex",
                                               OptionalValue<Text>{"aaabccc"},
                                               Text{"a.c"}),
              IsOkAndHolds(kPresent));
  EXPECT_THAT(InvokeExprOperator<OptionalUnit>(
                  "strings.contains_regex", OptionalValue<Text>{}, Text{"a.c"}),
              IsOkAndHolds(kMissing));
  EXPECT_THAT(InvokeExprOperator<DenseArray<Unit>>(
                  "strings.contains_regex",
                  CreateDenseArray<Text>({Text("aaabccc"), Text("cccbaaa"),
                                          Text("ac"), std::nullopt}),
                  Text{"a.c"}),
              IsOkAndHolds(ElementsAre(kUnit, std::nullopt, std::nullopt,
                                       std::nullopt)));
}

TEST_F(StringOperatorsTest, ExtractRegex) {
  EXPECT_THAT(
      InvokeExprOperator<OptionalValue<Text>>("strings.extract_regex",
                                              Text{"aaabccc"}, Text{"a.c"}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected regular expression with exactly one capturing "
                    "group; got `a.c` which contains 0 capturing groups")));

  EXPECT_THAT(InvokeExprOperator<OptionalValue<Text>>(
                  "strings.extract_regex", Text{"aaabccc"}, Text{"(a.c)"}),
              IsOkAndHolds("abc"));
  EXPECT_THAT(InvokeExprOperator<OptionalValue<Text>>(
                  "strings.extract_regex", Text{"cccbaaa"}, Text{"(a.c)"}),
              IsOkAndHolds(std::nullopt));
  EXPECT_THAT(InvokeExprOperator<OptionalValue<Text>>(
                  "strings.extract_regex", OptionalValue<Text>{"aaabccc"},
                  Text{"(a.c)"}),
              IsOkAndHolds("abc"));
  EXPECT_THAT(
      InvokeExprOperator<OptionalValue<Text>>(
          "strings.extract_regex", OptionalValue<Text>{}, Text{"(a.c)"}),
      IsOkAndHolds(std::nullopt));
  EXPECT_THAT(InvokeExprOperator<DenseArray<Text>>(
                  "strings.extract_regex",
                  CreateDenseArray<Text>({Text("aaabccc"), Text("cccbaaa"),
                                          Text("ac"), std::nullopt}),
                  Text{"(a.c)"}),
              IsOkAndHolds(ElementsAre("abc", std::nullopt, std::nullopt,
                                       std::nullopt)));
}

}  // namespace
}  // namespace arolla::expr_operators
