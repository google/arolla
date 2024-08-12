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
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/unit.h"

namespace arolla::testing {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;

class LogicOpsTest : public ::testing::Test {
  void SetUp() final { InitArolla(); }
};

TEST_F(LogicOpsTest, DenseArrayPresenceAndOp) {
  EXPECT_THAT(InvokeOperator<DenseArray<int>>(
                  "core.presence_and", CreateDenseArray<int>({1, 2, 3}),
                  CreateDenseArray<Unit>({kUnit, std::nullopt, kUnit})),
              IsOkAndHolds(ElementsAre(1, std::nullopt, 3)));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>(
          "core.presence_and", CreateDenseArray<int>({1, 2, std::nullopt}),
          CreateDenseArray<Unit>({kUnit, std::nullopt, kUnit})),
      IsOkAndHolds(ElementsAre(1, std::nullopt, std::nullopt)));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>(
          "core.presence_and", CreateDenseArray<int>({1, 2, std::nullopt}),
          CreateDenseArray<Unit>({kUnit, kUnit, kUnit})),
      IsOkAndHolds(ElementsAre(1, 2, std::nullopt)));
}

TEST_F(LogicOpsTest, DenseArrayPresenceOrOp) {
  EXPECT_THAT(InvokeOperator<DenseArray<int>>("core.presence_or",
                                              CreateDenseArray<int>({1, 2, 3}),
                                              CreateDenseArray<int>({4, 5, 6})),
              IsOkAndHolds(ElementsAre(1, 2, 3)));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>(
          "core.presence_or", CreateDenseArray<int>({1, 2, std::nullopt}),
          CreateDenseArray<int>({4, 5, 6})),
      IsOkAndHolds(ElementsAre(1, 2, 6)));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>(
          "core.presence_or", CreateDenseArray<int>({1, 2, std::nullopt}),
          CreateDenseArray<int>({4, 5, std::nullopt})),
      IsOkAndHolds(ElementsAre(1, 2, std::nullopt)));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>(
          "core.presence_or",
          CreateDenseArray<int>({std::nullopt, std::nullopt, std::nullopt}),
          CreateDenseArray<int>({4, 5, std::nullopt})),
      IsOkAndHolds(ElementsAre(4, 5, std::nullopt)));
}

TEST_F(LogicOpsTest, DenseArrayPresenceNotOp) {
  {
    auto full_int = CreateConstDenseArray<int>(35, 7);
    auto empty_unit = CreateEmptyDenseArray<Unit>(35);
    EXPECT_THAT(InvokeOperator<DenseArray<Unit>>("core.presence_not._builtin",
                                                 full_int),
                IsOkAndHolds(ElementsAreArray(empty_unit)));
  }
  {
    auto empty_int = CreateEmptyDenseArray<int>(35);
    auto full_unit = CreateConstDenseArray<Unit>(35, kUnit);
    EXPECT_THAT(InvokeOperator<DenseArray<Unit>>("core.presence_not._builtin",
                                                 empty_int),
                IsOkAndHolds(ElementsAreArray(full_unit)));
  }
  {
    std::vector<std::optional<int>> input(35, std::nullopt);
    input[15] = 5;
    input[24] = 7;
    std::vector<OptionalValue<Unit>> expected(input.size());
    for (int i = 0; i < input.size(); ++i) {
      expected[i].present = !input[i].has_value();
    }
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<DenseArray<Unit>>(
                      "core.presence_not._builtin",
                      CreateDenseArray<int>(input.begin(), input.end())));
    EXPECT_EQ(std::vector(res.begin(), res.end()), expected);
  }
}

TEST_F(LogicOpsTest, DenseArrayPresenceOrWithOptionalOp) {
  EXPECT_THAT(InvokeOperator<DenseArray<int>>("core.presence_or",
                                              CreateDenseArray<int>({1, 2, 3}),
                                              OptionalValue<int>(4)),
              IsOkAndHolds(ElementsAre(1, 2, 3)));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>(
          "core.presence_or", CreateDenseArray<int>({1, std::nullopt, 3}),
          OptionalValue<int>(4)),
      IsOkAndHolds(ElementsAre(1, 4, 3)));
  EXPECT_THAT(InvokeOperator<DenseArray<int>>(
                  "core.presence_or",
                  CreateDenseArray<int>({std::nullopt, std::nullopt}),
                  OptionalValue<int>(4)),
              IsOkAndHolds(ElementsAre(4, 4)));
  // missed
  EXPECT_THAT(InvokeOperator<DenseArray<int>>("core.presence_or",
                                              CreateDenseArray<int>({3, 2}),
                                              OptionalValue<int>()),
              IsOkAndHolds(ElementsAre(3, 2)));
  EXPECT_THAT(InvokeOperator<DenseArray<int>>(
                  "core.presence_or", CreateDenseArray<int>({3, std::nullopt}),
                  OptionalValue<int>()),
              IsOkAndHolds(ElementsAre(3, std::nullopt)));
  EXPECT_THAT(InvokeOperator<DenseArray<int>>(
                  "core.presence_or",
                  CreateDenseArray<int>({std::nullopt, std::nullopt}),
                  OptionalValue<int>()),
              IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt)));
}

TEST_F(LogicOpsTest, HasOp) {
  auto array = CreateDenseArray<float>({1.0, {}, 2.0, {}, 3.0});
  ASSERT_OK_AND_ASSIGN(
      auto mask, InvokeOperator<DenseArray<Unit>>("core.has._array", array));
  EXPECT_THAT(mask,
              ElementsAre(kUnit, std::nullopt, kUnit, std::nullopt, kUnit));
}

}  // namespace
}  // namespace arolla::testing
