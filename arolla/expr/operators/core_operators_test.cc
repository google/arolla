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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"

namespace arolla::expr_operators {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::expr::ExprNodePtr;
using ::arolla::testing::InvokeExprOperator;
using ::testing::ElementsAre;

TEST(CoreTest, ConstLikeBehavior) {
  EXPECT_THAT(InvokeExprOperator<float>("core.const_like", 5.f, 57),
              IsOkAndHolds(57.f));
  EXPECT_THAT(InvokeExprOperator<OptionalValue<float>>(
                  "core.const_like", OptionalValue<float>{5}, 57),
              IsOkAndHolds(OptionalValue<float>{57.f}));
  EXPECT_THAT(
      InvokeExprOperator<OptionalValue<float>>(
          "core.const_like", OptionalValue<float>{5}, MakeOptionalValue(57.f)),
      IsOkAndHolds(OptionalValue<float>{57.f}));

  EXPECT_THAT(InvokeExprOperator<DenseArray<float>>(
                  "core.const_like", CreateDenseArray<float>({}), 57),
              IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(
      InvokeExprOperator<DenseArray<float>>(
          "core.const_like", CreateDenseArray<float>({1.0, 2.0, 3.0}), 57),
      IsOkAndHolds(ElementsAre(57.f, 57.f, 57.f)));
}

TEST(CoreTest, PresenceAndOrBehavior) {
  EXPECT_THAT(
      InvokeExprOperator<float>("core._presence_and_or", 5.f, kPresent, 5.7f),
      IsOkAndHolds(5.f));
  EXPECT_THAT(
      InvokeExprOperator<float>("core._presence_and_or", 5.f, kMissing, 5.7f),
      IsOkAndHolds(5.7f));
  EXPECT_THAT(InvokeExprOperator<float>("core._presence_and_or",
                                        MakeOptionalValue(5.f), kPresent, 5.7f),
              IsOkAndHolds(5.f));
  EXPECT_THAT(InvokeExprOperator<float>("core._presence_and_or",
                                        MakeOptionalValue(5.f), kMissing, 5.7f),
              IsOkAndHolds(5.7f));
  EXPECT_THAT(
      InvokeExprOperator<OptionalValue<float>>(
          "core._presence_and_or", 5.f, kPresent, MakeOptionalValue(5.7f)),
      IsOkAndHolds(MakeOptionalValue(5.f)));
  EXPECT_THAT(
      InvokeExprOperator<OptionalValue<float>>(
          "core._presence_and_or", 5.f, kMissing, MakeOptionalValue(5.7f)),
      IsOkAndHolds(MakeOptionalValue(5.7f)));
  EXPECT_THAT(InvokeExprOperator<OptionalValue<float>>(
                  "core._presence_and_or", MakeOptionalValue(5.f), kPresent,
                  MakeOptionalValue(5.7f)),
              IsOkAndHolds(MakeOptionalValue(5.f)));
  EXPECT_THAT(InvokeExprOperator<OptionalValue<float>>(
                  "core._presence_and_or", MakeOptionalValue(5.f), kMissing,
                  MakeOptionalValue(5.7f)),
              IsOkAndHolds(MakeOptionalValue(5.7f)));
}

TEST(CoreTest, ZerosLikeBehavior) {
  EXPECT_THAT(InvokeExprOperator<float>("core.zeros_like", 5.f),
              IsOkAndHolds(0.f));
  EXPECT_THAT(InvokeExprOperator<OptionalValue<float>>("core.zeros_like",
                                                       OptionalValue<float>{5}),
              IsOkAndHolds(OptionalValue<float>{0.f}));
  EXPECT_THAT(InvokeExprOperator<DenseArray<float>>(
                  "core.zeros_like", CreateDenseArray<float>({})),
              IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(InvokeExprOperator<DenseArray<float>>(
                  "core.zeros_like", CreateDenseArray<float>({1.0, 2.0, 3.0})),
              IsOkAndHolds(ElementsAre(0.0f, 0.0f, 0.0f)));
}

TEST(CoreTest, OnesLikeBehavior) {
  EXPECT_THAT(InvokeExprOperator<float>("core.ones_like", 5.f),
              IsOkAndHolds(1.f));
  EXPECT_THAT(InvokeExprOperator<OptionalValue<float>>("core.ones_like",
                                                       OptionalValue<float>{5}),
              IsOkAndHolds(OptionalValue<float>{1.f}));
  EXPECT_THAT(InvokeExprOperator<DenseArray<float>>(
                  "core.ones_like", CreateDenseArray<float>({})),
              IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(
      InvokeExprOperator<DenseArray<float>>(
          "core.ones_like", CreateDenseArray<float>({1.0f, 2.0f, 3.0f})),
      IsOkAndHolds(ElementsAre(1.0f, 1.0f, 1.0f)));
}

}  // namespace
}  // namespace arolla::expr_operators
