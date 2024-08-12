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
#include "absl/status/status_matchers.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/init_arolla.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;

class BitwiseOperatorsTest : public ::testing::Test {
  void SetUp() final { InitArolla(); }
};

TEST_F(BitwiseOperatorsTest, BitwiseAnd) {
  EXPECT_THAT(
      InvokeOperator<int32_t>("bitwise.bitwise_and", int32_t{5}, int32_t{17}),
      IsOkAndHolds(1));
  EXPECT_THAT(
      InvokeOperator<int64_t>("bitwise.bitwise_and", int64_t{5}, int64_t{17}),
      IsOkAndHolds(1));

  EXPECT_THAT(
      InvokeOperator<int32_t>("bitwise.bitwise_and", int32_t{-2}, int32_t{17}),
      IsOkAndHolds(16));
  EXPECT_THAT(
      InvokeOperator<int32_t>("bitwise.bitwise_and", int32_t{-2}, int32_t{-2}),
      IsOkAndHolds(-2));
}

TEST_F(BitwiseOperatorsTest, BitwiseOr) {
  EXPECT_THAT(
      InvokeOperator<int32_t>("bitwise.bitwise_or", int32_t{5}, int32_t{17}),
      IsOkAndHolds(21));
  EXPECT_THAT(
      InvokeOperator<int64_t>("bitwise.bitwise_or", int64_t{5}, int64_t{17}),
      IsOkAndHolds(21));

  EXPECT_THAT(
      InvokeOperator<int32_t>("bitwise.bitwise_or", int32_t{-2}, int32_t{17}),
      IsOkAndHolds(-1));
  EXPECT_THAT(
      InvokeOperator<int32_t>("bitwise.bitwise_or", int32_t{-2}, int32_t{-2}),
      IsOkAndHolds(-2));
}

TEST_F(BitwiseOperatorsTest, BitwiseXor) {
  EXPECT_THAT(
      InvokeOperator<int32_t>("bitwise.bitwise_xor", int32_t{5}, int32_t{17}),
      IsOkAndHolds(20));
  EXPECT_THAT(
      InvokeOperator<int64_t>("bitwise.bitwise_xor", int64_t{5}, int64_t{17}),
      IsOkAndHolds(20));

  EXPECT_THAT(
      InvokeOperator<int32_t>("bitwise.bitwise_xor", int32_t{-2}, int32_t{17}),
      IsOkAndHolds(-17));
  EXPECT_THAT(
      InvokeOperator<int32_t>("bitwise.bitwise_xor", int32_t{-2}, int32_t{-2}),
      IsOkAndHolds(0));
}

TEST_F(BitwiseOperatorsTest, Invert) {
  EXPECT_THAT(InvokeOperator<int32_t>("bitwise.invert", int32_t{5}),
              IsOkAndHolds(-6));
  EXPECT_THAT(InvokeOperator<int64_t>("bitwise.invert", int64_t{5}),
              IsOkAndHolds(-6));
  EXPECT_THAT(InvokeOperator<int32_t>("bitwise.invert", int32_t{-2}),
              IsOkAndHolds(1));
}

}  // namespace
}  // namespace arolla
