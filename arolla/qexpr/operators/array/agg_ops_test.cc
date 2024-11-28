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
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//status/status.h"
#include "absl//status/status_matchers.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::absl_testing::StatusIs;

TEST(AggOpsTest, TestAggCountFull) {
  auto values = CreateArray<Unit>({kUnit, kUnit, kUnit, std::nullopt});
  auto splits = CreateArray<int64_t>({0, 2, 4});
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(splits));
  ASSERT_OK_AND_ASSIGN(
      auto res, InvokeOperator<Array<int64_t>>("array._count", values, edge));
  EXPECT_THAT(res, ElementsAre(2, 1));
}

TEST(AggOpsTest, TestAggSumFloat) {
  auto values = CreateArray<float>({1.0f, 2.0f, 3.0f, 10.0f, 20.0f, 30.0f});
  auto splits = CreateArray<int64_t>({0, 3, 6, 6});
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(splits));
  ASSERT_OK_AND_ASSIGN(auto res,
                       InvokeOperator<Array<float>>("math._sum", values, edge,
                                                    OptionalValue<float>(0)));
  EXPECT_THAT(res, ElementsAre(6.0, 60.0, 0.0));
  // Missing init.
  ASSERT_OK_AND_ASSIGN(res,
                       InvokeOperator<Array<float>>("math._sum", values, edge,
                                                    OptionalValue<float>{}));
  EXPECT_THAT(res, ElementsAre(6.0, 60.0, std::nullopt));
}

TEST(AggOpsTest, TestInverseCdf) {
  // clang-format off
  auto values = CreateArray<float>(
      {std::nullopt, 6.0, 4.0, 3.0, 5.0, 7.0, 2.0, -10.0, -4.0, std::nullopt,
       70.0, 20.0, 60.0, 15.0, -100.0});
  // clang-format on
  Array<int64_t> splits{CreateBuffer<int64_t>({0, 10, 15})};
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(splits));

  {
    ASSERT_OK_AND_ASSIGN(
        auto res,
        InvokeOperator<Array<float>>("math._inverse_cdf", values, edge, 0.3f));
    EXPECT_THAT(res, ElementsAre(2.0, 15.0));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto res,
        InvokeOperator<Array<float>>("math._inverse_cdf", values, edge, 0.0f));
    EXPECT_THAT(res, ElementsAre(-10.0, -100.0));
  }
}

TEST(AggOpsTest, TestInverseCdfErrors) {
  auto values = CreateArray<float>({1.0, 2.0, 3.0});
  Array<int64_t> splits{CreateBuffer<int64_t>({0, 3})};
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(splits));
  {
    EXPECT_THAT(
        InvokeOperator<Array<float>>("math._inverse_cdf", values, edge, -0.01f),
        StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(
        InvokeOperator<Array<float>>("math._inverse_cdf", values, edge,
                                     -std::numeric_limits<float>::infinity()),
        StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(
        InvokeOperator<Array<float>>("math._inverse_cdf", values, edge, 1.01f),
        StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(
        InvokeOperator<Array<float>>("math._inverse_cdf", values, edge,
                                     std::numeric_limits<float>::infinity()),
        StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(InvokeOperator<Array<float>>("math._inverse_cdf", values, edge,
                                             std::nanf("")),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

}  // namespace

}  // namespace arolla
