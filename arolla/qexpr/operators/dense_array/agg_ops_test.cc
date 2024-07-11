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
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;

class AggOpsTest : public ::testing::Test {
  void SetUp() final { InitArolla(); }
};

TEST_F(AggOpsTest, TestAggCountFull) {
  auto values = CreateDenseArray<Unit>({kUnit, kUnit, kUnit, std::nullopt});
  DenseArray<int64_t> splits{CreateBuffer<int64_t>({0, 2, 4})};
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromSplitPoints(splits));
  ASSERT_OK_AND_ASSIGN(auto res, InvokeOperator<DenseArray<int64_t>>(
                                     "array._count", values, edge));
  EXPECT_THAT(res, ElementsAre(2, 1));
}

TEST_F(AggOpsTest, TestAggSumFloat) {
  auto values =
      CreateDenseArray<float>({1.0f, 2.0f, 3.0f, 10.0f, 20.0f, 30.0f});
  DenseArray<int64_t> splits{CreateBuffer<int64_t>({0, 3, 6, 6})};
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromSplitPoints(splits));
  ASSERT_OK_AND_ASSIGN(
      auto res, InvokeOperator<DenseArray<float>>("math._sum", values, edge,
                                                  OptionalValue<float>(0)));
  EXPECT_THAT(res, ElementsAre(6.0, 60.0, 0.0));
  // Missing init.
  ASSERT_OK_AND_ASSIGN(
      res, InvokeOperator<DenseArray<float>>("math._sum", values, edge,
                                             OptionalValue<float>{}));
  EXPECT_THAT(res, ElementsAre(6.0, 60.0, std::nullopt));
}

TEST_F(AggOpsTest, TestInverseCdf) {
  // clang-format off
  auto values = CreateDenseArray<float>(
      {std::nullopt, 6.0, 4.0, 3.0, 5.0, 7.0, 2.0, -10.0, -4.0, std::nullopt,
       70.0, 20.0, 60.0, 15.0, -100.0});
  // clang-format on
  DenseArray<int64_t> splits{CreateBuffer<int64_t>({0, 10, 15})};
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromSplitPoints(splits));

  {
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<DenseArray<float>>("math._inverse_cdf", values,
                                                    edge, 0.3f));
    EXPECT_THAT(res, ElementsAre(2.0, 15.0));
  }
  // CDF out of legal range; will be clamped.
  {
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<DenseArray<float>>("math._inverse_cdf", values,
                                                    edge, -0.01f));
    EXPECT_THAT(res, ElementsAre(-10.0, -100.0));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<DenseArray<float>>("math._inverse_cdf", values,
                                                    edge, 0.0f));
    EXPECT_THAT(res, ElementsAre(-10.0, -100.0));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<DenseArray<float>>("math._inverse_cdf", values,
                                                    edge, 1.01f));
    EXPECT_THAT(res, ElementsAre(7.0, 70.0));
  }
}

}  // namespace

}  // namespace arolla
