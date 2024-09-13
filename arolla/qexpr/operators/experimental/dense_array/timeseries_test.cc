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
#include <initializer_list>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/qexpr/operators.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;

absl::StatusOr<DenseArrayEdge> CreateEdgeFromSplitPoints(
    std::initializer_list<int64_t> splits) {
  return DenseArrayEdge::FromSplitPoints({CreateBuffer<int64_t>(splits)});
}

const char kAggMovingAverage[] = "experimental.agg_moving_average";
constexpr auto NA = std::nullopt;

TEST(AggMovingAverage, FullTimeSeries) {
  // moving_average([1, 2, 3, 4, 5, 6, 7, 8], 3) -> [None, None, 2, 3, 4, 5, 6,
  // 7]
  const auto series = CreateDenseArray<float>({1, 2, 3, 4, 5, 6, 7, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, CreateEdgeFromSplitPoints({0, 8}));
  EXPECT_THAT(InvokeOperator<DenseArray<float>>(kAggMovingAverage, series,
                                                int64_t{3}, edge),
              IsOkAndHolds(ElementsAre(NA, NA, 2, 3, 4, 5, 6, 7)));
}

TEST(AggMovingAverage, TimeSeriesWithMissingValue) {
  // moving_average([1, 2, 3, None, 5, 6, 7, 8], 3) -> [None, None, 2, None,
  // None, None, 6, 7]
  const auto series = CreateDenseArray<float>({1, 2, 3, NA, 5, 6, 7, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, CreateEdgeFromSplitPoints({0, 8}));
  EXPECT_THAT(InvokeOperator<DenseArray<float>>(kAggMovingAverage, series,
                                                int64_t{3}, edge),
              IsOkAndHolds(ElementsAre(NA, NA, 2, NA, NA, NA, 6, 7)));
}

TEST(AggMovingAverage, ZeroWindow) {
  // moving_average([1, 2, 3, 4, 5, 6, 7, 8], 0) -> [None, None, None, None,
  // None, None, None, None]
  const auto series = CreateDenseArray<float>({1, 2, 3, 4, 5, 6, 7, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, CreateEdgeFromSplitPoints({0, 8}));
  EXPECT_THAT(InvokeOperator<DenseArray<float>>(kAggMovingAverage, series,
                                                int64_t{0}, edge),
              IsOkAndHolds(ElementsAre(NA, NA, NA, NA, NA, NA, NA, NA)));
}

TEST(AggMovingAverage, WindowSizeEqualToInputArraySize) {
  // moving_average([1, 2, 3, 4, 5, 6, 7, 8], 8) -> [None, None, None, None,
  // None, None, None, 4.5]
  const auto series = CreateDenseArray<float>({1, 2, 3, 4, 5, 6, 7, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, CreateEdgeFromSplitPoints({0, 8}));
  EXPECT_THAT(InvokeOperator<DenseArray<float>>(kAggMovingAverage, series,
                                                int64_t{8}, edge),
              IsOkAndHolds(ElementsAre(NA, NA, NA, NA, NA, NA, NA, 4.5)));
}

TEST(AggMovingAverage, WindowSizeLargerThanInputArraySize) {
  // moving_average([1, 2, 3, 4, 5, 6, 7, 8], 9) -> [None, None, None, None,
  // None, None, None, None]
  const auto series = CreateDenseArray<float>({1, 2, 3, 4, 5, 6, 7, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, CreateEdgeFromSplitPoints({0, 8}));
  EXPECT_THAT(InvokeOperator<DenseArray<float>>(kAggMovingAverage, series,
                                                int64_t{9}, edge),
              IsOkAndHolds(ElementsAre(NA, NA, NA, NA, NA, NA, NA, NA)));
}

TEST(AggMovingAverage, EmptyTimeSeries) {
  // moving_average([], 3) -> []
  ASSERT_OK_AND_ASSIGN(auto edge, CreateEdgeFromSplitPoints({0}));
  EXPECT_THAT(InvokeOperator<DenseArray<float>>(
                  kAggMovingAverage, DenseArray<float>(), int64_t{3}, edge),
              IsOkAndHolds(ElementsAre()));
}

TEST(AggMovingAverage, FullTimeSeriesWithTwoGroups) {
  // moving_average([[1, 2, 3, 4], [5, 6, 7, 8]], 3) -> [[None, None, 2, 3],
  // [None, None, 6, 7]]
  const auto series = CreateDenseArray<float>({1, 2, 3, 4, 5, 6, 7, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, CreateEdgeFromSplitPoints({0, 4, 8}));
  EXPECT_THAT(InvokeOperator<DenseArray<float>>(kAggMovingAverage, series,
                                                int64_t{3}, edge),
              IsOkAndHolds(ElementsAre(NA, NA, 2, 3, NA, NA, 6, 7)));
}

TEST(ExponentialWeightedMovingAverageOpTest, MissingValue_Adjust) {
  // ewma([1, 2, 3, None, 5, 6, 7, 8], 0.6) ->
  // [1., 1.71428571, 2.53846154, 2.53846154, 4.50832266, 5.50288031,
  // 6.43861754, 7.39069488]
  const auto series = CreateDenseArray<float>({1, 2, 3, NA, 5, 6, 7, 8});
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.6,
                                        /*adjust=*/true,
                                        /*ignore_missing=*/false),
      IsOkAndHolds(ElementsAre(1., 1.71428571, 2.53846154, 2.53846154,
                               4.50832266, 5.50288031, 6.43861754,
                               7.39069488)));
}

TEST(ExponentialWeightedMovingAverageOpTest,
     MissingValue_Adjust_IgnoreMissing) {
  // ewma([1, 2, 3, None, 5, 6, 7, 8], 0.6) ->
  // [1., 1.71428571, 2.53846154, 2.53846154, 4.05418719, 5.23375364,
  // 6.29786003, 7.32082003]
  const auto series = CreateDenseArray<float>({1, 2, 3, NA, 5, 6, 7, 8});
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.6,
                                        /*adjust=*/true,
                                        /*ignore_missing=*/true),
      IsOkAndHolds(ElementsAre(1., 1.71428571, 2.53846154, 2.53846154,
                               4.05418719, 5.23375364, 6.29786003,
                               7.32082003)));
}

TEST(ExponentialWeightedMovingAverageOpTest, FirstMissing_Adjust) {
  // ewma([None, 2, 3], 0.6) -> [None, 2., 2.71428571]
  const auto series = CreateDenseArray<float>({NA, 2, 3});
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.6,
                                        /*adjust=*/true,
                                        /*ignore_missing=*/false),
      IsOkAndHolds(ElementsAre(NA, 2., 2.71428571)));
}

TEST(ExponentialWeightedMovingAverageOpTest, FirstTwoMissing_Adjust) {
  // ewma([None, None, 3, None, 5], 0.6) -> [None, None, 3., 3., 4.72413793]
  const auto series = CreateDenseArray<float>({NA, NA, 3, NA, 5});
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.6,
                                        /*adjust=*/true,
                                        /*ignore_missing=*/false),
      IsOkAndHolds(ElementsAre(NA, NA, 3., 3., 4.72413793)));
}

TEST(ExponentialWeightedMovingAverageOpTest,
     FirstTwoMissing_Adjust_IgnoreMissing) {
  // ewma([None, None, 3, None, 5], 0.6) -> [None, None, 3., 3., 4.42857143]
  const auto series = CreateDenseArray<float>({NA, NA, 3, NA, 5});
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.6,
                                        /*adjust=*/true,
                                        /*ignore_missing=*/true),
      IsOkAndHolds(ElementsAre(NA, NA, 3., 3., 4.42857143)));
}

TEST(ExponentialWeightedMovingAverageOpTest, AlphaLessThanZero_Adjust) {
  // ewma([None, 2, 3], -1.2) -> invalid argument status
  const auto series = CreateDenseArray<float>({NA, 2, 3});
  ASSERT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma", series,
                                                -1.2, /*adjust=*/true,
                                                /*ignore_missing=*/false),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExponentialWeightedMovingAverageOpTest, AlphaEqualsZero_Adjust) {
  // ewma([None, 2, 3], -1.2) -> invalid argument status
  const auto series = CreateDenseArray<float>({NA, 2, 3});
  ASSERT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.,
                                                /*adjust=*/true,
                                                /*ignore_missing=*/false),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExponentialWeightedMovingAverageOpTest, AlphaGreaterThanOne_Adjust) {
  // ewma([None, 2, 3], 1.2) -> invalid argument status
  const auto series = CreateDenseArray<float>({NA, 2, 3});
  ASSERT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 1.2,
                                        /*adjust=*/true,
                                        /*ignore_missing=*/false),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExponentialWeightedMovingAverageOpTest, AlphaEqualsOne_Adjust) {
  // ewma([1, 2, 3, None, 5], 1) -> [1, 2, 3, 3, 5]
  const auto series = CreateDenseArray<float>({1, 2, 3, NA, 5});
  EXPECT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma", series, 1.,
                                                /*adjust=*/true,
                                                /*ignore_missing=*/false),
              IsOkAndHolds(ElementsAre(1, 2, 3, 3, 5)));
}

TEST(ExponentialWeightedMovingAverageOpTest,
     AlphaEqualsOne_Adjust_IgnoreMissing) {
  // ewma([1, 2, 3, None, 5], 1) -> [1, 2, 3, 3, 5]
  const auto series = CreateDenseArray<float>({1, 2, 3, NA, 5});
  EXPECT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma", series, 1.,
                                                /*adjust=*/true,
                                                /*ignore_missing=*/true),
              IsOkAndHolds(ElementsAre(1, 2, 3, 3, 5)));
}

TEST(ExponentialWeightedMovingAverageOpTest,
     AlphaEqualsOne_Adjust_FirstMissing) {
  // ewma([None, 2, 3, None, 5], 1) -> [None, 2.,  3.,  3.,  5.]
  const auto series = CreateDenseArray<float>({NA, 2, 3, NA, 5});
  EXPECT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma", series, 1.,
                                                /*adjust=*/true,
                                                /*ignore_missing=*/false),
              IsOkAndHolds(ElementsAre(NA, 2, 3, 3, 5)));
}

TEST(ExponentialWeightedMovingAverageOpTest, EmptyTimeSeries) {
  // ewma([], 3) -> []
  EXPECT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma",
                                                DenseArray<float>(), 1.,
                                                /*adjust=*/false,
                                                /*ignore_missing=*/false),
              IsOkAndHolds(ElementsAre()));
}

TEST(ExponentialWeightedMovingAverageOpTest, MissingValue) {
  // ewma([1, 2, 3, None, 5, 6, 7, 8], 0.6) ->
  // [1., 1.6, 2.44, 2.44, 4.46105263, 5.38442105, 6.35376842, 7.34150737]
  const auto series = CreateDenseArray<float>({1, 2, 3, NA, 5, 6, 7, 8});
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.6,
                                        /*adjust=*/false,
                                        /*ignore_missing=*/false),
      IsOkAndHolds(ElementsAre(1., 1.6, 2.44, 2.44, 4.46105263, 5.38442105,
                               6.35376842, 7.34150737)));
}

TEST(ExponentialWeightedMovingAverageOpTest, MissingValue_IgnoreMissing) {
  // ewma([1, 2, 3, None, 5, 6, 7, 8], 0.6) ->
  // [1., 1.6, 2.44, 2.44, 3.976, 5.1904, 6.27616, 7.310464]
  const auto series = CreateDenseArray<float>({1, 2, 3, NA, 5, 6, 7, 8});
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.6,
                                        /*adjust=*/false,
                                        /*ignore_missing=*/true),
      IsOkAndHolds(
          ElementsAre(1., 1.6, 2.44, 2.44, 3.976, 5.1904, 6.27616, 7.310464)));
}

TEST(ExponentialWeightedMovingAverageOpTest, FirstMissing) {
  // ewma([None, 2, 3], 0.6) -> [None, 2., 2.6]
  const auto series = CreateDenseArray<float>({NA, 2, 3});
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.6,
                                        /*adjust=*/false,
                                        /*ignore_missing=*/false),
      IsOkAndHolds(ElementsAre(NA, 2., 2.6)));
}

TEST(ExponentialWeightedMovingAverageOpTest, FirstTwoMissing) {
  // ewma([None, None, 3, None, 5], 0.6) -> [None, None, 3., 3., 4.57894737]
  const auto series = CreateDenseArray<float>({NA, NA, 3, NA, 5});
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.6,
                                        /*adjust=*/false,
                                        /*ignore_missing=*/false),
      IsOkAndHolds(ElementsAre(NA, NA, 3., 3., 4.57894737)));
}

TEST(ExponentialWeightedMovingAverageOpTest, FirstTwoMissing_IgnoreMissing) {
  // ewma([None, None, 3, None, 5], 0.6) -> [None, None, 3., 3., 4.2]
  const auto series = CreateDenseArray<float>({NA, NA, 3, NA, 5});
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.6,
                                        /*adjust=*/false,
                                        /*ignore_missing=*/true),
      IsOkAndHolds(ElementsAre(NA, NA, 3, 3, 4.2)));
}

TEST(ExponentialWeightedMovingAverageOpTest, AlphaLessThanZero) {
  // ewma([None, 2, 3], -1.2) -> invalid argument status
  const auto series = CreateDenseArray<float>({NA, 2, 3});
  ASSERT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma", series,
                                                -1.2, /*adjust=*/false,
                                                /*ignore_missing=*/false),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExponentialWeightedMovingAverageOpTest, AlphaEqualsZero) {
  // ewma([None, 2, 3], -1.2) -> invalid argument status
  const auto series = CreateDenseArray<float>({NA, 2, 3});
  ASSERT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma", series, 0.,
                                                /*adjust=*/false,
                                                /*ignore_missing=*/false),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExponentialWeightedMovingAverageOpTest, AlphaGreaterThanOne) {
  // ewma([None, 2, 3], 1.2) -> invalid argument status
  const auto series = CreateDenseArray<float>({NA, 2, 3});
  ASSERT_THAT(
      InvokeOperator<DenseArray<float>>("experimental.ewma", series, 1.2,
                                        /*adjust=*/false,
                                        /*ignore_missing=*/false),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExponentialWeightedMovingAverageOpTest, AlphaEqualsOne) {
  // ewma([1, 2, 3, None, 5], 1) -> [1, 2, 3, 3, 5]
  const auto series = CreateDenseArray<float>({1, 2, 3, NA, 5});
  EXPECT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma", series, 1.,
                                                /*adjust=*/false,
                                                /*ignore_missing=*/false),
              IsOkAndHolds(ElementsAre(1, 2, 3, 3, 5)));
}

TEST(ExponentialWeightedMovingAverageOpTest, AlphaEqualsOne_IgnoreMissing) {
  // ewma([1, 2, 3, None, 5], 1) -> [1, 2, 3, 3, 5]
  const auto series = CreateDenseArray<float>({1, 2, 3, NA, 5});
  EXPECT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma", series, 1.,
                                                /*adjust=*/false,
                                                /*ignore_missing=*/true),
              IsOkAndHolds(ElementsAre(1, 2, 3, 3, 5)));
}

TEST(ExponentialWeightedMovingAverageOpTest, AlphaEqualsOne_FirstMissing) {
  // ewma([None, 2, 3, None, 5], 1) -> [None, 2.,  3.,  3.,  5.]
  const auto series = CreateDenseArray<float>({NA, 2, 3, NA, 5});
  EXPECT_THAT(InvokeOperator<DenseArray<float>>("experimental.ewma", series, 1.,
                                                /*adjust=*/false,
                                                /*ignore_missing=*/false),
              IsOkAndHolds(ElementsAre(NA, 2, 3, 3, 5)));
}

}  // namespace
}  // namespace arolla
