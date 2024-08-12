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
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/init_arolla.h"

namespace arolla::testing {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;

class ArrayOpsTest : public ::testing::Test {
  void SetUp() final { InitArolla(); }
};

TEST_F(ArrayOpsTest, DenseArrayAtOp) {
  using OF = OptionalValue<float>;
  using OI = OptionalValue<int64_t>;
  auto arr = CreateDenseArray<float>({1, 2, 3, std::nullopt});
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, int64_t{1}),
              IsOkAndHolds(OF(2)));
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, OI(2)), IsOkAndHolds(OF(3)));
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, OI(3)), IsOkAndHolds(OF()));
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, OI(4)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "array index 4 out of range [0, 4)"));
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, OI(-1)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "array index -1 out of range [0, 4)"));
  EXPECT_THAT(InvokeOperator<OF>("array.at", arr, OI()), IsOkAndHolds(OF()));
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>(
          "array.at", arr, CreateDenseArray<int64_t>({2, 3, std::nullopt, 0})),
      IsOkAndHolds(ElementsAre(3.f, std::nullopt, std::nullopt, 1.f)));
  EXPECT_THAT(
      InvokeOperator<DenseArray<float>>(
          "array.at", arr, CreateDenseArray<int64_t>({2, 3, std::nullopt, 4})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "array index 4 out of range [0, 4)"));
}

TEST_F(ArrayOpsTest, TestArrayTakeOver) {
  auto x = CreateDenseArray<int>({1, 2, 3, std::nullopt, 5, 6, 7, 8});
  auto offsets = CreateDenseArray<int64_t>({0, 3, 2, 1, 4, 5, 6, std::nullopt});
  auto splits = CreateDenseArray<int64_t>({0, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>("array._take_over", x, offsets, edge),
      IsOkAndHolds(ElementsAre(1, std::nullopt, 3, 2, 5, 6, 7, std::nullopt)));
}

TEST_F(ArrayOpsTest, TestArrayTakeOverSplitPoints) {
  // Array:   [(1, 2, 3), (), (NA, 5), (6, 7), ( 8)]
  // Offsets: [(0, 2, 1), (), ( 0, 1), (1, 0), (NA)]
  auto x = CreateDenseArray<int>({1, 2, 3, std::nullopt, 5, 6, 7, 8});
  auto offsets = CreateDenseArray<int64_t>({0, 2, 1, 0, 1, 1, 0, std::nullopt});
  auto splits = CreateDenseArray<int64_t>({0, 3, 3, 5, 7, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>("array._take_over", x, offsets, edge),
      IsOkAndHolds(ElementsAre(1, 3, 2, std::nullopt, 5, 7, 6, std::nullopt)));
}

TEST_F(ArrayOpsTest, TestArrayTakeMapping) {
  auto x = CreateDenseArray<int>({1, 2, 3, std::nullopt, 5, 6, 7, 8});
  auto offsets = CreateDenseArray<int64_t>({0, 2, 1, 0, 1, 1, 0, std::nullopt});
  auto mapping = CreateDenseArray<int64_t>({0, 1, 1, 1, 1, 0, std::nullopt, 0});
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromMapping(mapping, 3));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>("array._take_over", x, offsets, edge),
      IsOkAndHolds(ElementsAre(1, std::nullopt, 3, 2, 3, 6, std::nullopt,
                               std::nullopt)));
}

TEST_F(ArrayOpsTest, TestArrayTakeOverErrors) {
  auto x = CreateDenseArray<int>({1, 2, 3, std::nullopt, 5, 6, 7, 8});
  auto offsets = CreateDenseArray<int64_t>({0, 2, 1, 0, 1, 1, 0, std::nullopt});
  auto splits = CreateDenseArray<int64_t>({0, 1});
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>("array._take_over", x, offsets, edge),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("argument sizes mismatch")));
  x = CreateDenseArray<int>({1});
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>("array._take_over", x, offsets, edge),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("argument sizes mismatch")));
}

TEST_F(ArrayOpsTest, TestArrayTakeOverOver) {
  auto x = CreateDenseArray<int>({1, 2, 3, std::nullopt, 5, 6, 7, 8});
  auto offsets = CreateDenseArray<int64_t>({0, 3, 1, 3, 1, std::nullopt});
  auto x_splits = CreateDenseArray<int64_t>({0, 4, 8});
  ASSERT_OK_AND_ASSIGN(auto x_edge, DenseArrayEdge::FromSplitPoints(x_splits));
  auto offsets_splits = CreateDenseArray<int64_t>({0, 3, 6});
  ASSERT_OK_AND_ASSIGN(auto offsets_edge,
                       DenseArrayEdge::FromSplitPoints(offsets_splits));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>("array._take_over_over", x, offsets,
                                      x_edge, offsets_edge),
      IsOkAndHolds(ElementsAre(1, std::nullopt, 2, 8, 6, std::nullopt)));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>("array._take_over_over", x, offsets,
                                      x_edge, x_edge),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("argument sizes mismatch: (8, 6)")));
}

TEST_F(ArrayOpsTest, Slice) {
  auto x = CreateDenseArray<int>({1, 2, 3, std::nullopt, 5, 6, 7, 8});
  ASSERT_OK_AND_ASSIGN(DenseArray<int> sliced,
                       InvokeOperator<DenseArray<int>>("array.slice", x,
                                                       int64_t{3}, int64_t{4}));
  EXPECT_THAT(sliced, ElementsAre(std::nullopt, 5, 6, 7));
  EXPECT_EQ(sliced.bitmap_bit_offset, 0);
  EXPECT_THAT(InvokeOperator<DenseArray<int>>("array.slice", x, int64_t{5},
                                              int64_t{-1}),
              IsOkAndHolds(ElementsAre(6, 7, 8)));
  EXPECT_THAT(InvokeOperator<DenseArray<int>>("array.slice", x, int64_t{-3},
                                              int64_t{4}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr(
                           "expected `offset` in [0, 8], but got -3")));
  EXPECT_THAT(
      InvokeOperator<DenseArray<int>>("array.slice", x, int64_t{3}, int64_t{8}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("expected `size` in [0, 5], but got 8")));
}

TEST_F(ArrayOpsTest, Concat) {
  auto x = CreateDenseArray<int>({1, 2, 3});
  auto y = CreateDenseArray<int>({std::nullopt, 4});
  auto z = CreateDenseArray<int>({});
  EXPECT_THAT(InvokeOperator<DenseArray<int>>("array.concat", x, x),
              IsOkAndHolds(ElementsAre(1, 2, 3, 1, 2, 3)));
  EXPECT_THAT(InvokeOperator<DenseArray<int>>("array.concat", x, y),
              IsOkAndHolds(ElementsAre(1, 2, 3, std::nullopt, 4)));
  EXPECT_THAT(InvokeOperator<DenseArray<int>>("array.concat", y, y),
              IsOkAndHolds(ElementsAre(std::nullopt, 4, std::nullopt, 4)));
  EXPECT_THAT(InvokeOperator<DenseArray<int>>("array.concat", x, z),
              IsOkAndHolds(ElementsAre(1, 2, 3)));
  EXPECT_THAT(InvokeOperator<DenseArray<int>>("array.concat", z, y),
              IsOkAndHolds(ElementsAre(std::nullopt, 4)));
}

}  // namespace
}  // namespace arolla::testing
