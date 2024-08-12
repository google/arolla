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
#include "arolla/dense_array/ops/dense_group_ops.h"

#include <cstdint>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/testing/util.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/operators/testing/accumulators.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::CreateDenseArrayFromIdValues;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::Test;

TEST(DenseGroupOps, FullArrayGroupSum) {
  auto values = CreateDenseArray<float>({5.0f, 8.0f, 3.0f, 6.0f});
  auto detail_to_group = CreateDenseArray<int64_t>({1, 1, 2, 3});
  auto splits = CreateDenseArray<int64_t>({0, 0, 2, 3, 4});

  DenseGroupOps<testing::AggSumAccumulator<float>> agg(GetHeapBufferFactory());

  // Test aggregation using detail to group mapping.
  ASSERT_OK_AND_ASSIGN(
      DenseArrayEdge edge1,
      DenseArrayEdge::FromMapping(detail_to_group, /*group_size=*/4));
  EXPECT_THAT(*agg.Apply(edge1, values),
              ElementsAre(std::nullopt, 13.0f, 3.0f, 6.0f));

  // Test aggregation using splits.
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2,
                       DenseArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(*agg.Apply(edge2, values),
              ElementsAre(std::nullopt, 13.0f, 3.0f, 6.0f));
}

TEST(DenseGroupOps, ForwardId) {
  auto splits = CreateDenseArray<int64_t>({0, 0, 2, 3, 4});
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge,
                       DenseArrayEdge::FromSplitPoints(splits));

  std::vector<int64_t> ids;
  DenseGroupOpsWithId<testing::CollectIdsAccumulator> op(
      GetHeapBufferFactory(), testing::CollectIdsAccumulator(&ids));
  EXPECT_OK(op.Apply(edge).status());
  EXPECT_THAT(ids, ElementsAre(0, 1, 2, 3));
}

TEST(DenseGroupOps, FullArrayAverageWithErrorStatus) {
  auto values = CreateDenseArray<float>({5.0f, 8.0f, 3.0f, 6.0f});
  auto detail_to_group = CreateDenseArray<int64_t>({1, 1, 2, 3});
  auto splits = CreateDenseArray<int64_t>({0, 0, 2, 3, 4});

  DenseGroupOps<testing::AverageAccumulator> agg(GetHeapBufferFactory());

  // Test error status using detail to group mapping.
  ASSERT_OK_AND_ASSIGN(
      DenseArrayEdge edge1,
      DenseArrayEdge::FromMapping(detail_to_group, /*group_size=*/4));
  EXPECT_THAT(
      agg.Apply(edge1, values),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("empty group")));

  // Test error status using splits.
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2,
                       DenseArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(
      agg.Apply(edge2, values),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("empty group")));
}

TEST(DenseGroupOps, AverageToScalar) {
  DenseGroupOps<testing::AverageAccumulator> agg(GetHeapBufferFactory());

  EXPECT_THAT(agg.Apply(DenseArrayGroupScalarEdge(3),
                        CreateDenseArray<float>({1.0f, 3.0f, 8.0f})),
              IsOkAndHolds(4.0f));
  EXPECT_THAT(
      agg.Apply(DenseArrayGroupScalarEdge(0), DenseArray<float>()),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("empty group")));
}

TEST(DenseGroupOps, AggregationToScalar) {
  auto values = CreateDenseArray<float>({5.0f, 8.0f, 3.0f, 6.0f});
  DenseGroupOps<testing::AggSumAccumulator<float>> agg(GetHeapBufferFactory());
  DenseArrayGroupScalarEdge edge(values.size());

  EXPECT_EQ(*agg.Apply(edge, values), 22.0f);
}

TEST(DenseGroupOps, RankValues) {
  auto values = CreateDenseArray<float>({3.0f, 5.0f, 2.0f, 1.0f, 3.1f, 7.0f});
  auto detail_to_group = CreateDenseArray<int64_t>({0, 0, 0, 0, 1, 1});
  auto splits = CreateDenseArray<int64_t>({0, 4, 6});

  DenseGroupOps<testing::RankValuesAccumulator<float>> agg(
      GetHeapBufferFactory());

  // Test using mapping.
  ASSERT_OK_AND_ASSIGN(
      DenseArrayEdge edge1,
      DenseArrayEdge::FromMapping(detail_to_group, /*group_size=*/2));
  EXPECT_THAT(*agg.Apply(edge1, values), ElementsAre(1, 0, 2, 3, 1, 0));

  // Test using splits.
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2,
                       DenseArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(*agg.Apply(edge2, values), ElementsAre(1, 0, 2, 3, 1, 0));

  // Group to scalar edge.
  EXPECT_THAT(*agg.Apply(DenseArrayGroupScalarEdge(values.size()), values),
              ElementsAre(5, 1, 4, 0, 2, 3));
}

TEST(DenseGroupOps, PartialSparseMapping) {
  // Full group columns
  auto a = CreateDenseArray<float>({2.0f, 1.0f, 1.0f});
  auto b = CreateDenseArray<float>({2.0f, 2.0f, 1.0f});
  auto c = CreateDenseArray<float>({0.0f, -1.0f, -1.0f});

  // Sparse detail columns
  auto x = CreateDenseArrayFromIdValues<float>(
      100, {{5, 1.0}, {10, 1.0}, {15, 1.0}, {20, 1.0}, {25, 1.0}, {30, 1.0}});
  auto y = CreateDenseArrayFromIdValues<float>(
      100, {{5, 1.0}, {10, 2.0}, {15, 3.0}, {20, 1.0}, {25, 3.0}, {30, 2.0}});
  auto z = CreateDenseArrayFromIdValues<float>(
      100, {{5, 1.0}, {10, 2.0}, {15, 1.0}, {20, 2.0}, {25, 1.0}, {30, 2.0}});

  // Sparse detail to group mapping.
  auto detail_to_group = CreateDenseArrayFromIdValues<int64_t>(
      100, {{0, 0}, {5, 1}, {10, 0}, {15, 1}, {20, 0}, {25, 1}});

  DenseGroupOps<testing::WeightedSumAccumulator> agg(GetHeapBufferFactory());

  auto expected = CreateDenseArrayFromIdValues<float>(
      100, {{5, 2.f}, {10, 6.f}, {15, 6.f}, {20, 4.f}, {25, 6.f}});

  // Test using mapping.
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge,
                       DenseArrayEdge::FromMapping(detail_to_group,
                                                   /*group_size=*/3));
  ASSERT_OK_AND_ASSIGN(auto res, agg.Apply(edge, a, b, c, x, y, z));
  EXPECT_EQ(res.size(), expected.size());
  for (int i = 0; i < res.size(); ++i) {
    EXPECT_EQ(expected[i], res[i]);
  }

  // No equivalent mapping using splits.
}

TEST(DenseGroupOps, PartialDenseMapping) {
  // Group columns
  auto a = CreateDenseArray<float>({2.0f, 1.0f, 1.0f});
  auto b = CreateDenseArray<float>({2.0f, 2.0f, std::nullopt});
  auto c = CreateDenseArray<float>({0.0f, -1.0f, -1.0f});

  // Detail columns
  auto x = CreateDenseArray<float>({1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
  auto y = CreateDenseArray<float>({1.0f, 2.0f, 3.0f, 1.0f, 3.0f, 2.0f});
  auto z = CreateDenseArray<float>({1.f, 2.f, 1.f, std::nullopt, 1.f, 2.f});

  auto splits = CreateDenseArray<int64_t>({0, 2, 5, 6});

  // Equivalent mapping from detail to group.
  auto detail_to_group = CreateDenseArray<int64_t>({0, 0, 1, 1, 1, 2});

  DenseGroupOps<testing::WeightedSumAccumulator> agg(GetHeapBufferFactory());

  // Test aggregation using splits.
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1,
                       DenseArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(*agg.Apply(edge1, a, b, c, x, y, z),
              ElementsAre(4.f, 6.f, 6.f, std::nullopt, 6.f, std::nullopt));

  // Test aggregation using equivalent mapping.
  ASSERT_OK_AND_ASSIGN(
      DenseArrayEdge edge2,
      DenseArrayEdge::FromMapping(detail_to_group, /*group_size=*/3));
  EXPECT_THAT(*agg.Apply(edge2, a, b, c, x, y, z),
              ElementsAre(4.f, 6.f, 6.f, std::nullopt, 6.f, std::nullopt));
}

TEST(DenseGroupOps, PartialGroupScalarEdge) {
  // Detail columns
  auto x = CreateDenseArray<float>({1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
  auto y = CreateDenseArray<float>({1.0f, 2.0f, 3.0f, 1.0f, 3.0f, 2.0f});
  auto z = CreateDenseArray<float>({1.f, 2.f, 1.f, std::nullopt, 1.f, 2.f});

  DenseGroupOps<testing::WeightedSumAccumulator> agg(GetHeapBufferFactory());

  EXPECT_THAT(
      *agg.Apply(DenseArrayGroupScalarEdge(6), 2.0f, 2.0f, 0.0f, x, y, z),
      ElementsAre(4.f, 6.f, 8.f, std::nullopt, 8.f, 6.f));
}

TEST(DenseGroupOps, OptionalText) {
  auto detail_to_group = CreateDenseArray<int64_t>({1, 1, 2, 3, 3});
  auto splits = CreateDenseArray<int64_t>({0, 0, 2, 3, 5});

  auto prefixes = CreateDenseArray<Text>(
      {Text("empty"), Text("some:\n"), Text("prefix:\n"), std::nullopt});
  auto values = CreateDenseArray<Text>(
      {Text("w1"), std::nullopt, Text("w3"), Text("w4"), Text("w5")});
  auto comments =
      CreateDenseArray<Text>({std::nullopt, Text("it is word #2"), std::nullopt,
                              Text("it is word #4"), std::nullopt});

  DenseGroupOps<testing::AggTextAccumulator> agg(GetHeapBufferFactory());

  // Aggregation using detail to group mapping.
  ASSERT_OK_AND_ASSIGN(
      DenseArrayEdge edge1,
      DenseArrayEdge::FromMapping(detail_to_group, /*group_size=*/4));
  ASSERT_OK_AND_ASSIGN(DenseArray<Text> res1,
                       agg.Apply(edge1, prefixes, values, comments));

  // Aggregation using split points.
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2,
                       DenseArrayEdge::FromSplitPoints(splits));
  ASSERT_OK_AND_ASSIGN(DenseArray<Text> res2,
                       agg.Apply(edge2, prefixes, values, comments));

  using V = absl::string_view;
  EXPECT_THAT(res1,
              ElementsAre(V("empty"), V("some:\nw1\n"), V("prefix:\nw3\n"),
                          V("w4 (it is word #4)\nw5\n")));

  EXPECT_EQ(res1.size(), res2.size());
  for (int64_t i = 0; i < res1.size(); ++i) {
    EXPECT_EQ(res1[i], res2[i]);
  }
}

}  // namespace
}  // namespace arolla
