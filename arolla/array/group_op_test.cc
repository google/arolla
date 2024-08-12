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
#include "arolla/array/group_op.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/id_filter.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/operators/testing/accumulators.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::Test;

template <class Accumulator>
using ArrayGroupOpNoDense = array_ops_internal::ArrayGroupOpImpl<
    Accumulator, typename Accumulator::parent_types,
    typename Accumulator::child_types, /*ForwardId=*/false,
    /*UseDenseGroupOps=*/false>;

template <class Accumulator>
using ArrayGroupOpWithIdNoDense = array_ops_internal::ArrayGroupOpImpl<
    Accumulator, typename Accumulator::parent_types,
    meta::tail_t<typename Accumulator::child_types>, /*ForwardId=*/true,
    /*UseDenseGroupOps=*/false>;

TEST(ArrayGroupOp, FullArrayGroupSum) {
  auto values = CreateArray<float>({5.0f, 8.0f, 3.0f, 6.0f});
  auto detail_to_group = CreateArray<int64_t>({1, 1, 2, 3});
  auto splits = CreateArray<int64_t>({0, 0, 2, 3, 4});

  ArrayGroupOpNoDense<testing::AggSumAccumulator<float>> agg(
      GetHeapBufferFactory());

  // Test aggregation using detail to group mapping.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge1, ArrayEdge::FromMapping(
                                            detail_to_group, /*group_size=*/4));
  EXPECT_THAT(*agg.Apply(edge1, values),
              ElementsAre(std::nullopt, 13.0f, 3.0f, 6.0f));

  // Test aggregation using splits.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge2, ArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(*agg.Apply(edge2, values),
              ElementsAre(std::nullopt, 13.0f, 3.0f, 6.0f));
}

TEST(ArrayGroupOp, DenseGroupOpShortcut) {
  auto values = CreateArray<float>({5.0f, 8.0f, 3.0f, 6.0f});
  auto detail_to_group = CreateArray<int64_t>({1, 1, 2, 3});
  auto splits = CreateArray<int64_t>({0, 0, 2, 3, 4});

  ArrayGroupOp<testing::AggSumAccumulator<float>> agg(GetHeapBufferFactory());

  // Test aggregation using detail to group mapping.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge1, ArrayEdge::FromMapping(
                                            detail_to_group, /*group_size=*/4));
  EXPECT_THAT(*agg.Apply(edge1, values),
              ElementsAre(std::nullopt, 13.0f, 3.0f, 6.0f));

  // Test aggregation using splits.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge2, ArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(*agg.Apply(edge2, values),
              ElementsAre(std::nullopt, 13.0f, 3.0f, 6.0f));
}

TEST(ArrayGroupOp, ForwardId) {
  auto splits = CreateArray<int64_t>({0, 0, 2, 3, 4});
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge, ArrayEdge::FromSplitPoints(splits));

  std::vector<int64_t> ids;
  ArrayGroupOpWithIdNoDense<testing::CollectIdsAccumulator> op(
      GetHeapBufferFactory(), testing::CollectIdsAccumulator(&ids));
  EXPECT_OK(op.Apply(edge).status());
  EXPECT_THAT(ids, ElementsAre(0, 1, 2, 3));
}

TEST(ArrayGroupOp, FullArrayAverageWithErrorStatus) {
  auto values = CreateArray<float>({5.0f, 8.0f, 3.0f, 6.0f});
  auto detail_to_group = CreateArray<int64_t>({1, 1, 2, 3});
  auto splits = CreateArray<int64_t>({0, 0, 2, 3, 4});

  ArrayGroupOpNoDense<testing::AverageAccumulator> agg(GetHeapBufferFactory());

  // Test error status using detail to group mapping.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge1, ArrayEdge::FromMapping(
                                            detail_to_group, /*group_size=*/4));
  EXPECT_THAT(
      agg.Apply(edge1, values),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("empty group")));

  // Test error status using splits.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge2, ArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(
      agg.Apply(edge2, values),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("empty group")));
}

TEST(ArrayGroupOp, AggregationOnVerySparseArray) {
  int parent_size = 100;
  int group_size = 5;
  int child_size = parent_size * group_size;
  Buffer<int64_t>::Builder splits_bldr(parent_size + 1);
  for (int i = 0; i < parent_size + 1; ++i) {
    splits_bldr.Set(i, i * group_size);
  }
  auto splits = Array<int64_t>(std::move(splits_bldr).Build());
  Buffer<int64_t>::Builder mapping_bldr(child_size);
  for (int i = 0; i < child_size; ++i) {
    mapping_bldr.Set((i + child_size / 2) % child_size, i / group_size);
  }
  auto mapping = Array<int64_t>(std::move(mapping_bldr).Build());

  ASSERT_OK_AND_ASSIGN(ArrayEdge splits_edge,
                       ArrayEdge::FromSplitPoints(splits));
  ASSERT_OK_AND_ASSIGN(ArrayEdge mapping_edge,
                       ArrayEdge::FromMapping(mapping, parent_size));

  {
    // testing::AggSumAccumulator, first group not empty
    ArrayGroupOpNoDense<testing::AggSumAccumulator<float>> agg(
        GetHeapBufferFactory());

    auto ids = CreateBuffer<int64_t>({3, 25, 438, 439});
    auto values = CreateBuffer<float>({1.0f, 2.0f, 3.0f, 4.0f});
    Array<float> array(child_size, IdFilter(child_size, ids), {values});

    ASSERT_OK_AND_ASSIGN(Array<float> res_splits,
                         agg.Apply(splits_edge, array));
    ASSERT_TRUE(res_splits.IsSparseForm());
    EXPECT_FALSE(res_splits.HasMissingIdValue());
    EXPECT_THAT(res_splits.id_filter().ids(), ElementsAre(0, 5, 87));
    EXPECT_THAT(res_splits.dense_data(), ElementsAre(1, 2, 7));

    ASSERT_OK_AND_ASSIGN(Array<float> res_mapping,
                         agg.Apply(mapping_edge, array));
    ASSERT_TRUE(res_mapping.IsSparseForm());
    EXPECT_FALSE(res_mapping.HasMissingIdValue());
    EXPECT_THAT(res_mapping.id_filter().ids(), ElementsAre(37, 50, 55));
    EXPECT_THAT(res_mapping.dense_data(), ElementsAre(7, 1, 2));
  }

  {
    // testing::AggSumAccumulator, first group empty
    ArrayGroupOpNoDense<testing::AggSumAccumulator<float>> agg(
        GetHeapBufferFactory());

    auto ids = CreateBuffer<int64_t>({23, 25, 438, 439});
    auto values = CreateBuffer<float>({1.0f, 2.0f, 3.0f, 4.0f});
    Array<float> array(child_size, IdFilter(child_size, ids), {values});

    ASSERT_OK_AND_ASSIGN(Array<float> res_splits,
                         agg.Apply(splits_edge, array));
    ASSERT_TRUE(res_splits.IsSparseForm());
    EXPECT_FALSE(res_splits.HasMissingIdValue());
    EXPECT_THAT(res_splits.id_filter().ids(), ElementsAre(4, 5, 87));
    EXPECT_THAT(res_splits.dense_data(), ElementsAre(1, 2, 7));

    ASSERT_OK_AND_ASSIGN(Array<float> res_mapping,
                         agg.Apply(mapping_edge, array));
    ASSERT_TRUE(res_mapping.IsSparseForm());
    EXPECT_FALSE(res_mapping.HasMissingIdValue());
    EXPECT_THAT(res_mapping.id_filter().ids(), ElementsAre(37, 54, 55));
    EXPECT_THAT(res_mapping.dense_data(), ElementsAre(7, 1, 2));
  }

  {
    // testing::AggCountAccumulator
    ArrayGroupOpNoDense<testing::AggCountAccumulator<float>> agg(
        GetHeapBufferFactory());

    auto ids = CreateBuffer<int64_t>({3, 25, 438, 439});
    auto values = CreateBuffer<float>({1.0f, 2.0f, 3.0f, 4.0f});
    Array<float> array(child_size, IdFilter(child_size, ids), {values});

    ASSERT_OK_AND_ASSIGN(Array<int64_t> res_splits,
                         agg.Apply(splits_edge, array));
    ASSERT_TRUE(res_splits.IsSparseForm());
    EXPECT_TRUE(res_splits.HasMissingIdValue());
    EXPECT_EQ(res_splits.missing_id_value().value, 0);
    EXPECT_THAT(res_splits.id_filter().ids(), ElementsAre(0, 5, 87));
    EXPECT_THAT(res_splits.dense_data(), ElementsAre(1, 1, 2));

    ASSERT_OK_AND_ASSIGN(Array<int64_t> res_mapping,
                         agg.Apply(mapping_edge, array));
    ASSERT_TRUE(res_mapping.IsSparseForm());
    EXPECT_TRUE(res_mapping.HasMissingIdValue());
    EXPECT_EQ(res_mapping.missing_id_value().value, 0);
    EXPECT_THAT(res_mapping.id_filter().ids(), ElementsAre(37, 50, 55));
    EXPECT_THAT(res_mapping.dense_data(), ElementsAre(2, 1, 1));
  }

  {  // testing::AverageAccumulator
    ArrayGroupOpNoDense<testing::AverageAccumulator> agg(
        GetHeapBufferFactory());

    auto ids = CreateBuffer<int64_t>({3, 25, 38, 39});
    auto values = CreateBuffer<float>({1.0f, 2.0f, 3.0f, 4.0f});
    Array<float> array(child_size, IdFilter(child_size, ids), {values});

    EXPECT_THAT(
        agg.Apply(splits_edge, array),
        StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("empty group")));
    EXPECT_THAT(
        agg.Apply(mapping_edge, array),
        StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("empty group")));
  }
}

TEST(ArrayGroupOp, AverageToScalar) {
  ArrayGroupOpNoDense<testing::AverageAccumulator> agg(GetHeapBufferFactory());

  EXPECT_THAT(agg.Apply(ArrayGroupScalarEdge(3),
                        CreateArray<float>({1.0f, 3.0f, 8.0f})),
              IsOkAndHolds(4.0f));
  EXPECT_THAT(
      agg.Apply(ArrayGroupScalarEdge(0), Array<float>()),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("empty group")));
}

TEST(ArrayGroupOp, AggregationToScalar) {
  ArrayGroupOpNoDense<testing::AggSumAccumulator<float>> agg(
      GetHeapBufferFactory());
  {  // dense
    auto values = CreateArray<float>({5.0f, 8.0f, 3.0f, 6.0f});
    ArrayGroupScalarEdge edge(values.size());
    EXPECT_EQ(*agg.Apply(edge, values), 22.0f);
  }
  {  // const
    auto values = Array<float>(10, 5.0f);
    ArrayGroupScalarEdge edge(values.size());
    EXPECT_EQ(*agg.Apply(edge, values), 50.0f);
  }
  {  // sparse
    auto values = CreateArray<float>({5.0f, std::nullopt, std::nullopt, 6.0f})
                      .ToSparseForm();
    ArrayGroupScalarEdge edge(values.size());
    EXPECT_EQ(*agg.Apply(edge, values), 11.0f);
  }
  {  // sparse with missing_id_value
    auto values =
        CreateArray<float>({5.0f, 3.0f, 3.0f, 6.0f}).ToSparseForm(3.0f);
    ArrayGroupScalarEdge edge(values.size());
    EXPECT_EQ(*agg.Apply(edge, values), 17.0f);
  }
}

TEST(ArrayGroupOp, RankValues) {
  auto values = CreateArray<float>({3.0f, 5.0f, 2.0f, 1.0f, 3.1f, 7.0f});
  auto detail_to_group = CreateArray<int64_t>({0, 0, 0, 0, 1, 1});
  auto splits = CreateArray<int64_t>({0, 4, 6});

  ArrayGroupOpNoDense<testing::RankValuesAccumulator<float>> agg(
      GetHeapBufferFactory());

  // Test using mapping.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge1, ArrayEdge::FromMapping(
                                            detail_to_group, /*group_size=*/2));
  EXPECT_THAT(*agg.Apply(edge1, values), ElementsAre(1, 0, 2, 3, 1, 0));

  // Test using splits.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge2, ArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(*agg.Apply(edge2, values), ElementsAre(1, 0, 2, 3, 1, 0));

  // Group to scalar edge.
  EXPECT_THAT(*agg.Apply(ArrayGroupScalarEdge(values.size()), values),
              ElementsAre(5, 1, 4, 0, 2, 3));
}

TEST(ArrayGroupOp, RankValuesSparse) {
  auto values = CreateArray<float>(100, {0, 10, 20, 30, 40, 50},
                                   {3.0f, 5.0f, 2.0f, 1.0f, 3.1f, 7.0f});
  auto detail_to_group =
      CreateArray<int64_t>(100, {0, 10, 20, 30, 40, 50}, {0, 0, 0, 0, 1, 1});
  auto splits = CreateArray<int64_t>({0, 40, 100});

  ArrayGroupOpNoDense<testing::RankValuesAccumulator<float>> agg(
      GetHeapBufferFactory());

  // Test using mapping.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge1, ArrayEdge::FromMapping(
                                            detail_to_group, /*group_size=*/2));
  {
    ASSERT_OK_AND_ASSIGN(auto res, agg.Apply(edge1, values));
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 10, 20, 30, 40, 50));
    EXPECT_THAT(res.dense_data(), ElementsAre(1, 0, 2, 3, 1, 0));
  }

  // Test using splits.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge2, ArrayEdge::FromSplitPoints(splits));
  {
    ASSERT_OK_AND_ASSIGN(auto res, agg.Apply(edge2, values));
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 10, 20, 30, 40, 50));
    EXPECT_THAT(res.dense_data(), ElementsAre(1, 0, 2, 3, 1, 0));
  }

  // Group to scalar edge.
  {
    ASSERT_OK_AND_ASSIGN(
        auto res, agg.Apply(ArrayGroupScalarEdge(values.size()), values));
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(0, 10, 20, 30, 40, 50));
    EXPECT_THAT(res.dense_data(), ElementsAre(5, 1, 4, 0, 2, 3));
  }
}

TEST(ArrayGroupOp, PartialSparseMapping) {
  // Full group columns
  auto a = CreateArray<float>({2.0f, 1.0f, 1.0f});
  auto b = CreateArray<float>({2.0f, 2.0f, 1.0f});
  auto c = CreateArray<float>({0.0f, -1.0f, -1.0f});

  // Sparse detail columns
  IdFilter ids(100, CreateBuffer<int64_t>({5, 10, 15, 20, 25, 30}));
  auto x = Array<float>(
      100, ids, CreateDenseArray<float>({1.0, 1.0, 1.0, 1.0, 1.0, 1.0}));
  auto y = Array<float>(
      100, ids, CreateDenseArray<float>({1.0, 2.0, 3.0, 1.0, 3.0, 2.0}));
  auto z = Array<float>(
      100, ids, CreateDenseArray<float>({1.0, 2.0, 1.0, 2.0, 1.0, 2.0}));

  // Sparse detail to group mapping.
  IdFilter edge_ids(100, CreateBuffer<int64_t>({0, 5, 10, 15, 20, 25}));
  auto detail_to_group = Array<int64_t>(
      100, edge_ids, CreateDenseArray<int64_t>({0, 0, 1, 1, 2, 2}));

  ArrayGroupOpNoDense<testing::WeightedSumAccumulator> agg(
      GetHeapBufferFactory());

  {  // Test using mapping.
    ASSERT_OK_AND_ASSIGN(ArrayEdge edge,
                         ArrayEdge::FromMapping(detail_to_group,
                                                /*group_size=*/3));
    ASSERT_OK_AND_ASSIGN(auto res, agg.Apply(edge, a, b, c, x, y, z));

    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_FALSE(res.HasMissingIdValue());
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(5, 10, 15, 20, 25));
    EXPECT_THAT(res.dense_data(), ElementsAre(4.f, 3.f, 6.f, 0.f, 3.f));
  }
  {  // Test using splits.
    ASSERT_OK_AND_ASSIGN(
        ArrayEdge edge,
        ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, 10, 20, 100})));
    ASSERT_OK_AND_ASSIGN(auto res, agg.Apply(edge, a, b, c, x, y, z));
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_FALSE(res.HasMissingIdValue());
    EXPECT_THAT(res.id_filter().ids(), ElementsAre(5, 10, 15, 20, 25, 30));
    EXPECT_THAT(res.dense_data(), ElementsAre(4.f, 3.f, 6.f, 0.f, 3.f, 1.0f));
  }
}

TEST(ArrayGroupOp, PartialDenseMapping) {
  // Group columns
  auto a = CreateArray<float>({2.0f, 1.0f, 1.0f});
  auto b = CreateArray<float>({2.0f, 2.0f, std::nullopt});
  auto c = CreateArray<float>({0.0f, -1.0f, -1.0f});

  // Detail columns
  auto x = CreateArray<float>({1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
  auto y = CreateArray<float>({1.0f, 2.0f, 3.0f, 1.0f, 3.0f, 2.0f});
  auto z = CreateArray<float>({1.f, 2.f, 1.f, std::nullopt, 1.f, 2.f});

  auto splits = CreateArray<int64_t>({0, 2, 5, 6});

  // Equivalent mapping from detail to group.
  auto detail_to_group = CreateArray<int64_t>({0, 0, 1, 1, 1, 2});

  ArrayGroupOpNoDense<testing::WeightedSumAccumulator> agg(
      GetHeapBufferFactory());

  // Test aggregation using splits.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge1, ArrayEdge::FromSplitPoints(splits));
  EXPECT_THAT(*agg.Apply(edge1, a, b, c, x, y, z),
              ElementsAre(4.f, 6.f, 6.f, std::nullopt, 6.f, std::nullopt));

  // Test aggregation using equivalent mapping.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge2, ArrayEdge::FromMapping(
                                            detail_to_group, /*group_size=*/3));
  EXPECT_THAT(*agg.Apply(edge2, a, b, c, x, y, z),
              ElementsAre(4.f, 6.f, 6.f, std::nullopt, 6.f, std::nullopt));
}

TEST(ArrayGroupOp, PartialGroupScalarEdge) {
  // Detail columns
  auto x = CreateArray<float>({1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
  auto y = CreateArray<float>({1.0f, 2.0f, 3.0f, 1.0f, 3.0f, 2.0f});
  auto z = CreateArray<float>({1.f, 2.f, 1.f, std::nullopt, 1.f, 2.f});

  ArrayGroupOpNoDense<testing::WeightedSumAccumulator> agg(
      GetHeapBufferFactory());

  EXPECT_THAT(*agg.Apply(ArrayGroupScalarEdge(6), 2.0f, 2.0f, 0.0f, x, y, z),
              ElementsAre(4.f, 6.f, 8.f, std::nullopt, 8.f, 6.f));
  EXPECT_THAT(
      *agg.Apply(ArrayGroupScalarEdge(6), 2.0f, 2.0f, 0.0f,
                 x.ToSparseForm(1.0f), y.ToSparseForm(1.0f), z.ToSparseForm()),
      ElementsAre(4.f, 6.f, 8.f, std::nullopt, 8.f, 6.f));
}

TEST(ArrayGroupOp, OptionalText) {
  auto detail_to_group = CreateArray<int64_t>({1, 1, 2, 3, 3});
  auto splits = CreateArray<int64_t>({0, 0, 2, 3, 5});

  auto prefixes = CreateArray<Text>(
      {Text("empty"), Text("some:\n"), Text("prefix:\n"), std::nullopt});
  auto values = CreateArray<Text>(
      {Text("w1"), std::nullopt, Text("w3"), Text("w4"), Text("w5")});
  auto comments =
      CreateArray<Text>({std::nullopt, Text("it is word #2"), std::nullopt,
                         Text("it is word #4"), std::nullopt});

  ArrayGroupOpNoDense<testing::AggTextAccumulator> agg(GetHeapBufferFactory());

  // Aggregation using detail to group mapping.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge1, ArrayEdge::FromMapping(
                                            detail_to_group, /*group_size=*/4));
  ASSERT_OK_AND_ASSIGN(Array<Text> res1,
                       agg.Apply(edge1, prefixes, values, comments));

  // Aggregation using split points.
  ASSERT_OK_AND_ASSIGN(ArrayEdge edge2, ArrayEdge::FromSplitPoints(splits));
  ASSERT_OK_AND_ASSIGN(Array<Text> res2,
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
