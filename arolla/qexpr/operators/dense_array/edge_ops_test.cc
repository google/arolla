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
#include "arolla/qtype/shape_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

class EdgeOpsTest : public ::testing::Test {
  void SetUp() final { InitArolla(); }
};

TEST_F(EdgeOpsTest, EdgeFromSplitPointsOp) {
  auto sizes = CreateDenseArray<int64_t>({0, 2, 5, 6, 6, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge.from_split_points", sizes));
  EXPECT_THAT(edge.edge_values().values, ElementsAre(0, 2, 5, 6, 6, 8));
}

TEST_F(EdgeOpsTest, EdgeFromMappingOp) {
  auto mapping = CreateDenseArray<int64_t>({0, 2, 5, 6, 6, 8});

  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<DenseArrayEdge>("edge.from_mapping", mapping,
                                                int64_t{10}));
  EXPECT_THAT(edge.edge_values().values, ElementsAre(0, 2, 5, 6, 6, 8));

  EXPECT_THAT(
      InvokeOperator<DenseArrayEdge>("edge.from_mapping", mapping, int64_t{5}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("parent_size=5, but parent id 8 is used")));
}

TEST_F(EdgeOpsTest, EdgeFromSizesOp) {
  auto sizes = CreateDenseArray<int64_t>({2, 3, 1, 0, 2});
  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<DenseArrayEdge>("edge.from_sizes", sizes));
  EXPECT_THAT(edge.edge_values().values, ElementsAre(0, 2, 5, 6, 6, 8));
}

TEST_F(EdgeOpsTest, EdgeFromShapeOp) {
  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayGroupScalarEdge>(
                                      "edge.from_shape", DenseArrayShape{5}));
  EXPECT_THAT(edge.child_size(), 5);
}

TEST_F(EdgeOpsTest, MappingOp) {
  {
    const auto mapping = CreateDenseArray<int64_t>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromMapping(mapping, 4));
    EXPECT_THAT(InvokeOperator<DenseArray<int64_t>>("edge.mapping", edge),
                IsOkAndHolds(ElementsAre(1, 2, 3)));
  }
  {
    const auto splits = CreateDenseArray<int64_t>({0, 2, 5});
    ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromSplitPoints(splits));
    EXPECT_THAT(InvokeOperator<DenseArray<int64_t>>("edge.mapping", edge),
                IsOkAndHolds(ElementsAre(0, 0, 1, 1, 1)));
  }
}

TEST_F(EdgeOpsTest, FromKindAndShapeOp) {
  auto split_points = CreateDenseArray<int64_t>({0, 2, 5, 6, 6, 8});
  ASSERT_OK_AND_ASSIGN(auto edge,
                       DenseArrayEdge::FromSplitPoints(split_points));
  EXPECT_THAT(InvokeOperator<DenseArrayShape>("edge.child_shape", edge),
              IsOkAndHolds(DenseArrayShape{8}));

  // scalar group
  EXPECT_THAT(InvokeOperator<DenseArrayShape>("edge.child_shape",
                                              DenseArrayGroupScalarEdge{5}),
              IsOkAndHolds(DenseArrayShape{5}));
}

TEST_F(EdgeOpsTest, IntoKindAndShapeOp) {
  auto split_points = CreateDenseArray<int64_t>({0, 2, 5, 6, 6, 8});
  ASSERT_OK_AND_ASSIGN(auto edge,
                       DenseArrayEdge::FromSplitPoints(split_points));
  EXPECT_THAT(InvokeOperator<DenseArrayShape>("edge.parent_shape", edge),
              IsOkAndHolds(DenseArrayShape{5}));

  // scalar group
  EXPECT_THAT(InvokeOperator<OptionalScalarShape>("edge.parent_shape",
                                                  DenseArrayGroupScalarEdge{5}),
              IsOkAndHolds(OptionalScalarShape{}));
}

TEST_F(EdgeOpsTest, ExpandOverMapping) {
  auto mapping =
      CreateDenseArray<int64_t>({0, 1, std::nullopt, 0, 1, 2, 2, 1, 0});
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromMapping(mapping, 3));
  ASSERT_OK_AND_ASSIGN(auto bad_edge, DenseArrayEdge::FromMapping(mapping, 4));

  {  // float
    auto values = CreateDenseArray<float>({0, std::nullopt, 1});
    ASSERT_OK_AND_ASSIGN(
        DenseArray<float> res,
        InvokeOperator<DenseArray<float>>("array._expand", values, edge));
    EXPECT_THAT(res, ElementsAre(0, std::nullopt, std::nullopt, 0, std::nullopt,
                                 1, 1, std::nullopt, 0));
    EXPECT_THAT(
        InvokeOperator<DenseArray<float>>("array._expand", values, bad_edge),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("argument sizes mismatch")));
  }
  {  // Text
    auto values = CreateDenseArray<Text>({Text("0"), std::nullopt, Text("1")});
    ASSERT_OK_AND_ASSIGN(
        DenseArray<Text> res,
        InvokeOperator<DenseArray<Text>>("array._expand", values, edge));
    EXPECT_THAT(res, ElementsAre(Text("0"), std::nullopt, std::nullopt,
                                 Text("0"), std::nullopt, Text("1"), Text("1"),
                                 std::nullopt, Text("0")));
    // check that string data is reused
    EXPECT_EQ(values[0].value.begin(), res[0].value.begin());
    EXPECT_EQ(values[0].value.begin(), res[3].value.begin());
    EXPECT_EQ(values[0].value.begin(), res[8].value.begin());
    EXPECT_EQ(values[2].value.begin(), res[5].value.begin());
    EXPECT_EQ(values[2].value.begin(), res[6].value.begin());
  }
}

TEST_F(EdgeOpsTest, ExpandOverSplitPoints) {
  auto values =
      CreateDenseArray<Bytes>({Bytes("first"), std::nullopt, Bytes("second")});
  auto split_points = CreateDenseArray<int64_t>({0, 3, 6, 10});
  ASSERT_OK_AND_ASSIGN(auto edge,
                       DenseArrayEdge::FromSplitPoints(split_points));

  ASSERT_OK_AND_ASSIGN(auto res, InvokeOperator<DenseArray<Bytes>>(
                                     "array._expand", values, edge));
  EXPECT_THAT(
      res, ElementsAre("first", "first", "first", std::nullopt, std::nullopt,
                       std::nullopt, "second", "second", "second", "second"));
  // check that string data is reused
  EXPECT_EQ(values[0].value.begin(), res[0].value.begin());
  EXPECT_EQ(values[0].value.begin(), res[2].value.begin());
  EXPECT_EQ(values[2].value.begin(), res[6].value.begin());
  EXPECT_EQ(values[2].value.begin(), res[9].value.begin());
}

TEST_F(EdgeOpsTest, ExpandOverSplitPointsNoBitmap) {
  auto values = CreateFullDenseArray<Bytes>({Bytes("first"), Bytes("second")});
  auto split_points = CreateDenseArray<int64_t>({0, 3, 7});
  ASSERT_OK_AND_ASSIGN(auto edge,
                       DenseArrayEdge::FromSplitPoints(split_points));

  ASSERT_OK_AND_ASSIGN(auto res, InvokeOperator<DenseArray<Bytes>>(
                                     "array._expand", values, edge));
  EXPECT_THAT(res, ElementsAre("first", "first", "first", "second", "second",
                               "second", "second"));
  // check that string data is reused
  EXPECT_EQ(values[0].value.begin(), res[0].value.begin());
  EXPECT_EQ(values[0].value.begin(), res[2].value.begin());
  EXPECT_EQ(values[1].value.begin(), res[3].value.begin());
  EXPECT_EQ(values[1].value.begin(), res[6].value.begin());
}

TEST_F(EdgeOpsTest, ExpandGroupScalarEdge) {
  auto edge = DenseArrayGroupScalarEdge(3);

  ASSERT_OK_AND_ASSIGN(
      auto res1, InvokeOperator<DenseArray<Bytes>>(
                     "array._expand", MakeOptionalValue(Bytes("first")), edge));
  EXPECT_THAT(res1, ElementsAre("first", "first", "first"));

  ASSERT_OK_AND_ASSIGN(auto res2,
                       InvokeOperator<DenseArray<Bytes>>(
                           "array._expand", OptionalValue<Bytes>(), edge));
  EXPECT_THAT(res2, ElementsAre(std::nullopt, std::nullopt, std::nullopt));
}

TEST_F(EdgeOpsTest, GroupByOp_Integral) {
  const auto series = CreateDenseArray<int64_t>({101, 102, 103, 104});
  ASSERT_OK_AND_ASSIGN(auto over, DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 4})));
  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 4);

  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3));
}

TEST_F(EdgeOpsTest, GroupByOp_Float) {
  const auto series = CreateDenseArray<float>({5., 7., 1., 2., 4.});
  ASSERT_OK_AND_ASSIGN(auto over, DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 5})));

  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 5);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3, 4));
}

TEST_F(EdgeOpsTest, GroupByOp_Bytes) {
  const auto series = CreateDenseArray<Bytes>(
      {Bytes("a"), Bytes("b"), Bytes("c"), Bytes("d"), Bytes("e")});
  ASSERT_OK_AND_ASSIGN(auto over, DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 5})));

  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 5);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3, 4));
}

TEST_F(EdgeOpsTest, GroupByOp_DuplicatesInInputSeries) {
  const auto series = CreateDenseArray<float>({5., 7., 5., 7., 4., 8.});
  ASSERT_OK_AND_ASSIGN(auto over, DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 6})));
  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 4);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 0, 1, 2, 3));
}

TEST_F(EdgeOpsTest, GroupByOp_DuplicatesInInputSeries_WithSplits) {
  // Array with splits: [(5, 7, 5), (7, 4, 8)]
  const auto series = CreateDenseArray<float>({5., 7., 5., 7., 7., 8.});
  ASSERT_OK_AND_ASSIGN(auto over, DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 3, 6})));
  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 4);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 0, 2, 2, 3));
}

TEST_F(EdgeOpsTest, GroupByOp_DuplicatesInInputSeries_WithMapping) {
  const auto series = CreateDenseArray<float>({5., 7., 5., 7., 7., 8.});
  ASSERT_OK_AND_ASSIGN(auto over,
                       DenseArrayEdge::FromMapping(
                           CreateDenseArray<int64_t>({1, 1, 0, 2, 1, 0}), 3));
  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 5);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3, 1, 4));
}

TEST_F(EdgeOpsTest, GroupByOp_MissingValuesAndDuplicates) {
  const auto series = CreateDenseArray<int64_t>({7, 8, std::nullopt, 7, 10, 8});
  ASSERT_OK_AND_ASSIGN(auto over, DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 6})));

  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 3);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, std::nullopt, 0, 2, 1));
}

TEST_F(EdgeOpsTest, GroupByOp_MissingValuesAndDuplicates_WithSplits) {
  // Array with splits: [(7, 6, 7), (5), (5), (NA, NA), (5, 5), (NA, 7 , 10.
  // 7)]
  const auto series =
      CreateDenseArray<int64_t>({7, 6, 7, 5, 5, std::nullopt, std::nullopt, 5,
                                 5, std::nullopt, 7, 10, 7});
  ASSERT_OK_AND_ASSIGN(auto over,
                       DenseArrayEdge::FromSplitPoints(
                           CreateDenseArray<int64_t>({0, 3, 4, 5, 7, 9, 13})));

  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 7);
  EXPECT_THAT(edge.edge_values(),
              ElementsAre(0, 1, 0, 2, 3, std::nullopt, std::nullopt, 4, 4,
                          std::nullopt, 5, 6, 5));
}

TEST_F(EdgeOpsTest, GroupByOp_EmptyDenseArray) {
  const auto series = CreateDenseArray<int64_t>({});
  ASSERT_OK_AND_ASSIGN(auto over, DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0})));

  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 0);
  EXPECT_THAT(edge.edge_values(), ElementsAre());
}

TEST_F(EdgeOpsTest, GroupByOp_MissingValuesAndDuplicates_WithMapping) {
  // DenseArray:     [7,  6, 6, 7, 5, 5, NA, NA, 5, 5, NA, 7, 10, 7,  5]
  // Mapping:        [2, NA, 2, 3, 1, 2,  2, NA, 1, 2,  4, 2,  3, 3, NA]
  // Child-to-Group: [0, NA, 1, 2, 3, 4, NA, NA, 3, 4, NA, 0,  5, 2, NA]
  const auto series =
      CreateDenseArray<int64_t>({7, 6, 6, 7, 5, 5, std::nullopt, std::nullopt,
                                 5, 5, std::nullopt, 7, 10, 7, 5});
  ASSERT_OK_AND_ASSIGN(
      auto over, DenseArrayEdge::FromMapping(
                     CreateDenseArray<int64_t>({2, std::nullopt, 2, 3, 1, 2, 2,
                                                std::nullopt, 1, 2, 4, 2, 3, 3,
                                                std::nullopt}),
                     5));
  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<DenseArrayEdge>(
                                      "edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 6);
  EXPECT_THAT(
      edge.edge_values(),
      ElementsAre(0, std::nullopt, 1, 2, 3, 4, std::nullopt, std::nullopt, 3, 4,
                  std::nullopt, 0, 5, 2, std::nullopt));
}

TEST_F(EdgeOpsTest, GroupByOp_IncompatibleOverEdge) {
  const auto series = CreateDenseArray<int64_t>({1, 2});
  ASSERT_OK_AND_ASSIGN(auto over, DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 3})));

  EXPECT_THAT(InvokeOperator<DenseArrayEdge>("edge._group_by", series, over),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("argument sizes mismatch")));
}

TEST_F(EdgeOpsTest, AggSizeEdgeOp_Mapping) {
  // Mapping [1, None, 1, None, 3]
  auto mapping =
      CreateDenseArray<int64_t>({0, std::nullopt, 0, std::nullopt, 2});
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromMapping(mapping, 3));
  ASSERT_OK_AND_ASSIGN(auto dense_array,
                       InvokeOperator<DenseArray<int64_t>>("edge.sizes", edge));
  EXPECT_THAT(dense_array, ElementsAre(2, 0, 1));
}

TEST_F(EdgeOpsTest, AggSizeEdgeOp_SplitPoints) {
  auto split_points = CreateDenseArray<int64_t>({0, 2, 4, 4, 8});
  ASSERT_OK_AND_ASSIGN(auto edge,
                       DenseArrayEdge::FromSplitPoints(split_points));
  ASSERT_OK_AND_ASSIGN(auto dense_array,
                       InvokeOperator<DenseArray<int64_t>>("edge.sizes", edge));
  EXPECT_THAT(dense_array, ElementsAre(2, 2, 0, 4));
}

TEST_F(EdgeOpsTest, TestAggCountScalarEdge) {
  auto mask =
      CreateDenseArray<Unit>({kUnit, std::nullopt, kUnit, std::nullopt});
  auto edge = DenseArrayGroupScalarEdge(4);
  EXPECT_THAT(InvokeOperator<int64_t>("array._count", mask, edge),
              IsOkAndHolds(2));
}

// See the full coverage test in
// py/arolla/operator_tests/edge_compose_test.py
TEST_F(EdgeOpsTest, EdgeComposeOp) {
  {
    // Split point inputs -> split point output.
    ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                         CreateDenseArray<int64_t>({0, 2, 3})));
    ASSERT_OK_AND_ASSIGN(auto edge2,
                         DenseArrayEdge::FromSplitPoints(
                             CreateDenseArray<int64_t>({0, 1, 2, 4})));
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         InvokeOperator<DenseArrayEdge>(
                             "edge.compose._dense_array", edge1, edge2));
    EXPECT_THAT(composed_edge.edge_values(), ElementsAre(0, 2, 4));
    EXPECT_THAT(composed_edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
  }
  {
    // Mapping input -> mapping output.
    ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                         CreateDenseArray<int64_t>({0, 2, 3})));
    ASSERT_OK_AND_ASSIGN(auto edge2,
                         DenseArrayEdge::FromMapping(
                             CreateDenseArray<int64_t>({0, 1, 2, 2}), 3));
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         InvokeOperator<DenseArrayEdge>(
                             "edge.compose._dense_array", edge1, edge2));
    EXPECT_THAT(composed_edge.edge_values(), ElementsAre(0, 0, 1, 1));
    EXPECT_THAT(composed_edge.edge_type(), DenseArrayEdge::MAPPING);
  }
}

}  // namespace
}  // namespace arolla
