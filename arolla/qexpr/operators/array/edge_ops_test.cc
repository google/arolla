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
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/status_matchers_backport.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::arolla::testing::IsOkAndHolds;
using ::arolla::testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

class EdgeOpsTest : public ::testing::Test {
  void SetUp() final { ASSERT_OK(InitArolla()); }
};

TEST_F(EdgeOpsTest, EdgeFromSplitPointsOp) {
  auto sizes = CreateArray<int64_t>({0, 2, 5, 6, 6, 8});
  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge.from_split_points", sizes));
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 2, 5, 6, 6, 8));
}

TEST_F(EdgeOpsTest, IndexEdgeOp) {
  auto mapping = CreateArray<int64_t>({0, 2, 5, 6, 6, 8});

  ASSERT_OK_AND_ASSIGN(
      auto edge,
      InvokeOperator<ArrayEdge>("edge.from_mapping", mapping, int64_t{10}));
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 2, 5, 6, 6, 8));

  EXPECT_THAT(
      InvokeOperator<ArrayEdge>("edge.from_mapping", mapping, int64_t{5}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("parent_size=5, but parent id 8 is used")));
}

TEST_F(EdgeOpsTest, EdgeFromSizesOp) {
  auto sizes = CreateArray<int64_t>({2, 3, 1, 0, 2});
  ASSERT_OK_AND_ASSIGN(auto edge,
                       InvokeOperator<ArrayEdge>("edge.from_sizes", sizes));
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 2, 5, 6, 6, 8));
}

TEST_F(EdgeOpsTest, EdgeFromShapeOp) {
  ASSERT_OK_AND_ASSIGN(auto edge, InvokeOperator<ArrayGroupScalarEdge>(
                                      "edge.from_shape", ArrayShape{5}));
  EXPECT_THAT(edge.child_size(), 5);
}

TEST_F(EdgeOpsTest, MappingOp) {
  {
    const auto mapping = CreateArray<int64_t>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromMapping(mapping, 4));
    EXPECT_THAT(InvokeOperator<Array<int64_t>>("edge.mapping", edge),
                IsOkAndHolds(ElementsAre(1, 2, 3)));
  }
  {
    const auto splits = CreateArray<int64_t>({0, 2, 5});
    ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(splits));
    EXPECT_THAT(InvokeOperator<Array<int64_t>>("edge.mapping", edge),
                IsOkAndHolds(ElementsAre(0, 0, 1, 1, 1)));
  }
}

TEST_F(EdgeOpsTest, FromKindAndShapeOp) {
  auto split_points = CreateArray<int64_t>({0, 2, 5, 6, 6, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(split_points));
  EXPECT_THAT(InvokeOperator<ArrayShape>("edge.child_shape", edge),
              IsOkAndHolds(ArrayShape{8}));

  // scalar group
  EXPECT_THAT(
      InvokeOperator<ArrayShape>("edge.child_shape", ArrayGroupScalarEdge{5}),
      IsOkAndHolds(ArrayShape{5}));
}

TEST_F(EdgeOpsTest, IntoKindAndShapeOp) {
  auto split_points = CreateArray<int64_t>({0, 2, 5, 6, 6, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(split_points));
  EXPECT_THAT(InvokeOperator<ArrayShape>("edge.parent_shape", edge),
              IsOkAndHolds(ArrayShape{5}));

  // scalar group
  EXPECT_THAT(InvokeOperator<OptionalScalarShape>("edge.parent_shape",
                                                  ArrayGroupScalarEdge{5}),
              IsOkAndHolds(OptionalScalarShape{}));
}

TEST_F(EdgeOpsTest, ExpandOverMapping) {
  auto mapping = CreateArray<int64_t>({0, 1, std::nullopt, 0, 1, 2, 2, 1, 0});
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromMapping(mapping, 3));
  ASSERT_OK_AND_ASSIGN(auto bad_edge, ArrayEdge::FromMapping(mapping, 4));

  {  // all missing
    Array<float> values(3);
    ASSERT_OK_AND_ASSIGN(Array<float> res, InvokeOperator<Array<float>>(
                                               "array._expand", values, edge));
    EXPECT_TRUE(res.IsAllMissingForm());
    EXPECT_EQ(res.size(), edge.child_size());
  }
  {  // dense values
    auto values = CreateArray<float>({0, std::nullopt, 1});
    ASSERT_OK_AND_ASSIGN(Array<float> res, InvokeOperator<Array<float>>(
                                               "array._expand", values, edge));
    EXPECT_THAT(res, ElementsAre(0, std::nullopt, std::nullopt, 0, std::nullopt,
                                 1, 1, std::nullopt, 0));

    EXPECT_THAT(
        InvokeOperator<Array<float>>("array._expand", values, bad_edge),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("parent size of edge: 4 must match size of array: 3 "
                           "in array._expand operator")));
  }
  {  // const values
    auto values = Array<Bytes>(3, Bytes("abc"));
    ASSERT_OK_AND_ASSIGN(Array<Bytes> res, InvokeOperator<Array<Bytes>>(
                                               "array._expand", values, edge));
    ASSERT_EQ(res.size(), mapping.size());
    absl::string_view abc_values = values[0].value;
    absl::string_view abc_res = res[0].value;
    EXPECT_EQ(abc_values, abc_res);
    // abc_values links to MissingIdValue while abc_res is in a StringsBuffer.
    EXPECT_NE(abc_values.begin(), abc_res.begin());
    for (int i = 1; i < res.size(); ++i) {
      if (i != 2) {
        ASSERT_TRUE(res[i].present);
        // check that there is only one copy of the string.
        EXPECT_EQ(res[i].value.begin(), abc_res.begin());
        EXPECT_EQ(res[i].value.end(), abc_res.end());
      } else {
        ASSERT_FALSE(res[i].present);
      }
    }
  }
  {  // const values, mapping with MissingIdValue.
    auto values = Array<Bytes>(3, Bytes("abc"));
    auto mapping2 =
        CreateArray<int64_t>({1, 1, std::nullopt, 0, 0, 1, 1, 1, std::nullopt})
            .ToSparseForm(1);
    ASSERT_OK_AND_ASSIGN(auto edge2, ArrayEdge::FromMapping(mapping2, 3));

    ASSERT_OK_AND_ASSIGN(Array<Bytes> res, InvokeOperator<Array<Bytes>>(
                                               "array._expand", values, edge2));
    EXPECT_THAT(res, ElementsAre("abc", "abc", std::nullopt, "abc", "abc",
                                 "abc", "abc", "abc", std::nullopt));
  }
  {  // sparse values without MissingIdValue
    auto values = CreateArray<Bytes>({Bytes("abc"), std::nullopt, Bytes("cdf")})
                      .ToSparseForm();
    ASSERT_OK_AND_ASSIGN(Array<Bytes> res, InvokeOperator<Array<Bytes>>(
                                               "array._expand", values, edge));
    EXPECT_THAT(
        res, ElementsAre("abc", std::nullopt, std::nullopt, "abc", std::nullopt,
                         "cdf", "cdf", std::nullopt, "abc"));
    EXPECT_EQ(res[0].value.begin(), res[3].value.begin());
    EXPECT_EQ(res[0].value.begin(), res[8].value.begin());
    EXPECT_EQ(res[5].value.begin(), res[6].value.begin());
  }
  {  // sparse values with MissingIdValue
    auto values = CreateArray<Bytes>({Bytes("abc"), std::nullopt, Bytes("cdf")})
                      .ToSparseForm(Bytes("abc"));
    ASSERT_OK_AND_ASSIGN(Array<Bytes> res, InvokeOperator<Array<Bytes>>(
                                               "array._expand", values, edge));
    EXPECT_THAT(
        res, ElementsAre("abc", std::nullopt, std::nullopt, "abc", std::nullopt,
                         "cdf", "cdf", std::nullopt, "abc"));
    EXPECT_EQ(res[0].value.begin(), res[3].value.begin());
    EXPECT_EQ(res[0].value.begin(), res[8].value.begin());
    EXPECT_EQ(res[5].value.begin(), res[6].value.begin());
  }
}

TEST_F(EdgeOpsTest, ExpandOverSplitPoints) {
  auto values =
      CreateArray<Bytes>({Bytes("first"), std::nullopt, Bytes("second")});
  auto split_points = CreateArray<int64_t>({0, 103, 206, 310});
  absl::Span<const int64_t> splits_span =
      split_points.dense_data().values.span();
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(split_points));

  auto check_res = [&](const Array<Bytes>& res, bool reuse0, bool reuse2) {
    ASSERT_EQ(splits_span.size(), 4);
    for (int i = splits_span[0]; i < splits_span[1]; ++i) {
      ASSERT_EQ(values[0], res[i]);
      if (reuse0) {
        ASSERT_EQ(values[0].value.begin(), res[i].value.begin());
      }
    }
    for (int i = splits_span[1]; i < splits_span[2]; ++i) {
      ASSERT_EQ(values[1], res[i]);
    }
    for (int i = splits_span[2]; i < splits_span[3]; ++i) {
      ASSERT_EQ(values[2], res[i]);
      if (reuse2) {
        ASSERT_EQ(values[2].value.begin(), res[i].value.begin());
      }
    }
  };

  {  // dense
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<Array<Bytes>>("array._expand", values, edge));
    check_res(res, true, true);
  }
  {  // sparse
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<Array<Bytes>>("array._expand",
                                               values.ToSparseForm(), edge));
    check_res(res, true, true);
  }
  {  // sparse with bitmap
    ASSERT_OK_AND_ASSIGN(
        auto res,
        InvokeOperator<Array<Bytes>>(
            "array._expand", values.ToSparseForm(Bytes("first")), edge));
    check_res(res, false, true);
  }
}

TEST_F(EdgeOpsTest, ExpandOverSplitPointsSizeMismatch) {
  auto values =
      CreateArray<Bytes>({Bytes("first"), std::nullopt, Bytes("second")});
  auto split_points = CreateArray<int64_t>({0, 3, 6, 10, 12});
  ASSERT_OK_AND_ASSIGN(auto bad_edge, ArrayEdge::FromSplitPoints(split_points));
  EXPECT_THAT(
      InvokeOperator<Array<Bytes>>("array._expand", values, bad_edge),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("parent size of edge: 4 must match size of array: 3 "
                         "in array._expand operator")));
}

TEST_F(EdgeOpsTest, ExpandSparseOverSplitPoints) {
  auto split_points = CreateArray<int64_t>({0, 3, 6, 9});
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(split_points));

  {  // without bitmap
    auto values = CreateArray<Bytes>({std::nullopt, Bytes("abc"), std::nullopt})
                      .ToSparseForm();
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<Array<Bytes>>("array._expand", values, edge));
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_EQ(res.size(), 9);
    EXPECT_EQ(res.dense_data().size(), 3);
    for (int i = 0; i < res.size(); ++i) {
      if (i >= 3 && i < 6) {
        EXPECT_EQ(res[i], "abc");
        EXPECT_EQ(res[i].value.begin(), values[1].value.begin());
      } else {
        EXPECT_EQ(res[i], std::nullopt);
      }
    }
  }
  auto values_with_missing_id_value_and_bitmap =
      CreateArray<Bytes>({Bytes("placeholder"), Bytes("abc"), std::nullopt})
          .ToSparseForm(Bytes("placeholder"));
  {  // with bitmap
    auto values = Array<Bytes>(
        values_with_missing_id_value_and_bitmap.size(),
        values_with_missing_id_value_and_bitmap.id_filter(),
        values_with_missing_id_value_and_bitmap.dense_data(), std::nullopt);
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<Array<Bytes>>("array._expand", values, edge));
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_EQ(res.size(), 9);
    EXPECT_EQ(res.dense_data().size(), 6);
    for (int i = 0; i < res.size(); ++i) {
      if (i >= 3 && i < 6) {
        EXPECT_EQ(res[i], "abc");
        EXPECT_EQ(res[i].value.begin(), values[1].value.begin());
      } else {
        EXPECT_EQ(res[i], std::nullopt);
      }
    }
  }
  {  // with MissingIdValue
    auto values = values_with_missing_id_value_and_bitmap;
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<Array<Bytes>>("array._expand", values, edge));
    EXPECT_TRUE(res.IsSparseForm());
    EXPECT_EQ(res.size(), 9);
    EXPECT_EQ(res.dense_data().size(), 6);
    EXPECT_EQ(res.missing_id_value(), "placeholder");
    for (int i = 0; i < res.size(); ++i) {
      if (i < 3) {
        EXPECT_EQ(res[i], "placeholder");
      } else if (i < 6) {
        EXPECT_EQ(res[i], "abc");
        EXPECT_EQ(res[i].value.begin(), values[1].value.begin());
      } else {
        EXPECT_EQ(res[i], std::nullopt);
      }
    }
  }
}

TEST_F(EdgeOpsTest, ExpandConstOverSplitPoints) {
  auto split_points = CreateArray<int64_t>({0, 3, 6});
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(split_points));

  {  // float
    auto values = Array<float>(2, 3.0);
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<Array<float>>("array._expand", values, edge));
    EXPECT_THAT(res, ElementsAre(3.0, 3.0, 3.0, 3.0, 3.0, 3.0));
    EXPECT_TRUE(res.IsConstForm());
  }
  {  // Bytes
    auto values = Array<Bytes>(2, Bytes("abc"));
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<Array<Bytes>>("array._expand", values, edge));
    EXPECT_TRUE(res.IsConstForm());
    EXPECT_EQ(res.size(), 6);
    EXPECT_EQ(res.missing_id_value(), Bytes("abc"));
  }
}

TEST_F(EdgeOpsTest, ExpandAllMissingOverSplitPoints) {
  auto values = Array<float>(2);
  auto split_points = CreateArray<int64_t>({0, 3, 6});
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(split_points));

  ASSERT_OK_AND_ASSIGN(
      auto res, InvokeOperator<Array<float>>("array._expand", values, edge));
  EXPECT_THAT(res, ElementsAre(std::nullopt, std::nullopt, std::nullopt,
                               std::nullopt, std::nullopt, std::nullopt));
  EXPECT_TRUE(res.IsAllMissingForm());
}

TEST_F(EdgeOpsTest, ExpandGroupScalarEdge) {
  auto edge = ArrayGroupScalarEdge(3);

  ASSERT_OK_AND_ASSIGN(
      auto res1, InvokeOperator<Array<Bytes>>(
                     "array._expand", MakeOptionalValue(Bytes("first")), edge));
  EXPECT_THAT(res1, ElementsAre("first", "first", "first"));

  ASSERT_OK_AND_ASSIGN(
      auto res2, InvokeOperator<Array<Bytes>>("array._expand",
                                              OptionalValue<Bytes>(), edge));
  EXPECT_THAT(res2, ElementsAre(std::nullopt, std::nullopt, std::nullopt));
}

TEST_F(EdgeOpsTest, AggSizeEdgeOp_Mapping) {
  // Mapping [1, None, 1, None, 3]
  auto mapping = CreateArray<int64_t>({0, std::nullopt, 0, std::nullopt, 2})
                     .ToSparseForm(0);
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromMapping(mapping, 3));
  ASSERT_OK_AND_ASSIGN(auto qblock,
                       InvokeOperator<Array<int64_t>>("edge.sizes", edge));
  EXPECT_THAT(qblock, ElementsAre(2, 0, 1));
}

TEST_F(EdgeOpsTest, AggSizeEdgeOp_SplitPoints) {
  auto split_points = CreateArray<int64_t>({0, 2, 4, 4, 8});
  ASSERT_OK_AND_ASSIGN(auto edge, ArrayEdge::FromSplitPoints(split_points));
  ASSERT_OK_AND_ASSIGN(auto qblock,
                       InvokeOperator<Array<int64_t>>("edge.sizes", edge));
  EXPECT_THAT(qblock, ElementsAre(2, 2, 0, 4));
}

TEST_F(EdgeOpsTest, TestAggCountScalarEdge) {
  auto mask = CreateArray<Unit>({kUnit, std::nullopt, kUnit, std::nullopt});
  auto edge = ArrayGroupScalarEdge(4);
  EXPECT_THAT(InvokeOperator<int64_t>("array._count", mask, edge),
              IsOkAndHolds(2));
}

TEST_F(EdgeOpsTest, GroupByOp_Integral) {
  const auto series = CreateArray<int64_t>({101, 102, 103, 104});
  ASSERT_OK_AND_ASSIGN(
      auto over, ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, 4})));
  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 4);

  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3));
}

TEST_F(EdgeOpsTest, GroupByOp_Float) {
  const auto series = CreateArray<float>({5., 7., 1., 2., 4.});
  ASSERT_OK_AND_ASSIGN(
      auto over, ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, 5})));

  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 5);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3, 4));
}

TEST_F(EdgeOpsTest, GroupByOp_Bytes) {
  const auto series = CreateArray<Bytes>(
      {Bytes("a"), Bytes("b"), Bytes("c"), Bytes("d"), Bytes("e")});
  ASSERT_OK_AND_ASSIGN(
      auto over, ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, 5})));

  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 5);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3, 4));
}

TEST_F(EdgeOpsTest, GroupByOp_DuplicatesInInputSeries) {
  const auto series = CreateArray<float>({5., 7., 5., 7., 4., 8.});
  ASSERT_OK_AND_ASSIGN(
      auto over, ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, 6})));
  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 4);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 0, 1, 2, 3));
}

TEST_F(EdgeOpsTest, GroupByOp_DuplicatesInInputSeries_WithSplits) {
  // Array with splits: [(5, 7, 5), (7, 4, 8)]
  const auto series = CreateArray<float>({5., 7., 5., 7., 7., 8.});
  ASSERT_OK_AND_ASSIGN(
      auto over, ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, 3, 6})));
  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 4);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 0, 2, 2, 3));
}

TEST_F(EdgeOpsTest, GroupByOp_DuplicatesInInputSeries_WithMapping) {
  const auto series = CreateArray<float>({5., 7., 5., 7., 7., 8.});
  ASSERT_OK_AND_ASSIGN(
      auto over,
      ArrayEdge::FromMapping(CreateArray<int64_t>({1, 1, 0, 2, 1, 0}), 3));
  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 5);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3, 1, 4));
}

TEST_F(EdgeOpsTest, GroupByOp_MissingValuesAndDuplicates) {
  const auto series = CreateArray<int64_t>({7, 8, std::nullopt, 7, 10, 8});
  ASSERT_OK_AND_ASSIGN(
      auto over, ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, 6})));

  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 3);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, std::nullopt, 0, 2, 1));
}

TEST_F(EdgeOpsTest, GroupByOp_MissingValuesAndDuplicates_WithSplits) {
  // Array with splits: [(7, 6, 7), (5), (5), (NA, NA), (5, 5), (NA, 7 , 10.
  // 7)]
  const auto series =
      CreateArray<int64_t>({7, 6, 7, 5, 5, std::nullopt, std::nullopt, 5, 5,
                            std::nullopt, 7, 10, 7});
  ASSERT_OK_AND_ASSIGN(
      auto over,
      ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, 3, 4, 5, 7, 9, 13})));

  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 7);
  EXPECT_THAT(edge.edge_values(),
              ElementsAre(0, 1, 0, 2, 3, std::nullopt, std::nullopt, 4, 4,
                          std::nullopt, 5, 6, 5));
}

TEST_F(EdgeOpsTest, GroupByOp_EmptyDenseArray) {
  const auto series = CreateArray<int64_t>({});
  ASSERT_OK_AND_ASSIGN(auto over,
                       ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0})));

  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 0);
  EXPECT_THAT(edge.edge_values(), ElementsAre());
}

TEST_F(EdgeOpsTest, GroupByOp_MissingValuesAndDuplicates_WithMapping) {
  // Array:         [7,  6, 6, 7, 5, 5, NA, NA, 5, 5, NA, 7, 10, 7,  5]
  // Mapping:        [2, NA, 2, 3, 1, 2,  2, NA, 1, 2,  4, 2,  3, 3, NA]
  // Child-to-Group: [0, NA, 1, 2, 3, 4, NA, NA, 3, 4, NA, 0,  5, 2, NA]
  const auto series =
      CreateArray<int64_t>({7, 6, 6, 7, 5, 5, std::nullopt, std::nullopt, 5, 5,
                            std::nullopt, 7, 10, 7, 5});
  ASSERT_OK_AND_ASSIGN(
      auto over,
      ArrayEdge::FromMapping(
          CreateArray<int64_t>({2, std::nullopt, 2, 3, 1, 2, 2, std::nullopt, 1,
                                2, 4, 2, 3, 3, std::nullopt}),
          5));
  ASSERT_OK_AND_ASSIGN(
      auto edge, InvokeOperator<ArrayEdge>("edge._group_by", series, over));
  EXPECT_EQ(edge.parent_size(), 6);
  EXPECT_THAT(
      edge.edge_values(),
      ElementsAre(0, std::nullopt, 1, 2, 3, 4, std::nullopt, std::nullopt, 3, 4,
                  std::nullopt, 0, 5, 2, std::nullopt));
}

TEST_F(EdgeOpsTest, GroupByOp_IncompatibleOverEdge) {
  const auto series = CreateArray<int64_t>({1, 2});
  ASSERT_OK_AND_ASSIGN(
      auto over, ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, 3})));

  EXPECT_THAT(InvokeOperator<ArrayEdge>("edge._group_by", series, over),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("argument sizes mismatch")));
}

// See the full coverage test in
// py/arolla/operator_tests/edge_compose_test.py
TEST_F(EdgeOpsTest, EdgeComposeOp) {
  {
    // Split point inputs -> split point output.
    ASSERT_OK_AND_ASSIGN(auto edge1, ArrayEdge::FromSplitPoints(
                                         CreateArray<int64_t>({0, 2, 3})));
    ASSERT_OK_AND_ASSIGN(auto edge2, ArrayEdge::FromSplitPoints(
                                         CreateArray<int64_t>({0, 1, 2, 4})));
    ASSERT_OK_AND_ASSIGN(
        auto composed_edge,
        InvokeOperator<ArrayEdge>("edge.compose._array", edge1, edge2));
    EXPECT_THAT(composed_edge.edge_values(), ElementsAre(0, 2, 4));
    EXPECT_THAT(composed_edge.edge_type(), ArrayEdge::SPLIT_POINTS);
  }
  {
    // Mapping input -> mapping output.
    ASSERT_OK_AND_ASSIGN(auto edge1, ArrayEdge::FromSplitPoints(
                                         CreateArray<int64_t>({0, 2, 3})));
    ASSERT_OK_AND_ASSIGN(
        auto edge2,
        ArrayEdge::FromMapping(CreateArray<int64_t>({0, 1, 2, 2}), 3));
    ASSERT_OK_AND_ASSIGN(
        auto composed_edge,
        InvokeOperator<ArrayEdge>("edge.compose._array", edge1, edge2));
    EXPECT_THAT(composed_edge.edge_values(), ElementsAre(0, 0, 1, 1));
    EXPECT_THAT(composed_edge.edge_type(), ArrayEdge::MAPPING);
  }
}

}  // namespace
}  // namespace arolla
