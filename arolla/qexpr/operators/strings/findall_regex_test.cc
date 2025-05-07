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
#include "arolla/qexpr/operators/strings/findall_regex.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/strings/regex.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/testing/repr_token_eq.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;
using ::testing::ElementsAre;
using ::testing::IsEmpty;

TEST(FindallRegexTest, OneCapturingGroup) {
  ASSERT_OK_AND_ASSIGN(auto regex, CompileRegex("(\\d+) bottles of beer"));
  ASSERT_OK_AND_ASSIGN(
      const auto result,
      FindallRegexOp()(
          CreateDenseArray<Text>(
              {Text("I had 5 bottles of beer tonight"), std::nullopt,
               Text("foo"),
               Text("100 bottles of beer, no, 1000 bottles of beer")}),
          regex));

  EXPECT_THAT(std::get<0>(result),
              ElementsAre(Text("5"), Text("100"), Text("1000")));
  auto edge_1 = std::get<1>(result);
  auto edge_2 = std::get<2>(result);

  EXPECT_THAT(edge_1.edge_values().values,
              ElementsAre(0, 1, 1, 1, 3));  // Sizes: 1, 0, 0, 2
  EXPECT_THAT(edge_2.edge_values().values,
              // Sizes: 1, 1, 1 because there is only 1 capturing group
              ElementsAre(0, 1, 2, 3));

  const auto split_points = CreateDenseArray<int64_t>({0, 4});
  ASSERT_OK_AND_ASSIGN(auto initial_edge,
                       DenseArrayEdge::FromSplitPoints(split_points));
  ASSERT_OK_AND_ASSIGN(auto shape, JaggedDenseArrayShape::FromEdges(
                                       {initial_edge, edge_1, edge_2}));
  auto tv = TypedValue::FromValue(shape);
  EXPECT_THAT(tv.GenReprToken(),
              ReprTokenEq("JaggedShape(4, [1, 0, 0, 2], 1)"));
}

TEST(FindallRegexTest, ManyCapturingGroups) {
  ASSERT_OK_AND_ASSIGN(auto regex, CompileRegex("(\\d+) (bottles) (of) beer"));
  ASSERT_OK_AND_ASSIGN(
      const auto result,
      FindallRegexOp()(
          CreateDenseArray<Text>(
              {Text("I had 5 bottles of beer tonight"), std::nullopt,
               Text("foo"),
               Text("100 bottles of beer, no, 1000 bottles of beer")}),
          regex));

  EXPECT_THAT(std::get<0>(result),
              ElementsAre(Text("5"), Text("bottles"), Text("of"), Text("100"),
                          Text("bottles"), Text("of"), Text("1000"),
                          Text("bottles"), Text("of")));
  auto edge_1 = std::get<1>(result);
  auto edge_2 = std::get<2>(result);

  EXPECT_THAT(edge_1.edge_values().values,
              ElementsAre(0, 1, 1, 1, 3));  // Sizes: 1, 0, 0, 2
  EXPECT_THAT(edge_2.edge_values().values,
              // Sizes: 3, 3, 3 because there are 3 capturing groups
              ElementsAre(0, 3, 6, 9));

  const auto split_points = CreateDenseArray<int64_t>({0, 4});
  ASSERT_OK_AND_ASSIGN(auto initial_edge,
                       DenseArrayEdge::FromSplitPoints(split_points));
  ASSERT_OK_AND_ASSIGN(auto shape, JaggedDenseArrayShape::FromEdges(
                                       {initial_edge, edge_1, edge_2}));
  auto tv = TypedValue::FromValue(shape);
  EXPECT_THAT(tv.GenReprToken(),
              ReprTokenEq("JaggedShape(4, [1, 0, 0, 2], 3)"));
}

TEST(FindallRegexTest, NoMatchesWhatsoever) {
  ASSERT_OK_AND_ASSIGN(auto regex, CompileRegex("(\\d+) bottles of beer"));
  ASSERT_OK_AND_ASSIGN(
      const auto result,
      FindallRegexOp()(
          CreateDenseArray<Text>(
              {Text("I had 5 bottles of wine tonight"), std::nullopt,
               Text("foo"),
               Text("100 bottles of rum, no, 1000 bottles of vodka")}),
          regex));

  EXPECT_THAT(std::get<0>(result), IsEmpty());
  auto edge_1 = std::get<1>(result);
  auto edge_2 = std::get<2>(result);

  EXPECT_THAT(edge_1.edge_values().values, ElementsAre(0, 0, 0, 0, 0));
  EXPECT_THAT(edge_2.edge_values().values, ElementsAre(0));

  const auto split_points = CreateDenseArray<int64_t>({0, 4});
  ASSERT_OK_AND_ASSIGN(auto initial_edge,
                       DenseArrayEdge::FromSplitPoints(split_points));
  ASSERT_OK_AND_ASSIGN(auto shape, JaggedDenseArrayShape::FromEdges(
                                       {initial_edge, edge_1, edge_2}));
  auto tv = TypedValue::FromValue(shape);
  EXPECT_THAT(tv.GenReprToken(), ReprTokenEq("JaggedShape(4, 0, [])"));
}

TEST(FindallRegexTest, WorksForBytes) {
  ASSERT_OK_AND_ASSIGN(auto regex, CompileRegex("(\\d+) (bottles) (of) beer"));
  ASSERT_OK_AND_ASSIGN(
      const auto result,
      FindallRegexOp()(
          CreateDenseArray<Bytes>(
              {Bytes("I had 5 bottles of beer tonight"), std::nullopt,
               Bytes("foo"),
               Bytes("100 bottles of beer, no, 1000 bottles of beer")}),
          regex));

  EXPECT_THAT(std::get<0>(result),
              ElementsAre(Bytes("5"), Bytes("bottles"), Bytes("of"),
                          Bytes("100"), Bytes("bottles"), Bytes("of"),
                          Bytes("1000"), Bytes("bottles"), Bytes("of")));
}

}  // namespace
}  // namespace arolla
