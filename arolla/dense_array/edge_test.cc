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
#include "arolla/dense_array/edge.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/fingerprint.h"

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;

namespace arolla {
namespace {

TEST(DenseArrayEdgeTest, FromSplitPoints) {
  DenseArray<int64_t> split_points{CreateBuffer<int64_t>({0, 10, 20})};
  ASSERT_OK_AND_ASSIGN(auto edge,
                       DenseArrayEdge::FromSplitPoints(split_points));

  EXPECT_THAT(edge.edge_type(), Eq(DenseArrayEdge::SPLIT_POINTS));
  EXPECT_THAT(edge.edge_values().values, ElementsAre(0, 10, 20));
  EXPECT_EQ(edge.parent_size(), 2);
  EXPECT_EQ(edge.child_size(), 20);
  EXPECT_EQ(edge.split_size(0), 10);
  EXPECT_EQ(edge.split_size(1), 10);
}

TEST(DenseArrayEdgeTest, FromSplitPointsEmptyGroup) {
  DenseArray<int64_t> split_points{CreateBuffer<int64_t>({0})};
  ASSERT_OK_AND_ASSIGN(auto edge,
                       DenseArrayEdge::FromSplitPoints(split_points));

  EXPECT_THAT(edge.edge_type(), Eq(DenseArrayEdge::SPLIT_POINTS));
  EXPECT_THAT(edge.edge_values().values, ElementsAre(0));
  EXPECT_EQ(edge.parent_size(), 0);
  EXPECT_EQ(edge.child_size(), 0);
}

TEST(DenseArrayEdgeTest, FromSplitPointsNotFull) {
  auto split_points = CreateDenseArray<int64_t>({0, 3, std::nullopt, 10});
  EXPECT_THAT(DenseArrayEdge::FromSplitPoints(split_points),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr("split points must be full")));
}

TEST(DenseArrayEdgeTest, FromSplitPointsTooFew) {
  EXPECT_THAT(DenseArrayEdge::FromSplitPoints(DenseArray<int64_t>()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr(
                           "split points array must have at least 1 element")));
}

TEST(DenseArrayEdgeTest, FromSplitPointsInBadOrder) {
  EXPECT_THAT(
      DenseArrayEdge::FromSplitPoints({CreateBuffer<int64_t>({10, 20, 30})}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr(
                   "split points array must have first element equal to 0")));

  EXPECT_THAT(
      DenseArrayEdge::FromSplitPoints({CreateBuffer<int64_t>({0, 40, 10})}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("split points must be sorted")));
}

TEST(DenseArrayEdgeTest, UnsafeFromSplitPoints) {
  DenseArray<int64_t> split_points(CreateDenseArray<int64_t>({0, 10, 20}));
  auto edge = DenseArrayEdge::UnsafeFromSplitPoints(split_points);

  EXPECT_THAT(edge.edge_type(), Eq(DenseArrayEdge::SPLIT_POINTS));
  EXPECT_THAT(edge.edge_values().values, ElementsAre(0, 10, 20));
  EXPECT_EQ(edge.parent_size(), 2);
  EXPECT_EQ(edge.child_size(), 20);
  EXPECT_EQ(edge.split_size(0), 10);
  EXPECT_EQ(edge.split_size(1), 10);
}

TEST(ArrayEdgeTest, UnsafeFromSplitPointsEmptyGroup) {
  DenseArray<int64_t> split_points(CreateDenseArray<int64_t>({0}));
  auto edge = DenseArrayEdge::UnsafeFromSplitPoints(split_points);

  EXPECT_THAT(edge.edge_type(), Eq(DenseArrayEdge::SPLIT_POINTS));
  EXPECT_THAT(edge.edge_values(), ElementsAre(0));
  EXPECT_EQ(edge.parent_size(), 0);
  EXPECT_EQ(edge.child_size(), 0);
}

TEST(DenseArrayEdgeTest, FromMapping) {
  DenseArray<int64_t> mapping{
      CreateBuffer<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2})};
  DenseArray<int64_t> bad_mapping{
      CreateBuffer<int64_t>({0, -1, 2, 0, 1, 2, 0, 1, 2})};
  EXPECT_THAT(
      DenseArrayEdge::FromMapping(mapping, -1),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("parent_size can not be negative")));
  EXPECT_THAT(
      DenseArrayEdge::FromMapping(mapping, 2),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("parent_size=2, but parent id 2 is used")));
  EXPECT_THAT(
      DenseArrayEdge::FromMapping(bad_mapping, 3),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("mapping can't contain negative values")));
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromMapping(mapping, 3));
  EXPECT_THAT(edge.edge_type(), Eq(DenseArrayEdge::MAPPING));
  EXPECT_THAT(edge.edge_values().values, Eq(mapping.values));
  EXPECT_EQ(edge.parent_size(), 3);
  EXPECT_EQ(edge.child_size(), 9);
}

TEST(DenseArrayEdgeTest, FromUniformGroups) {
  {
    // No parent.
    ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromUniformGroups(0, 5));
    EXPECT_THAT(edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_EQ(edge.parent_size(), 0);
    EXPECT_EQ(edge.child_size(), 0);
    EXPECT_THAT(edge.edge_values(), ElementsAre(0));
  }
  {
    // No children.
    ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromUniformGroups(3, 0));
    EXPECT_THAT(edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_EQ(edge.parent_size(), 3);
    EXPECT_EQ(edge.child_size(), 0);
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0, 0, 0));
  }
  {
    // Normal uniform edge.
    ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromUniformGroups(3, 4));
    EXPECT_THAT(edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_EQ(edge.parent_size(), 3);
    EXPECT_EQ(edge.child_size(), 12);
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 4, 8, 12));
  }
  {
    // Errors.
    EXPECT_THAT(DenseArrayEdge::FromUniformGroups(-1, 3),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "parent_size and group_size cannot be negative"));
    EXPECT_THAT(DenseArrayEdge::FromUniformGroups(3, -1),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "parent_size and group_size cannot be negative"));
  }
  {
    // Default (heap) -> owned.
    ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromUniformGroups(1, 1));
    EXPECT_TRUE(edge.edge_values().values.is_owner());
  }
  {
    // On arena -> not owned.
    UnsafeArenaBufferFactory arena{128};
    ASSERT_OK_AND_ASSIGN(auto edge,
                         DenseArrayEdge::FromUniformGroups(1, 1, arena));
    EXPECT_FALSE(edge.edge_values().values.is_owner());
  }
}

TEST(DenseArrayEdgeTest, DefaultEdge) {
  DenseArrayEdge edge;
  EXPECT_THAT(edge.edge_type(), Eq(DenseArrayEdge::MAPPING));
  EXPECT_THAT(edge.edge_values().values, ElementsAre());
  EXPECT_EQ(edge.parent_size(), 0);
  EXPECT_EQ(edge.child_size(), 0);
}

TEST(DenseArrayEdgeTest, Fingerprint) {
  const auto mapping = CreateDenseArray<int64_t>({0, 0, 0, 1, 1});
  const auto split_points = CreateDenseArray<int64_t>({0, 3, 5});
  ASSERT_OK_AND_ASSIGN(auto edge_from_mapping_1,
                       DenseArrayEdge::FromMapping(mapping, 2));
  ASSERT_OK_AND_ASSIGN(auto edge_from_mapping_2,
                       DenseArrayEdge::FromMapping(mapping, 2));
  ASSERT_OK_AND_ASSIGN(auto edge_from_split_points,
                       DenseArrayEdge::FromSplitPoints(split_points));
  EXPECT_EQ(FingerprintHasher("salt").Combine(edge_from_mapping_1).Finish(),
            FingerprintHasher("salt").Combine(edge_from_mapping_2).Finish());
  EXPECT_NE(FingerprintHasher("salt").Combine(edge_from_mapping_1).Finish(),
            FingerprintHasher("salt").Combine(edge_from_split_points).Finish());
}

TEST(DenseArrayEdgeTest, ToSplitPointsEdge_Success) {
  {
    // From split points (trivial).
    const auto split_points = CreateDenseArray<int64_t>({0, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto edge,
                         DenseArrayEdge::FromSplitPoints(split_points));
    ASSERT_OK_AND_ASSIGN(auto edge2, edge.ToSplitPointsEdge());
    EXPECT_EQ(edge.parent_size(), edge2.parent_size());
    EXPECT_EQ(edge.child_size(), edge2.child_size());
    EXPECT_EQ(edge2.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(edge2.edge_values().values, ElementsAre(0, 3, 5));
  }
  {
    // From mapping.
    const auto mapping = CreateDenseArray<int64_t>({0, 0, 1, 1, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto mapping_edge,
                         DenseArrayEdge::FromMapping(mapping, 8));
    ASSERT_OK_AND_ASSIGN(auto split_point_edge,
                         mapping_edge.ToSplitPointsEdge());
    EXPECT_EQ(mapping_edge.parent_size(), split_point_edge.parent_size());
    EXPECT_EQ(mapping_edge.child_size(), split_point_edge.child_size());
    EXPECT_EQ(split_point_edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(split_point_edge.edge_values().values,
                ElementsAre(0, 2, 4, 4, 5, 5, 6, 6, 6));
  }
}

TEST(DenseArrayEdgeTest, ToSplitPointsEdge_Errors) {
  {
    // Missing values in mapping edge.
    const auto mapping = CreateDenseArray<int64_t>({0, std::nullopt});
    ASSERT_OK_AND_ASSIGN(auto mapping_edge,
                         DenseArrayEdge::FromMapping(mapping, 2));
    EXPECT_THAT(mapping_edge.ToSplitPointsEdge(),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected a full mapping"));
  }
  {
    // Unsorted values in mapping edge.
    const auto mapping = CreateDenseArray<int64_t>({1, 0});
    ASSERT_OK_AND_ASSIGN(auto mapping_edge,
                         DenseArrayEdge::FromMapping(mapping, 2));
    EXPECT_THAT(mapping_edge.ToSplitPointsEdge(),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected a sorted mapping"));
  }
}

TEST(DenseArrayEdgeTest, ToSplitPointsEdge_BufferFactory) {
  {
    // Default (heap) -> owned.
    const auto mapping = CreateDenseArray<int64_t>({0, 0, 1, 1, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto mapping_edge,
                         DenseArrayEdge::FromMapping(mapping, 8));
    ASSERT_OK_AND_ASSIGN(auto split_point_edge,
                         mapping_edge.ToSplitPointsEdge());
    EXPECT_TRUE(split_point_edge.edge_values().values.is_owner());
  }
  {
    // On arena -> not owned.
    const auto mapping = CreateDenseArray<int64_t>({0, 0, 1, 1, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto mapping_edge,
                         DenseArrayEdge::FromMapping(mapping, 8));
    UnsafeArenaBufferFactory arena{128};
    ASSERT_OK_AND_ASSIGN(auto split_point_edge,
                         mapping_edge.ToSplitPointsEdge(arena));
    EXPECT_FALSE(split_point_edge.edge_values().values.is_owner());
  }
}

TEST(DenseArrayEdgeTest, ToMappingEdge) {
  {
    // From mapping (trivial).
    const auto mapping =
        CreateDenseArray<int64_t>({0, 1, 0, std::nullopt, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromMapping(mapping, 8));
    auto edge2 = edge.ToMappingEdge();
    EXPECT_EQ(edge.parent_size(), edge2.parent_size());
    EXPECT_EQ(edge.child_size(), edge2.child_size());
    EXPECT_EQ(edge2.edge_type(), DenseArrayEdge::MAPPING);
    EXPECT_THAT(edge2.edge_values(), ElementsAre(0, 1, 0, std::nullopt, 3, 5));
  }
  {
    // From split points.
    const auto split_points = CreateDenseArray<int64_t>({0, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto split_points_edge,
                         DenseArrayEdge::FromSplitPoints(split_points));
    auto mapping_edge = split_points_edge.ToMappingEdge();
    EXPECT_EQ(split_points_edge.parent_size(), mapping_edge.parent_size());
    EXPECT_EQ(split_points_edge.child_size(), mapping_edge.child_size());
    EXPECT_EQ(mapping_edge.edge_type(), DenseArrayEdge::MAPPING);
    EXPECT_THAT(mapping_edge.edge_values(), ElementsAre(0, 0, 0, 1, 1));
  }
  {
    // BufferFactory.
    const auto split_points = CreateDenseArray<int64_t>({0, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto split_points_edge,
                         DenseArrayEdge::FromSplitPoints(split_points));
    // Default (heap) -> owned.
    auto mapping_edge = split_points_edge.ToMappingEdge();
    EXPECT_TRUE(mapping_edge.edge_values().values.is_owner());
    // On arena -> not owned.
    UnsafeArenaBufferFactory arena{128};
    mapping_edge = split_points_edge.ToMappingEdge(arena);
    EXPECT_FALSE(mapping_edge.edge_values().values.is_owner());
  }
}

TEST(DenseArrayEdgeTest, IsEquivalentTo) {
  {
    // Split points edge - True.
    const auto split_points = CreateDenseArray<int64_t>({0, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto edge1,
                         DenseArrayEdge::FromSplitPoints(split_points));
    ASSERT_OK_AND_ASSIGN(auto edge2,
                         DenseArrayEdge::FromSplitPoints(split_points));
    EXPECT_TRUE(edge1.IsEquivalentTo(edge2));
  }
  {
    // Mapping edge - True.
    const auto mapping =
        CreateDenseArray<int64_t>({0, 0, std::nullopt, 1, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromMapping(mapping, 8));
    ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromMapping(mapping, 8));
    EXPECT_TRUE(edge1.IsEquivalentTo(edge2));
  }
  {
    // Different types - True if they represent the same data.
    const auto mapping = CreateDenseArray<int64_t>({0, 0, 0, 1, 1});
    ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromMapping(mapping, 2));
    const auto split_points = CreateDenseArray<int64_t>({0, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto edge2,
                         DenseArrayEdge::FromSplitPoints(split_points));
    EXPECT_TRUE(edge1.IsEquivalentTo(edge2));
  }
  {
    // Different types - False otherwise.
    const auto mapping = CreateDenseArray<int64_t>({0, 0, 1, 0, 1});
    ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromMapping(mapping, 2));
    const auto split_points = CreateDenseArray<int64_t>({0, 3, 5});
    ASSERT_OK_AND_ASSIGN(auto edge2,
                         DenseArrayEdge::FromSplitPoints(split_points));
    EXPECT_FALSE(edge1.IsEquivalentTo(edge2));
  }
  {
    // Different parent size - False.
    const auto mapping = CreateDenseArray<int64_t>({0, 0, 0, 1, 1});
    ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromMapping(mapping, 2));
    ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromMapping(mapping, 3));
    EXPECT_FALSE(edge1.IsEquivalentTo(edge2));
  }
  {
    // Different child size - False.
    const auto mapping1 = CreateDenseArray<int64_t>({0, 0, 0, 1, 1});
    ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromMapping(mapping1, 2));
    const auto mapping2 = CreateDenseArray<int64_t>({0, 1, 1});
    ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromMapping(mapping2, 2));
    EXPECT_FALSE(edge1.IsEquivalentTo(edge2));
  }
  {
    // Different values - False.
    const auto mapping1 = CreateDenseArray<int64_t>({0, 0, 0, 1, 1});
    ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromMapping(mapping1, 2));
    const auto mapping2 = CreateDenseArray<int64_t>({0, 0, 1, 1, 1});
    ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromMapping(mapping2, 2));
    EXPECT_FALSE(edge1.IsEquivalentTo(edge2));
  }
}

TEST(DenseArrayEdgeTest, ComposeEdges_SplitPoint) {
  ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 2})));
  ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 1, 3})));
  ASSERT_OK_AND_ASSIGN(
      auto edge3,
      DenseArrayEdge::FromSplitPoints(CreateDenseArray<int64_t>({0, 1, 2, 4})));
  ASSERT_OK_AND_ASSIGN(auto edge4,
                       DenseArrayEdge::FromSplitPoints(
                           CreateDenseArray<int64_t>({0, 3, 4, 11, 12})));
  {
    // 4 edges.
    ASSERT_OK_AND_ASSIGN(auto composed_edge, DenseArrayEdge::ComposeEdges(
                                                 {edge1, edge2, edge3, edge4}));
    EXPECT_EQ(composed_edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(composed_edge.edge_values(), ElementsAre(0, 12));
  }
  {
    // 3 edges.
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         DenseArrayEdge::ComposeEdges({edge2, edge3, edge4}));
    EXPECT_EQ(composed_edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(composed_edge.edge_values(), ElementsAre(0, 3, 12));
  }
  {
    // 2 edges.
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         DenseArrayEdge::ComposeEdges({edge3, edge4}));
    EXPECT_EQ(composed_edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(composed_edge.edge_values(), ElementsAre(0, 3, 4, 12));
  }
  {
    // Single edge.
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         DenseArrayEdge::ComposeEdges({edge4}));
    EXPECT_EQ(composed_edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(composed_edge.edge_values(), ElementsAre(0, 3, 4, 11, 12));
  }
  {
    // No edges error.
    EXPECT_THAT(DenseArrayEdge::ComposeEdges({}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "at least one edge must be present"));
  }
  {
    // Incompatible edges.
    EXPECT_THAT(DenseArrayEdge::ComposeEdges({edge1, edge3}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "incompatible edges: edges[0].child_size (2) != "
                         "edges[1].parent_size (3)"));
  }
}

TEST(DenseArrayEdgeTest, ComposeEdges_Mapping) {
  ASSERT_OK_AND_ASSIGN(auto edge1,
                       DenseArrayEdge::FromMapping(
                           CreateDenseArray<int64_t>({0, std::nullopt}), 5));
  ASSERT_OK_AND_ASSIGN(auto edge2,
                       DenseArrayEdge::FromMapping(
                           CreateDenseArray<int64_t>({0, 1, std::nullopt}), 2));
  ASSERT_OK_AND_ASSIGN(
      auto edge3,
      DenseArrayEdge::FromMapping(CreateDenseArray<int64_t>({1, 2, 0, 2}), 3));
  // Note: Split points edges.
  ASSERT_OK_AND_ASSIGN(auto edge4,
                       DenseArrayEdge::FromSplitPoints(
                           CreateDenseArray<int64_t>({0, 3, 4, 11, 12})));
  ASSERT_OK_AND_ASSIGN(
      auto edge5, DenseArrayEdge::FromSplitPoints(CreateDenseArray<int64_t>(
                      {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})));
  {
    // 5 edges.
    ASSERT_OK_AND_ASSIGN(
        auto composed_edge,
        DenseArrayEdge::ComposeEdges({edge1, edge2, edge3, edge4, edge5}));
    EXPECT_EQ(composed_edge.edge_type(), DenseArrayEdge::MAPPING);
    EXPECT_EQ(composed_edge.parent_size(), 5);
    EXPECT_THAT(composed_edge.edge_values(),
                ElementsAre(std::nullopt, std::nullopt, std::nullopt,
                            std::nullopt, 0, 0, 0, 0, 0, 0, 0, std::nullopt));
  }
  {
    // 3 edges.
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         DenseArrayEdge::ComposeEdges({edge2, edge3, edge4}));
    EXPECT_EQ(composed_edge.edge_type(), DenseArrayEdge::MAPPING);
    EXPECT_EQ(composed_edge.parent_size(), 2);
    EXPECT_THAT(
        composed_edge.edge_values(),
        ElementsAre(1, 1, 1, std::nullopt, 0, 0, 0, 0, 0, 0, 0, std::nullopt));
  }
  {
    // 2 edges - last is split points.
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         DenseArrayEdge::ComposeEdges({edge3, edge4}));
    EXPECT_EQ(composed_edge.edge_type(), DenseArrayEdge::MAPPING);
    EXPECT_EQ(composed_edge.parent_size(), 3);
    EXPECT_THAT(composed_edge.edge_values(),
                ElementsAre(1, 1, 1, 2, 0, 0, 0, 0, 0, 0, 0, 2));
  }
  {
    // 2 edges - first is split points.
    ASSERT_OK_AND_ASSIGN(
        auto composed_edge,
        DenseArrayEdge::ComposeEdges({edge4, edge5.ToMappingEdge()}));
    EXPECT_EQ(composed_edge.edge_type(), DenseArrayEdge::MAPPING);
    EXPECT_EQ(composed_edge.parent_size(), 4);
    EXPECT_THAT(composed_edge.edge_values(),
                ElementsAre(0, 0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 3));
  }
  {
    // 2 edges - only mappings.
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         DenseArrayEdge::ComposeEdges({edge2, edge3}));
    EXPECT_EQ(composed_edge.edge_type(), DenseArrayEdge::MAPPING);
    EXPECT_EQ(composed_edge.parent_size(), 2);
    EXPECT_THAT(composed_edge.edge_values(),
                ElementsAre(1, std::nullopt, 0, std::nullopt));
  }
  {
    // Single edge.
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         DenseArrayEdge::ComposeEdges({edge4.ToMappingEdge()}));
    EXPECT_EQ(composed_edge.edge_type(), DenseArrayEdge::MAPPING);
    EXPECT_EQ(composed_edge.parent_size(), 4);
    EXPECT_THAT(composed_edge.edge_values(),
                ElementsAre(0, 0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 3));
  }
  {
    // No edges error.
    EXPECT_THAT(DenseArrayEdge::ComposeEdges({}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "at least one edge must be present"));
  }
  {
    // Incompatible edges.
    EXPECT_THAT(DenseArrayEdge::ComposeEdges({edge1, edge3}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "incompatible edges: edges[0].child_size (2) != "
                         "edges[1].parent_size (3)"));
  }
}

TEST(DenseArrayEdgeTest, ComposeEdges_BufferFactory) {
  ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 2})));
  ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 1, 3})));
  {
    // Default (heap) -> owned.
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         DenseArrayEdge::ComposeEdges({edge1, edge2}));
    EXPECT_TRUE(composed_edge.edge_values().values.is_owner());
  }
  {
    // On arena -> not owned.
    UnsafeArenaBufferFactory arena{128};
    ASSERT_OK_AND_ASSIGN(auto composed_edge,
                         DenseArrayEdge::ComposeEdges({edge1, edge2}, arena));
    EXPECT_FALSE(composed_edge.edge_values().values.is_owner());
  }
}

}  // namespace
}  // namespace arolla
