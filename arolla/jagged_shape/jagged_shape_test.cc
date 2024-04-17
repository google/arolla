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
#include "arolla/jagged_shape/jagged_shape.h"  // IWYU pragma: keep

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/array/jagged_shape.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/repr_token_eq.h"
#include "arolla/util/testing/status_matchers_backport.h"

using ::arolla::testing::ReprTokenEq;
using ::arolla::testing::StatusIs;
using ::testing::ElementsAre;

namespace arolla {
namespace {

class JaggedArrayShapeHelper {
 public:
  using Shape = JaggedArrayShape;
  using ShapePtr = Shape::ShapePtr;
  using Edge = Shape::Edge;

  static absl::string_view ReprName() { return "JaggedArrayShape"; }

  static absl::StatusOr<ArrayEdge> EdgeFromSplitPoints(
      absl::Span<const OptionalValue<int64_t>> split_points) {
    return ArrayEdge::FromSplitPoints(CreateArray<int64_t>(split_points));
  }

  static absl::StatusOr<ArrayEdge> EdgeFromMapping(
      absl::Span<const OptionalValue<int64_t>> mapping, int64_t parent_size) {
    return ArrayEdge::FromMapping(CreateArray<int64_t>(mapping), parent_size);
  }

  static const Buffer<int64_t>& GetSplitPoints(const ArrayEdge& edge) {
    return edge.edge_values().dense_data().values;
  }
};

class JaggedDenseArrayShapeHelper {
 public:
  using Shape = JaggedDenseArrayShape;
  using ShapePtr = Shape::ShapePtr;
  using Edge = Shape::Edge;

  static absl::string_view ReprName() { return "JaggedShape"; }

  static absl::StatusOr<DenseArrayEdge> EdgeFromSplitPoints(
      absl::Span<const OptionalValue<int64_t>> split_points) {
    return DenseArrayEdge::FromSplitPoints(
        CreateDenseArray<int64_t>(split_points));
  }

  static absl::StatusOr<DenseArrayEdge> EdgeFromMapping(
      absl::Span<const OptionalValue<int64_t>> mapping, int64_t parent_size) {
    return DenseArrayEdge::FromMapping(CreateDenseArray<int64_t>(mapping),
                                       parent_size);
  }

  static const Buffer<int64_t>& GetSplitPoints(const DenseArrayEdge& edge) {
    return edge.edge_values().values;
  }
};

template <typename JaggedShapeHelper>
class JaggedShapeTest : public ::testing::Test {
 public:
  using Helper = JaggedShapeHelper;
  using Shape = typename JaggedShapeHelper::Shape;
  using ShapePtr = typename JaggedShapeHelper::ShapePtr;
};

using JaggedShapeTestTypes =
    ::testing::Types<JaggedArrayShapeHelper, JaggedDenseArrayShapeHelper>;
TYPED_TEST_SUITE(JaggedShapeTest, JaggedShapeTestTypes);

TYPED_TEST(JaggedShapeTest, Empty) {
  auto shape = TestFixture::Shape::Empty();
  EXPECT_EQ(shape->rank(), 0);
  EXPECT_EQ(shape->size(), 1);
  EXPECT_TRUE(shape->edges().empty());
}

TYPED_TEST(JaggedShapeTest, FromEdges) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  {
    // Empty.
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({}));
    EXPECT_EQ(shape->rank(), 0);
    EXPECT_EQ(shape->size(), 1);
    EXPECT_TRUE(shape->edges().empty());
  }
  {
    // Single input.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({std::move(edge)}));
    EXPECT_EQ(shape->rank(), 1);
    EXPECT_EQ(shape->size(), 2);
    auto edges = shape->edges();
    EXPECT_EQ(edges.size(), 1);
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 2));
  }
  {
    // Multiple inputs.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape,
                         Shape::FromEdges({std::move(edge1), std::move(edge2),
                                           std::move(edge3)}));
    EXPECT_EQ(shape->rank(), 3);
    EXPECT_EQ(shape->size(), 4);
    auto edges = shape->edges();
    EXPECT_EQ(edges.size(), 3);
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 2));
    EXPECT_THAT(Helper::GetSplitPoints(edges[1]), ElementsAre(0, 1, 3));
    EXPECT_THAT(Helper::GetSplitPoints(edges[2]), ElementsAre(0, 1, 2, 4));
  }
  {
    // Mapping edge.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 8}));
    ASSERT_OK_AND_ASSIGN(auto edge2,
                         Helper::EdgeFromMapping({0, 0, 1, 1, 3, 5}, 8));
    ASSERT_OK_AND_ASSIGN(
        auto shape, Shape::FromEdges({std::move(edge1), std::move(edge2)}));
    EXPECT_EQ(shape->rank(), 2);
    EXPECT_EQ(shape->size(), 6);
    auto edges = shape->edges();
    EXPECT_EQ(edges.size(), 2);
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 8));
    EXPECT_THAT(Helper::GetSplitPoints(edges[1]),
                ElementsAre(0, 2, 4, 4, 5, 5, 6, 6, 6));
  }
}

TYPED_TEST(JaggedShapeTest, FromEdgesErrors) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  {
    // parent_size of single edge != 1.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromSplitPoints({0, 2, 3}));
    EXPECT_THAT(Shape::FromEdges({std::move(edge)}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "incompatible dimensions - edges[0].parent_size "
                         "!= 1 (prior edge's child_size)"));
  }
  {
    // edge2.child_size != edge3.parent_size.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 4}));
    EXPECT_THAT(Shape::FromEdges(
                    {std::move(edge1), std::move(edge2), std::move(edge3)}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "incompatible dimensions - edges[2].parent_size "
                         "!= 3 (prior edge's child_size)"));
  }
  {
    // Missing values in mapping edge.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 1}));
    ASSERT_OK_AND_ASSIGN(auto edge2,
                         Helper::EdgeFromMapping({0, std::nullopt}, 1));
    EXPECT_THAT(Shape::FromEdges({std::move(edge1), std::move(edge2)}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected a full mapping"));
  }
  {
    // Unsorted values in mapping edge.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromMapping({1, 0}, 2));
    EXPECT_THAT(Shape::FromEdges({std::move(edge1), std::move(edge2)}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected a sorted mapping"));
  }
}

TYPED_TEST(JaggedShapeTest, FromEdges_BufferFactory) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  {
    // Default (heap) -> owned.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromMapping({0, 0}, 1));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({std::move(edge)}));
    EXPECT_TRUE(Helper::GetSplitPoints(shape->edges()[0]).is_owner());
  }
  {
    // On arena -> not owned.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromMapping({0, 0}, 1));
    UnsafeArenaBufferFactory arena{128};
    ASSERT_OK_AND_ASSIGN(auto shape,
                         Shape::FromEdges({std::move(edge)}, arena));
    EXPECT_FALSE(Helper::GetSplitPoints(shape->edges()[0]).is_owner());
  }
}

TYPED_TEST(JaggedShapeTest, FlatFromSize) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  {
    auto shape = Shape::FlatFromSize(3);
    EXPECT_EQ(shape->rank(), 1);
    EXPECT_EQ(shape->size(), 3);
    auto edges = shape->edges();
    EXPECT_EQ(edges.size(), 1);
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 3));
  }
  {
    // Default (heap) -> owned.
    auto shape = Shape::FlatFromSize(3);
    EXPECT_TRUE(Helper::GetSplitPoints(shape->edges()[0]).is_owner());
  }
  {
    // On arena -> not owned.
    UnsafeArenaBufferFactory arena{128};
    auto shape = Shape::FlatFromSize(3, arena);
    EXPECT_FALSE(Helper::GetSplitPoints(shape->edges()[0]).is_owner());
  }
}

TYPED_TEST(JaggedShapeTest, AddDims) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  {
    // No new edges.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({std::move(edge)}));
    ASSERT_OK_AND_ASSIGN(shape, shape->AddDims({}));
    EXPECT_EQ(shape->rank(), 1);
    EXPECT_EQ(shape->size(), 2);
    EXPECT_THAT(Helper::GetSplitPoints(shape->edges()[0]), ElementsAre(0, 2));
  }
  {
    // New edges. Split points and mapping.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({std::move(edge)}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromMapping({0, 1, 2, 2}, 3));
    ASSERT_OK_AND_ASSIGN(shape, shape->AddDims({edge2, edge3}));
    EXPECT_EQ(shape->rank(), 3);
    EXPECT_EQ(shape->size(), 4);
    auto edges = shape->edges();
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 2));
    EXPECT_THAT(Helper::GetSplitPoints(edges[1]), ElementsAre(0, 1, 3));
    EXPECT_THAT(Helper::GetSplitPoints(edges[2]), ElementsAre(0, 1, 2, 4));
  }
  {
    // BufferFactory - heap.
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({}));
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromMapping({0, 0}, 1));
    ASSERT_OK_AND_ASSIGN(shape, shape->AddDims({edge}));
    EXPECT_TRUE(Helper::GetSplitPoints(shape->edges()[0]).is_owner());
  }
  {
    // BufferFactory - arena.
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({}));
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromMapping({0, 0}, 1));
    UnsafeArenaBufferFactory arena{128};
    ASSERT_OK_AND_ASSIGN(shape, shape->AddDims({edge}, arena));
    EXPECT_FALSE(Helper::GetSplitPoints(shape->edges()[0]).is_owner());
  }
  {
    // Exceptions.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({edge}));
    EXPECT_THAT(shape->AddDims({edge}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "incompatible dimensions - edges[1].parent_size "
                         "!= 2 (prior edge's child_size)"));
  }
}

TYPED_TEST(JaggedShapeTest, RemoveDims) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  {
    // Empty case.
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({}));
    shape = shape->RemoveDims(0);
    EXPECT_EQ(shape->rank(), 0);
  }
  {
    // No edges removed.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({edge}));
    shape = shape->RemoveDims(1);
    EXPECT_EQ(shape->rank(), 1);
    EXPECT_THAT(Helper::GetSplitPoints(shape->edges()[0]), ElementsAre(0, 2));
  }
  {
    // All edges removed.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({edge, edge2}));
    shape = shape->RemoveDims(0);
    EXPECT_EQ(shape->rank(), 0);
  }
  {
    // Edges removed from the middle.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({edge, edge2, edge3}));
    shape = shape->RemoveDims(1);
    EXPECT_EQ(shape->rank(), 1);
    EXPECT_THAT(Helper::GetSplitPoints(shape->edges()[0]), ElementsAre(0, 2));
  }
}

TYPED_TEST(JaggedShapeTest, FlattenDims_RankDecrease) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
  ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
  ASSERT_OK_AND_ASSIGN(auto edge4,
                       Helper::EdgeFromSplitPoints({0, 3, 4, 11, 12}));
  ASSERT_OK_AND_ASSIGN(auto shape,
                       Shape::FromEdges({edge1, edge2, edge3, edge4}));
  {
    // No dims flattened.
    auto new_shape = shape->FlattenDims(0, 1);
    EXPECT_EQ(new_shape->rank(), 4);
    auto edges = new_shape->edges();
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 2));
    EXPECT_THAT(Helper::GetSplitPoints(edges[1]), ElementsAre(0, 1, 3));
    EXPECT_THAT(Helper::GetSplitPoints(edges[2]), ElementsAre(0, 1, 2, 4));
    EXPECT_THAT(Helper::GetSplitPoints(edges[3]), ElementsAre(0, 3, 4, 11, 12));
  }
  {
    // All dims flattened.
    auto new_shape = shape->FlattenDims(0, 4);
    EXPECT_EQ(new_shape->rank(), 1);
    auto edges = new_shape->edges();
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 12));
  }
  {
    // Middle dims flattened.
    auto new_shape = shape->FlattenDims(1, 3);
    EXPECT_EQ(new_shape->rank(), 3);
    auto edges = new_shape->edges();
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 2));
    EXPECT_THAT(Helper::GetSplitPoints(edges[1]), ElementsAre(0, 1, 4));
    EXPECT_THAT(Helper::GetSplitPoints(edges[2]), ElementsAre(0, 3, 4, 11, 12));
  }
  {
    // Default (heap) -> owned.
    auto new_shape = shape->FlattenDims(0, 4);
    EXPECT_TRUE(Helper::GetSplitPoints(new_shape->edges()[0]).is_owner());
  }
  {
    // On arena -> not owned.
    UnsafeArenaBufferFactory arena{128};
    auto new_shape = shape->FlattenDims(0, 4, arena);
    EXPECT_FALSE(Helper::GetSplitPoints(new_shape->edges()[0]).is_owner());
  }
}

TYPED_TEST(JaggedShapeTest, FlattenDims_RankIncrease) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
  ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
  ASSERT_OK_AND_ASSIGN(auto edge4,
                       Helper::EdgeFromSplitPoints({0, 3, 4, 11, 12}));
  ASSERT_OK_AND_ASSIGN(auto shape,
                       Shape::FromEdges({edge1, edge2, edge3, edge4}));
  {
    // Edge inserted in the beginning.
    auto new_shape = shape->FlattenDims(0, 0);
    EXPECT_EQ(new_shape->rank(), 5);
    auto edges = new_shape->edges();
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 1));
    EXPECT_THAT(Helper::GetSplitPoints(edges[1]), ElementsAre(0, 2));
    EXPECT_THAT(Helper::GetSplitPoints(edges[2]), ElementsAre(0, 1, 3));
    EXPECT_THAT(Helper::GetSplitPoints(edges[3]), ElementsAre(0, 1, 2, 4));
    EXPECT_THAT(Helper::GetSplitPoints(edges[4]), ElementsAre(0, 3, 4, 11, 12));
  }
  {
    // Edge inserted in the middle.
    auto new_shape = shape->FlattenDims(2, 2);
    EXPECT_EQ(new_shape->rank(), 5);
    auto edges = new_shape->edges();
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 2));
    EXPECT_THAT(Helper::GetSplitPoints(edges[1]), ElementsAre(0, 1, 3));
    EXPECT_THAT(Helper::GetSplitPoints(edges[2]), ElementsAre(0, 1, 2, 3));
    EXPECT_THAT(Helper::GetSplitPoints(edges[3]), ElementsAre(0, 1, 2, 4));
    EXPECT_THAT(Helper::GetSplitPoints(edges[4]), ElementsAre(0, 3, 4, 11, 12));
  }
  {
    // Edge inserted at the end.
    auto new_shape = shape->FlattenDims(4, 4);
    EXPECT_EQ(new_shape->rank(), 5);
    auto edges = new_shape->edges();
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 2));
    EXPECT_THAT(Helper::GetSplitPoints(edges[1]), ElementsAre(0, 1, 3));
    EXPECT_THAT(Helper::GetSplitPoints(edges[2]), ElementsAre(0, 1, 2, 4));
    EXPECT_THAT(Helper::GetSplitPoints(edges[3]), ElementsAre(0, 3, 4, 11, 12));
    EXPECT_THAT(Helper::GetSplitPoints(edges[4]),
                ElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12));
  }
  {
    // Rank() == 0.
    auto empty_shape = Shape::Empty();
    auto new_shape = empty_shape->FlattenDims(0, 0);
    EXPECT_EQ(new_shape->rank(), 1);
    auto edges = new_shape->edges();
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 1));
  }
  {
    // Default (heap) -> owned.
    auto new_shape = shape->FlattenDims(0, 0);
    EXPECT_TRUE(Helper::GetSplitPoints(new_shape->edges()[0]).is_owner());
  }
  {
    // On arena -> not owned.
    UnsafeArenaBufferFactory arena{128};
    auto new_shape = shape->FlattenDims(0, 0, arena);
    EXPECT_FALSE(Helper::GetSplitPoints(new_shape->edges()[0]).is_owner());
  }
}

TYPED_TEST(JaggedShapeTest, NotMovableAndCopyableClass) {
  using Shape = typename TestFixture::Shape;
  static_assert(!std::is_copy_constructible_v<Shape> &&
              !std::is_copy_assignable_v<Shape>);
  static_assert(!std::is_move_constructible_v<Shape> &&
                !std::is_move_assignable_v<Shape>);
}

TYPED_TEST(JaggedShapeTest, MovableAndCopyablePtr) {
  using Shape = typename TestFixture::Shape;
  using ShapePtr = typename TestFixture::ShapePtr;
  using Helper = typename TestFixture::Helper;
  {
    // Non-empty copyable.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({std::move(edge)}));
    ShapePtr shape_cpy = shape;
    EXPECT_EQ(shape_cpy->rank(), 1);
    EXPECT_EQ(shape_cpy->size(), 2);
    auto edges = shape_cpy->edges();
    EXPECT_EQ(edges.size(), 1);
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 2));
  }
  {
    // Non-empty movable.
    ASSERT_OK_AND_ASSIGN(auto edge, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({std::move(edge)}));
    ShapePtr shape_move = std::move(shape);
    EXPECT_EQ(shape_move->rank(), 1);
    EXPECT_EQ(shape_move->size(), 2);
    auto edges = shape_move->edges();
    EXPECT_EQ(edges.size(), 1);
    EXPECT_THAT(Helper::GetSplitPoints(edges[0]), ElementsAre(0, 2));
  }
}

TYPED_TEST(JaggedShapeTest, Fingerprint) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;

  // Identical shape -> same fingerprint.
  ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 2}));
  ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2}));
  ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2}));
  EXPECT_EQ(FingerprintHasher("salt").Combine(*shape1).Finish(),
            FingerprintHasher("salt").Combine(*shape2).Finish());

  // Change edge -> different fingerprint.
  ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 2, 4}));
  ASSERT_OK_AND_ASSIGN(auto shape3, Shape::FromEdges({edge1, edge3}));
  EXPECT_NE(FingerprintHasher("salt").Combine(*shape1).Finish(),
            FingerprintHasher("salt").Combine(*shape3).Finish());

  // Add new edge -> different fingerprint.
  ASSERT_OK_AND_ASSIGN(auto shape4, Shape::FromEdges({edge1, edge2, edge3}));
  EXPECT_NE(FingerprintHasher("salt").Combine(*shape1).Finish(),
            FingerprintHasher("salt").Combine(*shape4).Finish());
}

TYPED_TEST(JaggedShapeTest, FastEquivalenceCheck) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  JaggedShapeFastEquivalenceResult kEqSizes(
      JaggedShapeFastEquivalenceResult::kSizesEq);
  JaggedShapeFastEquivalenceResult kNotEq(
      JaggedShapeFastEquivalenceResult::kNotEq);
  JaggedShapeFastEquivalenceResult kEq(
      JaggedShapeFastEquivalenceResult::kEq);
  {
    SCOPED_TRACE("Empty is fully equal.");
    auto shape = Shape::Empty();
    EXPECT_EQ(shape->FastEquivalenceCheck(*shape), kEq);
  }
  {
    SCOPED_TRACE("Rank 1 is fully equal.");
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape, Shape::FromEdges({edge1}));
    EXPECT_EQ(shape->FastEquivalenceCheck(*shape), kEq);

    // Test a different pointers.
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1}));
    EXPECT_EQ(shape->FastEquivalenceCheck(*shape2), kEq);
    EXPECT_EQ(shape2->FastEquivalenceCheck(*shape), kEq);

    // Test a different pointers for edges as well.
    ASSERT_OK_AND_ASSIGN(auto edge1_new, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape2_new, Shape::FromEdges({edge1_new}));
    EXPECT_EQ(shape->FastEquivalenceCheck(*shape2_new), kEq);
    EXPECT_EQ(shape2_new->FastEquivalenceCheck(*shape), kEq);
  }
  {
    SCOPED_TRACE("Equal shapes.");
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2, edge3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2, edge3}));
    EXPECT_EQ(shape1->FastEquivalenceCheck(*shape1), kEq)
        << "the same pointer must be exact equal";
    EXPECT_EQ(shape1->FastEquivalenceCheck(*shape2), kEqSizes);
    EXPECT_EQ(shape2->FastEquivalenceCheck(*shape1), kEqSizes);
  }
  {
    SCOPED_TRACE("Different shapes.");
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge2}));
    EXPECT_EQ(shape1->FastEquivalenceCheck(*shape2), kNotEq);
    EXPECT_EQ(shape2->FastEquivalenceCheck(*shape1), kNotEq);
  }
  {
    SCOPED_TRACE("Different ranks.");
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1}));
    EXPECT_EQ(shape1->FastEquivalenceCheck(*shape2), kNotEq);
    EXPECT_EQ(shape2->FastEquivalenceCheck(*shape1), kNotEq);
  }
  {
    SCOPED_TRACE("False negative.");
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge4, Helper::EdgeFromSplitPoints({0, 2, 3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge3, edge4}));
    EXPECT_EQ(shape1->FastEquivalenceCheck(*shape2), kEqSizes);
    EXPECT_EQ(shape2->FastEquivalenceCheck(*shape1), kEqSizes);
  }
}

TYPED_TEST(JaggedShapeTest, EqOp) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  {
    // Equal shapes -> True.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2, edge3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2, edge3}));
    EXPECT_TRUE(shape1->IsEquivalentTo(*shape2));
    EXPECT_TRUE(shape2->IsEquivalentTo(*shape1));
    EXPECT_TRUE(shape1->IsEquivalentTo(*shape1));
  }
  {
    // Different shapes -> False.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge2}));
    EXPECT_FALSE(shape1->IsEquivalentTo(*shape2));
    EXPECT_FALSE(shape2->IsEquivalentTo(*shape1));
  }
  {
    // Different ranks -> False.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1}));
    EXPECT_FALSE(shape1->IsEquivalentTo(*shape2));
    EXPECT_FALSE(shape2->IsEquivalentTo(*shape1));
  }
  {
    // Different row splits but same sizes -> False.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge4, Helper::EdgeFromSplitPoints({0, 2, 3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge3, edge4}));
    EXPECT_FALSE(shape1->IsEquivalentTo(*shape2));
    EXPECT_FALSE(shape2->IsEquivalentTo(*shape1));
  }
}

TYPED_TEST(JaggedShapeTest, IsBroadcastableTo) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  {
    // Equal shapes -> True.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2, edge3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2, edge3}));
    EXPECT_TRUE(shape1->IsBroadcastableTo(*shape2));
    EXPECT_TRUE(shape2->IsBroadcastableTo(*shape1));
    EXPECT_TRUE(shape1->IsBroadcastableTo(*shape1));
  }
  {
    // Different shapes -> False.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge2}));
    EXPECT_FALSE(shape1->IsBroadcastableTo(*shape2));
    EXPECT_FALSE(shape2->IsBroadcastableTo(*shape1));
  }
  {
    // Equal prefix -> True & False.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 2, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 2, 4, 6}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2, edge3}));
    EXPECT_TRUE(shape1->IsBroadcastableTo(*shape2));
    EXPECT_FALSE(shape2->IsBroadcastableTo(*shape1));
  }
  {
    // Different prefix -> False & False.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2_1, Helper::EdgeFromSplitPoints({0, 2, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge2_2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 2, 4, 6}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2_1}));
    ASSERT_OK_AND_ASSIGN(auto shape2,
                         Shape::FromEdges({edge1, edge2_2, edge3}));
    EXPECT_FALSE(shape1->IsBroadcastableTo(*shape2));
    EXPECT_FALSE(shape2->IsBroadcastableTo(*shape1));
  }
}

TYPED_TEST(JaggedShapeTest, GetBroadcastEdge) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  {
    // Different shapes -> Valid Edge returned.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2, edge3}));
    auto edge = shape1->GetBroadcastEdge(*shape2);
    EXPECT_TRUE(edge.IsEquivalentTo(*Helper::EdgeFromSplitPoints({0, 1, 4})));
  }
  {
    // Equal shapes -> unit edge.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2, edge3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2, edge3}));
    auto edge = shape1->GetBroadcastEdge(*shape2);
    EXPECT_TRUE(
        edge.IsEquivalentTo(*Helper::EdgeFromSplitPoints({0, 1, 2, 3, 4})));
  }
  {
    // Different shapes - Default (heap) -> owned.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2, edge3}));
    auto edge = shape1->GetBroadcastEdge(*shape2);
    EXPECT_TRUE(Helper::GetSplitPoints(edge).is_owner());
  }
  {
    // Different shapes - On arena -> not owned.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2, edge3}));
    UnsafeArenaBufferFactory arena{128};
    auto edge = shape1->GetBroadcastEdge(*shape2, arena);
    EXPECT_FALSE(Helper::GetSplitPoints(edge).is_owner());
  }
  {
    // Same shape - Default (heap) -> owned.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2, edge3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2, edge3}));
    auto edge = shape1->GetBroadcastEdge(*shape2);
    EXPECT_TRUE(Helper::GetSplitPoints(edge).is_owner());
  }
  {
    // Same shape - On arena -> not owned.
    ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
    ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 1, 3}));
    ASSERT_OK_AND_ASSIGN(auto edge3, Helper::EdgeFromSplitPoints({0, 1, 2, 4}));
    ASSERT_OK_AND_ASSIGN(auto shape1, Shape::FromEdges({edge1, edge2, edge3}));
    ASSERT_OK_AND_ASSIGN(auto shape2, Shape::FromEdges({edge1, edge2, edge3}));
    UnsafeArenaBufferFactory arena{128};
    auto edge = shape1->GetBroadcastEdge(*shape2, arena);
    EXPECT_FALSE(Helper::GetSplitPoints(edge).is_owner());
  }
}

TYPED_TEST(JaggedShapeTest, EdgeT) {
  using Edge = typename TestFixture::Shape::Edge;
  using TestEdge = typename TestFixture::Helper::Edge;
  static_assert(std::is_same_v<Edge, TestEdge>);
}

TYPED_TEST(JaggedShapeTest, PtrT) {
  using ShapePtr = typename TestFixture::Shape::ShapePtr;
  using TestShapePtr = typename TestFixture::Helper::ShapePtr;
  static_assert(std::is_same_v<ShapePtr, TestShapePtr>);
}

TYPED_TEST(JaggedShapeTest, Repr) {
  using Shape = typename TestFixture::Shape;
  using Helper = typename TestFixture::Helper;
  ASSERT_OK_AND_ASSIGN(auto edge1, Helper::EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(auto edge2, Helper::EdgeFromSplitPoints({0, 2, 7}));
  ASSERT_OK_AND_ASSIGN(auto edge3,
                       Helper::EdgeFromSplitPoints({0, 1, 2, 3, 4, 5, 6, 7}));
  ASSERT_OK_AND_ASSIGN(auto edge4,
                       Helper::EdgeFromSplitPoints({0, 2, 3, 4, 5, 6, 7, 8}));
  ASSERT_OK_AND_ASSIGN(auto shape,
                       Shape::FromEdges({edge1, edge2, edge3, edge4}));
  std::string expected_repr = absl::StrCat(
      Helper::ReprName(), "(2, [2, 5], 1, [2, 1, 1, ..., 1, 1, 1])");
  EXPECT_THAT(GenReprToken(*shape), ReprTokenEq(expected_repr));
}

/****************** Benchmarks ******************/

template <typename ShapeHelper>
typename ShapeHelper::ShapePtr ShapeFromEdgesBM(
    const typename ShapeHelper::Shape::EdgeVec& edges,
    benchmark::State& state) {
  state.PauseTiming();
  auto edges_cpy = edges;
  state.ResumeTiming();
  return ShapeHelper::Shape::FromEdges(std::move(edges_cpy)).value();
}

template <typename ShapeHelper>
typename ShapeHelper::Edge GetSplitPointsEdge(int64_t parent_size,
                                              int64_t children) {
  std::vector<OptionalValue<int64_t>> split_points;
  split_points.reserve(parent_size + 1);
  for (int64_t i = 0; i <= parent_size; ++i) {
    split_points.push_back(i * children);
  }
  return ShapeHelper::EdgeFromSplitPoints(std::move(split_points)).value();
}

template <typename ShapeHelper>
typename ShapeHelper::Edge GetMappingEdge(int64_t parent_size,
                                          int64_t children) {
  std::vector<OptionalValue<int64_t>> mapping;
  mapping.reserve(parent_size * children);
  for (int64_t i = 0; i < parent_size; ++i) {
    for (int64_t j = 0; j < children; ++j) {
      mapping.push_back(i);
    }
  }
  return ShapeHelper::EdgeFromMapping(std::move(mapping), parent_size).value();
}

template <typename ShapeHelper>
typename ShapeHelper::ShapePtr GetShape(int64_t rank, int64_t num_children) {
  typename ShapeHelper::Shape::EdgeVec edges;
  edges.reserve(rank);
  for (int i = 0; i < rank; ++i) {
    edges.push_back(GetSplitPointsEdge<ShapeHelper>(std::pow(num_children, i),
                                                    num_children));
  }
  return ShapeHelper::Shape::FromEdges(std::move(edges)).value();
}

template <typename ShapeHelper>
void BM_JaggedShape_EmptyCreation(benchmark::State& state) {
  for (auto _ : state) {
    auto shape = ShapeHelper::Shape::Empty();
    benchmark::DoNotOptimize(shape);
  }
}

BENCHMARK(BM_JaggedShape_EmptyCreation<JaggedArrayShapeHelper>);
BENCHMARK(BM_JaggedShape_EmptyCreation<JaggedDenseArrayShapeHelper>);

template <typename ShapeHelper>
void BM_JaggedShape_FromSplitPointEdges(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  typename ShapeHelper::Shape::EdgeVec edges;
  edges.reserve(rank);
  for (int i = 0; i < rank; ++i) {
    edges.push_back(GetSplitPointsEdge<ShapeHelper>(std::pow(num_children, i),
                                                    num_children));
  }
  for (auto _ : state) {
    benchmark::DoNotOptimize(edges);
    auto shape = ShapeFromEdgesBM<ShapeHelper>(edges, state);
    benchmark::DoNotOptimize(shape);
  }
}

BENCHMARK(BM_JaggedShape_FromSplitPointEdges<JaggedArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);
BENCHMARK(BM_JaggedShape_FromSplitPointEdges<JaggedDenseArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);

template <typename ShapeHelper>
void BM_JaggedShape_FromMappingEdges(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  typename ShapeHelper::Shape::EdgeVec edges;
  edges.reserve(rank);
  for (int i = 0; i < rank; ++i) {
    edges.push_back(
        GetMappingEdge<ShapeHelper>(std::pow(num_children, i), num_children));
  }
  for (auto _ : state) {
    benchmark::DoNotOptimize(edges);
    auto shape = ShapeFromEdgesBM<ShapeHelper>(edges, state);
    benchmark::DoNotOptimize(shape);
  }
}

BENCHMARK(BM_JaggedShape_FromMappingEdges<JaggedArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);
BENCHMARK(BM_JaggedShape_FromMappingEdges<JaggedDenseArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);

template <typename ShapeHelper>
void BM_JaggedShape_FastEquivalenceCheck(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  // Avoid creating the edges from the same underlying buffers which is
  // optimized.
  auto shape1 = GetShape<ShapeHelper>(rank, num_children);
  auto shape2 = GetShape<ShapeHelper>(rank, num_children);
  for (auto _ : state) {
    benchmark::DoNotOptimize(shape1);
    benchmark::DoNotOptimize(shape2);
    auto eq = shape1->FastEquivalenceCheck(*shape2);
    benchmark::DoNotOptimize(eq);
  }
}

BENCHMARK(BM_JaggedShape_FastEquivalenceCheck<JaggedArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);
BENCHMARK(BM_JaggedShape_FastEquivalenceCheck<JaggedDenseArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);

template <typename ShapeHelper>
void BM_JaggedShape_IsEquivalentTo(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  // Avoid creating the edges from the same underlying buffers which is
  // optimized.
  auto shape1 = GetShape<ShapeHelper>(rank, num_children);
  auto shape2 = GetShape<ShapeHelper>(rank, num_children);
  for (auto _ : state) {
    benchmark::DoNotOptimize(shape1);
    benchmark::DoNotOptimize(shape2);
    auto eq = shape1->IsEquivalentTo(*shape2);
    benchmark::DoNotOptimize(eq);
  }
}

BENCHMARK(BM_JaggedShape_IsEquivalentTo<JaggedArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);
BENCHMARK(BM_JaggedShape_IsEquivalentTo<JaggedDenseArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);

template <typename ShapeHelper>
void BM_JaggedShape_IsEquivalentTo_SameObj(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  auto shape1 = GetShape<ShapeHelper>(rank, num_children);
  auto shape2 = shape1;
  for (auto _ : state) {
    benchmark::DoNotOptimize(shape1);
    benchmark::DoNotOptimize(shape2);
    auto eq = shape1->IsEquivalentTo(*shape2);
    benchmark::DoNotOptimize(eq);
  }
}

BENCHMARK(BM_JaggedShape_IsEquivalentTo_SameObj<JaggedArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(4, 100);
BENCHMARK(BM_JaggedShape_IsEquivalentTo_SameObj<JaggedDenseArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(4, 100);

template <typename ShapeHelper>
void BM_JaggedShape_FlattenDims(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  auto shape = GetShape<ShapeHelper>(rank, num_children);
  int from = state.range(2);
  int to = state.range(3);
  for (auto _ : state) {
    benchmark::DoNotOptimize(shape);
    benchmark::DoNotOptimize(from);
    benchmark::DoNotOptimize(to);
    auto flattened_shape = shape->FlattenDims(from, to);
    benchmark::DoNotOptimize(flattened_shape);
  }
}

BENCHMARK(BM_JaggedShape_FlattenDims<JaggedArrayShapeHelper>)
    // Rank, num children, from, to.
    // Flatten the entire shape.
    ->Args({6, 10, 0, 6})
    // Normal composition.
    ->Args({6, 10, 0, 2})
    ->Args({6, 10, 2, 4})
    ->Args({6, 10, 4, 6})
    // Return copy.
    ->Args({6, 10, 5, 6})
    // Insert dimension.
    ->Args({6, 10, 0, 0})
    ->Args({6, 10, 2, 2})
    ->Args({6, 10, 4, 4})
    ->Args({6, 10, 6, 6});
BENCHMARK(BM_JaggedShape_FlattenDims<JaggedDenseArrayShapeHelper>)
    // Rank, num children, from, to.
    // Flatten the entire shape.
    ->Args({6, 10, 0, 6})
    // Normal composition.
    ->Args({6, 10, 0, 2})
    ->Args({6, 10, 2, 4})
    ->Args({6, 10, 4, 6})
    // Return copy.
    ->Args({6, 10, 5, 6})
    // Insert dimension.
    ->Args({6, 10, 0, 0})
    ->Args({6, 10, 2, 2})
    ->Args({6, 10, 4, 4})
    ->Args({6, 10, 6, 6});

template <typename ShapeHelper>
void BM_JaggedShape_IsBroadcastableTo(benchmark::State& state) {
  const int rank_1 = state.range(0);
  const int rank_2 = state.range(1);
  const int num_children = state.range(2);
  // Avoid creating the edges from the same underlying buffers which is
  // optimized.
  auto shape1 = GetShape<ShapeHelper>(rank_1, num_children);
  auto shape2 = GetShape<ShapeHelper>(rank_2, num_children);
  for (auto _ : state) {
    benchmark::DoNotOptimize(shape1);
    benchmark::DoNotOptimize(shape2);
    auto is_broadcastable = shape1->IsBroadcastableTo(*shape2);
    benchmark::DoNotOptimize(is_broadcastable);
  }
}

BENCHMARK(BM_JaggedShape_IsBroadcastableTo<JaggedArrayShapeHelper>)
    // Rank shape_1, rank shape_2, num children.
    ->Args({1, 1, 1})
    ->Args({1, 5, 5})
    ->Args({4, 5, 5});
BENCHMARK(BM_JaggedShape_IsBroadcastableTo<JaggedDenseArrayShapeHelper>)
    // Rank shape_1, rank shape_2, num children.
    ->Args({1, 1, 1})
    ->Args({1, 5, 5})
    ->Args({4, 5, 5});

template <typename ShapeHelper>
void BM_JaggedShape_IsBroadcastableTo_SameObj(benchmark::State& state) {
  const int rank_1 = state.range(0);
  const int num_children = state.range(1);
  auto shape1 = GetShape<ShapeHelper>(rank_1, num_children);
  auto shape2 = shape1;
  for (auto _ : state) {
    benchmark::DoNotOptimize(shape1);
    benchmark::DoNotOptimize(shape2);
    auto is_broadcastable = shape1->IsBroadcastableTo(*shape2);
    benchmark::DoNotOptimize(is_broadcastable);
  }
}

BENCHMARK(BM_JaggedShape_IsBroadcastableTo_SameObj<JaggedArrayShapeHelper>)
    // Rank shape_1, num children.
    ->Args({1, 1})
    ->Args({1, 5})
    ->Args({4, 5});
BENCHMARK(BM_JaggedShape_IsBroadcastableTo_SameObj<JaggedDenseArrayShapeHelper>)
    // Rank shape_1, num children.
    ->Args({1, 1})
    ->Args({1, 5})
    ->Args({4, 5});

template <typename ShapeHelper>
void BM_JaggedShape_Copying(benchmark::State& state) {
  // Tests that the result of `FromEdges` is fast to copy.
  const int rank = state.range(0);
  const int num_children = state.range(1);
  auto shape = GetShape<ShapeHelper>(rank, num_children);
  for (auto _ : state) {
    benchmark::DoNotOptimize(shape);
    auto shape_copy = shape;
    benchmark::DoNotOptimize(shape_copy);
  }
}

BENCHMARK(BM_JaggedShape_Copying<JaggedArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);
BENCHMARK(BM_JaggedShape_Copying<JaggedDenseArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);

template <typename ShapeHelper>
void BM_JaggedShape_Repr(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  auto shape = GetShape<ShapeHelper>(rank, num_children);
  for (auto _ : state) {
    benchmark::DoNotOptimize(shape);
    auto repr = Repr(*shape);
    benchmark::DoNotOptimize(repr);
  }
}

BENCHMARK(BM_JaggedShape_Repr<JaggedArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);
BENCHMARK(BM_JaggedShape_Repr<JaggedDenseArrayShapeHelper>)
    // Rank, num children.
    ->ArgPair(1, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1, 100)
    ->ArgPair(4, 100);

}  // namespace
}  // namespace arolla
