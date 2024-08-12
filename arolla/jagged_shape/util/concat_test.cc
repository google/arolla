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
#include "arolla/jagged_shape/util/concat.h"

#include <cmath>
#include <cstdint>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/array/jagged_shape.h"
#include "arolla/jagged_shape/array/util/concat.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/jagged_shape/dense_array/util/concat.h"
#include "arolla/jagged_shape/jagged_shape.h"
#include "arolla/jagged_shape/testing/matchers.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"

using ::absl_testing::StatusIs;
using ::arolla::testing::IsEquivalentTo;
using ::testing::ElementsAre;

namespace arolla {
namespace {

class JaggedArrayShapeHelper {
 public:
  using Shape = JaggedArrayShape;
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

  template <typename T>
  static Array<T> MakeArray(absl::Span<const OptionalValue<T>> data) {
    return CreateArray<T>(data);
  }

  template <typename T>
  static Array<T> MakeArray(int64_t size, const T& value) {
    return Array<T>(CreateConstDenseArray<T>(size, value));
  }
};

class JaggedDenseArrayShapeHelper {
 public:
  using Shape = JaggedDenseArrayShape;
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

  template <typename T>
  static DenseArray<T> MakeArray(absl::Span<const OptionalValue<T>> data) {
    return CreateDenseArray<T>(data);
  }

  template <typename T>
  static DenseArray<T> MakeArray(int64_t size, const T& value) {
    return CreateConstDenseArray<T>(size, value);
  }
};

template <typename JaggedShapeHelper>
class ConcatTest : public ::testing::Test {
 public:
  using Helper = JaggedShapeHelper;
  using Shape = typename JaggedShapeHelper::Shape;

  Shape MakeShape(absl::Span<const absl::Span<const int64_t>> shape) {
    std::vector<typename Helper::Edge> edges;
    edges.reserve(shape.size());
    for (const auto& edge_sizes : shape) {
      std::vector<OptionalValue<int64_t>> split_points;
      split_points.reserve(edge_sizes.size() + 1);
      split_points.push_back(0);
      for (int64_t edge_size : edge_sizes) {
        split_points.push_back(split_points.back().value + edge_size);
      }
      auto edge = Helper::EdgeFromSplitPoints(split_points).value();
      edges.push_back(std::move(edge));
    }
    return Shape::FromEdges(std::move(edges)).value();
  }

  template <typename T>
  auto MakeArray(absl::Span<const T> values) {
    std::vector<OptionalValue<T>> array_values;
    array_values.reserve(values.size());
    for (const T& value : values) {
      array_values.push_back(OptionalValue<T>(value));
    }
    return Helper::MakeArray(absl::MakeConstSpan(array_values));
  }
};

using ConcatTestTypes =
    ::testing::Types<JaggedArrayShapeHelper, JaggedDenseArrayShapeHelper>;
TYPED_TEST_SUITE(ConcatTest, ConcatTestTypes);

TYPED_TEST(ConcatTest, StackOrConcatJaggedShapesAlongDimension) {
  {
    ASSERT_OK_AND_ASSIGN(
        typename TestFixture::Shape result_shape,
        StackJaggedShapesAlongDimension<typename TestFixture::Shape>(
            {
                TestFixture::Shape::Empty(),
            },
            0));
    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape({{1}})));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        typename TestFixture::Shape result_shape,
        StackJaggedShapesAlongDimension<typename TestFixture::Shape>(
            {
                TestFixture::Shape::Empty(),
                TestFixture::Shape::Empty(),
                TestFixture::Shape::Empty(),
            },
            0));
    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape({{3}})));
  }
  // Examples from function comment.
  {
    ASSERT_OK_AND_ASSIGN(
        typename TestFixture::Shape result_shape,
        StackJaggedShapesAlongDimension<typename TestFixture::Shape>(
            {
                TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                TestFixture::MakeShape({{3}, {1, 3, 1}, {6, 7, 8, 9, 10}}),
            },
            0));
    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape({
                                  {2},
                                  {2, 3},
                                  {1, 2, 1, 3, 1},
                                  {3, 4, 5, 6, 7, 8, 9, 10},
                              })));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        typename TestFixture::Shape result_shape,
        ConcatJaggedShapesAlongDimension<typename TestFixture::Shape>(
            {
                TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                TestFixture::MakeShape({{3}, {1, 3, 1}, {6, 7, 8, 9, 10}}),
            },
            0));
    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape({
                                  {5},
                                  {1, 2, 1, 3, 1},
                                  {3, 4, 5, 6, 7, 8, 9, 10},
                              })));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        typename TestFixture::Shape result_shape,
        StackJaggedShapesAlongDimension<typename TestFixture::Shape>(
            {
                TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                TestFixture::MakeShape({{2}, {1, 3}, {6, 7, 8, 9}}),
            },
            1));
    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape({
                                  {2},
                                  {2, 2},
                                  {1, 1, 2, 3},
                                  {3, 6, 4, 5, 7, 8, 9},
                              })));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        typename TestFixture::Shape result_shape,
        ConcatJaggedShapesAlongDimension<typename TestFixture::Shape>(
            {
                TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                TestFixture::MakeShape({{2}, {1, 3}, {6, 7, 8, 9}}),
            },
            1));
    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape({
                                  {2},
                                  {2, 5},
                                  {3, 6, 4, 5, 7, 8, 9},
                              })));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        typename TestFixture::Shape result_shape,
        StackJaggedShapesAlongDimension<typename TestFixture::Shape>(
            {
                TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                TestFixture::MakeShape({{2}, {1, 2}, {6, 7, 8}}),
            },
            2));
    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape({
                                  {2},
                                  {1, 2},
                                  {2, 2, 2},
                                  {3, 6, 4, 7, 5, 8},
                              })));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        typename TestFixture::Shape result_shape,
        ConcatJaggedShapesAlongDimension<typename TestFixture::Shape>(
            {
                TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                TestFixture::MakeShape({{2}, {1, 2}, {6, 7, 8}}),
            },
            2));
    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape({
                                  {2},
                                  {1, 2},
                                  {9, 11, 13},
                              })));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        typename TestFixture::Shape result_shape,
        ConcatJaggedShapesAlongDimension<typename TestFixture::Shape>(
            {
                TestFixture::MakeShape({{2}, {1, 2}, {1, 1, 2}, {1, 1, 1, 2}}),
                TestFixture::MakeShape({{2},
                                        {3, 1},
                                        {2, 3, 1, 4},
                                        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}),
            },
            1));
    EXPECT_THAT(result_shape,
                IsEquivalentTo(TestFixture::MakeShape(
                    {{2},
                     {4, 3},
                     {1, 2, 3, 1, 1, 2, 4},
                     {1, 1, 2, 3, 4, 5, 6, 1, 1, 2, 7, 8, 9, 10}})));
  }
  // Error cases and messages.
  {
    EXPECT_THAT(StackJaggedShapesAlongDimension<typename TestFixture::Shape>(
                    absl::Span<const typename TestFixture::Shape>{}, 0),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "concat/stack requires a nonzero number of inputs"));
  }
  {
    EXPECT_THAT(ConcatJaggedShapesAlongDimension<typename TestFixture::Shape>(
                    {
                        TestFixture::Shape::Empty(),
                    },
                    0),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "cannot concat shapes of rank zero"));
  }
  {
    EXPECT_THAT(StackJaggedShapesAlongDimension<typename TestFixture::Shape>(
                    {
                        TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                    },
                    -1),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "invalid dim = -1 for concat/stack"));
  }
  {
    EXPECT_THAT(ConcatJaggedShapesAlongDimension<typename TestFixture::Shape>(
                    {
                        TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                    },
                    3),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "invalid dim = 3 for concat/stack"));
  }
  {
    EXPECT_THAT(StackJaggedShapesAlongDimension<typename TestFixture::Shape>(
                    {
                        TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                    },
                    4),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "invalid dim = 4 for concat/stack"));
  }
  {
    EXPECT_THAT(StackJaggedShapesAlongDimension<typename TestFixture::Shape>(
                    {
                        TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                        TestFixture::MakeShape({{2}, {1, 2}}),
                    },
                    0),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "concat/stack requires all inputs to have the same "
                         "rank, got 3 and 2"));
  }
  {
    EXPECT_THAT(
        StackJaggedShapesAlongDimension<typename TestFixture::Shape>(
            {
                TestFixture::MakeShape({{2}, {1, 2}, {3, 4, 5}}),
                TestFixture::MakeShape({{2}, {2, 1}, {3, 4, 5}}),
            },
            2),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            "concat/stack requires all inputs to have the same shape prefix "
            "before the concatenation dimension"));
  }
}

TYPED_TEST(ConcatTest, StackOrConcatJaggedArraysAlongDimension) {
  {
    const auto array1 =
        TestFixture::MakeArray(absl::Span<const int>({1, 2, 3}));
    const auto shape1 = TestFixture::MakeShape({{3}});

    ASSERT_OK_AND_ASSIGN(
        (const auto& [result_array, result_shape]),
        StackJaggedArraysAlongDimension(absl::MakeConstSpan({array1}),
                                        absl::MakeConstSpan({shape1}), 0));

    EXPECT_THAT(result_shape,
                IsEquivalentTo(TestFixture::MakeShape({{1}, {3}})));
    EXPECT_THAT(result_array, ElementsAre(1, 2, 3));
  }
  {
    const auto array1 =
        TestFixture::MakeArray(absl::Span<const int>({1, 2, 3}));
    const auto shape1 = TestFixture::MakeShape({{3}});

    ASSERT_OK_AND_ASSIGN(
        (const auto& [result_array, result_shape]),
        ConcatJaggedArraysAlongDimension(absl::MakeConstSpan({array1}),
                                         absl::MakeConstSpan({shape1}), 0));

    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape({{3}})));
    EXPECT_THAT(result_array, ElementsAre(1, 2, 3));
  }
  {
    const auto array1 =
        TestFixture::MakeArray(absl::Span<const int>({1, 2, 3}));
    const auto shape1 = TestFixture::MakeShape({{3}});

    const auto array2 = TestFixture::MakeArray(absl::Span<const int>({4, 5}));
    const auto shape2 = TestFixture::MakeShape({{2}});

    ASSERT_OK_AND_ASSIGN((const auto& [result_array, result_shape]),
                         StackJaggedArraysAlongDimension(
                             absl::MakeConstSpan({array1, array2}),
                             absl::MakeConstSpan({shape1, shape2}), 0));

    EXPECT_THAT(result_shape,
                IsEquivalentTo(TestFixture::MakeShape({{2}, {3, 2}})));
    EXPECT_THAT(result_array, ElementsAre(1, 2, 3, 4, 5));
  }
  {
    const auto array1 =
        TestFixture::MakeArray(absl::Span<const int>({1, 2, 3}));
    const auto shape1 = TestFixture::MakeShape({{{2}, {1, 2}}});

    const auto array2 =
        TestFixture::MakeArray(absl::Span<const int>({4, 5, 6}));
    const auto shape2 = TestFixture::MakeShape({{{2}, {2, 1}}});

    ASSERT_OK_AND_ASSIGN((const auto& [result_array, result_shape]),
                         StackJaggedArraysAlongDimension(
                             absl::MakeConstSpan({array1, array2}),
                             absl::MakeConstSpan({shape1, shape2}), 0));

    EXPECT_THAT(
        result_shape,
        IsEquivalentTo(TestFixture::MakeShape({{2}, {2, 2}, {1, 2, 2, 1}})));
    EXPECT_THAT(result_array, ElementsAre(1, 2, 3, 4, 5, 6));
  }
  {
    // array1 = [[1], [2, 3]]
    const auto array1 =
        TestFixture::MakeArray(absl::Span<const int>({1, 2, 3}));
    const auto shape1 = TestFixture::MakeShape({{{2}, {1, 2}}});

    // array2 = [[4], [5, 6]]
    const auto array2 =
        TestFixture::MakeArray(absl::Span<const int>({4, 5, 6}));
    const auto shape2 = TestFixture::MakeShape({{{2}, {1, 2}}});

    ASSERT_OK_AND_ASSIGN((const auto& [result_array, result_shape]),
                         StackJaggedArraysAlongDimension(
                             absl::MakeConstSpan({array1, array2}),
                             absl::MakeConstSpan({shape1, shape2}), 1));

    // result_array = [[[1], [4]], [[2, 3], [5, 6]]]
    EXPECT_THAT(
        result_shape,
        IsEquivalentTo(TestFixture::MakeShape({{2}, {2, 2}, {1, 1, 2, 2}})));
    EXPECT_THAT(result_array, ElementsAre(1, 4, 2, 3, 5, 6));
  }
  {
    // array1 = [[[1, 2], [3]], [[4]]]
    // array2 = [[[5, 6], [7]], [[8]]]
    const auto array1 =
        TestFixture::MakeArray(absl::Span<const int>({1, 2, 3, 4}));
    const auto shape1 = TestFixture::MakeShape({{{2}, {2, 1}, {2, 1, 1}}});
    const auto array2 =
        TestFixture::MakeArray(absl::Span<const int>({5, 6, 7, 8}));
    const auto shape2 = TestFixture::MakeShape({{{2}, {2, 1}, {2, 1, 1}}});

    ASSERT_OK_AND_ASSIGN((const auto& [result_array, result_shape]),
                         ConcatJaggedArraysAlongDimension(
                             absl::MakeConstSpan({array1, array2}),
                             absl::MakeConstSpan({shape1, shape2}), 0));

    // result_array = [[[1, 2], [3]], [[4]], [[5, 6], [7]], [[8]]]
    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape(
                                  {{4}, {2, 1, 2, 1}, {2, 1, 1, 2, 1, 1}})));
    EXPECT_THAT(result_array, ElementsAre(1, 2, 3, 4, 5, 6, 7, 8));
  }
  {
    // array1 = [[[1, 2], [3]], [[4]]]
    // array2 = [[[5, 6], [7]], [[8]]]
    const auto array1 =
        TestFixture::MakeArray(absl::Span<const int>({1, 2, 3, 4}));
    const auto shape1 = TestFixture::MakeShape({{{2}, {2, 1}, {2, 1, 1}}});
    const auto array2 =
        TestFixture::MakeArray(absl::Span<const int>({5, 6, 7, 8}));
    const auto shape2 = TestFixture::MakeShape({{{2}, {2, 1}, {2, 1, 1}}});

    ASSERT_OK_AND_ASSIGN((const auto& [result_array, result_shape]),
                         ConcatJaggedArraysAlongDimension(
                             absl::MakeConstSpan({array1, array2}),
                             absl::MakeConstSpan({shape1, shape2}), 1));

    // result_array = [[[1, 2], [3], [5, 6], [7]], [[4], [8]]]
    EXPECT_THAT(result_shape, IsEquivalentTo(TestFixture::MakeShape(
                                  {{2}, {4, 2}, {2, 1, 2, 1, 1, 1}})));
    EXPECT_THAT(result_array, ElementsAre(1, 2, 3, 5, 6, 7, 4, 8));
  }
  {
    // array1 = [[[1, 2], [3]], [[4]]]
    // array2 = [[[5, 6], [7]], [[8]]]
    const auto array1 =
        TestFixture::MakeArray(absl::Span<const int>({1, 2, 3, 4}));
    const auto shape1 = TestFixture::MakeShape({{{2}, {2, 1}, {2, 1, 1}}});
    const auto array2 =
        TestFixture::MakeArray(absl::Span<const int>({5, 6, 7, 8}));
    const auto shape2 = TestFixture::MakeShape({{{2}, {2, 1}, {2, 1, 1}}});

    ASSERT_OK_AND_ASSIGN((const auto& [result_array, result_shape]),
                         ConcatJaggedArraysAlongDimension(
                             absl::MakeConstSpan({array1, array2}),
                             absl::MakeConstSpan({shape1, shape2}), 2));

    // result_array = [[[1, 2, 5, 6], [3, 7]], [[4, 8]]]
    EXPECT_THAT(
        result_shape,
        IsEquivalentTo(TestFixture::MakeShape({{2}, {2, 1}, {4, 2, 2}})));
    EXPECT_THAT(result_array, ElementsAre(1, 2, 5, 6, 3, 7, 4, 8));
  }
  // Error cases / messages.
  {
    const auto array1 =
        TestFixture::MakeArray(absl::Span<const int>({1, 2, 3}));
    const auto shape1 = TestFixture::MakeShape({{3}});

    const auto array2 = TestFixture::MakeArray(absl::Span<const int>({4, 5}));
    const auto shape2 = TestFixture::MakeShape({{2}});

    EXPECT_THAT(StackJaggedArraysAlongDimension(
                    absl::MakeConstSpan({array1}),
                    absl::MakeConstSpan({shape1, shape2}), 0),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "concat/stack expects `arrays` and `array_shapes` to "
                         "be 1:1, got sizes 1 and 2"));
  }
  {
    const auto array1 =
        TestFixture::MakeArray(absl::Span<const int>({1, 2, 3}));
    const auto shape1 = TestFixture::MakeShape({{3}});

    const auto array2 = TestFixture::MakeArray(absl::Span<const int>({4, 5}));
    const auto shape2 = TestFixture::MakeShape({{2}});

    EXPECT_THAT(
        StackJaggedArraysAlongDimension(absl::MakeConstSpan({array2, array2}),
                                        absl::MakeConstSpan({shape1, shape2}),
                                        0),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "concat/stack expects `arrays` and `array_shapes` to describe "
                 "arrays with the same size, but got 2 != 3 for index 0"));
  }
}

// Benchmarks

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

// Returns a shape with uniform fan-out and total size num_children ** rank.
template <typename ShapeHelper>
typename ShapeHelper::Shape GetShape(int64_t rank, int64_t num_children) {
  typename ShapeHelper::Shape::EdgeVec edges;
  edges.reserve(rank);
  for (int i = 0; i < rank; ++i) {
    edges.push_back(GetSplitPointsEdge<ShapeHelper>(std::pow(num_children, i),
                                                    num_children));
  }
  return ShapeHelper::Shape::FromEdges(std::move(edges)).value();
}

template <typename ShapeHelper>
void BM_ConcatJaggedShapesAlongDimension_StackFirst(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  const int num_shapes = state.range(2);

  std::vector<typename ShapeHelper::Shape> shapes;
  shapes.reserve(num_shapes);
  for (int i = 0; i < num_shapes; ++i) {
    shapes.push_back(GetShape<ShapeHelper>(rank, num_children));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(shapes);
    auto result_shape_or =
        StackJaggedShapesAlongDimension(absl::MakeConstSpan(shapes), 0);
    benchmark::DoNotOptimize(result_shape_or);
  }
}

BENCHMARK(
    BM_ConcatJaggedShapesAlongDimension_StackFirst<JaggedArrayShapeHelper>)
    // Rank, num_children, num_shapes
    ->Args({1, 1, 2})
    ->Args({4, 100, 2})
    ->Args({4, 100, 10});

BENCHMARK(
    BM_ConcatJaggedShapesAlongDimension_StackFirst<JaggedDenseArrayShapeHelper>)
    // Rank, num_children, num_shapes
    ->Args({1, 1, 2})
    ->Args({4, 100, 2})
    ->Args({4, 100, 10});

template <typename ShapeHelper>
void BM_StackJaggedShapesAlongDimension_StackLast(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  const int num_shapes = state.range(2);

  std::vector<typename ShapeHelper::Shape> shapes;
  shapes.reserve(num_shapes);
  for (int i = 0; i < num_shapes; ++i) {
    shapes.push_back(GetShape<ShapeHelper>(rank, num_children));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(shapes);
    auto result_shape_or =
        StackJaggedShapesAlongDimension(absl::MakeConstSpan(shapes), rank);
    benchmark::DoNotOptimize(result_shape_or);
  }
}

BENCHMARK(BM_StackJaggedShapesAlongDimension_StackLast<JaggedArrayShapeHelper>)
    // Rank, num_children, num_shapes
    ->Args({1, 1, 2})
    ->Args({4, 100, 2})
    ->Args({4, 100, 10});

BENCHMARK(
    BM_StackJaggedShapesAlongDimension_StackLast<JaggedDenseArrayShapeHelper>)
    // Rank, num_children, num_shapes
    ->Args({1, 1, 2})
    ->Args({4, 100, 2})
    ->Args({4, 100, 10});

template <typename ShapeHelper>
void BM_ConcatJaggedShapesAlongDimension_ConcatFirst(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  const int num_shapes = state.range(2);

  std::vector<typename ShapeHelper::Shape> shapes;
  shapes.reserve(num_shapes);
  for (int i = 0; i < num_shapes; ++i) {
    shapes.push_back(GetShape<ShapeHelper>(rank, num_children));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(shapes);
    auto result_shape_or =
        ConcatJaggedShapesAlongDimension(absl::MakeConstSpan(shapes), 0);
    benchmark::DoNotOptimize(result_shape_or);
  }
}

BENCHMARK(
    BM_ConcatJaggedShapesAlongDimension_ConcatFirst<JaggedArrayShapeHelper>)
    // Rank, num_children, num_shapes
    ->Args({1, 1, 2})
    ->Args({4, 100, 2})
    ->Args({4, 100, 10});

BENCHMARK(BM_ConcatJaggedShapesAlongDimension_ConcatFirst<
              JaggedDenseArrayShapeHelper>)
    // Rank, num_children, num_shapes
    ->Args({1, 1, 2})
    ->Args({4, 100, 2})
    ->Args({4, 100, 10});

template <typename ShapeHelper>
void BM_ConcatJaggedShapesAlongDimension_ConcatLast(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);
  const int num_shapes = state.range(2);

  std::vector<typename ShapeHelper::Shape> shapes;
  shapes.reserve(num_shapes);
  for (int i = 0; i < num_shapes; ++i) {
    shapes.push_back(GetShape<ShapeHelper>(rank, num_children));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(shapes);
    auto result_shape_or =
        ConcatJaggedShapesAlongDimension(absl::MakeConstSpan(shapes), rank - 1);
    benchmark::DoNotOptimize(result_shape_or);
  }
}

BENCHMARK(
    BM_ConcatJaggedShapesAlongDimension_ConcatLast<JaggedArrayShapeHelper>)
    // Rank, num_children, num_shapes
    ->Args({1, 1, 2})
    ->Args({4, 100, 2})
    ->Args({4, 100, 10});

BENCHMARK(
    BM_ConcatJaggedShapesAlongDimension_ConcatLast<JaggedDenseArrayShapeHelper>)
    // Rank, num_children, num_shapes
    ->Args({1, 1, 2})
    ->Args({4, 100, 2})
    ->Args({4, 100, 10});

template <typename ShapeHelper>
void BM_StackJaggedArraysAlongDimension_StackFirst(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);

  const auto shape1 = GetShape<ShapeHelper>(rank, num_children);
  const auto shape2 = GetShape<ShapeHelper>(rank, num_children);
  const auto array1 = ShapeHelper::MakeArray(std::pow(num_children, rank), 1);
  const auto array2 = ShapeHelper::MakeArray(std::pow(num_children, rank), 2);

  for (auto _ : state) {
    auto result_shape_or = StackJaggedArraysAlongDimension(
        absl::MakeConstSpan({array1, array2}),
        absl::MakeConstSpan({shape1, shape2}), 0);
    benchmark::DoNotOptimize(result_shape_or);
  }
}

BENCHMARK(
    BM_StackJaggedArraysAlongDimension_StackFirst<JaggedArrayShapeHelper>)
    // Rank, num_children
    ->Args({1, 1})
    ->Args({2, 1000});

BENCHMARK(
    BM_StackJaggedArraysAlongDimension_StackFirst<JaggedDenseArrayShapeHelper>)
    // Rank, num_children
    ->Args({1, 1})
    ->Args({2, 1000});

template <typename ShapeHelper>
void BM_StackJaggedArraysAlongDimension_StackLast(benchmark::State& state) {
  const int rank = state.range(0);
  const int num_children = state.range(1);

  const auto shape1 = GetShape<ShapeHelper>(rank, num_children);
  const auto shape2 = GetShape<ShapeHelper>(rank, num_children);
  const auto array1 = ShapeHelper::MakeArray(std::pow(num_children, rank), 1);
  const auto array2 = ShapeHelper::MakeArray(std::pow(num_children, rank), 2);

  for (auto _ : state) {
    auto result_shape_or = StackJaggedArraysAlongDimension(
        absl::MakeConstSpan({array1, array2}),
        absl::MakeConstSpan({shape1, shape2}), rank);
    benchmark::DoNotOptimize(result_shape_or);
  }
}

BENCHMARK(BM_StackJaggedArraysAlongDimension_StackLast<JaggedArrayShapeHelper>)
    // Rank, num_children
    ->Args({1, 1})
    ->Args({2, 1000});

BENCHMARK(
    BM_StackJaggedArraysAlongDimension_StackLast<JaggedDenseArrayShapeHelper>)
    // Rank, num_children
    ->Args({1, 1})
    ->Args({2, 1000});

}  // namespace
}  // namespace arolla
