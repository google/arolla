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
#ifndef AROLLA_JAGGED_SHAPE_UTIL_CONCAT_H_
#define AROLLA_JAGGED_SHAPE_UTIL_CONCAT_H_

#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/memory/buffer.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace jagged_shape_internal {

template <typename Shape>
absl::StatusOr<std::tuple<Shape, std::vector<int64_t>>>
ConcatJaggedShapesAlongDimension(absl::Span<const Shape> shapes, int64_t dim,
                                 bool insert_concat_dim);

template <typename ArrayType, typename Shape>
absl::StatusOr<std::tuple<ArrayType, Shape>> ConcatJaggedArraysAlongDimension(
    absl::Span<const ArrayType> arrays, absl::Span<const Shape> array_shapes,
    int64_t dim, bool insert_concat_dim);

}  // namespace jagged_shape_internal

// Returns the concatenation of the given jagged shapes along dimension `dim`,
// that is, the shape of the jagged array that would result if jagged arrays
// that these shapes describe were concatenated along `dim`. All shapes must
// have the same rank and must have the same shape for dimensions `[0, dim)`.
//
// The `concat` operation on jagged arrays agrees with the behavior of Numpy-
// style `np.concat` for arrays with all-uniform shape. If you think of jagged
// arrays as nested lists, then `concat` simultaneously iterates through all
// inputs at nesting depth `dim` and concatenates the sub-lists at that depth.
//
// Examples:
//   // Concat on first dimension.
//   ConcatJaggedShapesAlongDimension(
//       JaggedArrayShape([2], [1, 2], [3, 4, 5]),
//       JaggedArrayShape([3], [1, 3, 1], [6, 7, 8, 9, 10]),
//       /*dim=*/0)
//     -> JaggedArrayShape([5], [1, 2, 1, 3, 1], [3, 4, 5, 6, 7, 8, 9, 10])
//
//   // Concat on middle dimension
//   ConcatJaggedShapesAlongDimension(
//       JaggedArrayShape([2], [1, 2], [3, 4, 5]),
//       JaggedArrayShape([2], [1, 3], [6, 7, 8, 9]),
//       /*dim=*/1)
//     -> JaggedArrayShape([2], [2, 5], [3, 4, 5, 6, 7, 8, 9])
//
//   // Concat on last dimension
//   ConcatJaggedShapesAlongDimension(
//       JaggedArrayShape([2], [1, 2], [3, 4, 5]),
//       JaggedArrayShape([2], [1, 2], [6, 7, 8]),
//       /*dim=*/2)
//     -> JaggedArrayShape([2], [1, 2], [9, 11, 13])
//
template <typename Shape>
absl::StatusOr<Shape> ConcatJaggedShapesAlongDimension(
    absl::Span<const Shape> shapes, int64_t dim) {
  ASSIGN_OR_RETURN((auto [result_shape, group_sizes]),
                   jagged_shape_internal::ConcatJaggedShapesAlongDimension(
                       shapes, dim, false));
  return std::move(result_shape);
}

// Returns the shape that would result from stacking arrays with the given
// shapes along `dim`. This is equivalent to inserting a unit dimension just
// before `dim` and then concatenating along that inserted dim. All shapes must
// have the same rank and must have the same shape for dimensions `[0, dim)`.
//
// The `stack` operation on jagged arrays agrees with the behavior of Numpy-
// style `np.stack` for arrays with all-uniform shape. If you think of jagged
// arrays as nested lists, `stack` simultaneously iterates through all inputs
// at nesting depth `dim`, wrapping the sub-lists from each input in a new list
// in the result.
//
// Examples:
//   // Stack on first dimension, which essentially wraps the list of arguments
//   // in a new outer jagged dimension.
//   StackJaggedShapesAlongDimension(
//       JaggedArrayShape([2], [1, 2], [3, 4, 5]),
//       JaggedArrayShape([3], [1, 3, 1], [6, 7, 8, 9, 10]),
//       /*dim=*/0)
//     -> JaggedArrayShape(
//            [2], [2, 3], [1, 2, 1, 3, 1], [3, 4, 5, 6, 7, 8, 9, 10])
//
//   // Stack on middle dimension
//   ConcatJaggedShapesAlongDimension(
//       JaggedArrayShape([2], [1, 2], [3, 4, 5]),
//       JaggedArrayShape([2], [1, 3], [6, 7, 8, 9]),
//       /*dim=*/1, /*insert_concat_dim=*/true)
//     -> JaggedArrayShape([2], [2, 2], [1, 1, 2, 3], [3, 6, 4, 5, 7, 8, 9])
//
//   // Stack on last dimension
//   ConcatJaggedShapesAlongDimension(
//       JaggedArrayShape([2], [1, 2], [3, 4, 5]),
//       JaggedArrayShape([2], [1, 2], [6, 7, 8]),
//       /*dim=*/2, /*insert_concat_dim=*/true)
//     -> JaggedArrayShape([2], [1, 2], [2, 2, 2], [3, 6, 4, 7, 5, 8])
//
template <typename Shape>
absl::StatusOr<Shape> StackJaggedShapesAlongDimension(
    absl::Span<const Shape> shapes, int64_t dim) {
  ASSIGN_OR_RETURN((auto [result_shape, group_sizes]),
                   jagged_shape_internal::ConcatJaggedShapesAlongDimension(
                       shapes, dim, true));
  return std::move(result_shape);
}

// Concatenates a list of arrays with jagged shapes along `dim`, returning the
// resulting array and its shape. See `ConcatJaggedShapesAlongDimension` for
// more details.
//
// Expects `ArrayType` to be `arolla::Array<T>` or `arolla::DenseArray<T>`.
template <typename ArrayType, typename Shape>
absl::StatusOr<std::tuple<ArrayType, Shape>> ConcatJaggedArraysAlongDimension(
    absl::Span<const ArrayType> arrays, absl::Span<const Shape> array_shapes,
    int64_t dim) {
  return jagged_shape_internal::ConcatJaggedArraysAlongDimension(
      arrays, array_shapes, dim, false);
}

// Stacks a list of arrays with jagged shapes along `dim`, returning the
// resulting array and its shape. See `StackJaggedShapesAlongDimension` for
// more details.
//
// Expects `ArrayType` to be `arolla::Array<T>` or `arolla::DenseArray<T>`.
template <typename ArrayType, typename Shape>
absl::StatusOr<std::tuple<ArrayType, Shape>> StackJaggedArraysAlongDimension(
    absl::Span<const ArrayType> arrays, absl::Span<const Shape> array_shapes,
    int64_t dim) {
  return jagged_shape_internal::ConcatJaggedArraysAlongDimension(
      arrays, array_shapes, dim, true);
}

namespace jagged_shape_internal {

// Helper for ConcatJaggedShapesAlongDimension.
//
// Args:
//   shapes: input shapes.
//   dim: dimension of the input shapes we are concatenating.
//   group_sizes: strided [num_groups_per_input, shapes.size()] array
//     describing the sizes of the groups of splits in the input edges we are
//     currently processing that will be interleaved in the result.
//   num_groups_per_input: see `group_sizes`.
//   parent_size: the child size of the previous result edge.
//   is_concat_dim: whether this is the concatenation dimension.
//
// Returns:
//   (result_edge, group_sizes) where `group_sizes` is updated for the next
//   edge that needs processing (or the array itself, for the last edge).
template <typename Shape>
std::tuple<typename Shape::Edge, std::vector<int64_t>> BuildSuffixEdge(
    absl::Span<const Shape> shapes, int64_t dim,
    std::vector<int64_t> group_sizes, int64_t num_groups_per_input,
    int64_t parent_size, bool is_concat_dim) {
  // Invariant: the total number of elements that the current edges in
  // `result` would describe (if interpreted as a full shape) must match the
  // sum of `group_sizes`, which are supposed to describe the sizes of groups
  // of elements in that full shape.
  if (!is_concat_dim) {
    DCHECK_EQ(parent_size, absl::c_accumulate(group_sizes, int64_t{0}));
  } else {
    // If we are constructing the concatenation dimension, `group_sizes` will
    // be off by a factor of `shape.size()` because the concatenation
    // dimension's parent_size is ungrouped while its child_size is grouped.
    DCHECK_EQ(parent_size * shapes.size(),
              absl::c_accumulate(group_sizes, int64_t{0}));
  }

  // Mechanism for accumulating the splits for the result edge being created
  // by this i_dim loop iteration.
  Buffer<int64_t>::Builder edge_splits_builder(parent_size + 1);
  absl::Span<int64_t> edge_splits = edge_splits_builder.GetMutableSpan();
  edge_splits[0] = 0;
  int64_t edge_last_split_id = 0;
  int64_t edge_last_split = 0;
  const auto add_result_edge_split_with_size = [&](int64_t split_size) {
    ++edge_last_split_id;
    edge_last_split += split_size;
    edge_splits[edge_last_split_id] = edge_last_split;
  };

  // Because we need to iterate by concat index then shape to properly form
  // the result edge splits (because that's just how the concatenated data is
  // interleaved), we need to track which split in each shape we are currently
  // iterating over, for each shape.
  std::vector<int64_t> shape_i_split(shapes.size(), 0);

  int64_t group_index = 0;
  for (int64_t i_input_group = 0; i_input_group < num_groups_per_input;
       ++i_input_group) {
    int64_t total_input_group_size = 0;
    for (int64_t i_shape = 0; i_shape < shapes.size(); ++i_shape) {
      const auto& shape_edge = shapes[i_shape].edges()[dim];

      // The number of splits that will belong to this "group" (i.e.
      // (i_input_group, i_shape) pair) in the next edge is the sum of the
      // sizes of the splits that belong to it in the current edge. In other
      // words, we must expand the group sizes along the current edge to get
      // the next edge's group sizes.
      //
      // At the same time, copy the sizes of the splits into the result edge.
      // Because we are iterating in (i_input_group, i_shape) order, the
      // result edge split sizes will be properly interleaved from the input
      // shape edges' split sizes.
      int64_t new_group_size = 0;
      for (int64_t i = 0; i < group_sizes[group_index]; ++i) {
        const int64_t split_size =
            shape_edge.split_size(shape_i_split[i_shape]);
        new_group_size += split_size;
        ++shape_i_split[i_shape];

        // If `is_concat_dim`, we instead want to insert a single result
        // edge split for the whole input group (summed across all input
        // shapes), instead of for each split.
        if (!is_concat_dim) {
          add_result_edge_split_with_size(split_size);
        }
      }

      // Each element of `group_sizes` is read from once in this double loop,
      // so updating in place is safe (and more efficient).
      group_sizes[group_index] = new_group_size;
      ++group_index;
      total_input_group_size += new_group_size;
    }

    // See comment in loop above about special handling of `is_concat_dim`.
    if (is_concat_dim) {
      add_result_edge_split_with_size(total_input_group_size);
    }
  }

  DCHECK_EQ(edge_last_split_id, parent_size);
  return std::make_tuple(Shape::Edge::UnsafeFromSplitPoints(
      typename Shape::Edge::Values(std::move(edge_splits_builder).Build())),
      std::move(group_sizes));
}

// Returns (`jagged_shape`, `group_sizes`).
template <typename Shape>
absl::StatusOr<std::tuple<Shape, std::vector<int64_t>>>
ConcatJaggedShapesAlongDimension(absl::Span<const Shape> shapes, int64_t dim,
                                 bool insert_concat_dim) {
  if (shapes.empty()) {
    return absl::InvalidArgumentError(
        "concat/stack requires a nonzero number of inputs");
  }
  const int64_t input_rank = shapes[0].rank();
  const int64_t result_rank = insert_concat_dim ? input_rank + 1 : input_rank;

  if (result_rank == 0) {
    return absl::InvalidArgumentError("cannot concat shapes of rank zero");
  }

  if (dim < 0 || dim >= result_rank) {
    return absl::InvalidArgumentError(
        absl::StrFormat("invalid dim = %d for concat/stack", dim));
  }

  // Check that all shapes have the same rank and the same prefix.
  for (const Shape& shape : shapes) {
    if (shape.rank() != input_rank) {
      return absl::InvalidArgumentError(
          absl::StrFormat("concat/stack requires all inputs to have the same "
                          "rank, got %d and %d",
                          input_rank, shape.rank()));
    }
    for (int64_t i = 0; i < dim; ++i) {
      if (!shapes[0].edges()[i].IsEquivalentTo(shape.edges()[i])) {
        return absl::InvalidArgumentError(
            "concat/stack requires all inputs to have the same shape prefix "
            "before the concatenation dimension");
      }
    }
  }

  typename Shape::EdgeVec result_edges;
  result_edges.reserve(result_rank + 1);

  // Handle common prefix from dims [0, dim).
  for (int64_t i_dim = 0; i_dim < dim; ++i_dim) {
    result_edges.push_back(shapes[0].edges()[i_dim]);
  }

  // The number of groups of splits in each input shape that need to be
  // interleaved to produce the result shape.
  const int64_t num_groups_per_input =
      result_edges.empty() ? 1 : result_edges.back().child_size();

  if (insert_concat_dim) {
    ASSIGN_OR_RETURN(auto new_edge, Shape::Edge::FromUniformGroups(
                                        num_groups_per_input, shapes.size()));
    result_edges.push_back(std::move(new_edge));
  }

  // `group_sizes[i, j]` tracks the number of splits in result_edges.back()
  // that belong to shape `j` for input group `i`. If `insert_concat_dim` is
  // true, `i` is also the index in the concatenation dimension.
  //
  // The shape of `group_sizes` is  `(num_groups_per_input, shapes.size())` and
  // it is indexed in a strided fashion.
  std::vector<int64_t> group_sizes(num_groups_per_input * shapes.size(), 1);

  // Handle suffix shape from input dims [dim, rank).
  for (int64_t i_dim = dim; i_dim < input_rank; ++i_dim) {
    // Whether we are currently handling the concatenation dimension, which
    // behaves somewhat differently.
    const bool is_concat_dim = !insert_concat_dim && i_dim == dim;

    // The parent size of the result edge we're building must match the child
    // size of the previous result edge.
    const int64_t parent_size =
        result_edges.empty() ? 1 : result_edges.back().child_size();

    typename Shape::Edge result_edge;
    std::tie(result_edge, group_sizes) =
        BuildSuffixEdge(shapes, i_dim, std::move(group_sizes),
                        num_groups_per_input, parent_size, is_concat_dim);
    result_edges.push_back(std::move(result_edge));
  }

  ASSIGN_OR_RETURN(auto result_shape,
                   Shape::FromEdges(std::move(result_edges)));
  return std::make_tuple(std::move(result_shape), std::move(group_sizes));
}

// Helper to avoid dependencies on Array<T>/DenseArray<T>.
template <typename ArrayType>
struct ConcatResultArrayBuilderHelper {
  // Template specializations will have the following method:
  //
  // ArrayTypeBuilder operator()(absl::Span<const ArrayType> arrays) const;
  //
  // `ArrayTypeBuilder` will be either `DenseArrayBuilder<T>` if `ArrayType` is
  // `DenseArray<T>`, or `SparseArrayBuilder<T>` if `ArrayType` is `Array<T>`.
  //
  // Template specializations for each backing array type are defined in
  // "jagged_shape/<array_type>/util/concat.h".

  static_assert(
      false,
      "Missing helper template specialization for concat/stack. Please include "
      "the appropriate 'jagged_shape/<array_type>/util/concat.h' header file.");
};

template <typename ArrayType, typename Shape>
absl::StatusOr<std::tuple<ArrayType, Shape>> ConcatJaggedArraysAlongDimension(
    absl::Span<const ArrayType> arrays, absl::Span<const Shape> array_shapes,
    int64_t dim, bool insert_concat_dim) {
  using T = typename ArrayType::base_type;

  if (arrays.size() != array_shapes.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("concat/stack expects `arrays` and `array_shapes` to "
                        "be 1:1, got sizes %d and %d",
                        arrays.size(), array_shapes.size()));
  }

  // Compute the result shape (performs additional input checks).
  ASSIGN_OR_RETURN((auto [result_jagged_shape, group_sizes]),
                   jagged_shape_internal::ConcatJaggedShapesAlongDimension(
                       array_shapes, dim, insert_concat_dim));

  if (arrays.size() == 1) {
    return std::make_tuple(arrays[0], std::move(result_jagged_shape));
  }

  for (int64_t i_array = 0; i_array < arrays.size(); ++i_array) {
    if (arrays[i_array].size() != array_shapes[i_array].size()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "concat/stack expects `arrays` and `array_shapes` to describe arrays "
          "with the same size, but got %d != %d for index %d",
          arrays[i_array].size(), array_shapes[i_array].size(), i_array));
    }
  }

  const int64_t concat_dim_parent_size =
      result_jagged_shape.edges()[dim].parent_size();

  // `group_sizes` has size (concat_dim_parent_size * arrays.size()), indexed
  // in a strided fashion with shape (concat_dim_parent_size * arrays.size()).
  //
  // `group_sizes[i, j]` is the size of a group of values in array `j` that
  // must be copied to the result array; the result is formed by interleaving
  // all groups in (i, j) order.
  DCHECK_EQ(group_sizes.size(), concat_dim_parent_size * arrays.size());

  // Track the offset into each input array during the interleave operation.
  std::vector<int64_t> array_offsets(arrays.size(), 0);

  auto result_array_builder =
      ConcatResultArrayBuilderHelper<ArrayType>{}(arrays);
  int64_t result_offset = 0;
  int64_t group_index = 0;
  for (int64_t i_array_group = 0; i_array_group < concat_dim_parent_size;
       ++i_array_group) {
    for (int64_t i_array = 0; i_array < arrays.size(); ++i_array) {
      const int64_t group_size = group_sizes[group_index];
      arrays[i_array]
          .MakeUnowned()
          .Slice(array_offsets[i_array], group_size)
          .ForEachPresent([&](int64_t group_offset, view_type_t<T> value) {
            result_array_builder.Add(result_offset + group_offset, value);
          });
      result_offset += group_size;
      array_offsets[i_array] += group_size;
      ++group_index;
    }
  }

  return std::make_tuple(std::move(result_array_builder).Build(),
                         std::move(result_jagged_shape));
}

}  // namespace jagged_shape_internal

}  // namespace arolla

#endif  // AROLLA_JAGGED_SHAPE_UTIL_CONCAT_H_
