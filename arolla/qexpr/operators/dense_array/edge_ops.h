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
#ifndef AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_EDGE_OPS_H_
#define AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_EDGE_OPS_H_

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/operators/aggregation/group_op_accumulators.h"
#include "arolla/qexpr/operators/array_like/edge_ops.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/util/bits.h"
#include "arolla/util/status.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Returns an edge built from an indices mapping and target domain size.
struct DenseArrayEdgeFromMappingOp {
  absl::StatusOr<DenseArrayEdge> operator()(const DenseArray<int64_t>& mapping,
                                            int64_t parent_size) const {
    return DenseArrayEdge::FromMapping(mapping, parent_size);
  }
};

// Returns indices mapping from an edge.
struct DenseArrayEdgeMappingOp {
  DenseArray<int64_t> operator()(EvaluationContext* ctx,
                                 const DenseArrayEdge& edge) const {
    switch (edge.edge_type()) {
      case DenseArrayEdge::MAPPING:
        return edge.edge_values();
      case DenseArrayEdge::SPLIT_POINTS:
        return edge.ToMappingEdge(ctx->buffer_factory()).edge_values();
    }
    ABSL_UNREACHABLE();
  }

  DenseArray<int64_t> operator()(const DenseArrayGroupScalarEdge& edge) const {
    return CreateConstDenseArray<int64_t>(edge.child_size(), 0);
  }
};

// Returns an edge built from index split points.
struct DenseArrayEdgeFromSplitPointsOp {
  absl::StatusOr<DenseArrayEdge> operator()(
      const DenseArray<int64_t>& split_points) const {
    return DenseArrayEdge::FromSplitPoints(split_points);
  }
};

// edge.from_shape operator returns an edge-to-scalar with a given
// child_side_shape.
struct DenseArrayEdgeFromShapeOp {
  DenseArrayGroupScalarEdge operator()(const DenseArrayShape& shape) const {
    return DenseArrayGroupScalarEdge(shape.size);
  }
};

// edge.from_sizes operator returns an edge built from group sizes.
struct DenseArrayEdgeFromSizesOp {
  absl::StatusOr<DenseArrayEdge> operator()(
      EvaluationContext* ctx, const DenseArray<int64_t>& sizes) const {
    if (!sizes.IsFull()) {
      return absl::InvalidArgumentError(
          "operator edge.from_sizes expects no missing size values");
    }
    Buffer<int64_t>::Builder bldr(sizes.size() + 1, &ctx->buffer_factory());
    absl::Span<int64_t> split_points = bldr.GetMutableSpan();
    split_points[0] = 0;
    std::partial_sum(sizes.values.begin(), sizes.values.end(),
                     split_points.data() + 1);
    return DenseArrayEdge::FromSplitPoints({std::move(bldr).Build()});
  }
};

// edge.pair_left operator returns an edge from left of pairs to child item.
// Given child-to-parent edge, pairs mean cross product of children within each
// parent.
// For example,
// child_to_parent edge mapping is [0, 0, 0, 1, 1]
// pair_left_to_child mapping is  [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 4, 4]
//
// For child-to-parent "sizes", we have
//   parent count: sizes.size() elements
//   child count: sum(sizes) elements
//   pair count: sum(sizes**2) elements
struct DenseArrayEdgePairLeftOp {
  absl::StatusOr<DenseArrayEdge> operator()(
      EvaluationContext* ctx, const DenseArray<int64_t>& sizes) const {
    if (!sizes.IsFull()) {
      return absl::InvalidArgumentError(
          "operator edge.pair_left expects no missing size values");
    }
    int64_t child_count = 0;
    sizes.ForEachPresent([&](int64_t id, int64_t s) { child_count += s; });

    Buffer<int64_t>::Builder bldr(child_count + 1, &ctx->buffer_factory());
    auto inserter = bldr.GetInserter();
    int64_t offset = 0;
    sizes.ForEachPresent([&](int64_t id, int64_t s) {
      for (int64_t i = 0; i < s; ++i) {
        inserter.Add(offset);
        offset += s;
      }
    });
    inserter.Add(offset);

    DenseArray<int64_t> pair_sizes{std::move(bldr).Build()};
    return DenseArrayEdge::FromSplitPoints(pair_sizes);
  }
};

// edge.pair_right operator returns an edge from right of pairs to parent item.
// Given child-to-parent edge, pairs mean cross product of children within each
// parent.
// For example,
// child_to_parent edge mapping is [0, 0, 0, 1, 1]
// pari_right_to_child mapping is [0, 1, 2, 0, 1, 2, 0, 1, 2, 3, 4, 3, 4]
//
// For child-to-parent "sizes", we have
//   parent count: sizes.size() elements
//   child count: sum(sizes) elements
//   pair count: sum(sizes**2) elements
struct DenseArrayEdgePairRightOp {
  absl::StatusOr<DenseArrayEdge> operator()(
      EvaluationContext* ctx, const DenseArray<int64_t>& sizes) const {
    if (!sizes.IsFull()) {
      return absl::InvalidArgumentError(
          "operator edge.pair_right expects no missing size values");
    }
    int64_t child_count = 0;
    int64_t pair_count = 0;
    sizes.ForEachPresent([&](int64_t id, int64_t s) {
      child_count += s;
      pair_count += s * s;
    });

    Buffer<int64_t>::Builder bldr(pair_count, &ctx->buffer_factory());
    auto inserter = bldr.GetInserter();
    int64_t offset = 0;
    sizes.ForEachPresent([&](int64_t id, int64_t s) {
      for (int64_t i = 0; i < s; ++i) {
        for (int64_t j = 0; j < s; ++j) {
          inserter.Add(offset + j);
        }
      }
      offset += s;
    });

    DenseArray<int64_t> pair_sizes{std::move(bldr).Build()};
    return DenseArrayEdge::FromMapping(pair_sizes, child_count);
  }
};

// edge.child_shape operator returns kind and shape of the edge's
// source domain.
struct DenseArrayEdgeChildShapeOp {
  DenseArrayShape operator()(const DenseArrayEdge& edge) const {
    return DenseArrayShape{edge.child_size()};
  }
  DenseArrayShape operator()(const DenseArrayGroupScalarEdge& edge) const {
    return DenseArrayShape{edge.child_size()};
  }
};

// edge.parent_shape operator returns kind and shape of the edge's
// target domain.
struct DenseArrayEdgeParentShapeOp {
  DenseArrayShape operator()(const DenseArrayEdge& edge) const {
    return DenseArrayShape{edge.parent_size()};
  }
  OptionalScalarShape operator()(const DenseArrayGroupScalarEdge& edge) const {
    return OptionalScalarShape{};
  }
};

// array.expand maps the values of a parent array to a child array as specified
// by the edge. DenseGroupOps is not used for performance reasons.
struct DenseArrayExpandOp {
  template <typename T>
  absl::StatusOr<DenseArray<T>> operator()(EvaluationContext* ctx,
                                           const DenseArray<T>& parent_array,
                                           const DenseArrayEdge& edge) const {
    if (edge.parent_size() != parent_array.size()) {
      return arolla::SizeMismatchError(
          {edge.parent_size(), parent_array.size()});
    }
    if (edge.edge_type() == DenseArrayEdge::EdgeType::SPLIT_POINTS) {
      absl::Span<const int64_t> split_points = edge.edge_values().values.span();
      typename Buffer<T>::ReshuffleBuilder values_bldr(
          split_points.back(), parent_array.values, {}, &ctx->buffer_factory());
      if (parent_array.bitmap.empty()) {
        for (size_t i = 0; i < parent_array.size(); ++i) {
          values_bldr.CopyValueToRange(split_points[i], split_points[i + 1], i);
        }
        return DenseArray<T>{std::move(values_bldr).Build()};
      } else {
        bitmap::Bitmap::Builder bitmap_bldr(
            bitmap::BitmapSize(split_points.back()), &ctx->buffer_factory());
        absl::Span<bitmap::Word> bits = bitmap_bldr.GetMutableSpan();
        std::memset(bits.begin(), 0, bits.size() * sizeof(bitmap::Word));
        for (size_t i = 0; i < parent_array.size(); ++i) {
          if (parent_array.present(i)) {
            values_bldr.CopyValueToRange(split_points[i], split_points[i + 1],
                                         i);
            SetBitsInRange(bits.begin(), split_points[i], split_points[i + 1]);
          }
        }
        return DenseArray<T>{std::move(values_bldr).Build(),
                             std::move(bitmap_bldr).Build()};
      }
    } else {
      typename Buffer<T>::ReshuffleBuilder values_bldr(edge.child_size(),
                                                       parent_array.values, {});
      bitmap::AlmostFullBuilder bitmap_bldr(edge.child_size(),
                                            &ctx->buffer_factory());
      edge.edge_values().ForEach(
          [&](int64_t child_id, bool present, int64_t parent_id) {
            if (present && parent_array.present(parent_id)) {
              values_bldr.CopyValue(child_id, parent_id);
            } else {
              bitmap_bldr.AddMissed(child_id);
            }
          });
      return DenseArray<T>{std::move(values_bldr).Build(),
                           std::move(bitmap_bldr).Build()};
    }
  }

  template <typename T>
  absl::StatusOr<DenseArray<T>> operator()(
      EvaluationContext* ctx, const OptionalValue<T>& group_scalar,
      const DenseArrayGroupScalarEdge& edge) const {
    if (group_scalar.present) {
      return CreateConstDenseArray<T>(edge.child_size(), group_scalar.value,
                                      &ctx->buffer_factory());
    } else {
      return CreateEmptyDenseArray<T>(edge.child_size(),
                                      &ctx->buffer_factory());
    }
  }
};

// Returns an edge that maps the unique values of the input array to the same
// group.
struct DenseArrayGroupByOp {
  template <typename T, typename Edge>
  absl::StatusOr<DenseArrayEdge> operator()(EvaluationContext* ctx,
                                            const DenseArray<T>& series,
                                            const Edge& over) const {
    int64_t group_counter = 0;
    DenseGroupOps<GroupByAccumulator<T>> op(
        &ctx->buffer_factory(), GroupByAccumulator<T>(&group_counter));
    ASSIGN_OR_RETURN(DenseArray<int64_t> mapping, op.Apply(over, series));
    return DenseArrayEdge::UnsafeFromMapping(std::move(mapping), group_counter);
  }
};

// edge.sizes operator returns an array of sizes corresponding to the number of
// children of each parent index.
struct DenseArrayEdgeSizesOp {
  absl::StatusOr<DenseArray<int64_t>> operator()(
      EvaluationContext* ctx, const DenseArrayEdge& edge) const {
    Buffer<int64_t>::Builder builder(edge.parent_size(),
                                     &ctx->buffer_factory());
    if (edge.edge_type() == DenseArrayEdge::EdgeType::SPLIT_POINTS) {
      // All split points are present.
      auto inserter = builder.GetInserter();
      const auto& values = edge.edge_values().values;
      for (int64_t i = 1; i < values.size(); ++i) {
        inserter.Add(values[i] - values[i - 1]);
      }
    } else {
      auto sizes = builder.GetMutableSpan();
      std::memset(sizes.begin(), 0, sizes.size() * sizeof(int64_t));
      edge.edge_values().ForEach([&](int64_t, bool present, int64_t parent_id) {
        if (present) {
          sizes[parent_id]++;
        }
      });
    }
    return DenseArray<int64_t>{std::move(builder).Build()};
  }

  int64_t operator()(const DenseArrayGroupScalarEdge& edge) const {
    return edge.child_size();
  }
};

// Returns the number of present items
struct DenseArrayCountOp {
  int64_t operator()(const DenseArray<Unit>& arg,
                     const DenseArrayGroupScalarEdge&) const {
    return bitmap::CountBits(arg.bitmap, arg.bitmap_bit_offset, arg.size());
  }
};

// TODO: Can we reduce duplication with array/edge_ops.h?
// Given a SPLIT_POINTS edge, resizes and reorders the items within each
// group, and returns an edge between the new id space and the child id space of
// the old edge.
struct DenseArrayEdgeResizeGroupsChildSide {
  template <typename NewSizeGetterFn>
  absl::StatusOr<DenseArrayEdge> FromSplitPoints(
      EvaluationContext* ctx, absl::Span<const int64_t> split_points,
      NewSizeGetterFn new_size_getter, int64_t total_size,
      const DenseArray<int64_t>& offsets) const {
    int64_t child_size = split_points.back();
    if (total_size < 0) {
      return absl::InvalidArgumentError(
          "got a negative size value in operator "
          "edge.resize_groups_child_side");
    }
    DenseArrayBuilder<int64_t> builder(total_size, &ctx->buffer_factory());
    std::vector<bool> set_indices(total_size);
    if (split_points.size() < 2) {
      return DenseArrayEdge();
    }
    int64_t split_points_idx = 1;
    int64_t new_base_id = 0;
    int64_t new_group_size = new_size_getter(0);
    bool negative_offset = false;
    bool duplicate_offsets = false;
    offsets.ForEachPresent([&](int64_t old_id, int64_t new_offset) {
      while (old_id >= split_points[split_points_idx]) {
        new_base_id += new_group_size;
        ++split_points_idx;
        new_group_size = new_size_getter(split_points_idx - 1);
      }
      if (new_offset < 0) {
        negative_offset = true;
      } else if (new_offset < new_group_size) {
        int64_t new_id = new_base_id + new_offset;
        builder.Set(new_base_id + new_offset, old_id);
        if (set_indices[new_id]) {
          duplicate_offsets = true;
        } else {
          set_indices[new_id] = true;
        }
      }
    });

    if (negative_offset) {
      return absl::InvalidArgumentError(
          "got a negative offset in operator "
          "edge.resize_groups_child_side");
    }
    if (duplicate_offsets) {
      return absl::InvalidArgumentError(
          "duplicate offsets in the same group in operator "
          "edge.resize_groups_child_side");
    }

    return DenseArrayEdge::FromMapping(std::move(builder).Build(), child_size);
  }

  absl::Status CheckEdge(const DenseArrayEdge& edge) const {
    DCHECK(edge.edge_values().IsFull());
    if (edge.edge_type() != DenseArrayEdge::SPLIT_POINTS) {
      return absl::UnimplementedError(
          "operator edge.resize_groups_child_side is only supported for "
          "SPLIT_POINTS edges");
    }
    return absl::OkStatus();
  }

  absl::Status CheckNewSizes(const DenseArray<int64_t>& new_sizes,
                             const DenseArrayEdge& edge) const {
    if (!new_sizes.IsFull()) {
      return absl::InvalidArgumentError(
          "`new_sizes` should be a full array for operator "
          "edge.resize_groups_child_side");
    }
    if (new_sizes.size() + 1 != edge.edge_values().size()) {
      return absl::InvalidArgumentError(
          "number of new sizes should match number of edge parent-side groups "
          "in operator edge.resize_groups_child_side");
    }
    return absl::OkStatus();
  }

  template <typename EdgeType>
  absl::Status CheckNewOffsets(const DenseArray<int64_t>& new_offsets,
                               const EdgeType& edge) const {
    if (new_offsets.size() != edge.child_size()) {
      return absl::InvalidArgumentError(
          "`new_offsets` argument should be the same size as the child side of "
          "the edge in edge.resize_groups_child_side");
    }
    return absl::OkStatus();
  }

  absl::StatusOr<DenseArrayEdge> operator()(
      EvaluationContext* ctx, const DenseArrayEdge& edge, int64_t new_size,
      const DenseArray<int64_t>& new_offsets) const {
    RETURN_IF_ERROR(CheckEdge(edge));
    RETURN_IF_ERROR(CheckNewOffsets(new_offsets, edge));

    return FromSplitPoints(
        ctx, edge.edge_values().values.span(),
        [&new_size](int64_t _) { return new_size; },
        new_size * edge.parent_size(), new_offsets);
  }

  absl::StatusOr<DenseArrayEdge> operator()(
      EvaluationContext* ctx, const DenseArrayGroupScalarEdge& scalar_edge,
      int64_t new_size, const DenseArray<int64_t>& new_offsets) const {
    RETURN_IF_ERROR(CheckNewOffsets(new_offsets, scalar_edge));
    return FromSplitPoints(
        ctx, {0, scalar_edge.child_size()},
        [&new_size](int64_t) { return new_size; }, new_size, new_offsets);
  }

  absl::StatusOr<DenseArrayEdge> operator()(
      EvaluationContext* ctx, const DenseArrayEdge& edge,
      const DenseArray<int64_t>& new_sizes,
      const DenseArray<int64_t>& new_offsets) const {
    RETURN_IF_ERROR(CheckEdge(edge));
    RETURN_IF_ERROR(CheckNewSizes(new_sizes, edge));
    RETURN_IF_ERROR(CheckNewOffsets(new_offsets, edge));

    const auto& new_sizes_buffer = new_sizes.values;
    int64_t total_size = 0;
    for (int64_t i = 0; i < new_sizes_buffer.size(); ++i) {
      total_size += new_sizes_buffer[i];
    }

    DCHECK(edge.edge_values().IsFull());
    return FromSplitPoints(
        ctx, edge.edge_values().values.span(),
        [&new_sizes_buffer](int64_t idx) { return new_sizes_buffer[idx]; },
        total_size, new_offsets);
  }
};

// Given a SPLIT_POINTS edge and a new_size, truncates or pads the number of
// child items per parent item to the given size, and returns an edge between
// the new id space and the parent space of the old edge.
struct DenseArrayEdgeResizeGroupsParentSide {
  absl::StatusOr<DenseArrayEdge> operator()(EvaluationContext* ctx,
                                            const DenseArrayEdge& edge,
                                            int64_t new_size) const {
    if (edge.edge_type() != DenseArrayEdge::SPLIT_POINTS) {
      return absl::UnimplementedError(
          "operator edge.resize_groups_parent_side is only supported for "
          "SPLIT_POINTS edges");
    }

    if (new_size < 0) {
      return absl::InvalidArgumentError(
          "`size` argument should be a non-negative integer for operator "
          "edge.resize_groups_parent_side");
    }

    int64_t split_points_num = edge.parent_size() + 1;
    Buffer<int64_t>::Builder bldr(split_points_num, &ctx->buffer_factory());
    absl::Span<int64_t> split_points = bldr.GetMutableSpan();
    split_points[0] = 0;
    for (int64_t i = 1; i < split_points_num; ++i) {
      split_points[i] = split_points[i - 1] + new_size;
    }
    return DenseArrayEdge::FromSplitPoints({std::move(bldr).Build()});
  }

  absl::StatusOr<DenseArrayGroupScalarEdge> operator()(
      EvaluationContext* ctx, const DenseArrayGroupScalarEdge& scalar_edge,
      int64_t new_size) const {
    if (new_size < 0) {
      return absl::InvalidArgumentError(
          "`size` argument should be a non-negative integer for operator "
          "edge.resize_groups_parent_side");
    }

    return DenseArrayGroupScalarEdge(new_size);
  }

  absl::StatusOr<DenseArrayEdge> operator()(
      EvaluationContext* ctx, const DenseArrayEdge& edge,
      const DenseArray<int64_t>& new_sizes) const {
    if (edge.edge_type() != DenseArrayEdge::SPLIT_POINTS) {
      return absl::UnimplementedError(
          "operator edge.resize_groups_parent_side is only supported for "
          "SPLIT_POINTS edges");
    }
    if (new_sizes.size() != edge.parent_size()) {
      return absl::InvalidArgumentError(
          "number of new sizes should match number of edge parent-side groups "
          "in operator edge.resize_groups_parent_side");
    }
    return DenseArrayEdgeFromSizesOp()(ctx, new_sizes);
  }
};

// `edge.compose._dense_array` operator.
class DenseArrayEdgeComposeOperatorFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_qtypes,
      QTypePtr output_qtype) const final {
    return EnsureOutputQTypeMatches(
        OperatorPtr(std::make_unique<EdgeComposeOperator<DenseArrayEdge>>(
            input_qtypes.size())),
        input_qtypes, output_qtype);
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_EDGE_OPS_H_
