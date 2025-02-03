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
#ifndef AROLLA_QEXPR_OPERATORS_ARRAY_EDGE_OPS_H_
#define AROLLA_QEXPR_OPERATORS_ARRAY_EDGE_OPS_H_

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/group_op.h"
#include "arolla/array/id_filter.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/operators/aggregation/group_op_accumulators.h"
#include "arolla/qexpr/operators/array/factory_ops.h"
#include "arolla/qexpr/operators/array_like/edge_ops.h"
#include "arolla/qexpr/operators/dense_array/edge_ops.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/util/bits.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Returns an edge built from an indices mapping and target domain size.
struct ArrayEdgeFromMappingOp {
  absl::StatusOr<ArrayEdge> operator()(const Array<int64_t>& mapping,
                                       int64_t parent_size) const {
    return ArrayEdge::FromMapping(mapping, parent_size);
  }
};

// Returns indices mapping from an edge.
struct ArrayEdgeMappingOp {
  Array<int64_t> operator()(EvaluationContext* ctx,
                            const ArrayEdge& edge) const {
    switch (edge.edge_type()) {
      case ArrayEdge::MAPPING:
        return edge.edge_values();
      case ArrayEdge::SPLIT_POINTS:
        return edge.ToMappingEdge(ctx->buffer_factory()).edge_values();
    }
    ABSL_UNREACHABLE();
  }

  Array<int64_t> operator()(const ArrayGroupScalarEdge& edge) const {
    return Array<int64_t>(edge.child_size(), 0);
  }
};

// Returns an edge built from index split points.
struct ArrayEdgeFromSplitPointsOp {
  absl::StatusOr<ArrayEdge> operator()(
      const Array<int64_t>& split_points) const {
    return ArrayEdge::FromSplitPoints(split_points);
  }
};

// edge.from_shape operator returns an edge-to-scalar with a given
// child_side_shape.
struct ArrayEdgeFromShapeOp {
  ArrayGroupScalarEdge operator()(const ArrayShape& shape) const {
    return ArrayGroupScalarEdge(shape.size);
  }
};

// edge.from_sizes operator returns an edge constructed from an array of sizes.
struct ArrayEdgeFromSizesOp {
  absl::StatusOr<ArrayEdge> operator()(EvaluationContext* ctx,
                                       const Array<int64_t>& sizes) const {
    DenseArray<int64_t> dense_sizes = sizes.ToDenseForm().dense_data();
    if (!dense_sizes.IsFull()) {
      return absl::InvalidArgumentError(
          "operator edge.from_sizes expects no missing size values");
    }
    Buffer<int64_t>::Builder bldr(dense_sizes.size() + 1,
                                  &ctx->buffer_factory());
    absl::Span<int64_t> split_points = bldr.GetMutableSpan();
    split_points[0] = 0;
    std::partial_sum(dense_sizes.values.begin(), dense_sizes.values.end(),
                     split_points.data() + 1);
    DenseArray<int64_t> dense_splits{std::move(bldr).Build()};
    return ArrayEdge::FromSplitPoints(Array<int64_t>(dense_splits));
  }
};

// edge.pair_left operator returns an edge from left of pairs to child item.
// Given child-to-parent edge, pairs mean cross product of children within each
// parent.
// For example,
// child_to_parent edge sizes are [3, 2]
// child_to_parent edge mapping is [0, 0, 0, 1, 1]
// pair_left_to_child mapping is  [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 4, 4]
//
// For child-to-parent "sizes", we have
//   parent count: sizes.size() elements
//   child count: sum(sizes) elements
//   pair count: sum(sizes**2) elements
struct ArrayEdgePairLeftOp {
  absl::StatusOr<ArrayEdge> operator()(EvaluationContext* ctx,
                                       const Array<int64_t>& sizes) const {
    DenseArray<int64_t> dense_sizes = sizes.ToDenseForm().dense_data();
    if (!dense_sizes.IsFull()) {
      return absl::InvalidArgumentError(
          "operator edge.pair_left expects no missing size values");
    }
    int64_t child_count = 0;
    dense_sizes.ForEachPresent(
        [&](int64_t id, int64_t s) { child_count += s; });

    Buffer<int64_t>::Builder bldr(child_count + 1, &ctx->buffer_factory());
    auto inserter = bldr.GetInserter();
    int64_t offset = 0;
    dense_sizes.ForEachPresent([&](int64_t id, int64_t s) {
      for (int64_t i = 0; i < s; ++i) {
        inserter.Add(offset);
        offset += s;
      }
    });
    inserter.Add(offset);

    DenseArray<int64_t> pair_sizes{std::move(bldr).Build()};
    return ArrayEdge::FromSplitPoints(Array<int64_t>(pair_sizes));
  }
};

// edge.pair_right operator returns an edge from right of pairs to parent item.
// Given child-to-parent edge, pairs mean cross product of children within each
// parent.
// For example,
// child_to_parent edge sizes are [3, 2]
// child_to_parent edge mapping is [0, 0, 0, 1, 1]
// pari_right_to_child mapping is [0, 1, 2, 0, 1, 2, 0, 1, 2, 3, 4, 3, 4]
//
// For child-to-parent "sizes", we have
//   parent count: sizes.size() elements
//   child count: sum(sizes) elements
//   pair count: sum(sizes**2) elements
struct ArrayEdgePairRightOp {
  absl::StatusOr<ArrayEdge> operator()(EvaluationContext* ctx,
                                       const Array<int64_t>& sizes) const {
    DenseArray<int64_t> dense_sizes = sizes.ToDenseForm().dense_data();
    if (!dense_sizes.IsFull()) {
      return absl::InvalidArgumentError(
          "operator edge.pair_right expects no missing size values");
    }
    int64_t child_count = 0;
    int64_t pair_count = 0;
    dense_sizes.ForEachPresent([&](int64_t id, int64_t s) {
      child_count += s;
      pair_count += s * s;
    });

    Buffer<int64_t>::Builder bldr(pair_count, &ctx->buffer_factory());
    auto inserter = bldr.GetInserter();
    int64_t offset = 0;
    dense_sizes.ForEachPresent([&](int64_t id, int64_t s) {
      for (int64_t i = 0; i < s; ++i) {
        for (int64_t j = 0; j < s; ++j) {
          inserter.Add(offset + j);
        }
      }
      offset += s;
    });

    DenseArray<int64_t> pair_mapping{std::move(bldr).Build()};
    return ArrayEdge::FromMapping(Array<int64_t>(pair_mapping), child_count);
  }
};

// edge.child_shape operator returns kind and shape of the edge's
// source domain.
struct ArrayEdgeChildShapeOp {
  ArrayShape operator()(const ArrayEdge& edge) const {
    return ArrayShape{edge.child_size()};
  }
  ArrayShape operator()(const ArrayGroupScalarEdge& edge) const {
    return ArrayShape{edge.child_size()};
  }
};

// edge.parent_shape operator returns kind and shape of the edge's
// target domain.
struct ArrayEdgeParentShapeOp {
  ArrayShape operator()(const ArrayEdge& edge) const {
    return ArrayShape{edge.parent_size()};
  }
  OptionalScalarShape operator()(const ArrayGroupScalarEdge& edge) const {
    return OptionalScalarShape{};
  }
};

// array.expand maps the values of a parent array to a child array as specified
// by the edge. ArrayGroupOp is not used for performance reasons.
struct ArrayExpandOp {
  template <typename T>
  absl::StatusOr<Array<T>> operator()(EvaluationContext* ctx,
                                      const Array<T>& parent_array,
                                      const ArrayEdge& edge) const {
    if (edge.parent_size() != parent_array.size()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("parent size of edge: %d must match size of array: "
                          "%d in array._expand "
                          "operator",
                          edge.parent_size(), parent_array.size()));
    }
    if (edge.edge_type() == ArrayEdge::MAPPING) {
      return ExpandOverMapping(ctx, parent_array, edge);
    } else {
      return ExpandOverSplitPoints(ctx, parent_array, edge);
    }
  }

  template <typename T>
  absl::StatusOr<Array<T>> operator()(EvaluationContext* ctx,
                                      const OptionalValue<T>& group_scalar,
                                      const ArrayGroupScalarEdge& edge) const {
    return Array<T>(edge.child_size(), group_scalar);
  }

 private:
  template <typename T>
  absl::StatusOr<Array<T>> ExpandOverMapping(EvaluationContext* ctx,
                                             const Array<T>& parent_array,
                                             const ArrayEdge& edge) const {
    DCHECK_EQ(edge.edge_type(), ArrayEdge::MAPPING);
    if (parent_array.IsAllMissingForm()) {
      return Array<T>(edge.child_size());
    }
    if (parent_array.IsConstForm()) {
      const Array<int64_t>& mapping = edge.edge_values();
      typename Buffer<T>::Builder values_builder(mapping.dense_data().size(),
                                                 &ctx->buffer_factory());
      values_builder.SetNConst(0, mapping.dense_data().size(),
                               parent_array.missing_id_value().value);
      return Array<T>(mapping.size(), mapping.id_filter(),
                      DenseArray<T>{std::move(values_builder).Build(),
                                    mapping.dense_data().bitmap,
                                    mapping.dense_data().bitmap_bit_offset},
                      mapping.HasMissingIdValue()
                          ? parent_array.missing_id_value()
                          : std::nullopt);
    }

    // `id_to_offset` is a mapping from `id` in `parent_array` to the position
    // in `parent_array.dense_data()`.
    // Special values:
    //   kDefaultValueOffset - the id doesn't present in `id_filter` and hence
    //       doesn't have corresponding position in dense_data. In this case
    //       the value is `parent_array.missing_id_value()`.
    //   kMissingValueOffset - the id has corresponding position in
    //       `dense_data`, but the value there is missing. The value is missing
    //       regardless if `parent_array.missing_id_value()`.
    constexpr int64_t kDefaultValueOffset = -1;
    constexpr int64_t kMissingValueOffset = -2;
    std::vector<int64_t> id_to_offset(parent_array.size());

    if (parent_array.IsDenseForm()) {
      parent_array.dense_data().ForEach(
          [&](int64_t offset, bool present, view_type_t<T>) {
            id_to_offset[offset] = present ? offset : kMissingValueOffset;
          });
    } else {
      // fill with -1 (kDefaultValueOffset)
      static_assert(kDefaultValueOffset == -1);
      std::memset(id_to_offset.data(), 0xff,
                  id_to_offset.size() * sizeof(int64_t));
      parent_array.dense_data().ForEach(
          [&](int64_t offset, bool present, view_type_t<T>) {
            id_to_offset[parent_array.id_filter().IdsOffsetToId(offset)] =
                present ? offset : kMissingValueOffset;
          });
    }

    int64_t max_new_data_size = edge.edge_values().PresentCount();
    Buffer<int64_t>::Builder ids_bldr(max_new_data_size,
                                      &ctx->buffer_factory());
    auto ids_inserter = ids_bldr.GetInserter();
    // ReshuffleBuilder is important in case of Bytes/Text arrays as it allows
    // to reuse parent_array's string data buffer. Memory usage is reduced since
    // multiple items can point to the same string data location in memory.
    typename Buffer<T>::ReshuffleBuilder values_bldr(
        max_new_data_size, parent_array.dense_data().values,
        parent_array.missing_id_value(), &ctx->buffer_factory());
    int64_t new_offset = 0;

    if (parent_array.HasMissingIdValue()) {
      edge.edge_values().ForEachPresent(
          [&](int64_t child_id, int64_t parent_id) {
            int64_t offset = id_to_offset[parent_id];
            static_assert(kDefaultValueOffset < 0 && kMissingValueOffset < 0);
            if (offset >= 0) {  // negative offsets are special values.
              values_bldr.CopyValue(new_offset, offset);
            }
            if (offset != kMissingValueOffset) {
              ids_inserter.Add(child_id);
              new_offset++;
            }
          });
    } else {
      edge.edge_values().ForEachPresent(
          [&](int64_t child_id, int64_t parent_id) {
            int64_t offset = id_to_offset[parent_id];
            static_assert(kDefaultValueOffset < 0 && kMissingValueOffset < 0);
            if (offset >= 0) {  // negative offsets are special values.
              values_bldr.CopyValue(new_offset++, offset);
              ids_inserter.Add(child_id);
            }
          });
    }

    IdFilter id_filter(edge.child_size(),
                       std::move(ids_bldr).Build(new_offset));
    return Array<T>(edge.child_size(), std::move(id_filter),
                    DenseArray<T>{std::move(values_bldr).Build(new_offset)});
  }

  template <typename T>
  absl::StatusOr<Array<T>> ExpandOverSplitPoints(EvaluationContext* ctx,
                                                 const Array<T>& parent_array,
                                                 const ArrayEdge& edge) const {
    DCHECK_EQ(edge.edge_type(), ArrayEdge::SPLIT_POINTS);
    if (parent_array.IsConstForm()) {
      return Array<T>(edge.child_size(), parent_array.missing_id_value());
    }
    if (parent_array.IsDenseForm()) {
      ASSIGN_OR_RETURN(DenseArray<T> res,
                       DenseArrayExpandOp()(ctx, parent_array.dense_data(),
                                            edge.ToDenseArrayEdge()));
      return Array<T>(std::move(res));
    }

    DCHECK(edge.edge_values().IsFullForm());
    absl::Span<const int64_t> split_points =
        edge.edge_values().dense_data().values.span();
    absl::Span<const IdFilter::IdWithOffset> ids =
        parent_array.id_filter().ids().span();
    const IdFilter& id_filter = parent_array.id_filter();
    int64_t new_dense_size = 0;
    for (IdFilter::IdWithOffset id_with_offset : ids) {
      int64_t id = id_with_offset - id_filter.ids_offset();
      new_dense_size += split_points[id + 1] - split_points[id];
    }
    Buffer<int64_t>::Builder ids_bldr(new_dense_size, &ctx->buffer_factory());
    auto ids_inserter = ids_bldr.GetInserter();
    // ReshuffleBuilder is important in case of Bytes/Text arrays as it allows
    // to reuse parent_array's string data buffer. Memory usage is reduced since
    // multiple items can point to the same string data location in memory.
    typename Buffer<T>::ReshuffleBuilder values_bldr(
        new_dense_size, parent_array.dense_data().values, {},
        &ctx->buffer_factory());
    DenseArray<T> new_dense_data;

    int64_t new_offset = 0;
    if (parent_array.dense_data().bitmap.empty()) {
      for (int64_t offset = 0; offset < ids.size(); ++offset) {
        int64_t id = id_filter.IdsOffsetToId(offset);
        for (int64_t new_id = split_points[id]; new_id < split_points[id + 1];
             ++new_id) {
          ids_inserter.Add(new_id);
        }
        int64_t count = split_points[id + 1] - split_points[id];
        values_bldr.CopyValueToRange(new_offset, new_offset + count, offset);
        new_offset += count;
      }
      new_dense_data.values = std::move(values_bldr).Build();
    } else {
      bitmap::Bitmap::Builder bitmap_bldr(bitmap::BitmapSize(new_dense_size),
                                          &ctx->buffer_factory());
      absl::Span<bitmap::Word> bits = bitmap_bldr.GetMutableSpan();
      std::memset(bits.begin(), 0, bits.size() * sizeof(bitmap::Word));
      const DenseArray<T>& dense_data = parent_array.dense_data();
      for (size_t offset = 0; offset < dense_data.size(); ++offset) {
        int64_t id = id_filter.IdsOffsetToId(offset);
        for (int64_t new_id = split_points[id]; new_id < split_points[id + 1];
             ++new_id) {
          ids_inserter.Add(new_id);
        }
        int64_t count = split_points[id + 1] - split_points[id];
        if (dense_data.present(offset)) {
          values_bldr.CopyValueToRange(new_offset, new_offset + count, offset);
          SetBitsInRange(bits.begin(), new_offset, new_offset + count);
        }
        new_offset += count;
      }
      new_dense_data.values = std::move(values_bldr).Build();
      new_dense_data.bitmap = std::move(bitmap_bldr).Build();
    }

    IdFilter new_id_filter(split_points.back(), std::move(ids_bldr).Build());
    return Array<T>(split_points.back(), std::move(new_id_filter),
                    std::move(new_dense_data), parent_array.missing_id_value());
  }
};

// edge.sizes operator returns an array of sizes corresponding to the number of
// children of each parent index.
struct ArrayEdgeSizesOp {
  absl::StatusOr<Array<int64_t>> operator()(EvaluationContext* ctx,
                                            const ArrayEdge& edge) const {
    Buffer<int64_t>::Builder builder(edge.parent_size(),
                                     &ctx->buffer_factory());
    if (edge.edge_type() == ArrayEdge::EdgeType::SPLIT_POINTS) {
      // All split points are present.
      auto inserter = builder.GetInserter();
      const auto& values = edge.edge_values().dense_data().values;
      for (int64_t i = 1; i < values.size(); ++i) {
        inserter.Add(values[i] - values[i - 1]);
      }
    } else {
      auto sizes = builder.GetMutableSpan();
      std::memset(sizes.begin(), 0, sizes.size() * sizeof(int64_t));
      const Array<int64_t>& mapping = edge.edge_values();
      if (mapping.HasMissingIdValue()) {
        sizes[mapping.missing_id_value().value] =
            mapping.size() - mapping.dense_data().size();
      }
      mapping.dense_data().ForEach(
          [&](int64_t, bool present, int64_t parent_id) {
            if (present) {
              sizes[parent_id]++;
            }
          });
    }
    return Array<int64_t>(DenseArray<int64_t>{std::move(builder).Build()});
  }

  int64_t operator()(const ArrayGroupScalarEdge& edge) const {
    return edge.child_size();
  }
};

// Returns the number of present items
struct ArrayCountOp {
  int64_t operator()(const Array<Unit>& arg,
                     const ArrayGroupScalarEdge&) const {
    return arg.PresentCount();
  }
};

// Returns an edge that maps the unique values of the input array to the same
// group.
struct ArrayGroupByOp {
  template <typename T, typename Edge>
  absl::StatusOr<ArrayEdge> operator()(EvaluationContext* ctx,
                                       const Array<T>& series,
                                       const Edge& over) const {
    int64_t group_counter = 0;
    ArrayGroupOp<GroupByAccumulator<T>> op(
        &ctx->buffer_factory(), GroupByAccumulator<T>(&group_counter));
    ASSIGN_OR_RETURN(Array<int64_t> mapping, op.Apply(over, series));
    return ArrayEdge::UnsafeFromMapping(std::move(mapping), group_counter);
  }
};

// edge._as_dense_array_edge operator
struct ArrayEdgeAsDenseArrayOp {
  absl::StatusOr<DenseArrayEdge> operator()(EvaluationContext* ctx,
                                            const ArrayEdge& edge) const {
    auto dense_array_values = ArrayAsDenseArrayOp()(ctx, edge.edge_values());
    if (edge.edge_type() == ArrayEdge::MAPPING) {
      return DenseArrayEdge::FromMapping(std::move(dense_array_values),
                                         edge.parent_size());
    } else {
      DCHECK(edge.edge_type() == ArrayEdge::SPLIT_POINTS);
      return DenseArrayEdge::FromSplitPoints(std::move(dense_array_values));
    }
  }
  DenseArrayGroupScalarEdge operator()(EvaluationContext* ctx,
                                       const ArrayGroupScalarEdge& edge) const {
    return DenseArrayGroupScalarEdge(edge.child_size());
  }
};

// TODO: Can we reduce duplication with dense_array/edge_ops.h and
// move to qexpr/operators/array_like?
//
// Given a SPLIT_POINTS edge, resizes and reorders the items within each
// group, and returns an edge between the new id space and the child id space of
// the old edge.
struct ArrayEdgeResizeGroupsChildSide {
  template <typename NewSizeGetterFn>
  absl::StatusOr<ArrayEdge> FromSplitPoints(
      EvaluationContext* ctx, absl::Span<const int64_t> split_points,
      NewSizeGetterFn new_size_getter, int64_t total_size,
      const Array<int64_t>& offsets) const {
    int64_t child_size = split_points.back();
    if (total_size < 0) {
      return absl::InvalidArgumentError(
          "got a negative size value in operator "
          "edge.resize_groups_child_side");
    }
    DenseArrayBuilder<int64_t> builder(total_size, &ctx->buffer_factory());
    std::vector<bool> set_indices(total_size);
    if (split_points.size() < 2) {
      return ArrayEdge();
    }
    int64_t split_point_idx = 1;
    int64_t new_base_id = 0;
    int64_t new_group_size = new_size_getter(0);
    bool negative_offset = false;
    bool duplicate_offsets = false;
    offsets.ForEachPresent([&](int64_t old_id, int64_t new_offset) {
      while (old_id >= split_points[split_point_idx]) {
        new_base_id += new_group_size;
        ++split_point_idx;
        new_group_size = new_size_getter(split_point_idx - 1);
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

    return ArrayEdge::FromMapping(
        Array<int64_t>(DenseArray<int64_t>{std::move(builder).Build()}),
        child_size);
  }

  absl::Status CheckEdge(const ArrayEdge& edge) const {
    DCHECK(edge.edge_values().dense_data().IsFull());
    if (edge.edge_type() != ArrayEdge::SPLIT_POINTS) {
      return absl::UnimplementedError(
          "operator edge.resize_groups_child_side is only supported for "
          "SPLIT_POINTS edges");
    }
    return absl::OkStatus();
  }

  absl::Status CheckNewSizes(const Array<int64_t>& new_sizes,
                             const ArrayEdge& edge) const {
    if (!new_sizes.IsFullForm()) {
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
  absl::Status CheckNewOffsets(const Array<int64_t>& new_offsets,
                               const EdgeType& edge) const {
    if (new_offsets.size() != edge.child_size()) {
      return absl::InvalidArgumentError(
          "`new_offsets` argument should be the same size as the child side of "
          "the edge in edge.resize_groups_child_side");
    }
    return absl::OkStatus();
  }

  absl::StatusOr<ArrayEdge> operator()(
      EvaluationContext* ctx, const ArrayEdge& edge, int64_t new_size,
      const Array<int64_t>& new_offsets) const {
    RETURN_IF_ERROR(CheckEdge(edge));
    RETURN_IF_ERROR(CheckNewOffsets(new_offsets, edge));
    return FromSplitPoints(
        ctx, edge.edge_values().dense_data().values.span(),
        [&new_size](int64_t _) { return new_size; },
        new_size * edge.parent_size(), new_offsets);
  }

  absl::StatusOr<ArrayEdge> operator()(
      EvaluationContext* ctx, const ArrayGroupScalarEdge& scalar_edge,
      int64_t new_size, const Array<int64_t>& new_offsets) const {
    RETURN_IF_ERROR(CheckNewOffsets(new_offsets, scalar_edge));
    return FromSplitPoints(
        ctx, {0, scalar_edge.child_size()},
        [&new_size](int64_t) { return new_size; }, new_size, new_offsets);
  }

  absl::StatusOr<ArrayEdge> operator()(
      EvaluationContext* ctx, const ArrayEdge& edge,
      const Array<int64_t>& new_sizes,
      const Array<int64_t>& new_offsets) const {
    RETURN_IF_ERROR(CheckEdge(edge));
    RETURN_IF_ERROR(CheckNewSizes(new_sizes, edge));
    RETURN_IF_ERROR(CheckNewOffsets(new_offsets, edge));
    const auto& new_sizes_buffer = new_sizes.dense_data().values;
    int64_t total_size = 0;
    for (int64_t i = 0; i < new_sizes_buffer.size(); ++i) {
      total_size += new_sizes_buffer[i];
    }

    DCHECK(edge.edge_values().dense_data().IsFull());
    return FromSplitPoints(
        ctx, edge.edge_values().dense_data().values.span(),
        [&new_sizes_buffer](int64_t idx) { return new_sizes_buffer[idx]; },
        total_size, new_offsets);
  }
};

// Given a SPLIT_POINTS edge and a new_size, truncates or pads the number of
// child items per parent item to the given size, and returns an edge between
// the new id space and the parent space of the old edge.
struct ArrayEdgeResizeGroupsParentSide {
  absl::StatusOr<ArrayEdge> operator()(EvaluationContext* ctx,
                                       const ArrayEdge& edge,
                                       int64_t new_size) const {
    if (edge.edge_type() != ArrayEdge::SPLIT_POINTS) {
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
    return ArrayEdge::FromSplitPoints(Array<int64_t>{std::move(bldr).Build()});
  }

  absl::StatusOr<ArrayGroupScalarEdge> operator()(
      EvaluationContext* ctx, const ArrayGroupScalarEdge& scalar_edge,
      int64_t new_size) const {
    if (new_size < 0) {
      return absl::InvalidArgumentError(
          "`size` argument should be a non-negative integer for operator "
          "edge.resize_groups_parent_side");
    }

    return ArrayGroupScalarEdge(new_size);
  }

  absl::StatusOr<ArrayEdge> operator()(EvaluationContext* ctx,
                                       const ArrayEdge& edge,
                                       const Array<int64_t>& new_sizes) const {
    if (edge.edge_type() != ArrayEdge::SPLIT_POINTS) {
      return absl::UnimplementedError(
          "operator edge.resize_groups_parent_side is only supported for "
          "SPLIT_POINTS edges");
    }
    if (new_sizes.size() != edge.parent_size()) {
      return absl::InvalidArgumentError(
          "number of new sizes should match number of edge parent-side groups "
          "in operator edge.resize_groups_parent_side");
    }
    return ArrayEdgeFromSizesOp()(ctx, new_sizes);
  }
};

// `edge.compose._array` operator.
class ArrayEdgeComposeOperatorFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_qtypes,
      QTypePtr output_qtype) const final {
    return EnsureOutputQTypeMatches(
        OperatorPtr(std::make_unique<EdgeComposeOperator<ArrayEdge>>(
            input_qtypes.size())),
        input_qtypes, output_qtype);
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_ARRAY_EDGE_OPS_H_
