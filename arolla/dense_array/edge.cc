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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace {

absl::StatusOr<DenseArrayEdge> ComposeMappingEdge(
    absl::Span<const DenseArrayEdge> edges, RawBufferFactory& buf_factory) {
  DCHECK_GE(edges.size(), 2);
  DenseArray<int64_t> mapping =
      edges.back().ToMappingEdge(buf_factory).edge_values();
  for (int i = edges.size() - 2; i >= 0; --i) {
    const auto mapping_edge = edges[i].ToMappingEdge(buf_factory);
    DenseArrayBuilder<int64_t> bldr(edges.back().child_size(), &buf_factory);
    mapping.ForEachPresent([&bldr, &mapping_edge](int64_t id, int64_t value) {
      bldr.Set(id, mapping_edge.edge_values()[value]);
    });
    mapping = std::move(bldr).Build();
  }
  return DenseArrayEdge::UnsafeFromMapping(std::move(mapping),
                                           edges.front().parent_size());
}

absl::StatusOr<DenseArrayEdge> ComposeSplitPointsEdge(
    absl::Span<const DenseArrayEdge> edges, RawBufferFactory& buf_factory) {
  DCHECK_GE(edges.size(), 2);
  Buffer<int64_t>::Builder bldr(edges.front().edge_values().size(),
                                &buf_factory);
  auto mut_bldr_span = bldr.GetMutableSpan();
  auto previous_split_points = edges.front().edge_values().values.span();
  for (size_t i = 1; i < edges.size(); ++i) {
    auto split_points = edges[i].edge_values().values.span();
    for (size_t j = 0; j < mut_bldr_span.size(); ++j) {
      mut_bldr_span[j] = split_points[previous_split_points[j]];
    }
    previous_split_points = mut_bldr_span;
  }
  return DenseArrayEdge::UnsafeFromSplitPoints({std::move(bldr).Build()});
}

}  // namespace

absl::StatusOr<DenseArrayEdge> DenseArrayEdge::FromSplitPoints(
    DenseArray<int64_t> split_points) {
  if (!split_points.IsFull()) {
    return absl::InvalidArgumentError("split points must be full");
  }
  if (split_points.empty()) {
    return absl::InvalidArgumentError(
        "split points array must have at least 1 element");
  }
  if (split_points.values[0] != 0) {
    return absl::InvalidArgumentError(
        "split points array must have first element equal to 0");
  }
  int64_t parent_size = split_points.size() - 1;
  int64_t child_size = split_points.values.back();
  if (!std::is_sorted(split_points.values.begin(), split_points.values.end())) {
    return absl::InvalidArgumentError("split points must be sorted");
  }
  return DenseArrayEdge(DenseArrayEdge::SPLIT_POINTS, parent_size, child_size,
                        std::move(split_points));
}

absl::StatusOr<DenseArrayEdge> DenseArrayEdge::FromMapping(
    DenseArray<int64_t> mapping, int64_t parent_size) {
  if (parent_size < 0) {
    return absl::InvalidArgumentError("parent_size can not be negative");
  }
  int64_t max_value = -1;
  bool negative = false;
  mapping.ForEach([&max_value, &negative](int64_t, bool present, int64_t v) {
    if (present) {
      max_value = std::max(max_value, v);
      if (v < 0) negative = true;
    }
  });
  if (negative) {
    return absl::InvalidArgumentError("mapping can't contain negative values");
  }
  if (max_value >= parent_size) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "parent_size=%d, but parent id %d is used", parent_size, max_value));
  }
  return UnsafeFromMapping(std::move(mapping), parent_size);
}

absl::StatusOr<DenseArrayEdge> DenseArrayEdge::FromUniformGroups(
    int64_t parent_size, int64_t group_size, RawBufferFactory& buf_factory) {
  if (parent_size < 0 || group_size < 0) {
    return absl::InvalidArgumentError(
        "parent_size and group_size cannot be negative");
  }

  Buffer<int64_t>::Builder split_points_builder(parent_size + 1, &buf_factory);
  auto inserter = split_points_builder.GetInserter();
  for (int64_t i = 0; i <= parent_size; ++i) inserter.Add(i * group_size);
  return UnsafeFromSplitPoints({std::move(split_points_builder).Build()});
}

DenseArrayEdge DenseArrayEdge::UnsafeFromMapping(DenseArray<int64_t> mapping,
                                                 int64_t parent_size) {
  int64_t child_size = mapping.size();
  return DenseArrayEdge(DenseArrayEdge::MAPPING, parent_size, child_size,
                        std::move(mapping));
}

DenseArrayEdge DenseArrayEdge::UnsafeFromSplitPoints(
    DenseArray<int64_t> split_points) {
  int64_t parent_size = split_points.size() - 1;
  int64_t child_size = split_points.values.back();
  return DenseArrayEdge(DenseArrayEdge::SPLIT_POINTS, parent_size, child_size,
                        std::move(split_points));
}

DenseArrayEdge DenseArrayEdge::ToMappingEdge(
    RawBufferFactory& buf_factory) const {
  switch (edge_type()) {
    case DenseArrayEdge::MAPPING:
      return *this;
    case DenseArrayEdge::SPLIT_POINTS: {
      Buffer<int64_t>::Builder bldr(child_size(), &buf_factory);
      int64_t* mapping = bldr.GetMutableSpan().begin();
      const int64_t* splits = edge_values().values.begin();
      for (int64_t parent_id = 0; parent_id < parent_size(); ++parent_id) {
        std::fill(mapping + splits[parent_id], mapping + splits[parent_id + 1],
                  parent_id);
      }
      return DenseArrayEdge::UnsafeFromMapping({std::move(bldr).Build()},
                                               parent_size());
    }
  }
  ABSL_UNREACHABLE();
}

absl::StatusOr<DenseArrayEdge> DenseArrayEdge::ToSplitPointsEdge(
    RawBufferFactory& buf_factory) const {
  if (edge_type() == DenseArrayEdge::SPLIT_POINTS) return *this;
  if (!edge_values().IsFull()) {
    return absl::InvalidArgumentError("expected a full mapping");
  }
  Buffer<int64_t>::Builder split_points_builder(parent_size() + 1,
                                                &buf_factory);
  auto inserter = split_points_builder.GetInserter();
  inserter.Add(0);
  int64_t current_bin = 0;
  auto values = edge_values().values.span();
  for (size_t i = 0; i < values.size(); ++i) {
    // This can only happen if UnsafeFromMapping was used to construct the edge,
    // and it's the responsibility of the user to ensure it's correct.
    DCHECK_LE(values[i], parent_size());
    if (values[i] < current_bin) {
      return absl::InvalidArgumentError("expected a sorted mapping");
    }
    for (; current_bin < values[i]; ++current_bin) inserter.Add(i);
  }
  for (; current_bin < parent_size(); ++current_bin) {
    inserter.Add(edge_values().size());
  }
  return DenseArrayEdge::UnsafeFromSplitPoints(
      DenseArray<int64_t>{std::move(split_points_builder).Build()});
}

bool DenseArrayEdge::IsEquivalentTo(const DenseArrayEdge& other) const {
  if (parent_size() != other.parent_size() ||
      child_size() != other.child_size()) {
    return false;
  }
  if (edge_type() == other.edge_type()) {
    return ArraysAreEquivalent(edge_values(), other.edge_values());
  }
  // Both edges must be representable with split points if they are equivalent.
  // We attempt this conversion, rather than conversion to mapping, to avoid
  // blow-up.
  ASSIGN_OR_RETURN(auto this_edge, ToSplitPointsEdge(),
                   _.With([](const auto&) { return false; }));
  ASSIGN_OR_RETURN(auto other_edge, other.ToSplitPointsEdge(),
                   _.With([](const auto&) { return false; }));
  return ArraysAreEquivalent(this_edge.edge_values(), other_edge.edge_values());
}

absl::StatusOr<DenseArrayEdge> DenseArrayEdge::ComposeEdges(
    absl::Span<const DenseArrayEdge> edges, RawBufferFactory& buf_factory) {
  if (edges.empty()) {
    return absl::InvalidArgumentError("at least one edge must be present");
  }
  if (edges.size() == 1) return edges[0];
  int64_t prior_child_size = edges[0].parent_size();
  for (size_t i = 0; i < edges.size(); ++i) {
    if (edges[i].parent_size() != prior_child_size) {
      return absl::InvalidArgumentError(
          absl::StrFormat("incompatible edges: edges[%d].child_size (%d) != "
                          "edges[%d].parent_size (%d)",
                          i - 1, prior_child_size, i, edges[i].parent_size()));
    }
    prior_child_size = edges[i].child_size();
  }
  // Compose consecutive split point edges using the split point algorithm, then
  // compose the remaining edges using the mapping algorithm.
  std::vector<DenseArrayEdge> transformed_edges;
  transformed_edges.reserve(edges.size());
  size_t i = 0;
  while (i < edges.size()) {
    // Find consecutive split point edges starting at `i`.
    size_t split_points_end = i;
    while (split_points_end < edges.size() &&
           edges[split_points_end].edge_type() ==
               DenseArrayEdge::SPLIT_POINTS) {
      split_points_end++;
    }
    if (split_points_end - i >= 2) {
      // Combine two (or more) split point edges into one.
      ASSIGN_OR_RETURN(auto composed_edge,
                       ComposeSplitPointsEdge(
                           absl::MakeSpan(edges.begin() + i,
                                          edges.begin() + split_points_end),
                           buf_factory));
      transformed_edges.push_back(std::move(composed_edge));
      i = split_points_end;
    } else {
      // Otherwise, we need to use the mapping algorithm.
      transformed_edges.push_back(edges[i]);
      i++;
    }
  }
  if (transformed_edges.size() == 1) {
    return std::move(transformed_edges[0]);
  } else {
    return ComposeMappingEdge(transformed_edges, buf_factory);
  }
}

void FingerprintHasherTraits<DenseArrayEdge>::operator()(
    FingerprintHasher* hasher, const DenseArrayEdge& value) const {
  hasher->Combine(value.edge_type(), value.parent_size(), value.child_size(),
                  value.edge_values());
}

void FingerprintHasherTraits<DenseArrayGroupScalarEdge>::operator()(
    FingerprintHasher* hasher, const DenseArrayGroupScalarEdge& value) const {
  hasher->Combine(value.child_size());
}

}  // namespace arolla
