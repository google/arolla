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
#include "arolla/array/edge.h"

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

absl::StatusOr<ArrayEdge> ArrayEdge::FromSplitPoints(
    Array<int64_t> split_points) {
  split_points = split_points.ToDenseForm();
  if (split_points.empty()) {
    return absl::InvalidArgumentError(
        "split points array should have at least 1 element");
  }
  if (split_points.size() != split_points.dense_data().size() ||
      !split_points.dense_data().IsFull()) {
    return absl::InvalidArgumentError("split points should be full");
  }
  if (split_points.dense_data().values[0] != 0) {
    return absl::InvalidArgumentError(
        "split points array should have first element equal to 0");
  }
  int64_t parent_size = split_points.size() - 1;
  int64_t child_size = split_points.dense_data().values.back();
  if (!std::is_sorted(split_points.dense_data().values.begin(),
                      split_points.dense_data().values.end())) {
    return absl::InvalidArgumentError("split points should be sorted");
  }
  return ArrayEdge(ArrayEdge::SPLIT_POINTS, parent_size, child_size,
                   std::move(split_points));
}

ArrayEdge ArrayEdge::UnsafeFromSplitPoints(Array<int64_t> split_points) {
  split_points = split_points.ToDenseForm();
  int64_t parent_size = split_points.size() - 1;
  int64_t child_size = split_points.dense_data().values.back();
  return ArrayEdge(ArrayEdge::SPLIT_POINTS, parent_size, child_size,
                   std::move(split_points));
}

absl::StatusOr<ArrayEdge> ArrayEdge::FromMapping(Array<int64_t> mapping,
                                                 int64_t parent_size) {
  if (parent_size < 0) {
    return absl::InvalidArgumentError("parent_size can not be negative");
  }
  int64_t max_value = -1;
  bool negative = false;
  mapping.ForEachPresent([&max_value, &negative](int64_t, int64_t v) {
    max_value = std::max(max_value, v);
    if (v < 0) negative = true;
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

absl::StatusOr<ArrayEdge> ArrayEdge::FromUniformGroups(
    int64_t parent_size, int64_t group_size, RawBufferFactory& buf_factory) {
  ASSIGN_OR_RETURN(
      DenseArrayEdge edge,
      DenseArrayEdge::FromUniformGroups(parent_size, group_size, buf_factory));
  return ArrayEdge::FromDenseArrayEdge(std::move(edge));
}

ArrayEdge ArrayEdge::UnsafeFromMapping(Array<int64_t> mapping,
                                       int64_t parent_size) {
  int64_t child_size = mapping.size();
  return ArrayEdge(ArrayEdge::MAPPING, parent_size, child_size,
                   std::move(mapping));
}

ArrayEdge ArrayEdge::ToMappingEdge(RawBufferFactory& buf_factory) const {
  if (edge_type() == ArrayEdge::MAPPING) return *this;
  return ArrayEdge::FromDenseArrayEdge(
      ToDenseArrayEdge(buf_factory).ToMappingEdge(buf_factory));
}

absl::StatusOr<ArrayEdge> ArrayEdge::ToSplitPointsEdge(
    RawBufferFactory& buf_factory) const {
  if (edge_type() == ArrayEdge::SPLIT_POINTS) return *this;
  ASSIGN_OR_RETURN(
      DenseArrayEdge edge,
      ToDenseArrayEdge(buf_factory).ToSplitPointsEdge(buf_factory));
  return ArrayEdge::FromDenseArrayEdge(std::move(edge));
}

absl::StatusOr<ArrayEdge> ArrayEdge::ComposeEdges(
    absl::Span<const ArrayEdge> edges, RawBufferFactory& buf_factory) {
  // Note: We offload to the DenseArray implementation. This is "free" for
  // split points and fast for non-sparse mappings as well.
  std::vector<DenseArrayEdge> converted_edges;
  converted_edges.reserve(edges.size());
  for (const auto& edge : edges) {
    converted_edges.push_back(edge.ToDenseArrayEdge(buf_factory));
  }
  ASSIGN_OR_RETURN(DenseArrayEdge edge,
                   DenseArrayEdge::ComposeEdges(converted_edges, buf_factory));
  return ArrayEdge::FromDenseArrayEdge(std::move(edge));
}

bool ArrayEdge::IsEquivalentTo(const ArrayEdge& other) const {
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

void FingerprintHasherTraits<ArrayEdge>::operator()(
    FingerprintHasher* hasher, const ArrayEdge& value) const {
  hasher->Combine(value.edge_type(), value.parent_size(), value.child_size(),
                  value.edge_values());
}

void FingerprintHasherTraits<ArrayGroupScalarEdge>::operator()(
    FingerprintHasher* hasher, const ArrayGroupScalarEdge& value) const {
  hasher->Combine(value.child_size());
}

}  // namespace arolla
