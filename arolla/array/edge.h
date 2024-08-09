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
#ifndef AROLLA_ARRAY_EDGE_H_
#define AROLLA_ARRAY_EDGE_H_

#include <cstdint>
#include <optional>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/api.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

// An array edge represents a mapping of the rows of one Array onto another.
class AROLLA_API ArrayEdge {
 public:
  using Values = Array<int64_t>;

  ArrayEdge()
      : edge_type_(MAPPING),
        parent_size_(0),
        child_size_(0),
        edge_values_(0, std::nullopt) {}

  // Creates a ArrayEdge from a Array of `split_points`, which must be
  // full and sorted. The size of the split points array should be equal to
  // the size of the parent index plus one additional value at the end.
  // The size is used to infer the size of the associated child index.
  static absl::StatusOr<ArrayEdge> FromSplitPoints(Array<int64_t> split_points);

  // Creates an ArrayEdge from an Array of `split_points` _without_
  // performing validation, making it possible to create invalid edges.
  static ArrayEdge UnsafeFromSplitPoints(Array<int64_t> split_points);

  // Creates an ArrayEdge with a uniform number of children per parent. The
  // resulting edge is always a SPLIT_POINT edge. Requires parent_size >= 0 and
  // group_size >= 0.
  static absl::StatusOr<ArrayEdge> FromUniformGroups(
      int64_t parent_size, int64_t group_size,
      RawBufferFactory& buf_factory = *GetHeapBufferFactory());

  // Creates a ArrayEdge from a mapping from the child row IDs into parent
  // row IDs. The mapping may be sparse, and in any order. The parent row IDs
  // stored in the mapping must be within the range [0, parent_size).
  static absl::StatusOr<ArrayEdge> FromMapping(Array<int64_t> mapping,
                                               int64_t parent_size);

  static ArrayEdge UnsafeFromMapping(Array<int64_t> mapping,
                                     int64_t parent_size);

  // Composes several edges A->B, B->C, ... Y->Z into A->Z, when each edge is
  // viewed as a one-to-many parent-to-child mapping.
  //  - edges[i].child_size() == edges[i + 1].parent_size(), for all i.
  //  - If any edge is a MAPPING edge, the result is a MAPPING edge. Otherwise,
  //  it's a SPLIT_POINTS edge.
  static absl::StatusOr<ArrayEdge> ComposeEdges(
      absl::Span<const ArrayEdge> edges,
      RawBufferFactory& buf_factory = *GetHeapBufferFactory());

  using EdgeType = DenseArrayEdge::EdgeType;
  static constexpr EdgeType MAPPING =  // NOLINT (enum alias)
      DenseArrayEdge::MAPPING;
  static constexpr EdgeType SPLIT_POINTS =  // NOLINT (enum alias)
      DenseArrayEdge::SPLIT_POINTS;

  // Returns the mapping type of this ArrayEdge.
  EdgeType edge_type() const { return edge_type_; }

  // Returns the size of the associated parent index.
  int64_t parent_size() const { return parent_size_; }

  // Returns the size of the associated child index.
  int64_t child_size() const { return child_size_; }

  // Returns the raw edge values whose interpretation depends on edge_type().
  // For SPLIT_POINT edges, this will always be full and sorted. For MAPPING
  // edges, it may be sparse and/or unsorted.
  const Array<int64_t>& edge_values() const { return edge_values_; }

  // Returns the number of child rows that correspond to parent row `i`.
  // Requires that this is a SPLIT_POINT edge.
  int64_t split_size(int64_t i) const {
    DCHECK_EQ(edge_type_, SPLIT_POINTS);
    DCHECK_GE(i, 0);
    DCHECK_LT(i, edge_values_.dense_data().size() - 1);
    return edge_values_.dense_data().values[i + 1] -
           edge_values_.dense_data().values[i];
  }

  // Creates a DenseArrayEdge from an ArrayEdge.
  DenseArrayEdge ToDenseArrayEdge(
      RawBufferFactory& buf_factory = *GetHeapBufferFactory()) const {
    return DenseArrayEdge(edge_type_, parent_size_, child_size_,
                          edge_values_.ToDenseForm(&buf_factory).dense_data());
  }

  // Creates an ArrayEdge from a DenseArrayEdge.
  static ArrayEdge FromDenseArrayEdge(const DenseArrayEdge& edge) {
    return ArrayEdge{edge.edge_type(), edge.parent_size(), edge.child_size(),
                     Array<int64_t>{edge.edge_values()}};
  }

  // Converts the edge to a SPLIT_POINTS edge. Requires the underlying mapping
  // to be full and sorted. Split point edges will be returned as-is.
  absl::StatusOr<ArrayEdge> ToSplitPointsEdge(
      RawBufferFactory& buf_factory = *GetHeapBufferFactory()) const;

  // Converts the edge to a MAPPING edge. Mapping edges will be returned as-is.
  ArrayEdge ToMappingEdge(
      RawBufferFactory& buf_factory = *GetHeapBufferFactory()) const;

  // Returns true iff this edge represents the same edge as other.
  bool IsEquivalentTo(const ArrayEdge& other) const;

 private:
  ArrayEdge(EdgeType edge_values_type, int64_t parent_size, int64_t child_size,
            Array<int64_t> edge_values)
      : edge_type_(edge_values_type),
        parent_size_(parent_size),
        child_size_(child_size),
        edge_values_(std::move(edge_values)) {}

  EdgeType edge_type_;
  int64_t parent_size_;
  int64_t child_size_;
  Array<int64_t> edge_values_;
};

// A ArrayGroupScalarEdge represents a mapping of a Array to a scalar.
class AROLLA_API ArrayGroupScalarEdge {
 public:
  ArrayGroupScalarEdge() : size_(0) {}
  explicit ArrayGroupScalarEdge(int64_t size) : size_{size} {}

  int64_t child_size() const { return size_; }

  DenseArrayGroupScalarEdge ToDenseArrayGroupScalarEdge() const {
    return DenseArrayGroupScalarEdge(size_);
  }

 private:
  int64_t size_;
};

// Note that the fingerprint for two Edges representing identical mappings are
// not guaranteed to be equal. For example, a MAPPING edge will not have the
// same hash value as an equivalent SPLIT_POINTS edge.
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(ArrayEdge);

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(ArrayGroupScalarEdge);

}  // namespace arolla

#endif  // AROLLA_ARRAY_EDGE_H_
