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
#ifndef AROLLA_DENSE_ARRAY_EDGE_H_
#define AROLLA_DENSE_ARRAY_EDGE_H_

#include <cstdint>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/api.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

// A block edge represents a mapping of the rows of one DenseArray onto another.
class AROLLA_API DenseArrayEdge {
 public:
  using Values = DenseArray<int64_t>;

  DenseArrayEdge() : edge_type_(MAPPING), parent_size_(0), child_size_(0) {}

  // Creates a DenseArrayEdge from a DenseArray of `split_points`, which must be
  // full and sorted. The size of the split points array should be equal to
  // the size of the parent index plus one additional value at the end.
  // The size is used to infer the size of the associated child index.
  static absl::StatusOr<DenseArrayEdge> FromSplitPoints(
      DenseArray<int64_t> split_points);

  // Creates a DenseArrayEdge from a mapping from the child row IDs into parent
  // row IDs. The mapping may be sparse, and in any order. The parent row IDs
  // stored in the mapping must be within the range [0, parent_size).
  static absl::StatusOr<DenseArrayEdge> FromMapping(DenseArray<int64_t> mapping,
                                                    int64_t parent_size);

  // Creates a DenseArrayEdge with a uniform number of children per parent. The
  // resulting edge is always a SPLIT_POINT edge. Requires parent_size >= 0 and
  // group_size >= 0.
  static absl::StatusOr<DenseArrayEdge> FromUniformGroups(
      int64_t parent_size, int64_t group_size,
      RawBufferFactory& buf_factory = *GetHeapBufferFactory());

  // Creates a DenseArrayEdge from a mapping from the child row IDs into parent
  // row IDs _without_ performing validation, making it possible to create
  // invalid edges.
  static DenseArrayEdge UnsafeFromMapping(DenseArray<int64_t> mapping,
                                          int64_t parent_size);

  // Creates a DenseArrayEdge from a DenseArray of `split_points` _without_
  // performing validation, making it possible to create invalid edges.
  static DenseArrayEdge UnsafeFromSplitPoints(DenseArray<int64_t> split_points);

  // Composes several edges A->B, B->C, ... Y->Z into A->Z, when each edge is
  // viewed as a one-to-many parent-to-child mapping.
  //  - edges[i].child_size() == edges[i + 1].parent_size(), for all i.
  //  - If any edge is a MAPPING edge, the result is a MAPPING edge. Otherwise,
  //  it's a SPLIT_POINTS edge.
  static absl::StatusOr<DenseArrayEdge> ComposeEdges(
      absl::Span<const DenseArrayEdge> edges,
      RawBufferFactory& buf_factory = *GetHeapBufferFactory());

  enum EdgeType {
    // Edge is represented by an array of parent index RowIds corresponding
    // to each child index RowId.
    MAPPING = 1,

    // Edge is represented by an array of RowIds containing the 'split points'
    // of contiguous ranges of rows in the child index corresponding to
    // individual rows in the parent index.
    SPLIT_POINTS = 2,
  };

  // Returns the mapping type of this DenseArrayEdge.
  EdgeType edge_type() const { return edge_type_; }

  // Returns the size of the associated parent index.
  int64_t parent_size() const { return parent_size_; }

  // Returns the size of the associated child index.
  int64_t child_size() const { return child_size_; }

  // Returns the raw edge values whose interpretation depends on edge_type().
  // For SPLIT_POINT edges, this will always be full and sorted. For MAPPING
  // edges, it may be sparse and/or unsorted.
  const DenseArray<int64_t>& edge_values() const { return edge_values_; }

  // Returns the number of child rows that correspond to parent row `i`.
  // Requires that this is a SPLIT_POINT edge.
  int64_t split_size(int64_t i) const {
    DCHECK_EQ(edge_type_, SPLIT_POINTS);
    DCHECK_GE(i, 0);
    DCHECK_LT(i, edge_values_.size() - 1);
    return edge_values_.values[i + 1] - edge_values_.values[i];
  }

  // Converts the edge to a SPLIT_POINTS edge. Requires the underlying mapping
  // to be full and sorted. Split point edges will be returned as-is.
  absl::StatusOr<DenseArrayEdge> ToSplitPointsEdge(
      RawBufferFactory& buf_factory = *GetHeapBufferFactory()) const;

  // Converts the edge to a MAPPING edge. Mapping edges will be returned as-is.
  DenseArrayEdge ToMappingEdge(
      RawBufferFactory& buf_factory = *GetHeapBufferFactory()) const;

  // Returns true iff this edge represents the same edge as other.
  bool IsEquivalentTo(const DenseArrayEdge& other) const;

 private:
  friend class ArrayEdge;
  DenseArrayEdge(EdgeType edge_values_type, int64_t parent_size,
                 int64_t child_size, DenseArray<int64_t> edge_values)
      : edge_type_(edge_values_type),
        parent_size_(parent_size),
        child_size_(child_size),
        edge_values_(std::move(edge_values)) {}

  EdgeType edge_type_;
  int64_t parent_size_;
  int64_t child_size_;
  DenseArray<int64_t> edge_values_;
};

// A DenseArrayGroupScalarEdge represents a mapping of a DenseArray to a scalar.
class AROLLA_API DenseArrayGroupScalarEdge {
 public:
  DenseArrayGroupScalarEdge() : size_(0) {}
  explicit DenseArrayGroupScalarEdge(int64_t size) : size_{size} {}

  int64_t child_size() const { return size_; }

 private:
  int64_t size_;
};

// Note that the fingerprint for two Edges representing identical mappings are
// not guaranteed to be equal. For example, a MAPPING edge will not have the
// same hash value as an equivalent SPLIT_POINTS edge.
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(DenseArrayEdge);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(DenseArrayGroupScalarEdge);

}  // namespace arolla

#endif  // AROLLA_DENSE_ARRAY_EDGE_H_
