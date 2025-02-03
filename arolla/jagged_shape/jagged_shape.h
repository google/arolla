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
#ifndef AROLLA_JAGGED_SHAPE_JAGGED_SHAPE_H_
#define AROLLA_JAGGED_SHAPE_JAGGED_SHAPE_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// FastEquivalenceResult contains non full information about
// equivalence of the shape.
// Such information can be computed quickly.
class JaggedShapeFastEquivalenceResult {
 public:
  enum Result { kNotEq = 0, kEq = 1, kSizesEq = 2 };

  explicit JaggedShapeFastEquivalenceResult(Result result) : result_(result) {}

  bool IsGuaranteedNotEq() const { return result_ == kNotEq; }
  bool IsGuaranteedEq() const { return result_ == kEq; }

  // Returns true if all total edge sizes are equal.
  // Some operations may trade safety to performance by proceeding
  // with this low cost verification.
  bool AreAllSizesEqual() const { return result_ != kNotEq; }

  bool operator==(const JaggedShapeFastEquivalenceResult& other) const {
    return result_ == other.result_;
  }

 private:
  Result result_;
};

// Shape that represents multidimensional jagged data. Each dimension `i` is
// represented using an array-to-array Edge with one edge per dimension
// (edges().size() == rank()). edges[i + 1] specifies how to partition the rows
// in the i'th dimension, partitioning edges[i + 1].parent_size() rows in the
// i'th dimension into edges[i + 1].child_size() rows in the (i+1)th dimension.
//
// The shape is represented (printed) using _sizes_, where uniform edges (where
// all splits are the same size) are represented by a single value.
//
// Requirements for each edge:
//   - edges[0].parent_size() == 1
//   - edges[i + 1].parent_size() == edges[i].child_size(), for all i.
//   - edges[i] must be representable using split points.
//     - mapping edges will be converted to split point edges.
//
// See go/jagged-shape for details.
template <typename EdgeT>
class JaggedShape {
  struct PrivateConstructorTag {};

 public:
  // Note: using absl::InlinedVector<Edge, 4> is generally slower for our uses.
  // This includes construction of empty shapes since RefcountPtr is used. If
  // e.g. JaggedShape::Empty is changed to not return a RefcountPtr,
  // absl::InlinedVector will likely be faster again.
  using EdgeVec = std::vector<EdgeT>;
  using Edge = EdgeT;

  // Creates an empty shape (rank 0, size 1).
  static const JaggedShape& Empty() {
    static absl::NoDestructor<JaggedShape> ptr;
    return *ptr;
  }

  // Creates a JaggedShape from edges, and ensures that the resulting shape is
  // sound. Requirements:
  //   - edges[0].parent_size() == 1
  //   - edges[i + 1].parent_size() == edges[i].child_size(), for all i.
  //   - edges[i] must be representable using split points.
  //     - mapping edges will be converted to split point edges.
  //
  // `buf_factory` specifies the memory location of the converted split points.
  static absl::StatusOr<JaggedShape> FromEdges(
      EdgeVec edges, RawBufferFactory& buf_factory = *GetHeapBufferFactory()) {
    if (edges.empty()) return Empty();
    int64_t child_size = 1;
    for (size_t i = 0; i < edges.size(); ++i) {
      if (edges[i].parent_size() != child_size) {
        return absl::InvalidArgumentError(
            absl::StrFormat("incompatible dimensions - edges[%d].parent_size "
                            "!= %d (prior edge's child_size)",
                            i, child_size));
      }
      if (edges[i].edge_type() != Edge::SPLIT_POINTS) {
        ASSIGN_OR_RETURN(edges[i], edges[i].ToSplitPointsEdge(buf_factory));
      }
      child_size = edges[i].child_size();
    }
    return JaggedShape(std::move(edges));
  }

  // Creates a 1-dimensional JaggedShape from the size. This is especially
  // useful when creating a JaggedShape representing Array / DenseArray values.
  static JaggedShape FlatFromSize(
      int64_t size, RawBufferFactory& buf_factory = *GetHeapBufferFactory()) {
    auto edge_or = Edge::FromUniformGroups(/*parent_size=*/1,
                                           /*group_size=*/size, buf_factory);
    DCHECK_OK(edge_or.status());  // Cannot fail for valid shapes.
    auto shape_or = FromEdges({*std::move(edge_or)});
    DCHECK_OK(shape_or.status());  // Cannot fail for valid shapes.
    return *std::move(shape_or);
  }

  // Returns the rank of the shape.
  size_t rank() const { return impl_->edges.size(); }

  // Returns the size of the shape, which equals the total number of
  // corresponding elements.
  //   * rank() == 0 -> scalar -> size() == 1.
  //   * rank() > 0 -> non-scalar -> size() == edges().back().child_size().
  int64_t size() const {
    return impl_->edges.empty() ? 1 : impl_->edges.back().child_size();
  }

  // Returns the edges of the shape. The size of the span is always equal to the
  // rank.
  absl::Span<const Edge> edges() const { return impl_->edges; }

  // Returns a copy of this shape with `edges` appended. Has the same
  // restrictions as JaggedShape::FromEdges.
  //
  // `buf_factory` specifies the memory location of the converted split points.
  absl::StatusOr<JaggedShape> AddDims(
      absl::Span<const Edge> edges,
      RawBufferFactory& buf_factory = *GetHeapBufferFactory()) const {
    EdgeVec new_edges = impl_->edges;
    new_edges.insert(new_edges.end(), edges.begin(), edges.end());
    return JaggedShape::FromEdges(std::move(new_edges), buf_factory);
  }

  // Returns a copy of this shape containing the dims [0, from). Requires
  // 0 <= from <= rank().
  JaggedShape RemoveDims(size_t from) const {
    DCHECK_GE(from, 0);
    DCHECK_LE(from, rank());
    EdgeVec new_edges{impl_->edges.begin(), impl_->edges.begin() + from};
    return JaggedShape(std::move(new_edges));
  }

  // Flattens the dimensions between [from, to) into a single dimension,
  // or inserts a "unit" dimension at `from` when `from` == `to`.
  //
  // Requires `0 <= from <= to <= rank()`.
  // The resulting shape has `rank() == old_rank - (to - from) + 1`.
  //
  // Example:
  //   shape = JaggedShape([[0, 2], [0, 1, 3], [0, 1, 2, 4]])
  //   shape.FlattenDims(1, 3) -> JaggedShape([[0, 2], [0, 1, 4]]).
  //
  // Unit dimension example:
  //   shape = JaggedShape([[0, 2], [0, 1, 3]])
  //   shape.FlattenDims(1, 1) -> JaggedShape([[0, 2], [0, 1, 2], [0, 1, 3]]).
  JaggedShape FlattenDims(
      size_t from, size_t to,
      RawBufferFactory& buf_factory = *GetHeapBufferFactory()) const {
    DCHECK_GE(from, 0);
    DCHECK_LE(to, rank());
    DCHECK_LE(from, to);
    if (to - from == 1) return *this;
    if (to - from == rank()) return FlatFromSize(size(), buf_factory);
    EdgeVec new_edges;
    new_edges.reserve(rank() - (to - from) + 1);
    new_edges.insert(new_edges.end(), impl_->edges.begin(),
                     impl_->edges.begin() + from);
    if (from == to) {
      // Insert a unit-edge at `from`.
      int64_t parent_size = from == 0 ? 1 : impl_->edges[from - 1].child_size();
      auto unit_edge =
          Edge::FromUniformGroups(parent_size, /*group_size=*/1, buf_factory);
      DCHECK_OK(unit_edge.status());  // Cannot fail for valid shapes.
      new_edges.push_back(*std::move(unit_edge));
    } else {
      auto composed_edge =
          Edge::ComposeEdges(edges().subspan(from, to - from), buf_factory);
      DCHECK_OK(composed_edge.status());  // Cannot fail for valid shapes.
      new_edges.push_back(*std::move(composed_edge));
    }
    new_edges.insert(new_edges.end(), impl_->edges.begin() + to,
                     impl_->edges.end());
    return JaggedShape(std::move(new_edges));
  }

  // Heuristically checks if this_shape == that_shape.
  // Result may be exact or "partial".
  // See `FastEquivalenceResult::AreAllSizesEqual`.
  JaggedShapeFastEquivalenceResult FastEquivalenceCheck(
      const JaggedShape& other) const {
    if (impl_ == other.impl_) {
      return JaggedShapeFastEquivalenceResult(
          JaggedShapeFastEquivalenceResult::kEq);
    }
    const size_t rnk = rank();
    if (rnk != other.rank()) {
      return JaggedShapeFastEquivalenceResult(
          JaggedShapeFastEquivalenceResult::kNotEq);
    }
    if (rnk == 0) {
      return JaggedShapeFastEquivalenceResult(
          JaggedShapeFastEquivalenceResult::kEq);
    }
    // NOTE: we are going in reverse order since size of the first dimensions
    // are more likely to be the same.
    auto this_edge = impl_->edges.rbegin();
    auto other_edge = other.impl_->edges.rbegin();
    if (this_edge->child_size() != other_edge->child_size()) {
      return JaggedShapeFastEquivalenceResult(
          JaggedShapeFastEquivalenceResult::kNotEq);
    }
    if (rnk == 1) {
      return JaggedShapeFastEquivalenceResult(
          JaggedShapeFastEquivalenceResult::kEq);
    }
    ++this_edge;
    ++other_edge;
    for (; this_edge != impl_->edges.rend(); ++this_edge, ++other_edge) {
      if (this_edge->child_size() != other_edge->child_size()) {
        return JaggedShapeFastEquivalenceResult(
            JaggedShapeFastEquivalenceResult::kNotEq);
      }
    }
    return JaggedShapeFastEquivalenceResult(
        JaggedShapeFastEquivalenceResult::kSizesEq);
  }

  // Checks if this_shape == that_shape.
  bool IsEquivalentTo(const JaggedShape& other) const {
    auto result = FastEquivalenceCheck(other);
    if (result.IsGuaranteedNotEq()) return false;
    if (result.IsGuaranteedEq()) return true;
    // Note: we start from `1` since size of the first edge is already verified.
    for (size_t i = 1; i < rank(); ++i) {
      if (!impl_->edges[i].IsEquivalentTo(other.impl_->edges[i])) return false;
    }
    return true;
  }

  // Returns true if `this` is a prefix of `other`. This means that `other`
  // shape edges in the front are all equivalent and in the same order as edges
  // in `this`.
  //
  // Equivalent shapes are also expandable to each other.
  bool IsBroadcastableTo(const JaggedShape& other) const {
    if (impl_ == other.impl_) return true;
    if (other.rank() < rank()) return false;
    for (int i = 0; i < rank(); ++i) {
      if (!impl_->edges[i].IsEquivalentTo(other.impl_->edges[i])) return false;
    }
    return true;
  }

  // Returns an Edge that broadcasts `this` to `other`, such that
  // `size() == other.size()`.
  //
  // Requires `shape.IsBroadcastableTo(other)`.
  //
  // Example:
  //   flat_values = [1, 2, 3]
  //   this_shape = [[0, 2], [0, 2, 3]]
  //   other_shape = [[0, 2], [0, 2, 3], [0, 1, 3, 4]]
  //   edge = this_shape.GetBroadcastEdge(other_shape)
  //     # Returns: [0, 1, 3, 4]
  //   flat_values.Expand(edge)
  //     # Returns: [1, 2, 2, 3]
  Edge GetBroadcastEdge(
      const JaggedShape& other,
      RawBufferFactory& buf_factory = *GetHeapBufferFactory()) const {
    DCHECK(IsBroadcastableTo(other));
    if (rank() == other.rank()) {
      auto unit_edge =
          Edge::FromUniformGroups(size(), /*group_size=*/1, buf_factory);
      DCHECK_OK(unit_edge.status());
      return *std::move(unit_edge);
    } else {
      auto res_edge =
          Edge::ComposeEdges(other.edges().subspan(rank()), buf_factory);
      DCHECK_OK(res_edge.status());
      return *std::move(res_edge);
    }
  }

  // Creates an empty shape (rank 0, size 0).
  //
  // Prefer to use JaggedShape::Empty() instead.
  JaggedShape() : impl_(Impl::Empty()) {}

 private:
  struct Impl : arolla::RefcountedBase {
    Impl() = default;
    explicit Impl(EdgeVec edges) : edges(std::move(edges)) {}
    static const arolla::RefcountPtr<Impl>& Empty() {
      static absl::NoDestructor<arolla::RefcountPtr<Impl>> ptr(
          arolla::RefcountPtr<Impl>::Make());
      return *ptr;
    }

    EdgeVec edges;
  };

  explicit JaggedShape(EdgeVec edges)
      : impl_(arolla::RefcountPtr<Impl>::Make(std::move(edges))) {}

  arolla::RefcountPtr<Impl> impl_;
};

template <typename Edge>
struct FingerprintHasherTraits<JaggedShape<Edge>> {
  void operator()(FingerprintHasher* hasher,
                  const JaggedShape<Edge>& value) const {
    hasher->Combine(value.rank());
    for (const auto& edge : value.edges()) {
      hasher->Combine(edge);
    }
  }
};

}  // namespace arolla

#endif  // AROLLA_JAGGED_SHAPE_JAGGED_SHAPE_H_
