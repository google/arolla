// Copyright 2025 Google LLC
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

#include <cstdint>

#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/shape_qtype.h"

namespace arolla {

// edge.child_shape operator returns kind and shape of the edge's
// source domain.
struct EdgeChildShapeOp {
  OptionalScalarShape operator()(const ScalarToScalarEdge&) const {
    return OptionalScalarShape{};
  }
};

// edge.parent_shape operator returns kind and shape of the edge's
// source domain.
struct EdgeParentShapeOp {
  OptionalScalarShape operator()(const ScalarToScalarEdge&) const {
    return OptionalScalarShape{};
  }
};

// edge.from_shape operator returns an edge-to-scalar with a given
// child_side_shape.
struct EdgeFromShapeOp {
  ScalarToScalarEdge operator()(const ScalarShape&) const {
    return ScalarToScalarEdge{};
  }
  ScalarToScalarEdge operator()(const OptionalScalarShape&) const {
    return ScalarToScalarEdge{};
  }
};

// edge.sizes operator returns an array of sizes corresponding to the number of
// children of each parent index.
struct EdgeSizesOp {
  constexpr int64_t operator()(const ScalarToScalarEdge&) const { return 1; }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_ARRAY_EDGE_OPS_H_
