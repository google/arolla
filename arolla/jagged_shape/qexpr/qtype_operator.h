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
#ifndef AROLLA_JAGGED_SHAPE_QEXPR_QTYPE_OPERATOR_H_
#define AROLLA_JAGGED_SHAPE_QEXPR_QTYPE_OPERATOR_H_

#include "arolla/jagged_shape/qtype/qtype.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// jagged.make_jagged_shape_qtype operator.
struct MakeJaggedShapeQTypeOp {
  QTypePtr operator()(QTypePtr edge_qtype) const {
    auto maybe_shape =
        GetJaggedShapeQTypeFromEdgeQType(edge_qtype).value_or(nullptr);
    return maybe_shape ? maybe_shape : GetNothingQType();
  }
};

// jagged.is_jagged_shape_qtype operator.
struct IsJaggedShapeQTypeOp {
  OptionalUnit operator()(QTypePtr x) const {
    return OptionalUnit(IsJaggedShapeQType(x));
  }
};

// jagged.get_edge_qtype operator.
struct GetEdgeQType {
  QTypePtr operator()(QTypePtr shape_qtype) const {
    if (auto qtype = dynamic_cast<const JaggedShapeQType*>(shape_qtype)) {
      return qtype->edge_qtype();
    } else {
      return GetNothingQType();
    }
  }
};

}  // namespace arolla

#endif  // AROLLA_JAGGED_SHAPE_QEXPR_QTYPE_OPERATOR_H_
