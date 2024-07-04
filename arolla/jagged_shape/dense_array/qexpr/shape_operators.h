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
#ifndef AROLLA_JAGGED_SHAPE_DENSE_ARRAY_QEXPR_SHAPE_OPERATORS_H_
#define AROLLA_JAGGED_SHAPE_DENSE_ARRAY_QEXPR_SHAPE_OPERATORS_H_

#include <memory>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"  // IWYU pragma: keep
#include "arolla/jagged_shape/qexpr/shape_operators.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// jagged.dense_array_shape_from_edges operator returns a jagged array shape
// constructed from multiple dense array edges.
class JaggedDenseArrayShapeFromEdgesOperatorFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_qtypes,
      QTypePtr output_qtype) const final {
    return EnsureOutputQTypeMatches(
        OperatorPtr(std::make_unique<
                    JaggedShapeFromEdgesOperator<JaggedDenseArrayShape>>(
            "jagged.dense_array_shape_from_edges", input_qtypes.size())),
        input_qtypes, output_qtype);
  }
};

// jagged.add_dims._dense_array operator.
class JaggedDenseArrayShapeAddDimsOperatorFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_qtypes,
      QTypePtr output_qtype) const final {
    if (input_qtypes.empty()) {
      return absl::InvalidArgumentError("input_qtypes must not be empty");
    }
    return EnsureOutputQTypeMatches(
        OperatorPtr(
            std::make_unique<JaggedShapeAddDimsOperator<JaggedDenseArrayShape>>(
                "jagged.add_dims._dense_array", input_qtypes.size())),
        input_qtypes, output_qtype);
  }
};

// jagged.edges._dense_array operator.
class JaggedDenseArrayShapeEdgesOperatorFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_qtypes,
      QTypePtr output_qtype) const final {
    if (input_qtypes.size() != 1) {
      return absl::InvalidArgumentError("input_qtypes.length() != 1");
    }
    return EnsureOutputQTypeMatches(
        OperatorPtr(
            std::make_unique<JaggedShapeEdgesOperator<JaggedDenseArrayShape>>(
                "jagged.edges._dense_array")),
        input_qtypes, output_qtype);
  }
};

struct JaggedDenseArrayShapeRankOp : JaggedShapeRankOp<JaggedDenseArrayShape> {
};
struct JaggedDenseArrayShapeEdgeAtOp
    : JaggedShapeEdgeAtOp<JaggedDenseArrayShape> {};
struct JaggedDenseArrayShapeRemoveDimsOp
    : JaggedShapeRemoveDimsOp<JaggedDenseArrayShape> {};
struct JaggedDenseArrayShapeFlattenOp
    : JaggedShapeFlattenOp<JaggedDenseArrayShape> {};
struct JaggedDenseArrayShapeSizeOp : JaggedShapeSizeOp<JaggedDenseArrayShape> {
};
struct JaggedDenseArrayShapeIsBroadcastableToOp
    : JaggedShapeIsBroadcastableToOp<JaggedDenseArrayShape> {};
struct JaggedDenseArrayShapeEqualOp
    : JaggedShapeEqualOp<JaggedDenseArrayShape> {};

}  // namespace arolla

#endif  // AROLLA_JAGGED_SHAPE_DENSE_ARRAY_QEXPR_SHAPE_OPERATORS_H_
