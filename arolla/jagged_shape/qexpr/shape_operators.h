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
// Collection of templated operators for jagged shapes.

#ifndef AROLLA_JAGGED_SHAPE_QEXPR_SHAPE_OPERATORS_H_
#define AROLLA_JAGGED_SHAPE_QEXPR_SHAPE_OPERATORS_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// jagged.(dense_)?array_shape_from_edges operator implementation.
template <typename Shape>
class JaggedShapeFromEdgesOperator : public InlineOperator {
  using Edge = typename Shape::Edge;
  using EdgeVec = typename Shape::EdgeVec;

 public:
  JaggedShapeFromEdgesOperator(std::string name, size_t tuple_size)
      : InlineOperator(
            std::move(name),
            QExprOperatorSignature::Get(
                std::vector<QTypePtr>(tuple_size, ::arolla::GetQType<Edge>()),
                ::arolla::GetQType<Shape>())) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    std::vector<Slot<Edge>> edge_slots;
    edge_slots.reserve(input_slots.size());
    for (const auto& input_slot : input_slots) {
      ASSIGN_OR_RETURN(Slot<Edge> edge_slot, input_slot.ToSlot<Edge>());
      edge_slots.push_back(std::move(edge_slot));
    }
    ASSIGN_OR_RETURN(Slot<Shape> shape_slot, output_slot.ToSlot<Shape>());
    return MakeBoundOperator([edge_slots = std::move(edge_slots),
                              shape_slot = std::move(shape_slot)](
                                 EvaluationContext* ctx, FramePtr frame) {
      EdgeVec edges;
      edges.reserve(edge_slots.size());
      for (const auto& edge_slot : edge_slots) {
        edges.push_back(frame.Get(edge_slot));
      }
      ASSIGN_OR_RETURN(auto jagged_shape, Shape::FromEdges(std::move(edges)),
                       ctx->set_status(std::move(_)));
      frame.Set(shape_slot, std::move(jagged_shape));
    });
  }
};

// jagged.add_dims._(dense_)?array operator implementation.
template <typename Shape>
class JaggedShapeAddDimsOperator : public InlineOperator {
  using Edge = typename Shape::Edge;
  using EdgeVec = typename Shape::EdgeVec;

 public:
  JaggedShapeAddDimsOperator(std::string name, size_t input_size)
      : InlineOperator(std::move(name),
                       QExprOperatorSignature::Get(
                           [input_size]() {
                             std::vector<QTypePtr> qtypes{
                                 input_size, ::arolla::GetQType<Edge>()};
                             qtypes[0] = ::arolla::GetQType<Shape>();
                             return qtypes;
                           }(),
                           ::arolla::GetQType<Shape>())) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    DCHECK_GE(input_slots.size(), 1);
    ASSIGN_OR_RETURN(Slot<Shape> input_shape_slot,
                     input_slots[0].ToSlot<Shape>());
    std::vector<Slot<Edge>> edge_slots;
    edge_slots.reserve(input_slots.size() - 1);
    for (size_t i = 1; i < input_slots.size(); ++i) {
      ASSIGN_OR_RETURN(Slot<Edge> edge_slot, input_slots[i].ToSlot<Edge>());
      edge_slots.push_back(std::move(edge_slot));
    }
    ASSIGN_OR_RETURN(Slot<Shape> output_shape_slot,
                     output_slot.ToSlot<Shape>());
    return MakeBoundOperator([input_shape_slot = std::move(input_shape_slot),
                              edge_slots = std::move(edge_slots),
                              output_shape_slot = std::move(output_shape_slot)](
                                 EvaluationContext* ctx, FramePtr frame) {
      const Shape& input_shape = frame.Get(input_shape_slot);
      EdgeVec edges;
      edges.reserve(edge_slots.size());
      for (const auto& edge_slot : edge_slots) {
        edges.push_back(frame.Get(edge_slot));
      }
      ASSIGN_OR_RETURN(auto output_shape, input_shape.AddDims(edges),
                       ctx->set_status(std::move(_)));
      frame.Set(output_shape_slot, std::move(output_shape));
    });
  }
};

// jagged.edges operator implementation.
template <typename Shape>
class JaggedShapeEdgesOperator : public InlineOperator {
  using Edge = typename Shape::Edge;

 public:
  explicit JaggedShapeEdgesOperator(std::string name)
      : InlineOperator(
            std::move(name),
            QExprOperatorSignature::Get(
                {::arolla::GetQType<Shape>()},
                ::arolla::GetSequenceQType(::arolla::GetQType<Edge>()))) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    DCHECK_EQ(input_slots.size(), 1);
    ASSIGN_OR_RETURN(Slot<Shape> shape_slot, input_slots[0].ToSlot<Shape>());
    ASSIGN_OR_RETURN(Slot<arolla::Sequence> sequence_slot,
                     output_slot.ToSlot<arolla::Sequence>());
    return MakeBoundOperator([shape_slot = std::move(shape_slot),
                              sequence_slot = std::move(sequence_slot)](
                                 EvaluationContext* ctx, FramePtr frame) {
      const Shape& shape = frame.Get(shape_slot);
      ASSIGN_OR_RETURN(
          auto mutable_sequence,
          arolla::MutableSequence::Make(arolla::GetQType<Edge>(), shape.rank()),
          ctx->set_status(std::move(_)));
      for (size_t i = 0; i < shape.rank(); ++i) {
        mutable_sequence.UnsafeSetRef(
            i, arolla::TypedRef::FromValue(shape.edges()[i]));
      }
      frame.Set(sequence_slot, std::move(mutable_sequence).Finish());
    });
  }
};

// Clamps `index` in the range `[0, max_index]` using python indexing rules.
inline int64_t GetPosIndex(int64_t index, int64_t max_index) {
  int64_t pos_index = index < 0 ? index + max_index : index;
  return std::clamp(pos_index, int64_t{0}, max_index);
}

// jagged.rank operator.
template <typename Shape>
struct JaggedShapeRankOp {
  int64_t operator()(const Shape& shape) const { return shape.rank(); }
};

// jagged.edge_at operator.
template <typename Shape>
struct JaggedShapeEdgeAtOp {
  absl::StatusOr<typename Shape::Edge> operator()(const Shape& shape,
                                                  int64_t dim) const {
    int64_t pos_dim = dim < 0 ? dim + shape.rank() : dim;
    if (pos_dim < 0 || shape.rank() <= pos_dim) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected rank > %d, but got rank = %d, when trying "
                          "to get the edge at dim = %d",
                          dim >= 0 ? dim : -dim - 1, shape.rank(), dim));
    } else {
      return shape.edges()[pos_dim];
    }
  }
};

// jagged.remove_dims operator.
template <typename Shape>
struct JaggedShapeRemoveDimsOp {
  Shape operator()(const Shape& shape, int64_t from) const {
    int64_t pos_from = GetPosIndex(from, /*max_index=*/shape.rank());
    return shape.RemoveDims(pos_from);
  }
};

// jagged._flatten operator.
template <typename Shape>
struct JaggedShapeFlattenOp {
  Shape operator()(const Shape& shape, int64_t from, int64_t to) const {
    int64_t pos_from = GetPosIndex(from, /*max_index=*/shape.rank());
    int64_t pos_to = GetPosIndex(to, /*max_index=*/shape.rank());
    return shape.FlattenDims(pos_from, std::max(pos_to, pos_from));
  }
};

// jagged.size operator.
template <typename Shape>
struct JaggedShapeSizeOp {
  int64_t operator()(const Shape& shape) const { return shape.size(); }
};

// jagged.is_broadcastable_to operator.
template <typename Shape>
struct JaggedShapeIsBroadcastableToOp {
  OptionalUnit operator()(const Shape& shape, const Shape& other_shape) const {
    return OptionalUnit(shape.IsBroadcastableTo(other_shape));
  }
};

// jagged.equal operator.
template <typename Shape>
struct JaggedShapeEqualOp {
  OptionalUnit operator()(const Shape& shape, const Shape& other_shape) const {
    return OptionalUnit(shape.IsEquivalentTo(other_shape));
  }
};

}  // namespace arolla

#endif  // AROLLA_JAGGED_SHAPE_QEXPR_SHAPE_OPERATORS_H_
