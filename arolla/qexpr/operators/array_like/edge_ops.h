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
#ifndef AROLLA_QEXPR_OPERATORS_ARRAY_LIKE_EDGE_OPS_H_
#define AROLLA_QEXPR_OPERATORS_ARRAY_LIKE_EDGE_OPS_H_

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// `edge.compose` operator for array-likes.
template <typename EdgeT>
class EdgeComposeOperator : public InlineOperator {
 public:
  explicit EdgeComposeOperator(size_t size)
      : InlineOperator(QExprOperatorSignature::Get(
            std::vector<QTypePtr>(size, ::arolla::GetQType<EdgeT>()),
            ::arolla::GetQType<EdgeT>())) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    std::vector<Slot<EdgeT>> input_edge_slots;
    input_edge_slots.reserve(input_slots.size());
    for (const auto& input_slot : input_slots) {
      ASSIGN_OR_RETURN(Slot<EdgeT> edge_slot, input_slot.ToSlot<EdgeT>());
      input_edge_slots.push_back(std::move(edge_slot));
    }
    ASSIGN_OR_RETURN(Slot<EdgeT> output_edge_slot, output_slot.ToSlot<EdgeT>());
    return MakeBoundOperator([input_edge_slots = std::move(input_edge_slots),
                              output_edge_slot = std::move(output_edge_slot)](
                                 EvaluationContext* ctx, FramePtr frame) {
      std::vector<EdgeT> edges;
      edges.reserve(input_edge_slots.size());
      for (const auto& edge_slot : input_edge_slots) {
        edges.push_back(frame.Get(edge_slot));
      }
      ASSIGN_OR_RETURN(auto composed_edge,
                       EdgeT::ComposeEdges(edges, ctx->buffer_factory()),
                       ctx->set_status(std::move(_)));
      frame.Set(output_edge_slot, std::move(composed_edge));
    });
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_ARRAY_LIKE_EDGE_OPS_H_
