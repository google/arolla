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
#ifndef AROLLA_CODEGEN_QEXPR_TESTING_ID_OPERATOR_FAMILY_H_
#define AROLLA_CODEGEN_QEXPR_TESTING_ID_OPERATOR_FAMILY_H_

#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"

namespace arolla::testing {

// Identity operator just returns its input.
class IdOperatorFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const final {
    if (input_types.size() != 1) {
      return absl::InvalidArgumentError("exactly one argument is expected");
    }
    return EnsureOutputQTypeMatches(
        OperatorPtr(std::make_unique<IdOp>(input_types[0])), input_types,
        output_type);
  }

  class IdOp : public QExprOperator {
   public:
    explicit IdOp(QTypePtr type)
        : QExprOperator(QExprOperatorSignature::Get({type}, type)) {}

   private:
    absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
        absl::Span<const TypedSlot> input_slots,
        TypedSlot output_slot) const final {
      return MakeBoundOperator(
          [input_slot = input_slots[0], output_slot = output_slot](
              EvaluationContext*, FramePtr frame) {
            input_slot.CopyTo(frame, output_slot, frame);
          });
    }
  };
};
}  // namespace arolla::testing

#endif  // AROLLA_CODEGEN_QEXPR_TESTING_ID_OPERATOR_FAMILY_H_
