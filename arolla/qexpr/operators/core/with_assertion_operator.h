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
#ifndef AROLLA_OPERATORS_CORE_WITH_ASSERTION_OPERATOR_H_
#define AROLLA_OPERATORS_CORE_WITH_ASSERTION_OPERATOR_H_

#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla {

// core._with_assertion operator.
class WithAssertionOperatorFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const final {
    if (input_types.size() != 3) {
      return absl::InvalidArgumentError("exactly three arguments are expected");
    }
    if (input_types[1] != GetOptionalQType<Unit>()) {
      return absl::InvalidArgumentError("unexpected second argument type");
    }
    if (input_types[2] != GetQType<Text>()) {
      return absl::InvalidArgumentError("unexpected third argument type");
    }
    return EnsureOutputQTypeMatches(
        std::make_shared<CoreWithAssertion>(input_types[0]), input_types,
        output_type);
  }

  class CoreWithAssertion : public QExprOperator {
   public:
    explicit CoreWithAssertion(QTypePtr input_qtype)
        : QExprOperator("core.with_assertion",
                        QExprOperatorSignature::Get(
                            {input_qtype, GetOptionalQType<Unit>(),
                             ::arolla::GetQType<Text>()},
                            input_qtype)) {}

    absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
        absl::Span<const TypedSlot> input_slots,
        TypedSlot output_slot) const final {
      return MakeBoundOperator(
          [value_slot = input_slots[0],
           condition_slot = input_slots[1].UnsafeToSlot<OptionalValue<Unit>>(),
           message_slot = input_slots[2].UnsafeToSlot<Text>(),
           output_slot = output_slot](EvaluationContext* ctx, FramePtr frame) {
            const auto condition = frame.Get(condition_slot);
            const auto message = frame.Get(message_slot);
            if (!condition.present) {
              ctx->set_status(absl::FailedPreconditionError(message));
              return;
            }
            value_slot.CopyTo(frame, output_slot, frame);
          });
    }
  };
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_CORE_WITH_ASSERTION_OPERATOR_H_
