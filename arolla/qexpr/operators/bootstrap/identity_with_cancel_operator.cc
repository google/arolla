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
#include "arolla/qexpr/operators/bootstrap/identity_with_cancel_operator.h"

#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

class IdentityWithCancelOperator : public QExprOperator {
 public:
  using QExprOperator::QExprOperator;

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator([x_slot = input_slots[0],
                              output_slot = output_slot,
                              msg_slot = input_slots[1].UnsafeToSlot<Text>()](
                                 EvaluationContext* ctx, FramePtr frame) {
      if (auto* cancellation_context =
              CancellationContext::ScopeGuard::current_cancellation_context()) {
        cancellation_context->Cancel(absl::CancelledError(frame.Get(msg_slot)));
        // Note: We copy the `x` value to the output in case context
        // cancellation doesn't stop the computation. This operator is primarily
        // intended for testing.
        x_slot.CopyTo(frame, output_slot, frame);
      } else {
        ctx->set_status(
            absl::FailedPreconditionError("no cancellation context"));
      }
    });
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> IdentityWithCancelOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.size() != 2 || input_types[1] != GetQType<Text>() ||
      output_type != input_types[0]) {
    return absl::InvalidArgumentError("unexpected input/output types");
  }
  return std::make_shared<IdentityWithCancelOperator>(
      QExprOperatorSignature::Get(input_types, output_type));
}

}  // namespace arolla
