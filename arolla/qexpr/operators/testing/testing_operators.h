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
#ifndef AROLLA_QEXPR_TESTING_TESTING_OPERATORS_H_
#define AROLLA_QEXPR_TESTING_TESTING_OPERATORS_H_

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
#include "arolla/qtype/typed_slot.h"

namespace arolla {

// test.fail operator.
class FailOpFamily final : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_qtypes,
      QTypePtr output_qtype) const final {
    return std::make_shared<FailOp>(
        "test.fail", QExprOperatorSignature::Get(input_qtypes, output_qtype));
  }

  class FailOp final : public QExprOperator {
    using QExprOperator::QExprOperator;
    absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
        absl::Span<const TypedSlot>, TypedSlot) const final {
      return MakeBoundOperator([](EvaluationContext* ctx, FramePtr) {
        ctx->set_status(absl::CancelledError(
            "intentional failure at `test.fail` instruction"));
      });
    }
  };
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_TESTING_TESTING_OPERATORS_H_
