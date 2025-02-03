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
#ifndef AROLLA_LAZY_OPERATORS_QEXPR_OPERATORS_LIB_H_
#define AROLLA_LAZY_OPERATORS_QEXPR_OPERATORS_LIB_H_

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/lazy/lazy.h"
#include "arolla/lazy/lazy_qtype.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {

// lazy.get_lazy_qtype operator.
struct GetLazyQTypeOp {
  QTypePtr operator()(QTypePtr value_qtype) const {
    return GetLazyQType(value_qtype);
  }
};

// lazy.get operator.
class LazyGetOpFamily final : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_qtypes,
      QTypePtr output_qtype) const final {
    if (input_qtypes.size() != 1 || !IsLazyQType(input_qtypes[0])) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "unexpected argument: %s", JoinTypeNames(input_qtypes)));
    }
    return EnsureOutputQTypeMatches(
        std::make_shared<LazyGetOp>(input_qtypes[0]), input_qtypes,
        output_qtype);
  }

  struct LazyGetOp final : QExprOperator {
    explicit LazyGetOp(QTypePtr input_qtype)
        : QExprOperator("lazy.get",
                        QExprOperatorSignature::Get(
                            {input_qtype}, input_qtype->value_qtype())) {}

    absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
        absl::Span<const TypedSlot> input_slots,
        TypedSlot output_slot) const final {
      return MakeBoundOperator(
          [input_slot = input_slots[0].UnsafeToSlot<LazyPtr>(), output_slot](
              EvaluationContext* ctx, FramePtr frame) {
            const auto& lazy = frame.Get(input_slot);
            if (lazy == nullptr) {
              ctx->set_status(absl::InvalidArgumentError("lazy is nullptr"));
              return;
            }
            auto result = lazy->Get();
            if (!result.ok()) {
              ctx->set_status(std::move(result).status());
              return;
            }
            ctx->set_status(result->CopyToSlot(output_slot, frame));
          });
    }
  };
};

}  // namespace arolla

#endif  // AROLLA_LAZY_OPERATORS_QEXPR_OPERATORS_LIB_H_
