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
#ifndef AROLLA_OPERATORS_SEQ_SEQUENCE_OPERATORS_H_
#define AROLLA_OPERATORS_SEQ_SEQUENCE_OPERATORS_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
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
#include "arolla/qtype/typed_ref.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// seq.range operator.
class SequenceRangeOpFamily final : public OperatorFamily {
  static absl::StatusOr<Sequence> MakeRange(int64_t start, int64_t stop,
                                            int64_t step) {
    if (step == 0) {
      return absl::InvalidArgumentError("seq.range(): `step` must be non-zero");
    }
    size_t seq_size = 0;
    if (step > 0) {
      if (start < stop) {
        seq_size = (static_cast<uint64_t>(stop) - static_cast<uint64_t>(start) +
                    static_cast<uint64_t>(step) - 1) /
                   static_cast<uint64_t>(step);
      }
    } else {
      if (stop < start) {
        seq_size =
            ((-static_cast<uint64_t>(stop)) - (-static_cast<uint64_t>(start)) +
             (-static_cast<uint64_t>(step)) - 1) /
            (-static_cast<uint64_t>(step));
      }
    }
    if (seq_size == 0) {
      return Sequence(::arolla::GetQType<int64_t>(), 0, nullptr);
    }
    ASSIGN_OR_RETURN(
        auto mutable_seq,
        MutableSequence::Make(::arolla::GetQType<int64_t>(), seq_size));
    auto y = start;
    for (auto& x : mutable_seq.UnsafeSpan<int64_t>()) {
      x = y;
      y += step;
    }
    return std::move(mutable_seq).Finish();
  }

  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const final {
    if (input_types.size() == 3 && IsIntegralScalarQType(input_types[0]) &&
        IsIntegralScalarQType(DecayOptionalQType(input_types[1])) &&
        IsIntegralScalarQType(input_types[2])) {
      return EnsureOutputQTypeMatches(OperatorPtr(new SequenceRangeOp),
                                      input_types, output_type);
    }
    return absl::InvalidArgumentError(absl::StrFormat(
        "unexpected argument types: %s", JoinTypeNames(input_types)));
  }

  struct SequenceRangeOp final : public QExprOperator {
    SequenceRangeOp()
        : QExprOperator(QExprOperatorSignature::Get(
              {::arolla::GetQType<int64_t>(),
               ::arolla::GetOptionalQType<int64_t>(),
               ::arolla::GetQType<int64_t>()},
              GetSequenceQType<int64_t>())) {}

    absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
        absl::Span<const TypedSlot> input_slots,
        TypedSlot output_slot) const final {
      return MakeBoundOperator(
          [arg_1_slot = input_slots[0].UnsafeToSlot<int64_t>(),
           arg_2_slot = input_slots[1].UnsafeToSlot<OptionalValue<int64_t>>(),
           step_slot = input_slots[2].UnsafeToSlot<int64_t>(),
           output_slot = output_slot.UnsafeToSlot<Sequence>()](
              EvaluationContext* ctx, FramePtr frame) {
            int64_t start, stop;
            if (frame.Get(arg_2_slot).present) {
              start = frame.Get(arg_1_slot);
              stop = frame.Get(arg_2_slot).value;
            } else {
              start = 0;
              stop = frame.Get(arg_1_slot);
            }
            ASSIGN_OR_RETURN(auto sequence,
                             MakeRange(start, stop, frame.Get(step_slot)),
                             ctx->set_status(std::move(_)));
            frame.Set(output_slot, std::move(sequence));
          });
    }
  };
};

// seq.make operator.
class SequenceMakeOpFamily final : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const final {
    const auto value_type =
        input_types.empty() ? GetNothingQType() : input_types[0];
    for (auto* type : input_types) {
      if (type != value_type) {
        return absl::InvalidArgumentError(
            "expected all arguments to have the same type");
      }
    }
    if (output_type != GetSequenceQType(value_type)) {
      return absl::InvalidArgumentError("unexpected output type");
    }
    return std::make_shared<SequenceMakeOp>(input_types, output_type);
  }

  struct SequenceMakeOp final : public QExprOperator {
    SequenceMakeOp(absl::Span<const QTypePtr> input_types, QTypePtr output_type)
        : QExprOperator("seq.make",
                        QExprOperatorSignature::Get(input_types, output_type)) {
    }

    absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
        absl::Span<const TypedSlot> input_slots,
        TypedSlot output_slot) const final {
      return MakeBoundOperator(
          [input_slots = std::vector(input_slots.begin(), input_slots.end()),
           output_slot = output_slot](EvaluationContext* ctx, FramePtr frame) {
            ASSIGN_OR_RETURN(
                auto mutable_sequence,
                MutableSequence::Make(output_slot.GetType()->value_qtype(),
                                      input_slots.size()),
                ctx->set_status(std::move(_)));
            for (size_t i = 0; i < input_slots.size(); ++i) {
              mutable_sequence.UnsafeSetRef(
                  i, TypedRef::FromSlot(input_slots[i], frame));
            }
            frame.Set(output_slot.UnsafeToSlot<Sequence>(),
                      std::move(mutable_sequence).Finish());
          });
    }
  };
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_SEQ_SEQUENCE_OPERATORS_H_
