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

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
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
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"

namespace arolla {

// seq.at operator.
class SequenceAtOpFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const final {
    if (input_types.size() != 2) {
      return absl::InvalidArgumentError("exactly two arguments are expected");
    }
    if (!IsSequenceQType(input_types[0])) {
      return absl::InvalidArgumentError("unexpected first argument type");
    }
    if (!IsIntegralScalarQType(input_types[1])) {
      return absl::InvalidArgumentError("unexpected second argument type");
    }
    return EnsureOutputQTypeMatches(
        std::make_shared<SequenceAtOp>(input_types[0]), input_types,
        output_type);
  }

  class SequenceAtOp : public QExprOperator {
   public:
    explicit SequenceAtOp(QTypePtr input_qtype)
        : QExprOperator(QExprOperatorSignature::Get(
              {input_qtype, ::arolla::GetQType<int64_t>()},
              input_qtype->value_qtype())) {}

    absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
        absl::Span<const TypedSlot> input_slots,
        TypedSlot output_slot) const final {
      DCHECK_EQ(input_slots[0].GetType()->value_qtype(), output_slot.GetType());
      return MakeBoundOperator(
          [sequence_slot = input_slots[0].UnsafeToSlot<Sequence>(),
           index_slot = input_slots[1].UnsafeToSlot<int64_t>(),
           output_offset = output_slot.byte_offset(),
           element_size = output_slot.GetType()->type_layout().AllocSize()](
              EvaluationContext* ctx, FramePtr frame) {
            const auto& sequence = frame.Get(sequence_slot);
            const auto index = frame.Get(index_slot);
            if (0 <= index && static_cast<size_t>(index) < sequence.size()) {
              sequence.value_qtype()->UnsafeCopy(
                  sequence.RawAt(index, element_size),
                  frame.GetRawPointer(output_offset));
            } else {
              ctx->set_status(absl::InvalidArgumentError(
                  absl::StrFormat("sequence index out of range [0, %d): %d",
                                  sequence.size(), index)));
            }
          });
    }
  };
};

// seq.size operator.
class SequenceSizeOpFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const final {
    if (input_types.size() != 1) {
      return absl::InvalidArgumentError("exactly one argument is expected");
    }
    if (!IsSequenceQType(input_types[0])) {
      return absl::InvalidArgumentError("unexpected argument type");
    }
    return EnsureOutputQTypeMatches(
        std::make_shared<GetSequenceSizeOp>(input_types[0]), input_types,
        output_type);
  }

  class GetSequenceSizeOp : public QExprOperator {
   public:
    explicit GetSequenceSizeOp(QTypePtr input_qtype)
        : QExprOperator(QExprOperatorSignature::Get(
                            {input_qtype}, ::arolla::GetQType<int64_t>())) {}

    absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
        absl::Span<const TypedSlot> input_slots,
        TypedSlot output_slot) const final {
      return MakeBoundOperator(
          [input_slot = input_slots[0].UnsafeToSlot<Sequence>(),
           output_slot = output_slot.UnsafeToSlot<int64_t>()](
              EvaluationContext* ctx, FramePtr frame) {
            frame.Set(output_slot, frame.Get(input_slot).size());
          });
    }
  };
};

// seq.slice operator
class SequenceSliceOpFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type) const final;
};

// seq.zip operator.
// Scans sequences in parallel, producing tuples with a field from each one.
// Example:
//    >>> seq.zip(Sequence(1, 2, 3), Sequence('a', 'b', 'c'))
//    Sequence(Tuple(1, 'a'), Tuple(2, 'b'), Tuple(3, 'c'))
class SequenceZipOpFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type) const final;
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_SEQ_SEQUENCE_OPERATORS_H_
