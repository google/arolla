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
#include "arolla/qexpr/operators/bootstrap/qtype_operator.h"

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

// core.make_tuple(sequence)
class MakeTupleQTypeFromSequenceOp : public QExprOperator {
 public:
  explicit MakeTupleQTypeFromSequenceOp(QTypePtr input_qtype)
      : QExprOperator(
            "qtype.make_tuple_qtype",
            QExprOperatorSignature::Get({input_qtype}, GetQTypeQType())) {
    DCHECK(IsSequenceQType(input_qtype));
    DCHECK(input_qtype->value_qtype() == GetQTypeQType() ||
           input_qtype->value_qtype() == GetNothingQType());
  }

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator([input = input_slots[0].UnsafeToSlot<Sequence>(),
                              output = output_slot.UnsafeToSlot<QTypePtr>()](
                                 EvaluationContext* ctx, FramePtr frame) {
      const auto& seq = frame.Get(input);
      if (seq.value_qtype() == GetQTypeQType()) {
        frame.Set(output, MakeTupleQType(seq.UnsafeSpan<QTypePtr>()));
      } else if (seq.value_qtype() == GetNothingQType() && seq.size() == 0) {
        frame.Set(output, MakeTupleQType({}));
      } else {
        ctx->set_status(absl::InvalidArgumentError(
            absl::StrCat("unexpected argument: %s", Repr(seq))));
      }
    });
  }
};

// core.make_tuple(field_qtype_0, field_qtype_1, ...)
class MakeTupleQTypeFromFieldsOp : public QExprOperator {
 public:
  explicit MakeTupleQTypeFromFieldsOp(size_t n)
      : QExprOperator(QExprOperatorSignature::Get(
            std::vector<QTypePtr>(n, GetQTypeQType()), GetQTypeQType())) {}

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    std::vector<FrameLayout::Slot<QTypePtr>> inputs;
    inputs.reserve(input_slots.size());
    for (auto& input_slot : input_slots) {
      inputs.emplace_back(input_slot.UnsafeToSlot<QTypePtr>());
    }
    return MakeBoundOperator([inputs = std::move(inputs),
                              output = output_slot.UnsafeToSlot<QTypePtr>()](
                                 EvaluationContext*, FramePtr frame) {
      std::vector<QTypePtr> input_qtypes(inputs.size());
      for (size_t i = 0; i < inputs.size(); ++i) {
        input_qtypes[i] = frame.Get(inputs[i]);
      }
      frame.Set(output, MakeTupleQType(input_qtypes));
    });
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> MakeTupleQTypeOpFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_qtypes, QTypePtr output_type) const {
  // make_tuple_qtype(sequence_of_qtypes)
  static const auto* sequence_of_qtypes = GetSequenceQType(GetQTypeQType());
  if (input_qtypes.size() == 1 && input_qtypes[0] == sequence_of_qtypes) {
    static absl::NoDestructor result(
        std::make_shared<MakeTupleQTypeFromSequenceOp>(sequence_of_qtypes));
    return EnsureOutputQTypeMatches(*result, input_qtypes, output_type);
  }
  // make_tuple_qtype(empty_sequence)
  static const auto* sequence_of_nothing = GetSequenceQType(GetNothingQType());
  if (input_qtypes.size() == 1 && input_qtypes[0] == sequence_of_nothing) {
    static absl::NoDestructor result(
        std::make_shared<MakeTupleQTypeFromSequenceOp>(sequence_of_nothing));
    return EnsureOutputQTypeMatches(*result, input_qtypes, output_type);
  }
  // make_tuple_qtype(field_qtype_0, field_qtype_1, ...)
  for (QTypePtr input_qtype : input_qtypes) {
    if (input_qtype != GetQTypeQType()) {
      return absl::InvalidArgumentError("unexpected argument type");
    }
  }
  return EnsureOutputQTypeMatches(
      std::make_shared<MakeTupleQTypeFromFieldsOp>(input_qtypes.size()),
      input_qtypes, output_type);
}

namespace {

class GetFieldQTypesOp : public QExprOperator {
 public:
  GetFieldQTypesOp()
      : QExprOperator(QExprOperatorSignature::Get(
            {GetQTypeQType()}, GetSequenceQType<QTypePtr>())) {}

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator(
        [input_slot = input_slots[0].UnsafeToSlot<QTypePtr>(),
         output_slot = output_slot.UnsafeToSlot<Sequence>()](
            EvaluationContext* ctx, FramePtr frame) {
          auto* qtype = frame.Get(input_slot);
          const auto& fields = qtype->type_fields();
          ASSIGN_OR_RETURN(
              auto mutable_sequence,
              MutableSequence::Make(GetQTypeQType(), fields.size()),
              ctx->set_status(std::move(_)));
          auto span = mutable_sequence.UnsafeSpan<QTypePtr>();
          for (size_t i = 0; i < fields.size(); ++i) {
            span[i] = fields[i].GetType();
          }
          frame.Set(output_slot, std::move(mutable_sequence).Finish());
        });
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> GetFieldQTypesOpFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("exactly one argument is expected");
  }
  if (input_types[0] != GetQTypeQType()) {
    return absl::InvalidArgumentError("unexpected argument type");
  }
  static absl::NoDestructor result(std::make_shared<GetFieldQTypesOp>());
  return EnsureOutputQTypeMatches(*result, input_types, output_type);
}

}  // namespace arolla
