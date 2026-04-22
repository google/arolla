// Copyright 2025 Google LLC
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

#include "absl/algorithm/container.h"
#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

// core.make_tuple(sequence)
class MakeTupleQTypeFromSequenceOp : public QExprOperator {
 public:
  explicit MakeTupleQTypeFromSequenceOp(QTypePtr input_qtype)
      : QExprOperator({input_qtype}, GetQTypeQType()) {
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
      : QExprOperator(std::vector<QTypePtr>(n, GetQTypeQType()),
                      GetQTypeQType()) {}

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
      : QExprOperator({GetQTypeQType()}, GetSequenceQType<QTypePtr>()) {}

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

namespace {

class GetFieldNamesOp : public QExprOperator {
 public:
  GetFieldNamesOp()
      : QExprOperator({GetQTypeQType()}, GetSequenceQType<Text>()) {}

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator(
        [input_slot = input_slots[0].UnsafeToSlot<QTypePtr>(),
         output_slot = output_slot.UnsafeToSlot<Sequence>()](
            EvaluationContext* ctx, FramePtr frame) {
          auto* qtype = frame.Get(input_slot);
          auto names = GetFieldNames(qtype);
          ASSIGN_OR_RETURN(
              auto mutable_sequence,
              MutableSequence::Make(GetQType<Text>(), names.size()),
              ctx->set_status(std::move(_)));
          absl::c_copy(names, mutable_sequence.UnsafeSpan<Text>().begin());
          frame.Set(output_slot, std::move(mutable_sequence).Finish());
        });
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> GetFieldNamesOpFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("exactly one argument is expected");
  }
  if (input_types[0] != GetQTypeQType()) {
    return absl::InvalidArgumentError("unexpected argument type");
  }
  static absl::NoDestructor result(std::make_shared<GetFieldNamesOp>());
  return EnsureOutputQTypeMatches(*result, input_types, output_type);
}

namespace {

class ParseInputQTypeSequenceOp : public QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator(
        [output_slot,
         signature_pattern_slot = input_slots[0].UnsafeToSlot<Bytes>(),
         input_qtype_seq_slot = input_slots[1].UnsafeToSlot<Sequence>()](
            EvaluationContext* ctx, FramePtr frame) -> void {
          const auto nothing_qtype = GetNothingQType();
          const absl::string_view pattern = frame.Get(signature_pattern_slot);
          absl::Span<const QTypePtr> inputs =
              frame.Get(input_qtype_seq_slot).UnsafeSpan<QTypePtr>();
          const size_t output_size = output_slot.SubSlotCount();
          const auto set_output_readiness = [&](bool ready) {
            DCHECK_GT(output_size, 0);  // Checked in DoGetOperator.
            frame.Set(output_slot.SubSlot(0).UnsafeToSlot<OptionalUnit>(),
                      OptionalUnit(ready));
          };
          const auto set_output_qtype = [&](size_t offset, QTypePtr qtype) {
            if (offset < output_size) {
              frame.Set(output_slot.SubSlot(offset).UnsafeToSlot<QTypePtr>(),
                        qtype);
            }
          };
          bool ok = true;
          size_t output_offset = 1;
          for (char c : pattern) {
            if (c == 'p') {
              if (inputs.empty()) {
                ok = false;
                break;
              }
              inputs.remove_prefix(1);
            } else if (c == 'P') {
              if (inputs.empty() || inputs[0] == nothing_qtype) {
                ok = false;
                break;
              }
              set_output_qtype(output_offset++, inputs[0]);
              inputs.remove_prefix(1);
            } else if (c == 'v') {
              inputs = {};
            } else if (c == 'V') {
              if (absl::c_contains(inputs, nothing_qtype)) {
                ok = false;
                break;
              } else {
                set_output_qtype(output_offset++, MakeTupleQType(inputs));
              }
              inputs = {};
            } else [[unlikely]] {
              // ignore other characters (verified at the expr-operator level)
            }
          }
          if (ok && inputs.empty()) {
            set_output_readiness(true);
            for (; output_offset < output_size; ++output_offset) {
              set_output_qtype(output_offset, nothing_qtype);
            }
          } else {
            set_output_readiness(false);
            for (size_t j = 1; j < output_size; ++j) {
              set_output_qtype(j, nothing_qtype);
            }
          }
        });
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> ParseInputQTypeSequenceOpFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("exactly two arguments are expected");
  }
  if (input_types[0] != GetQType<Bytes>()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "expected bytes, got signature_pattern: ", input_types[0]->name()));
  }
  if (input_types[1] != GetSequenceQType(GetQTypeQType())) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected sequence of qtypes, got input_qtype_seq: ",
                     input_types[1]->name()));
  }
  auto fields = output_type->type_fields();
  if (!IsTupleQType(output_type) || fields.empty()) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected output qtype to be a non-empty tuple, got: ",
                     output_type->name()));
  }
  if (fields[0].GetType() != GetQType<OptionalUnit>()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "expected the first output fields to be OPTIONAL_UNIT, got: ",
        fields[0].GetType()->name()));
  }
  for (size_t i = 1; i < fields.size(); ++i) {
    if (fields[i].GetType() != GetQTypeQType()) {
      return absl::InvalidArgumentError(
          absl::StrCat("expected the trailing output fields to be qtypes, "
                       "got: ",
                       fields[i].GetType()->name()));
    }
  }
  return std::make_shared<ParseInputQTypeSequenceOp>(input_types, output_type);
}

}  // namespace arolla
