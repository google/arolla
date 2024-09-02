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
#include "arolla/qexpr/operators/bootstrap/sequence_operators.h"

#include <algorithm>
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
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

QTypePtr TupleQTypeOfInputValueQTypes(absl::Span<const QTypePtr> input_qtypes) {
  std::vector<QTypePtr> value_qtypes(input_qtypes.size());
  for (size_t i = 0; i < input_qtypes.size(); ++i) {
    value_qtypes[i] = input_qtypes[i]->value_qtype();
  }
  return MakeTupleQType(value_qtypes);
}

QTypePtr SequenceZipOutputQType(absl::Span<const QTypePtr> input_qtypes) {
  return GetSequenceQType(TupleQTypeOfInputValueQTypes(input_qtypes));
}

absl::StatusOr<size_t> SizeOfSequences(
    FramePtr frame, absl::Span<const TypedSlot> input_slots) {
  const size_t seq_size =
      frame.Get(input_slots[0].UnsafeToSlot<Sequence>()).size();
  for (size_t i = 1; i < input_slots.size(); ++i) {
    const auto& cur_seq = frame.Get(input_slots[i].UnsafeToSlot<Sequence>());
    if (cur_seq.size() != seq_size) {
      return absl::InvalidArgumentError(
          absl::StrFormat("all sequences should have equal sizes, %d != %d",
                          seq_size, cur_seq.size()));
    }
  }
  return seq_size;
}

class SequenceSliceOp : public QExprOperator {
 public:
  explicit SequenceSliceOp(QTypePtr input_qtype)
      : QExprOperator(QExprOperatorSignature::Get(
            {input_qtype, ::arolla::GetQType<int64_t>(),
             ::arolla::GetQType<int64_t>()},
            input_qtype)) {}

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator(
        [sequence_slot = input_slots[0].UnsafeToSlot<Sequence>(),
         start_slot = input_slots[1].UnsafeToSlot<int64_t>(),
         stop_slot = input_slots[2].UnsafeToSlot<int64_t>(),
         output_slot = output_slot.UnsafeToSlot<Sequence>()](
            EvaluationContext* ctx, FramePtr frame) {
          const auto& sequence = frame.Get(sequence_slot);
          auto start = frame.Get(start_slot);
          auto stop = frame.Get(stop_slot);
          const int64_t sequence_size = sequence.size();
          if (start < 0) {
            start = sequence_size + start;
          }
          if (stop < 0) {
            stop = sequence_size + stop;
          }
          start =
              std::min(std::max(static_cast<int64_t>(0), start), sequence_size);
          stop = std::min(std::max(start, stop), sequence_size);

          frame.Set(output_slot, sequence.subsequence(start, stop - start));
        });
  }
};

class SequenceZipOp : public QExprOperator {
 public:
  explicit SequenceZipOp(absl::Span<const QTypePtr> input_qtypes)
      : QExprOperator(QExprOperatorSignature::Get(
            input_qtypes, SequenceZipOutputQType(input_qtypes))) {}

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator(
        [input_slots = std::vector(input_slots.begin(), input_slots.end()),
         output_slot = output_slot.UnsafeToSlot<Sequence>()](
            EvaluationContext* ctx, FramePtr frame) {
          const size_t args_count = input_slots.size();
          std::vector<QTypePtr> input_qtypes(args_count);
          for (size_t i = 0; i < args_count; ++i) {
            input_qtypes[i] = input_slots[i].GetType();
          }
          const QTypePtr output_tuple_qtype =
              TupleQTypeOfInputValueQTypes(input_qtypes);

          ASSIGN_OR_RETURN(size_t seq_size, SizeOfSequences(frame, input_slots),
                           ctx->set_status(std::move(_)));

          ASSIGN_OR_RETURN(auto mutable_sequence,
                           MutableSequence::Make(output_tuple_qtype, seq_size),
                           ctx->set_status(std::move(_)));

          std::vector<TypedRef> cur_tuple;
          cur_tuple.reserve(args_count);
          for (size_t seq_id = 0; seq_id < seq_size; ++seq_id) {
            cur_tuple.clear();
            for (size_t tuple_id = 0; tuple_id < args_count; ++tuple_id) {
              const auto& cur_seq =
                  frame.Get(input_slots[tuple_id].UnsafeToSlot<Sequence>());
              cur_tuple.push_back(cur_seq.GetRef(seq_id));
            }
            auto tuple_value = MakeTuple(cur_tuple);
            mutable_sequence.UnsafeSetRef(seq_id, tuple_value.AsRef());
          }
          frame.Set(output_slot, std::move(mutable_sequence).Finish());
        });
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> SequenceSliceOpFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.size() != 3) {
    return absl::InvalidArgumentError("exactly three arguments are expected");
  }
  if (!IsSequenceQType(input_types[0])) {
    return absl::InvalidArgumentError("unexpected argument type");
  }
  if (!IsIntegralScalarQType(input_types[1])) {
    return absl::InvalidArgumentError("unexpected argument type");
  }
  if (!IsIntegralScalarQType(input_types[2])) {
    return absl::InvalidArgumentError("unexpected argument type");
  }
  return EnsureOutputQTypeMatches(
      std::make_shared<SequenceSliceOp>(input_types[0]), input_types,
      output_type);
}

absl::StatusOr<OperatorPtr> SequenceZipOpFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.empty()) {
    return absl::InvalidArgumentError("at least one argument is expected");
  }
  for (const auto input_type : input_types) {
    if (!IsSequenceQType(input_type)) {
      return absl::InvalidArgumentError("unexpected argument type");
    }
  }
  return EnsureOutputQTypeMatches(std::make_shared<SequenceZipOp>(input_types),
                                  input_types, output_type);
}

}  // namespace arolla
