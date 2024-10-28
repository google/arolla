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
#include "arolla/qexpr/operators/strings/join.h"

#include <memory>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operator_errors.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {
using std::unique_ptr;

template <typename T>
using Slot = FrameLayout::Slot<T>;

// BoundOperator implementation for join of scalar parts.
template <class StringType>
class JoinBoundOperator : public BoundOperator {
 public:
  JoinBoundOperator(Slot<StringType> delimiter_slot,
                    std::vector<Slot<StringType>> part_slots,
                    Slot<StringType> output_slot)
      : delimiter_slot_(delimiter_slot),
        part_slots_(std::move(part_slots)),
        output_slot_(output_slot) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    // Extract string_views for joining.
    absl::InlinedVector<absl::string_view, 10> parts;
    parts.reserve(part_slots_.size());
    for (auto& s : part_slots_) {
      parts.push_back(frame.Get(s));
    }

    // Save joined Text to output slot.
    frame.Set(output_slot_,
              StringType(absl::StrJoin(parts.begin(), parts.end(),
                                       frame.Get(delimiter_slot_))));
  }

 private:
  Slot<StringType> delimiter_slot_;
  std::vector<Slot<StringType>> part_slots_;
  Slot<StringType> output_slot_;
};

// Operator implementation for join of scalar parts.
template <class StringType>
class JoinOperator : public QExprOperator {
 public:
  explicit JoinOperator(const QExprOperatorSignature* type)
      : QExprOperator(type) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> typed_input_slots,
      TypedSlot typed_output_slot) const final {
    ASSIGN_OR_RETURN(Slot<StringType> delimiter_slot,
                     typed_input_slots[0].ToSlot<StringType>());
    std::vector<Slot<StringType>> part_slots;
    part_slots.reserve(typed_input_slots.size() - 1);

    std::vector<Slot<bool>> presence_slots;
    presence_slots.reserve(typed_input_slots.size() - 1);

    for (const auto& s : typed_input_slots.subspan(1)) {
      if (IsOptionalQType(s.GetType())) {
        ASSIGN_OR_RETURN(auto input_slot,
                         s.ToSlot<OptionalValue<StringType>>());
        presence_slots.push_back(GetPresenceSubslotFromOptional(input_slot));
        part_slots.push_back(GetValueSubslotFromOptional(input_slot));
      } else {
        ASSIGN_OR_RETURN(Slot<StringType> part_slot, s.ToSlot<StringType>());
        part_slots.push_back(part_slot);
      }
    }
    if (presence_slots.empty()) {
      ASSIGN_OR_RETURN(Slot<StringType> output_slot,
                       typed_output_slot.ToSlot<StringType>());
      return {std::make_unique<JoinBoundOperator<StringType>>(
          delimiter_slot, std::move(part_slots), output_slot)};
    } else {
      // If any of the inputs are optional, then so must be the output.
      ASSIGN_OR_RETURN(auto output_slot,
                       typed_output_slot.ToSlot<OptionalValue<StringType>>());
      auto join_op = JoinBoundOperator<StringType>(
          delimiter_slot, std::move(part_slots),
          GetValueSubslotFromOptional(output_slot));
      return {unique_ptr<BoundOperator>(new WhereAllBoundOperator(
          presence_slots, GetPresenceSubslotFromOptional(output_slot),
          std::move(join_op)))};
    }
  }
};

}  // unnamed namespace

template <class StringType>
absl::StatusOr<OperatorPtr> GetJoinOperator(
    absl::Span<const QTypePtr> input_types) {
  bool has_optional =
      std::any_of(input_types.begin(), input_types.end(),
                  [](QTypePtr qtype) { return IsOptionalQType(qtype); });
  const QTypePtr part_type = DecayOptionalQType(input_types[1]);
  QTypePtr output_type = part_type;
  if (has_optional) {
    ASSIGN_OR_RETURN(output_type, ToOptionalQType(output_type));
  }
  auto operator_qtype = QExprOperatorSignature::Get(input_types, output_type);

  auto scalar_qtype = GetQType<StringType>();
  if (part_type == scalar_qtype) {
    return OperatorPtr(
        std::make_unique<JoinOperator<StringType>>(operator_qtype));
  } else {
    return OperatorNotDefinedError(
        kJoinOperatorName, input_types,
        absl::StrFormat("with a separator of type %s, joined parts must be %s",
                        scalar_qtype->name(), scalar_qtype->name()));
  }
}

absl::StatusOr<OperatorPtr> JoinOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.size() < 2) {
    return OperatorNotDefinedError(kJoinOperatorName, input_types,
                                   "expected at least 2 arguments.");
  }
  if (input_types[0] != GetQType<Text>() &&
      input_types[0] != GetQType<Bytes>()) {
    return OperatorNotDefinedError(
        kJoinOperatorName, input_types,
        absl::StrFormat("first argument must be TEXT or BYTES but was %s.",
                        input_types[0]->name()));
  }

  const QTypePtr part_type = DecayOptionalQType(input_types[1]);
  for (const QTypePtr t : input_types.subspan(2)) {
    if (DecayOptionalQType(t) != part_type) {
      return OperatorNotDefinedError(kJoinOperatorName, input_types,
                                     "joined parts must have same type.");
    }
  }

  if (input_types[0] == GetQType<Text>()) {
    return EnsureOutputQTypeMatches(GetJoinOperator<Text>(input_types),
                                    input_types, output_type);
  } else {
    return EnsureOutputQTypeMatches(GetJoinOperator<Bytes>(input_types),
                                    input_types, output_type);
  }
}

}  // namespace arolla
