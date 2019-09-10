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
#include "arolla/qexpr/operators/core/logic_operators.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operator_errors.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/common_qtype.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

template <typename T>
using Slot = FrameLayout::Slot<T>;

constexpr absl::string_view kPresenceOrVarargsOperatorName =
    "core._presence_or";

namespace {

// This is the operator implementation for all except `Unit` value types.
// Supported signatures are:
//
// 1. (OptionalValue<T>... optionals) -> OptionalValue<T>
// 2. (OptionalValue<T>... optionals, T default) -> T
//
// Do not use this implementation if T is `Unit`.
class PresenceOrVarargsOperator : public QExprOperator {
 public:
  explicit PresenceOrVarargsOperator(const QExprOperatorSignature* qtype)
      : QExprOperator(std::string(kPresenceOrVarargsOperatorName), qtype) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    // Get presence and value subslots from input slots.
    std::vector<Slot<bool>> presence_in_slots;
    presence_in_slots.reserve(input_slots.size());
    std::vector<TypedSlot> value_in_slots;
    value_in_slots.reserve(input_slots.size());
    for (int i = 0; i < input_slots.size(); ++i) {
      if (IsOptionalQType(input_slots[i].GetType())) {
        ASSIGN_OR_RETURN(Slot<bool> presence_in_slot,
                         GetPresenceSubslotFromOptional(input_slots[i]));
        presence_in_slots.push_back(presence_in_slot);
        ASSIGN_OR_RETURN(TypedSlot value_in_slot,
                         GetValueSubslotFromOptional(input_slots[i]));
        value_in_slots.push_back(value_in_slot);
      } else {
        // Only final input can be non-optional.
        DCHECK(i == input_slots.size() - 1);
        value_in_slots.push_back(input_slots[i]);
      }
    }

    if (IsOptionalQType(output_slot.GetType())) {
      // Get presence and value subslot from output slot.
      ASSIGN_OR_RETURN(Slot<bool> presence_out_slot,
                       GetPresenceSubslotFromOptional(output_slot));
      ASSIGN_OR_RETURN(TypedSlot value_out_slot,
                       GetValueSubslotFromOptional(output_slot));
      return std::make_unique<BoundImpl>(presence_in_slots, value_in_slots,
                                         presence_out_slot, value_out_slot);
    } else {
      return std::make_unique<BoundImpl>(presence_in_slots, value_in_slots,
                                         std::nullopt, output_slot);
    }
  }

  class BoundImpl : public BoundOperator {
   public:
    BoundImpl(std::vector<Slot<bool>> presence_in_slots,
              std::vector<TypedSlot> value_in_slots,
              std::optional<Slot<bool>> presence_out_slot,
              TypedSlot value_out_slot)
        : presence_in_slots_(std::move(presence_in_slots)),
          value_in_slots_(std::move(value_in_slots)),
          presence_out_slot_(presence_out_slot),
          value_out_slot_(value_out_slot) {
      if (presence_out_slot_.has_value()) {
        DCHECK_EQ(value_in_slots_.size(), presence_in_slots_.size());
      } else {
        DCHECK_EQ(value_in_slots_.size(), presence_in_slots_.size() + 1);
      }
    }

    void Run(EvaluationContext*, FramePtr frame) const override {
      auto iter =
          std::find_if(presence_in_slots_.begin(), presence_in_slots_.end(),
                       [&frame](Slot<bool> slot) { return frame.Get(slot); });
      int position = std::distance(presence_in_slots_.begin(), iter);
      bool has_output = position < value_in_slots_.size();
      if (presence_out_slot_.has_value()) {
        frame.Set(*presence_out_slot_, has_output);
      }
      if (has_output) {
        value_in_slots_[position].CopyTo(frame, value_out_slot_, frame);
      }
    }

   private:
    std::vector<Slot<bool>> presence_in_slots_;
    std::vector<TypedSlot> value_in_slots_;
    std::optional<Slot<bool>> presence_out_slot_;
    TypedSlot value_out_slot_;
  };
};

// Use this implementation where inputs are all OptionalValue<Unit>. Note in
// this case it doesn't make sense to accept a non-optional default, since that
// should be replaced by a literal `kPresent` value.
class PresenceOrVarargsUnitOperator : public QExprOperator {
 public:
  explicit PresenceOrVarargsUnitOperator(const QExprOperatorSignature* qtype)
      : QExprOperator(std::string(kPresenceOrVarargsOperatorName), qtype) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    std::vector<Slot<bool>> presence_in_slots;
    presence_in_slots.reserve(input_slots.size());
    for (int i = 0; i < input_slots.size(); ++i) {
      ASSIGN_OR_RETURN(Slot<bool> presence_in_slot,
                       GetPresenceSubslotFromOptional(input_slots[i]));
      presence_in_slots.push_back(presence_in_slot);
    }
    ASSIGN_OR_RETURN(Slot<bool> presence_out_slot,
                     GetPresenceSubslotFromOptional(output_slot));
    return std::make_unique<BoundImpl>(presence_in_slots, presence_out_slot);
  }

  class BoundImpl : public BoundOperator {
   public:
    BoundImpl(std::vector<Slot<bool>> presence_in_slots,
              Slot<bool> presence_out_slot)
        : presence_in_slots_(std::move(presence_in_slots)),
          presence_out_slot_(presence_out_slot) {}

    void Run(EvaluationContext*, FramePtr frame) const override {
      bool any_present =
          std::any_of(presence_in_slots_.begin(), presence_in_slots_.end(),
                      [&frame](Slot<bool> slot) { return frame.Get(slot); });
      frame.Set(presence_out_slot_, any_present);
    }

   private:
    std::vector<Slot<bool>> presence_in_slots_;
    Slot<bool> presence_out_slot_;
  };
};

class FakeShortCircuitWhereOperator : public QExprOperator {
 public:
  explicit FakeShortCircuitWhereOperator(const QExprOperatorSignature* qtype)
      : QExprOperator("core._short_circuit_where", qtype) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    return absl::InternalError(
        "FakeShortCircuitWhereOperator is not supposed to be used");
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> PresenceOrVarargsOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  auto not_defined_error = [&](absl::string_view detail) {
    return OperatorNotDefinedError(kPresenceOrVarargsOperatorName, input_types,
                                   detail);
  };

  // Check for minimum number of arguments.
  if (input_types.size() < 2) {
    return not_defined_error("expected at least two arguments");
  }

  // Check that all but last argument is optional.
  for (int i = 0; i < input_types.size() - 1; ++i) {
    if (!IsOptionalQType(input_types[i])) {
      return not_defined_error(
          "expected all except last argument to be optional");
    }
  }

  // Check that all arguments have common value type.
  QTypePtr first_value_type = DecayOptionalQType(input_types[0]);
  for (int i = 1; i < input_types.size(); ++i) {
    QTypePtr value_type = DecayOptionalQType(input_types[i]);
    if (value_type != first_value_type) {
      return not_defined_error(
          "expected all arguments to have a common value type");
    }
  }

  // Operator output is optional if final argument is optional.
  bool has_optional_output = IsOptionalQType(input_types.back());
  const QExprOperatorSignature* op_qtype =
      QExprOperatorSignature::Get(input_types, input_types.back());

  // Select an Operator implementation compatible with given argument types.
  OperatorPtr op;
  if (first_value_type == GetQType<Unit>()) {
    if (has_optional_output) {
      op = std::make_unique<PresenceOrVarargsUnitOperator>(op_qtype);
    } else {
      // Not needed, since this case simplifies to literal `kPresent`.
      return not_defined_error(
          "for Unit value type, expected final argument to be optional");
    }
  } else {
    op = std::make_unique<PresenceOrVarargsOperator>(op_qtype);
  }
  return EnsureOutputQTypeMatches(op, input_types, output_type);
}

absl::StatusOr<OperatorPtr> FakeShortCircuitWhereOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  auto not_defined_error = [&](absl::string_view detail) {
    return OperatorNotDefinedError("core._short_circuit_where", input_types,
                                   detail);
  };
  if (input_types.size() < 3) {
    return not_defined_error("expected 3 arguments");
  }
  if (input_types[0] != GetQType<OptionalUnit>()) {
    return not_defined_error("first argument must be OPTIONAL_UNIT");
  }

  QTypePtr true_type = input_types[1];
  QTypePtr false_type = input_types[2];
  const QType* common_type =
      CommonQType(true_type, false_type, /*enable_broadcasting=*/false);
  if (common_type == nullptr) {
    return not_defined_error("no common type between operator branches");
  }

  return EnsureOutputQTypeMatches(
      std::make_unique<FakeShortCircuitWhereOperator>(
          QExprOperatorSignature::Get(
              {GetQType<OptionalUnit>(), common_type, common_type},
              common_type)),
      input_types, output_type);
}

}  // namespace arolla
