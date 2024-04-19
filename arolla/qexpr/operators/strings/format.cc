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
#include "arolla/qexpr/operators/strings/format.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
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
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

template <typename T>
using Slot = FrameLayout::Slot<T>;

// ValueHolder is used as temporary storage for values which need to be
// converted to a new type prior to being wrapped with absl::FormatArg.
// Currently only required for Bytes (which is converted to string_view).
class ValueHolder {
 public:
  // Adds a value to this object, and returns a reference to it. The reference
  // will remain valid for the lifetime of this object.
  const absl::string_view& AddValue(absl::string_view value) {
    return values_.emplace_back(value);
  }

 private:
  // Important to use a container type for which appending doesn't move values,
  // ie. not std::vector.
  std::deque<absl::string_view> values_;
};

// Wrap a typed ref of a particular type. The default implementation returns
// a FormatArg which references to original value. No temporary data is written
// to value_holder.
template <typename T>
absl::FormatArg WrapValueImpl(const void* source, ValueHolder*) {
  const T& value_ref = *(reinterpret_cast<const T*>(source));
  return absl::FormatArg(value_ref);
}

// The Bytes implementation of WrapValueImpl uses the ValueHolder to store
// a string_view, which is then wrapped.
template <>
absl::FormatArg WrapValueImpl<Bytes>(const void* source,
                                     ValueHolder* value_holder) {
  const Bytes& value_ref = *(reinterpret_cast<const Bytes*>(source));
  return absl::FormatArg(value_holder->AddValue(value_ref));
}

using WrapValueFn = absl::FormatArg (*)(const void*, ValueHolder*);

absl::StatusOr<WrapValueFn> GetWrapValueFn(QTypePtr qtype) {
  static const Indestructible<absl::flat_hash_map<QTypePtr, WrapValueFn>>
      converter_map([](void* self) {
        new (self) absl::flat_hash_map<QTypePtr, WrapValueFn>{
            {GetQType<int32_t>(), &WrapValueImpl<int32_t>},
            {GetQType<int64_t>(), &WrapValueImpl<int64_t>},
            {GetQType<float>(), &WrapValueImpl<float>},
            {GetQType<double>(), &WrapValueImpl<double>},
            {GetWeakFloatQType(), &WrapValueImpl<double>},
            {GetQType<Bytes>(), &WrapValueImpl<Bytes>},
            {GetQType<bool>(), &WrapValueImpl<bool>}};
      });
  auto iter = converter_map->find(qtype);
  if (iter == converter_map->end()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "%s is not a supported format argument type", qtype->name()));
  }

  return iter->second;
}

// Wrapper around a typed slot and the corresponding code needed to convert its
// contents into an absl::FormatArg.
class SlotFormatter {
 public:
  // Creates a SlotFormatter. Returns an InvalidArgumentError if the slot type
  // does is not supported as a format argument.
  static absl::StatusOr<SlotFormatter> Create(TypedSlot slot) {
    ASSIGN_OR_RETURN(auto wrap_value_fn, GetWrapValueFn(slot.GetType()));
    return SlotFormatter(slot, wrap_value_fn);
  }

  // Returns a FormatArg corresponding to this formatters slot in the provided
  // frame. May add a temporary value to value_holder if needed.
  absl::FormatArg Format(FramePtr frame, ValueHolder* value_holder) const {
    TypedRef ref = TypedRef::FromSlot(slot_, frame);
    return wrap_value_fn_(ref.GetRawPointer(), value_holder);
  }

 private:
  SlotFormatter(TypedSlot slot, WrapValueFn wrap_value_fn)
      : slot_(slot), wrap_value_fn_(wrap_value_fn) {}

  TypedSlot slot_;
  WrapValueFn wrap_value_fn_;
};

class FormatBoundOperator : public BoundOperator {
 public:
  FormatBoundOperator(Slot<Bytes> format_spec_slot,
                      std::vector<SlotFormatter> slot_formatters,
                      Slot<Bytes> output_slot)
      : format_spec_slot_(format_spec_slot),
        slot_formatters_(std::move(slot_formatters)),
        output_slot_(output_slot) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    absl::string_view fmt_spec = frame.Get(format_spec_slot_);
    absl::UntypedFormatSpec fmt(fmt_spec);
    ValueHolder value_holder;
    std::vector<absl::FormatArg> fmt_args;
    fmt_args.reserve(slot_formatters_.size());
    for (const auto& slot_formatter : slot_formatters_) {
      fmt_args.push_back(slot_formatter.Format(frame, &value_holder));
    }

    std::string out;
    if (absl::FormatUntyped(&out, fmt, fmt_args)) {
      frame.Set(output_slot_, Bytes(std::move(out)));
    } else {
      ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
          "format specification '%s' doesn't match format arguments",
          fmt_spec)));
    }
  }

 private:
  Slot<Bytes> format_spec_slot_;
  std::vector<SlotFormatter> slot_formatters_;

  Slot<Bytes> output_slot_;
};

class FormatOperator : public QExprOperator {
 public:
  explicit FormatOperator(const QExprOperatorSignature* type)
      : QExprOperator(std::string(kFormatOperatorName), type) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> typed_input_slots,
      TypedSlot typed_output_slot) const override {
    std::vector<Slot<bool>> presence_slots;

    Slot<Bytes> format_spec_slot = Slot<Bytes>::UnsafeSlotFromOffset(
        0);  // Will override in the next expression.
    if (IsOptionalQType(typed_input_slots[0].GetType())) {
      DCHECK_EQ(typed_input_slots[0].SubSlotCount(), 2);
      ASSIGN_OR_RETURN(auto presence_slot,
                       typed_input_slots[0].SubSlot(0).ToSlot<bool>());
      presence_slots.push_back(presence_slot);
      ASSIGN_OR_RETURN(format_spec_slot,
                       typed_input_slots[0].SubSlot(1).ToSlot<Bytes>());
    } else {
      ASSIGN_OR_RETURN(format_spec_slot, typed_input_slots[0].ToSlot<Bytes>());
    }

    auto arg_slots = typed_input_slots.subspan(1);
    std::vector<SlotFormatter> slot_formatters;
    slot_formatters.reserve(arg_slots.size());
    for (auto arg_slot : arg_slots) {
      TypedSlot value_slot = arg_slot;
      if (IsOptionalQType(arg_slot.GetType())) {
        ASSIGN_OR_RETURN(Slot<bool> presence_slot,
                         GetPresenceSubslotFromOptional(arg_slot));
        presence_slots.push_back(presence_slot);
        ASSIGN_OR_RETURN(value_slot, GetValueSubslotFromOptional(arg_slot));
      }
      ASSIGN_OR_RETURN(auto slot_formatter, SlotFormatter::Create(value_slot));
      slot_formatters.push_back(slot_formatter);
    }

    if (presence_slots.empty()) {
      ASSIGN_OR_RETURN(Slot<Bytes> output_slot,
                       typed_output_slot.ToSlot<Bytes>());
      return {std::make_unique<FormatBoundOperator>(
          format_spec_slot, slot_formatters, output_slot)};
    } else {
      ASSIGN_OR_RETURN(Slot<OptionalValue<Bytes>> output_slot,
                       typed_output_slot.ToSlot<OptionalValue<Bytes>>());
      FormatBoundOperator format_op(format_spec_slot, slot_formatters,
                                    GetValueSubslotFromOptional(output_slot));
      return {std::unique_ptr<BoundOperator>(new WhereAllBoundOperator(
          presence_slots, GetPresenceSubslotFromOptional(output_slot),
          std::move(format_op)))};
    }
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> FormatOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.empty()) {
    return OperatorNotDefinedError(kFormatOperatorName, input_types,
                                   "expected at least 1 argument");
  }
  if (DecayOptionalQType(input_types[0]) != GetQType<Bytes>()) {
    return OperatorNotDefinedError(kFormatOperatorName, input_types,
                                   "format_spec must have BYTES QType");
  }

  // Ensure remaining arguments are supported value types.
  bool has_optional_arg = IsOptionalQType(input_types[0]);
  for (size_t i = 1; i < input_types.size(); ++i) {
    QTypePtr value_type = input_types[i];
    if (IsOptionalQType(value_type)) {
      has_optional_arg = true;
      value_type = DecayOptionalQType(value_type);
    }
    RETURN_IF_ERROR(GetWrapValueFn(value_type).status());
  }

  // Construct format operator
  QTypePtr result_type =
      has_optional_arg ? GetQType<OptionalValue<Bytes>>() : GetQType<Bytes>();
  return EnsureOutputQTypeMatches(
      OperatorPtr(std::make_unique<FormatOperator>(
          QExprOperatorSignature::Get(input_types, result_type))),
      input_types, output_type);
}

}  // namespace arolla
