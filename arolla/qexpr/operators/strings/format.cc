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

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
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
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

constexpr absl::string_view kPrintfOperatorName = "strings.printf";
constexpr absl::string_view kFormatOperatorName = "strings.format";

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
  static const absl::NoDestructor<absl::flat_hash_map<QTypePtr, WrapValueFn>>
      converter_map({{GetQType<int32_t>(), &WrapValueImpl<int32_t>},
                     {GetQType<int64_t>(), &WrapValueImpl<int64_t>},
                     {GetQType<float>(), &WrapValueImpl<float>},
                     {GetQType<double>(), &WrapValueImpl<double>},
                     {GetWeakFloatQType(), &WrapValueImpl<double>},
                     {GetQType<Bytes>(), &WrapValueImpl<Bytes>},
                     {GetQType<bool>(), &WrapValueImpl<bool>}});
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

absl::StatusOr<Slot<Bytes>> ReadFormatSpecSlot(
    const TypedSlot& slot, std::vector<Slot<bool>>& presence_slots) {
  if (IsOptionalQType(slot.GetType())) {
    DCHECK_EQ(slot.SubSlotCount(), 2);
    ASSIGN_OR_RETURN(auto presence_slot, slot.SubSlot(0).ToSlot<bool>());
    presence_slots.push_back(presence_slot);
    return slot.SubSlot(1).ToSlot<Bytes>();
  } else {
    return slot.ToSlot<Bytes>();
  }
}

absl::StatusOr<std::vector<TypedSlot>> ReadValueArgSlots(
    absl::Span<const TypedSlot> arg_slots,
    std::vector<Slot<bool>>& presence_slots) {
  std::vector<TypedSlot> value_arg_slots;
  value_arg_slots.reserve(arg_slots.size());
  for (auto arg_slot : arg_slots) {
    TypedSlot value_slot = arg_slot;
    if (IsOptionalQType(arg_slot.GetType())) {
      ASSIGN_OR_RETURN(Slot<bool> presence_slot,
                       GetPresenceSubslotFromOptional(arg_slot));
      presence_slots.push_back(presence_slot);
      ASSIGN_OR_RETURN(value_slot, GetValueSubslotFromOptional(arg_slot));
    }
    value_arg_slots.push_back(value_slot);
  }
  return value_arg_slots;
}

class PrintfBoundOperator : public BoundOperator {
 public:
  PrintfBoundOperator(Slot<Bytes> format_spec_slot,
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

class PrintfOperator : public QExprOperator {
 public:
  explicit PrintfOperator(const QExprOperatorSignature* type)
      : QExprOperator(type) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> typed_input_slots,
      TypedSlot typed_output_slot) const override {
    std::vector<Slot<bool>> presence_slots;

    ASSIGN_OR_RETURN(Slot<Bytes> format_spec_slot,
                     ReadFormatSpecSlot(typed_input_slots[0], presence_slots));

    ASSIGN_OR_RETURN(
        auto arg_value_slots,
        ReadValueArgSlots(typed_input_slots.subspan(1), presence_slots));
    std::vector<SlotFormatter> slot_formatters;
    slot_formatters.reserve(arg_value_slots.size());
    for (auto value_slot : arg_value_slots) {
      ASSIGN_OR_RETURN(auto slot_formatter, SlotFormatter::Create(value_slot));
      slot_formatters.push_back(slot_formatter);
    }

    if (presence_slots.empty()) {
      ASSIGN_OR_RETURN(Slot<Bytes> output_slot,
                       typed_output_slot.ToSlot<Bytes>());
      return std::make_unique<PrintfBoundOperator>(
          format_spec_slot, std::move(slot_formatters), output_slot);
    } else {
      ASSIGN_OR_RETURN(Slot<OptionalValue<Bytes>> output_slot,
                       typed_output_slot.ToSlot<OptionalValue<Bytes>>());
      PrintfBoundOperator printf_op(format_spec_slot,
                                    std::move(slot_formatters),
                                    GetValueSubslotFromOptional(output_slot));
      return std::unique_ptr<BoundOperator>(new WhereAllBoundOperator(
          presence_slots, GetPresenceSubslotFromOptional(output_slot),
          std::move(printf_op)));
    }
  }
};

// Very limited Python style `str.format` parser.
class PyFormatParser {
 public:
  // Creates a parser for the given format specification.
  static absl::StatusOr<PyFormatParser> Parse(absl::string_view fmt_spec) {
    return Factory(fmt_spec).Build();
  }

  // Processes the format specification for the given argument names and values.
  // arg_names_id maps arg name to its index in arg_value_slots.
  absl::StatusOr<std::string> Process(
      const absl::flat_hash_map<absl::string_view, int64_t>& arg_names_id,
      absl::Span<const TypedSlot> arg_value_slots, FramePtr frame) const {
    DCHECK_EQ(arg_names_.size() + 1, regular_texts_.size());
    std::string result = regular_texts_[0];
    for (size_t i = 0; i < arg_names_.size(); ++i) {
      int64_t id = -1;
      if (auto it = arg_names_id.find(arg_names_[i]);
          it != arg_names_id.end()) {
        id = it->second;
      } else {
        return absl::InvalidArgumentError(
            absl::StrFormat("argument name '%s' is not found", arg_names_[i]));
      }
      TypedRef arg_value_ref = TypedRef::FromSlot(arg_value_slots[id], frame);
      ASSIGN_OR_RETURN(std::string txt, FormatTypeReference(arg_value_ref));
      absl::StrAppend(&result, txt, regular_texts_[i + 1]);
    }
    return result;
  }

 private:
  friend class Factory;

  class Factory {
   public:
    explicit Factory(absl::string_view fmt_spec) : fmt_spec_(fmt_spec) {}

    absl::StatusOr<PyFormatParser> Build() && {
      regular_texts_ = {""};
      for (size_t i = 0; i < fmt_spec_.size(); ++i) {
        char c = fmt_spec_[i];
        if (in_curly_) {
          RETURN_IF_ERROR(ProcessInCurly(c));
          continue;
        }
        if (c == '}') {
          if (i + 1 < fmt_spec_.size() && fmt_spec_[i + 1] == '}') {
            regular_texts_.back().push_back('}');
            ++i;
            continue;
          }
          return IncorrectSpec();
        }
        if (c == '{') {
          if (i + 1 < fmt_spec_.size() && fmt_spec_[i + 1] == '{') {
            regular_texts_.back().push_back('{');
            ++i;
            continue;
          }
          in_curly_ = true;
          continue;
        }
        regular_texts_.back().push_back(c);
      }
      if (in_curly_) {
        return IncorrectSpec();
      }
      return PyFormatParser(std::move(regular_texts_), std::move(arg_names_));
    }

   private:
    absl::Status ProcessInCurly(char c) {
      if (c == '_' || absl::ascii_isalnum(c)) {
        arg_name_.push_back(c);
        return absl::OkStatus();
      }
      if (c == '}') {
        regular_texts_.push_back("");
        in_curly_ = false;
        RETURN_IF_ERROR(ValidateAlphaNumArgName());
        arg_names_.push_back(std::move(arg_name_));
        arg_name_.clear();
        return absl::OkStatus();
      }
      return IncorrectSpec();
    }

    absl::Status IncorrectSpec() {
      return absl::InvalidArgumentError(
          absl::StrFormat("incorrect format specification '%s'", fmt_spec_));
    };

    absl::Status ValidateAlphaNumArgName() {
      if (arg_name_.empty() ||
          (arg_name_[0] != '_' && !absl::ascii_isalpha(arg_name_[0]))) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "incorrect arg name '%s' in format specification '%s'", arg_name_,
            fmt_spec_));
      }
      return absl::OkStatus();
    };

    absl::string_view fmt_spec_;
    bool in_curly_ = false;
    std::string arg_name_;

    std::vector<std::string> regular_texts_;
    std::vector<std::string> arg_names_;
  };

  explicit PyFormatParser(std::vector<std::string> regular_texts,
                          std::vector<std::string> arg_names)
      : regular_texts_(std::move(regular_texts)),
        arg_names_(std::move(arg_names)) {}

  absl::StatusOr<std::string> FormatTypeReference(
      TypedRef arg_value_ref) const {
    if (arg_value_ref.GetType() == GetQType<Bytes>()) {
      return arg_value_ref.UnsafeAs<Bytes>();
    }
    if (arg_value_ref.GetType() == GetQType<int32_t>() ||
        arg_value_ref.GetType() == GetQType<float>() ||
        arg_value_ref.GetType() == GetQType<bool>()) {
      return arg_value_ref.Repr();
    }
    if (arg_value_ref.GetType() == GetQType<int64_t>()) {
      return absl::StrCat(arg_value_ref.UnsafeAs<int64_t>());
    }
    if (arg_value_ref.GetType() == GetQType<double>()) {
      return absl::StrCat(arg_value_ref.UnsafeAs<double>());
    }
    if (arg_value_ref.GetType() == GetWeakFloatQType()) {
      return absl::StrCat(arg_value_ref.UnsafeAs<double>());
    }
    return absl::FailedPreconditionError(
        absl::StrCat("Unknown type: ", arg_value_ref.GetType()->name()));
  }

  // Regular texts before, between and after arg names. Always have
  // arg_names_.size() + 1 elements.
  std::vector<std::string> regular_texts_;
  std::vector<std::string> arg_names_;
};

class FormatBoundOperator : public BoundOperator {
 public:
  FormatBoundOperator(Slot<Bytes> format_spec_slot, Slot<Text> arg_names_slot,
                      std::vector<TypedSlot> arg_value_slots,
                      Slot<Bytes> output_slot)
      : format_spec_slot_(format_spec_slot),
        arg_names_slot_(arg_names_slot),
        arg_value_slots_(std::move(arg_value_slots)),
        output_slot_(output_slot) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    absl::string_view fmt_spec = frame.Get(format_spec_slot_);
    absl::string_view arg_names = frame.Get(arg_names_slot_);
    absl::flat_hash_map<absl::string_view, int64_t> arg_names_id;
    int64_t arg_name_count = 0;
    if (!arg_names.empty()) {
      for (auto name : absl::StrSplit(arg_names, ',')) {
        if (bool inserted = arg_names_id.emplace(name, arg_name_count++).second;
            !inserted) {
          ctx->set_status(absl::InvalidArgumentError(
              absl::StrFormat("arg names specification '%s' contains duplicate "
                              "names",
                              arg_names)));
          return;
        }
      }
    }
    if (arg_name_count != arg_value_slots_.size()) {
      ctx->set_status(absl::InvalidArgumentError(
          absl::StrFormat("arg names specification doesn't match number of "
                          "arguments: %s (expected #%d)",
                          arg_names, arg_value_slots_.size())));
      return;
    }

    absl::StatusOr<PyFormatParser> parser = PyFormatParser::Parse(fmt_spec);
    RETURN_IF_ERROR(parser.status()).With(ctx->set_status());
    absl::StatusOr<std::string> result =
        parser->Process(arg_names_id, arg_value_slots_, frame);
    RETURN_IF_ERROR(result.status()).With(ctx->set_status());
    frame.Set(output_slot_, Bytes(std::move(*result)));
  }

 private:
  Slot<Bytes> format_spec_slot_;
  Slot<Text> arg_names_slot_;
  std::vector<TypedSlot> arg_value_slots_;

  Slot<Bytes> output_slot_;
};

class FormatOperator : public QExprOperator {
 public:
  explicit FormatOperator(const QExprOperatorSignature* type)
      : QExprOperator(type) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> typed_input_slots,
      TypedSlot typed_output_slot) const override {
    std::vector<Slot<bool>> presence_slots;

    ASSIGN_OR_RETURN(auto format_spec_slot,
                     ReadFormatSpecSlot(typed_input_slots[0], presence_slots));
    ASSIGN_OR_RETURN(auto arg_names_slot, typed_input_slots[1].ToSlot<Text>());

    ASSIGN_OR_RETURN(
        auto arg_value_slots,
        ReadValueArgSlots(typed_input_slots.subspan(2), presence_slots));

    if (presence_slots.empty()) {
      ASSIGN_OR_RETURN(Slot<Bytes> output_slot,
                       typed_output_slot.ToSlot<Bytes>());
      return std::make_unique<FormatBoundOperator>(
          format_spec_slot, arg_names_slot, std::move(arg_value_slots),
          output_slot);
    } else {
      ASSIGN_OR_RETURN(Slot<OptionalValue<Bytes>> output_slot,
                       typed_output_slot.ToSlot<OptionalValue<Bytes>>());
      FormatBoundOperator format_op(format_spec_slot, arg_names_slot,
                                    std::move(arg_value_slots),
                                    GetValueSubslotFromOptional(output_slot));
      return std::unique_ptr<BoundOperator>(new WhereAllBoundOperator(
          presence_slots, GetPresenceSubslotFromOptional(output_slot),
          std::move(format_op)));
    }
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> PrintfOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.empty()) {
    return OperatorNotDefinedError(kPrintfOperatorName, input_types,
                                   "expected at least 1 argument");
  }
  if (DecayOptionalQType(input_types[0]) != GetQType<Bytes>()) {
    return OperatorNotDefinedError(kPrintfOperatorName, input_types,
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
      OperatorPtr(std::make_unique<PrintfOperator>(
          QExprOperatorSignature::Get(input_types, result_type))),
      input_types, output_type);
}

absl::StatusOr<OperatorPtr> FormatOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.size() <= 1) {
    return OperatorNotDefinedError(kFormatOperatorName, input_types,
                                   "expected at least 2 argument");
  }
  if (DecayOptionalQType(input_types[0]) != GetQType<Bytes>()) {
    return OperatorNotDefinedError(kFormatOperatorName, input_types,
                                   "format_spec must have BYTES QType");
  }
  if (input_types[1] != GetQType<Text>()) {
    return OperatorNotDefinedError(kFormatOperatorName, input_types,
                                   "arg_names  must have TEXT QType");
  }

  // Ensure remaining arguments are supported value types.
  bool has_optional_arg = IsOptionalQType(input_types[0]);
  for (size_t i = 2; i < input_types.size(); ++i) {
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
