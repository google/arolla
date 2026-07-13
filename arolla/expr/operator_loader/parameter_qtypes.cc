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
#include "arolla/expr/operator_loader/parameter_qtypes.h"

#include <cstddef>
#include <sstream>
#include <string>
#include <utility>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/string.h"

namespace arolla::operator_loader {

using ::arolla::expr::ExprOperatorSignature;

using Param = ExprOperatorSignature::Parameter;

absl::StatusOr<ParameterQTypes> ExtractParameterQTypes(
    const ExprOperatorSignature& signature,
    const Sequence& input_qtype_sequence) {
  const auto is_unknown_qtype = [](QTypePtr qtype) {
    static const auto nothing_qtype = GetNothingQType();
    DCHECK(qtype != nullptr);
    return qtype == nothing_qtype || qtype == nullptr;
  };
  if (input_qtype_sequence.value_qtype() != GetQTypeQType()) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected a sequence of QTYPEs, got SEQUENCE[",
                     input_qtype_sequence.value_qtype()->name(), "]"));
  }
  auto inputs = input_qtype_sequence.UnsafeSpan<QTypePtr>();
  ParameterQTypes result;
  result.reserve(signature.parameters.size());
  for (const auto& param : signature.parameters) {
    switch (param.kind) {
      case Param::Kind::kPositionalOrKeyword:
        if (inputs.empty()) {
          return absl::FailedPreconditionError("unexpected number of inputs");
        }
        if (!is_unknown_qtype(inputs.front())) {
          result[param.name] = inputs.front();
        }
        inputs.remove_prefix(1);
        break;
      case Param::Kind::kVariadicPositional:
        if (!absl::c_any_of(inputs, is_unknown_qtype)) {
          result[param.name] = MakeTupleQType(inputs);
        }
        inputs = {};
        break;
    }
  }
  if (!inputs.empty()) {
    return absl::FailedPreconditionError("unexpected number of inputs");
  }
  return result;
}

namespace {

void FormatKwargsTo(std::string& result, absl::string_view spec,
                    const ParameterQTypes& parameter_qtypes) {
  auto [name, exclude_filter] =
      std::pair<absl::string_view, absl::string_view>(
          absl::StrSplit(spec, absl::MaxSplits('!', 1)));

  auto it = parameter_qtypes.find(name);
  if (it == parameter_qtypes.end() || !IsNamedTupleQType(it->second)) {
    absl::StrAppend(&result, "{**", spec, "}");
    return;
  }

  const auto& fields = it->second->type_fields();
  const auto& field_names = GetFieldNames(it->second);
  DCHECK_EQ(fields.size(), field_names.size());
  bool is_first = true;
  for (size_t k = 0; k < fields.size() && k < field_names.size(); ++k) {
    if (fields[k].GetType()->name() != exclude_filter) {
      absl::StrAppend(&result, NonFirstComma(is_first), field_names[k],
                      ": ", fields[k].GetType()->name());
    }
  }
}

void FormatArgsTo(std::string& result, absl::string_view spec,
                  const ParameterQTypes& parameter_qtypes) {
  auto it = parameter_qtypes.find(spec);
  if (it == parameter_qtypes.end() || !IsTupleQType(it->second)) {
    absl::StrAppend(&result, "{*", spec, "}");
    return;
  }

  bool is_first = true;
  for (const auto& field : it->second->type_fields()) {
    absl::StrAppend(&result, NonFirstComma(is_first),
                    field.GetType()->name());
  }
}

void FormatArgTo(std::string& result, absl::string_view spec,
                 const ParameterQTypes& parameter_qtypes) {
  auto it = parameter_qtypes.find(spec);
  if (it == parameter_qtypes.end()) {
    absl::StrAppend(&result, "{", spec, "}");
    return;
  }
  absl::StrAppend(&result, it->second->name());
}

}  // namespace

std::string FormatParameterQTypes(absl::string_view message,
                                  const ParameterQTypes& parameter_qtypes) {
  std::string result;
  result.reserve(message.size());
  for (size_t i = 0; i < message.size();) {
    const size_t close_bracket = message.find('}', message.find('{', i));
    if (close_bracket == absl::string_view::npos) {
      result.append(message.substr(i));
      break;
    }

    const size_t open_bracket = message.rfind('{', close_bracket - 1);
    result.append(message.substr(i, open_bracket - i));
    i = close_bracket + 1;

    absl::string_view spec =
        message.substr(open_bracket + 1, close_bracket - open_bracket - 1);

    if (absl::ConsumePrefix(&spec, "**")) {
      FormatKwargsTo(result, spec, parameter_qtypes);
    } else if (absl::ConsumePrefix(&spec, "*")) {
      FormatArgsTo(result, spec, parameter_qtypes);
    } else {
      FormatArgTo(result, spec, parameter_qtypes);
    }
  }
  return result;
}

std::string FormatInputQTypes(const ExprOperatorSignature& signature,
                              const Sequence& input_qtype_sequence) {
  const auto is_unknown_qtype = [](QTypePtr qtype) {
    static const auto nothing_qtype = GetNothingQType();
    DCHECK(qtype != nullptr);
    return qtype == nothing_qtype || qtype == nullptr;
  };
  if (input_qtype_sequence.value_qtype() != GetQTypeQType()) {
    return "<invalid input_qtype_sequence>";
  }
  auto inputs = input_qtype_sequence.UnsafeSpan<QTypePtr>();
  std::ostringstream result;
  bool first_param = true;
  for (const auto& param : signature.parameters) {
    switch (param.kind) {
      case Param::Kind::kPositionalOrKeyword: {
        if (inputs.empty()) {
          return "<unexpected number of inputs>";
        }
        result << NonFirstComma(first_param) << param.name << ": "
               << (is_unknown_qtype(inputs.front()) ? "NOTHING"
                                                    : inputs.front()->name());
        inputs.remove_prefix(1);
        break;
      }
      case Param::Kind::kVariadicPositional: {
        result << NonFirstComma(first_param) << "*" << param.name << ": (";
        bool first_field = true;
        for (QTypePtr input : inputs) {
          result << NonFirstComma(first_field)
                 << (is_unknown_qtype(input) ? "NOTHING" : input->name());
        }
        result << ")";
        inputs = {};
        break;
      }
    }
  }
  if (!inputs.empty()) {
    return "<unexpected number of inputs>";
  }
  return std::move(result).str();
}

}  // namespace arolla::operator_loader
