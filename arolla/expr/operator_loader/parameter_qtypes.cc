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
#include <string>
#include <utility>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/string.h"

namespace arolla::operator_loader {

using Param = ::arolla::expr::ExprOperatorSignature::Parameter;

absl::StatusOr<ParameterQTypes> ExtractParameterQTypes(
    const arolla::expr::ExprOperatorSignature& signature,
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

std::string FormatParameterQTypes(absl::string_view message,
                                  const ParameterQTypes& parameter_qtypes) {
  absl::flat_hash_map<std::string, std::string> replacements;
  replacements.reserve(parameter_qtypes.size());
  for (const auto& [param_name, param_qtype] : parameter_qtypes) {
    replacements[absl::StrCat("{", param_name, "}")] = param_qtype->name();
    if (IsTupleQType(param_qtype)) {
      std::string out;
      bool is_first = true;
      for (const auto& field : param_qtype->type_fields()) {
        absl::StrAppend(&out, NonFirstComma(is_first), field.GetType()->name());
      }
      replacements[absl::StrCat("{*", param_name, "}")] = std::move(out);
    }
    if (IsNamedTupleQType(param_qtype)) {
      const auto& fields = param_qtype->type_fields();
      const auto& field_names = GetFieldNames(param_qtype);
      DCHECK_EQ(fields.size(), field_names.size());
      std::string out;
      bool is_first = true;
      for (size_t i = 0; i < fields.size() && i < field_names.size(); ++i) {
        absl::StrAppend(&out, NonFirstComma(is_first), field_names[i], ": ",
                        fields[i].GetType()->name());
      }
      replacements[absl::StrCat("{**", param_name, "}")] = std::move(out);
    }
  }
  return absl::StrReplaceAll(message, replacements);
}

}  // namespace arolla::operator_loader
