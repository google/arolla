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

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/eval/thread_safe_model_executor.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/io/wildcard_input_loader.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {

using ::arolla::expr::CompileModelExecutor;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::ThreadSafeModelExecutor;

using Param = ExprOperatorSignature::Parameter;

absl::StatusOr<ParameterQTypes> ExtractParameterQTypes(
    const ExprOperatorSignature& signature,
    absl::Span<const ExprAttributes> inputs) {
  const auto nothing_qtype = GetNothingQType();
  for (const auto& input : inputs) {
    if (input.qtype() == nothing_qtype) {
      return absl::InvalidArgumentError(
          "inputs of type NOTHING are unsupported");
    }
  }
  ParameterQTypes result;
  result.reserve(signature.parameters.size());
  for (const auto& param : signature.parameters) {
    const QType* param_qtype = nullptr;
    switch (param.kind) {
      case Param::Kind::kPositionalOrKeyword:
        if (inputs.empty()) {
          return absl::FailedPreconditionError("unexpected number of inputs");
        }
        param_qtype = inputs.front().qtype();
        inputs.remove_prefix(1);
        break;
      case Param::Kind::kVariadicPositional:
        if (HasAllAttrQTypes(inputs)) {
          std::vector<QTypePtr> vararg_qtypes;
          vararg_qtypes.reserve(inputs.size());
          for (auto& attr : inputs) {
            vararg_qtypes.push_back(attr.qtype());
          }
          param_qtype = MakeTupleQType(vararg_qtypes);
        }
        inputs = {};
        break;
    }
    if (param_qtype != nullptr) {
      result[param.name] = param_qtype;
    }
  }
  if (!inputs.empty()) {
    return absl::FailedPreconditionError("unexpected number of inputs");
  }
  return result;
}

absl::StatusOr<ThreadSafeModelExecutor<ParameterQTypes, TypedValue>>
MakeParameterQTypeModelExecutor(ExprNodePtr expr) {
  auto accessor = [](const ParameterQTypes& parameter_qtypes,
                     absl::string_view parameter_name) -> QTypePtr {
    if (auto it = parameter_qtypes.find(parameter_name);
        it != parameter_qtypes.end()) {
      return it->second;
    }
    return GetNothingQType();
  };
  ASSIGN_OR_RETURN(auto input_loader,
                   WildcardInputLoader<ParameterQTypes>::Build(accessor));
  ASSIGN_OR_RETURN(auto model_executor,
                   CompileModelExecutor<TypedValue>(expr, *input_loader));
  return ThreadSafeModelExecutor<ParameterQTypes, TypedValue>(
      std::move(model_executor));
}

std::string FormatParameterQTypes(const ParameterQTypes& parameter_qtypes) {
  std::vector<std::pair<absl::string_view, absl::string_view>> items;
  items.reserve(parameter_qtypes.size());
  for (const auto& [name, qtype] : parameter_qtypes) {
    items.emplace_back(name, qtype->name());
  }
  std::sort(items.begin(), items.end());
  return absl::StrJoin(items, ", ", [](std::string* out, const auto& item) {
    absl::StrAppend(out, item.first, ":", item.second);
  });
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
