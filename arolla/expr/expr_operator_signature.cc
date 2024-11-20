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
#include "arolla/expr/expr_operator_signature.h"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using Param = ExprOperatorSignature::Parameter;

absl::Status ValidateSignatureParameterNames(
    const ExprOperatorSignature& signature) {
  // All parameter names are legal.
  for (const auto& param : signature.parameters) {
    if (!IsIdentifier(param.name)) {
      return absl::InvalidArgumentError(absl::StrCat(
          "illegal parameter name: '", absl::CEscape(param.name), "'"));
    }
  }
  // All parameter names are unique.
  absl::flat_hash_set<absl::string_view> param_names;
  param_names.reserve(signature.parameters.size());
  for (const auto& param : signature.parameters) {
    if (!param_names.insert(param.name).second) {
      return absl::InvalidArgumentError(
          absl::StrCat("non-unique parameter name: '", param.name, "'"));
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateSignatureParameterKinds(
    const ExprOperatorSignature& signature) {
  for (const auto& param : signature.parameters) {
    if (param.kind != Param::Kind::kPositionalOrKeyword &&
        param.kind != Param::Kind::kVariadicPositional) {
      return absl::InvalidArgumentError(
          absl::StrCat("parameter '", param.name,
                       "' has illegal kind: ", static_cast<int>(param.kind)));
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateSignaturePositionalOrKeywordParameters(
    const ExprOperatorSignature& signature) {
  bool had_default_value = false;
  for (const auto& param : signature.parameters) {
    if (param.kind != Param::Kind::kPositionalOrKeyword) {
      break;
    }
    if (!param.default_value.has_value()) {
      if (had_default_value) {
        return absl::InvalidArgumentError(
            "parameter without a default value goes after a parameter with "
            "a default value");
      }
    } else {
      had_default_value = true;
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateSignatureVariadicParameters(
    const ExprOperatorSignature& signature) {
  // All but the last parameter must not be variadic.
  for (size_t i = 0; i + 1 < signature.parameters.size(); ++i) {
    if (signature.parameters[i].kind == Param::Kind::kVariadicPositional) {
      return absl::InvalidArgumentError("variadic parameter must be the last");
    }
  }
  // Variadic parameter cannot have a default value.
  if (!signature.parameters.empty() &&
      signature.parameters.back().kind == Param::Kind::kVariadicPositional &&
      signature.parameters.back().default_value.has_value()) {
    return absl::InvalidArgumentError(
        "variadic parameter cannot have a default value");
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ValidateSignature(const ExprOperatorSignature& signature) {
  RETURN_IF_ERROR(ValidateSignatureParameterNames(signature));
  RETURN_IF_ERROR(ValidateSignatureParameterKinds(signature));
  RETURN_IF_ERROR(ValidateSignaturePositionalOrKeywordParameters(signature));
  RETURN_IF_ERROR(ValidateSignatureVariadicParameters(signature));
  return absl::OkStatus();
}

bool HasVariadicParameter(const ExprOperatorSignature& signature) {
  return !signature.parameters.empty() &&
         signature.parameters.back().kind == Param::Kind::kVariadicPositional;
}

namespace {

absl::Status MultipleValuesForArgumentError(absl::string_view name) {
  return absl::InvalidArgumentError(
      absl::StrCat("multiple values for argument: '", name, "'"));
}

absl::Status UnexpectedParameterKindError(Param::Kind kind) {
  return absl::InternalError(
      absl::StrCat("unexpected parameter kind: ", static_cast<int>(kind)));
}

absl::Status UnexpectedKeywordArgumentsError(
    std::vector<absl::string_view> unexpected_keyword_arguments) {
  if (unexpected_keyword_arguments.size() == 1) {
    return absl::InvalidArgumentError(
        absl::StrCat("unexpected keyword argument: '",
                     unexpected_keyword_arguments[0], "'"));
  }
  std::sort(unexpected_keyword_arguments.begin(),
            unexpected_keyword_arguments.end());
  return absl::InvalidArgumentError(
      absl::StrCat("unexpected keyword arguments: '",
                   absl::StrJoin(unexpected_keyword_arguments, "', '"), "'"));
}

absl::Status MissingArgumentsError(
    absl::Span<const absl::string_view> missing_arguments) {
  if (missing_arguments.size() == 1) {
    return absl::InvalidArgumentError(absl::StrCat(
        "missing 1 required argument: '", missing_arguments[0], "'"));
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "missing ", missing_arguments.size(), " required arguments: '",
      absl::StrJoin(missing_arguments, "', '"), "'"));
}

}  // namespace

absl::Status ValidateDepsCount(const ExprOperatorSignature& signature,
                               size_t deps_count, absl::StatusCode error_code) {
  const bool has_variadic_param = HasVariadicParameter(signature);
  size_t count_required_params = has_variadic_param
                                     ? signature.parameters.size() - 1
                                     : signature.parameters.size();
  if (deps_count < count_required_params ||
      (!has_variadic_param && deps_count > count_required_params)) {
    return absl::Status(
        error_code,
        absl::StrFormat("incorrect number of dependencies passed to an "
                        "operator node: expected %d but got %d",
                        count_required_params, deps_count));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::vector<ExprNodePtr>> BindArguments(
    const ExprOperatorSignature& signature, absl::Span<const ExprNodePtr> args,
    const absl::flat_hash_map<std::string, ExprNodePtr>& kwargs) {
  DCHECK_OK(ValidateSignature(signature));

  std::vector<ExprNodePtr> result;
  result.reserve(args.size() + kwargs.size());

  size_t paramIdx = 0;
  size_t argIdx = 0;
  for (; paramIdx < signature.parameters.size() && argIdx < args.size();
       ++paramIdx) {
    const auto& param = signature.parameters[paramIdx];
    if (param.kind == Param::Kind::kPositionalOrKeyword) {
      if (kwargs.count(param.name) != 0) {
        return MultipleValuesForArgumentError(param.name);
      }
      result.push_back(args[argIdx++]);
    } else if (param.kind == Param::Kind::kVariadicPositional) {
      result.insert(result.end(), args.begin() + argIdx, args.end());
      argIdx = args.size();
    } else {
      return UnexpectedParameterKindError(param.kind);
    }
  }
  if (argIdx < args.size()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "too many positional arguments passed: expected maximumum is ",
        result.size(), " but got ", args.size()));
  }
  std::vector<absl::string_view> missing_arguments;
  absl::flat_hash_set<absl::string_view> used_kwargs;
  used_kwargs.reserve(args.size() + kwargs.size());
  for (; paramIdx < signature.parameters.size(); ++paramIdx) {
    const auto& param = signature.parameters[paramIdx];
    if (param.kind == Param::Kind::kPositionalOrKeyword) {
      if (const auto it = kwargs.find(param.name); it != kwargs.end()) {
        used_kwargs.insert(param.name);
        result.push_back(it->second);
      } else if (param.default_value.has_value()) {
        result.push_back(Literal(*param.default_value));
      } else {
        missing_arguments.push_back(param.name);
      }
    } else if (param.kind != Param::Kind::kVariadicPositional) {
      return UnexpectedParameterKindError(param.kind);
    }
  }
  std::vector<absl::string_view> unexpected_keyword_arguments;
  for (const auto& kv : kwargs) {
    if (!used_kwargs.contains(kv.first)) {
      unexpected_keyword_arguments.push_back(kv.first);
    }
  }
  if (!unexpected_keyword_arguments.empty()) {
    return UnexpectedKeywordArgumentsError(
        std::move(unexpected_keyword_arguments));
  }
  if (!missing_arguments.empty()) {
    return MissingArgumentsError(missing_arguments);
  }
  return result;
}

ExprOperatorSignature ExprOperatorSignature::MakeArgsN(size_t n) {
  ExprOperatorSignature result;
  if (n == 1) {
    result.parameters.push_back({absl::StrCat("arg")});
  } else {
    for (size_t i = 0; i < n; ++i) {
      result.parameters.push_back({absl::StrCat("arg", i + 1)});
    }
  }
  return result;
}

ExprOperatorSignature ExprOperatorSignature::MakeVariadicArgs() {
  return ExprOperatorSignature{
      {"args", std::nullopt,
       ExprOperatorSignature::Parameter::Kind::kVariadicPositional}};
}

absl::StatusOr<ExprOperatorSignature> ExprOperatorSignature::Make(
    absl::string_view signature_spec,
    absl::Span<const TypedValue> default_values) {
  ExprOperatorSignature result;
  if (auto pos = signature_spec.find('|'); pos < signature_spec.size()) {
    result.aux_policy = std::string(
        absl::StripLeadingAsciiWhitespace(signature_spec.substr(pos + 1)));
    signature_spec = signature_spec.substr(0, pos);
  }
  signature_spec = absl::StripAsciiWhitespace(signature_spec);
  std::vector<absl::string_view> param_defs;
  if (!signature_spec.empty()) {
    param_defs = absl::StrSplit(signature_spec, ',');
  }
  size_t i = 0;
  for (auto param_def : param_defs) {
    Param param;
    param_def = absl::StripAsciiWhitespace(param_def);
    if (absl::StartsWith(param_def, "*")) {
      param_def = absl::StripLeadingAsciiWhitespace(param_def.substr(1));
      param.kind = Param::Kind::kVariadicPositional;
    } else {
      param.kind = Param::Kind::kPositionalOrKeyword;
    }
    if (absl::EndsWith(param_def, "=")) {
      param_def = absl::StripTrailingAsciiWhitespace(
          param_def.substr(0, param_def.size() - 1));
      if (i >= default_values.size()) {
        return absl::InvalidArgumentError(absl::StrCat(
            "default value expected, but not provided for parameter: '",
            param_def, "'"));
      }
      param.default_value = default_values[i];
      i += 1;
    }
    param.name = std::string(param_def);
    result.parameters.push_back(std::move(param));
  }
  if (i != default_values.size()) {
    return absl::InvalidArgumentError(
        "some of the provided default values left unused");
  }
  RETURN_IF_ERROR(ValidateSignature(result));
  return result;
}

std::string GetExprOperatorSignatureSpec(
    const ExprOperatorSignature& signature) {
  std::ostringstream result;
  bool first = true;
  for (const auto& param : signature.parameters) {
    result << NonFirstComma(first);
    switch (param.kind) {
      case Param::Kind::kPositionalOrKeyword:
        break;
      case Param::Kind::kVariadicPositional:
        result << '*';
    }
    result << param.name;
    if (param.default_value.has_value()) {
      result << '=';
    }
  }
  if (!signature.aux_policy.empty()) {
    result << "|" << signature.aux_policy;
  }
  return std::move(result).str();
}

}  // namespace arolla::expr

namespace arolla {

void FingerprintHasherTraits<expr::ExprOperatorSignature>::operator()(
    FingerprintHasher* hasher,
    const expr::ExprOperatorSignature& signature) const {
  hasher->Combine(signature.parameters.size());
  for (const auto& param : signature.parameters) {
    hasher->Combine(param.name, param.kind);
    hasher->Combine(param.default_value ? param.default_value->GetFingerprint()
                                        : Fingerprint{});
  }
  hasher->Combine(signature.aux_policy);
}

}  // namespace arolla
