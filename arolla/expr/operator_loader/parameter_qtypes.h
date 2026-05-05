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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_PARAMETER_QTYPES_H_
#define AROLLA_EXPR_OPERATOR_LOADER_PARAMETER_QTYPES_H_

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/sequence/sequence.h"

namespace arolla::operator_loader {

// A mapping from a parameter name to qtype.
//
// NOTE: A variadic-positional parameter gets represented as a tuple qtype.
//
// NOTE: We use inheritance instead of
//
//   using ParameterQTypes = absl::flat_hash_map<std::string, QTypePtr>;
//
// because it reduces the C++ mangling, and saves ~10KB in the binary size.
//
struct ParameterQTypes : absl::flat_hash_map<std::string, QTypePtr> {
  using absl::flat_hash_map<std::string, QTypePtr>::flat_hash_map;
};

// Returns a mapping from a parameter name to qtype; if a parameter qtype is
// unknown, the corresponding key will be missing.
absl::StatusOr<ParameterQTypes> ExtractParameterQTypes(
    const expr::ExprOperatorSignature& signature,
    const Sequence& input_qtype_sequence);

// Substitutes {param_name} placeholders in a message with their corresponding
// QType names. For tuple or namedtuple QTypes, {*param_name} and
// {**param_name} syntaxes are also supported.
std::string FormatParameterQTypes(absl::string_view message,
                                  const ParameterQTypes& parameter_qtypes);

// Returns a formatted string with the input qtypes for the given signature.
std::string FormatInputQTypes(
    const arolla::expr::ExprOperatorSignature& signature,
    const Sequence& input_qtype_sequence);

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_PARAMETER_QTYPES_H_
