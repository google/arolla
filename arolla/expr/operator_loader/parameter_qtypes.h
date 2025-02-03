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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_PARAMETER_QTYPES_H_
#define AROLLA_EXPR_OPERATOR_LOADER_PARAMETER_QTYPES_H_

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/thread_safe_model_executor.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"

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
//
// Please note that the `inputs` need to be the same as those used in
// the ExprOperator::InferAttributes() method, i.e., its elements should
// correspond to the node dependencies.
//
// Assuming that you have an expr node, the expected use case for this function
// is to collect qtypes from the node dependencies and pass them to
// this function along with the associated operator's signature.
//
// For more information, please check implementation of expr::BindArguments().
//
absl::StatusOr<ParameterQTypes> ExtractParameterQTypes(
    const expr::ExprOperatorSignature& signature,
    absl::Span<const expr::ExprAttributes> inputs);

// Compiles a model that takes values from ParameterQTypes and returns
// TypedValue.
absl::StatusOr<expr::ThreadSafeModelExecutor<ParameterQTypes, TypedValue>>
MakeParameterQTypeModelExecutor(expr::ExprNodePtr expr);

// Returns a string that describbes the parameter qtypes.
std::string FormatParameterQTypes(const ParameterQTypes& parameter_qtypes);

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_PARAMETER_QTYPES_H_
