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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_QTYPE_CONSTRAINT_H_
#define AROLLA_EXPR_OPERATOR_LOADER_QTYPE_CONSTRAINT_H_

#include <functional>
#include <string>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/operator_loader/parameter_qtypes.h"

namespace arolla::operator_loader {

// A helper structure that binds together a constraint predicate and
// a corresponding error message.
struct QTypeConstraint {
  // A predicate expression.
  //
  // A predicate takes parameter qtypes and returns OptionatUnit{true} if the
  // constraint is fulfilled, or OptionatUnit{false} otherwise.
  expr::ExprNodePtr predicate_expr;

  // An error message.
  //
  // Placeholders, like {parameter_name}, get replaced with the actual
  // type names during formatting.
  std::string error_message;
};

// Compiled QTypeConstraints.
//
// Returns `true` if all qtype constraints are met, `false` if no constraints
// are violated but some needed parameters are missing, or returns an error if
// any constraint is violated.
using QTypeConstraintFn =
    std::function<absl::StatusOr<bool>(const ParameterQTypes& qtypes)>;

// Returns std::function<> that checks the given predicates.
absl::StatusOr<QTypeConstraintFn> MakeQTypeConstraintFn(
    absl::Span<const QTypeConstraint> constraints);

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_QTYPE_CONSTRAINT_H_
