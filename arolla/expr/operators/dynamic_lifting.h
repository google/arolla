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
#ifndef AROLLA_EXPR_OPERATORS_DYNAMIC_LIFTING_H_
#define AROLLA_EXPR_OPERATORS_DYNAMIC_LIFTING_H_

#include "absl/status/statusor.h"
#include "arolla/expr/expr_operator.h"

namespace arolla::expr_operators {

// Constructs an operator that lifts the argument dynamically using core.map.
// The resulting operator lowers directly into the original operator in case if
// no array arguments are present, or into core.map(original_operator, ...) if
// there is at least one array argument.
absl::StatusOr<expr::ExprOperatorPtr> LiftDynamically(
    const absl::StatusOr<expr::ExprOperatorPtr>& op_or);

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_DYNAMIC_LIFTING_H_
