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
#ifndef AROLLA_EXPR_EVAL_EXPR_UTILS_H_
#define AROLLA_EXPR_EVAL_EXPR_UTILS_H_

#include <functional>

#include "absl/status/statusor.h"
#include "arolla/expr/expr_node.h"

namespace arolla::expr::eval_internal {

// Wraps a part of the expression into a lambda and replaces this subexpression
// with a call of this lambda.
//
// The function traverses the expression stopping at the nodes where
// is_in_lambda returns false. These nodes and (all their subtrees) will be kept
// outside.
// NOTE: is_in_lambda must return false for leaves, because they are prohibited
// in lambdas.
absl::StatusOr<ExprNodePtr> ExtractLambda(
    const ExprNodePtr& expr,
    std::function<absl::StatusOr<bool>(const ExprNodePtr&)> is_in_lambda);

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_EXPR_UTILS_H_
