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
#ifndef AROLLA_EXPR_OPERATORS_WHILE_LOOP_WHILE_LOOP_IMPL_H_
#define AROLLA_EXPR_OPERATORS_WHILE_LOOP_WHILE_LOOP_IMPL_H_

// The functions needed for while_loop implementation, extracted into a separate
// file for unit testing.

#include <functional>
#include <string>
#include <utility>

#include "absl//status/statusor.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/operators/while_loop/while_loop.h"

namespace arolla::expr_operators::while_loop_impl {

// Extracts subexpressions that depend on leaves, but not on placeholders (aka
// immutable in the while_loop context). The subexpressions are replaced with
// placeholders named using naming_function. The mapping from the placeholder
// name to the immutable subexpression is returned as a second result.
absl::StatusOr<std::pair<expr::ExprNodePtr, NamedExpressions>>
ExtractImmutables(
    const expr::ExprNodePtr& expr,
    std::function<std::string(const expr::ExprNodePtr& node)> naming_function);

}  // namespace arolla::expr_operators::while_loop_impl

#endif  // AROLLA_EXPR_OPERATORS_WHILE_LOOP_WHILE_LOOP_IMPL_H_
