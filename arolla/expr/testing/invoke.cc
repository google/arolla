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
#include "arolla/expr/testing/invoke.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::testing {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::Invoke;
using ::arolla::expr::Literal;
using ::arolla::expr::LookupOperator;

absl::StatusOr<TypedValue> InvokeExprOperator(
    const ExprOperatorPtr& op, absl::Span<const TypedValue> args,
    absl::Span<const KeywordArg> kwargs) {
  std::vector<ExprNodePtr> arg_nodes;
  arg_nodes.reserve(args.size());
  for (const auto& a : args) {
    arg_nodes.emplace_back(Literal(a));
  }
  absl::flat_hash_map<std::string, ExprNodePtr> kwarg_nodes;
  for (const auto& a : kwargs) {
    kwarg_nodes.emplace(std::string(a.keyword), Literal(a.value));
  }
  ASSIGN_OR_RETURN(auto expr, BindOp(op, arg_nodes, kwarg_nodes));
  return Invoke(expr, {});
}

absl::StatusOr<TypedValue> InvokeExprOperator(
    absl::string_view op_name, absl::Span<const TypedValue> args,
    absl::Span<const KeywordArg> kwargs) {
  ASSIGN_OR_RETURN(auto op, LookupOperator(op_name));
  return InvokeExprOperator(std::move(op), args, kwargs);
}

}  // namespace arolla::testing
