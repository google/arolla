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
#ifndef AROLLA_EXPR_OPERATORS_WHILE_LOOP_WHILE_LOOP_H_
#define AROLLA_EXPR_OPERATORS_WHILE_LOOP_WHILE_LOOP_H_

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"

namespace arolla::expr_operators {

using NamedExpressions = absl::flat_hash_map<std::string, expr::ExprNodePtr>;

// Constructs an expression that runs a loop with the given initial_state,
// condition and body.
//
//   initial_state: a mapping from the loop internal state variable names to
//   their initial values.
//
//   condition: an expression that returns OptionalUnit indicating whether the
//   loop has to be continued.
//
//   body: a mapping from the loop internal state variable names to expressions
//   evaluating their next values.
//
// Returns: named expressions that evaluate to values of the last invocation of
// the corresponding body expression (or to the initial values if no loop
// iterations happened).
//
// The `condition` and `body` expressions can use placeholders named as loop
// internal state variables (keys in initial_state map). All other placeholders
// are prohibited (although, they are allowed in `initial_state`).
//
// Usage example (computes GCD of L.a and L.b):
//
//   from arolla.experimental import while_loop
//
//   gcd = while_loop.while_loop(
//       initial_state=dict(x=L.a, y=L.b),
//       condition=P.y != 0,
//       body=dict(x=P.y, y=P.x % P.y))['x']
//
absl::StatusOr<expr::ExprNodePtr> MakeWhileLoop(NamedExpressions initial_state,
                                                expr::ExprNodePtr condition,
                                                NamedExpressions body);

// While loop Expr operator.
//
// NOTE: Consider using MakeWhileLoop instead. It provides essential syntactic
// sugar.
//
// It is a stateful operator parameterized by loop body and condition. The first
// argument is the initial value for the loop mutable state. All the remaining
// arguments represent immutable state and are passed to the loop's body and
// condition on each iteration. The operator runs until condition evaluates to
// `false` and returns the last value of its mutable state.
//
class WhileLoopOperator final : public expr::BuiltinExprOperatorTag,
                                public expr::ExprOperatorWithFixedSignature {
  struct PrivateConstrutorTag {};

 public:
  // Creates a loop operator with the given signature, condition, body and
  // (optional) operator name.
  // Body and condition must have exactly same signature as the loop itself.
  // Condition must return OptionalUnit, body must return the same type as the
  // first input (the loop's mutable state).
  static absl::StatusOr<std::shared_ptr<WhileLoopOperator>> Make(
      const expr::ExprOperatorSignature& signature,
      const expr::ExprOperatorPtr& condition,
      const expr::ExprOperatorPtr& body);
  static absl::StatusOr<std::shared_ptr<WhileLoopOperator>> Make(
      absl::string_view name, const expr::ExprOperatorSignature& signature,
      const expr::ExprOperatorPtr& condition,
      const expr::ExprOperatorPtr& body);

  // Private constructor, use Make() instead.
  WhileLoopOperator(PrivateConstrutorTag, absl::string_view name,
                    const expr::ExprOperatorSignature& signature,
                    const expr::ExprOperatorPtr& condition,
                    const expr::ExprOperatorPtr& body);

  absl::StatusOr<expr::ExprAttributes> InferAttributes(
      absl::Span<const expr::ExprAttributes> inputs) const final;

  const expr::ExprOperatorPtr& condition() const { return condition_; }
  const expr::ExprOperatorPtr& body() const { return body_; }

  absl::string_view py_qvalue_specialization_key() const final {
    return "::arolla::expr_operators::WhileLoopOperator";
  }

 private:
  expr::ExprOperatorPtr condition_;
  expr::ExprOperatorPtr body_;
};

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_WHILE_LOOP_WHILE_LOOP_H_
