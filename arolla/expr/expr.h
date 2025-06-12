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
#ifndef AROLLA_EXPR_EXPR_H_
#define AROLLA_EXPR_EXPR_H_

#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::expr {

// Applies ToLowerLevel transformation to the top node.
absl::StatusOr<ExprNodePtr> ToLowerNode(const ExprNodePtr& node);

// Expands the given expression to the lowest possible level.
absl::StatusOr<ExprNodePtr> ToLowest(const ExprNodePtr& expr);

// Creates a Literal node from a value.
template <typename T>
ExprNodePtr Literal(T&& value) {
  return ExprNode::MakeLiteralNode(
      TypedValue::FromValue(std::forward<T>(value)));
}

// Creates a Literal node from a value.
template <typename T>
ExprNodePtr Literal(const T& value) {
  return ExprNode::MakeLiteralNode(TypedValue::FromValue(value));
}

// Creates a Literal node from a value.
inline ExprNodePtr Literal(const TypedValue& qvalue) {
  return ExprNode::MakeLiteralNode(TypedValue(qvalue));
}

// Creates a Literal node from a value.
inline ExprNodePtr Literal(TypedValue& qvalue) {
  return ExprNode::MakeLiteralNode(TypedValue(qvalue));
}

// Creates a Literal node from a value.
inline ExprNodePtr Literal(TypedValue&& qvalue) {
  return ExprNode::MakeLiteralNode(std::move(qvalue));
}

// Returns a Leaf with a given leaf key.
inline ExprNodePtr Leaf(absl::string_view leaf_key) {
  return ExprNode::MakeLeafNode(leaf_key);
}

// Returns a Placeholder with a given placeholder key.
inline ExprNodePtr Placeholder(absl::string_view placeholder_key) {
  return ExprNode::MakePlaceholderNode(placeholder_key);
}

// Binds the given operator with the arguments.
absl::StatusOr<ExprNodePtr> BindOp(
    ExprOperatorPtr op, absl::Span<const ExprNodePtr> args,
    const absl::flat_hash_map<std::string, ExprNodePtr>& kwargs);

// Finds an operator in ExprOperatorRegistry by name, and binds it with
// given arguments.
//
// This function parses the op_name string and returns an error if the parsing
// is not successful. Prefer using this version only with string constants.
absl::StatusOr<ExprNodePtr> BindOp(
    absl::string_view op_name, absl::Span<const ExprNodePtr> args,
    const absl::flat_hash_map<std::string, ExprNodePtr>& kwargs);

// Like BindOp, but taking absl::StatusOr<ExprNodePtr> instead of ExprNodePtr.
// Propagates errors upwards.
absl::StatusOr<ExprNodePtr> CallOp(
    absl::StatusOr<ExprOperatorPtr> status_or_op,
    std::initializer_list<absl::StatusOr<ExprNodePtr>> status_or_args,
    std::initializer_list<std::pair<std::string, absl::StatusOr<ExprNodePtr>>>
        status_or_kwargs = {});

absl::StatusOr<ExprNodePtr> CallOp(
    absl::StatusOr<ExprOperatorPtr> status_or_op,
    std::vector<absl::StatusOr<ExprNodePtr>> status_or_args,
    absl::flat_hash_map<std::string, absl::StatusOr<ExprNodePtr>>
        status_or_kwargs = {});

absl::StatusOr<ExprNodePtr> CallOp(
    absl::string_view op_name,
    std::initializer_list<absl::StatusOr<ExprNodePtr>> status_or_args,
    std::initializer_list<std::pair<std::string, absl::StatusOr<ExprNodePtr>>>
        status_or_kwargs = {});

absl::StatusOr<ExprNodePtr> CallOp(
    absl::string_view op_name,
    std::vector<absl::StatusOr<ExprNodePtr>> status_or_args,
    absl::flat_hash_map<std::string, absl::StatusOr<ExprNodePtr>>
        status_or_kwargs = {});

// Creates a node with given operator and dependencies.
//
// NOTE: This function expects that `deps` is appropriately aligned with
// operator's parameters. The provided dependencies will be attached to
// the new node AS-IS.
absl::StatusOr<ExprNodePtr> MakeOpNode(ExprOperatorPtr op,
                                       std::vector<ExprNodePtr> deps);

// Creates a new node by cloning a given one and replacing the expr operator.
absl::StatusOr<ExprNodePtr> WithNewOperator(const ExprNodePtr& node,
                                            ExprOperatorPtr op);

// Returns a new expression node that is same as a given one, but with new
// dependencies.
absl::StatusOr<ExprNodePtr> WithNewDependencies(const ExprNodePtr& node,
                                                std::vector<ExprNodePtr> deps);

// Returns an ordered set of leaf keys from the expression.
std::vector<std::string> GetLeafKeys(const ExprNodePtr& expr);

// Returns an ordered set of placeholder keys from the expression.
std::vector<std::string> GetPlaceholderKeys(const ExprNodePtr& expr);

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EXPR_H_
