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

#include "absl/base/nullability.h"
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
absl::StatusOr<ExprNodePtr absl_nonnull> ToLowerNode(  // clang-format hint
    const ExprNodePtr absl_nonnull& node);

// Expands the given expression to the lowest possible level.
absl::StatusOr<ExprNodePtr absl_nonnull> ToLowest(  // clang-format hint
    const ExprNodePtr absl_nonnull& expr);

// Creates a Literal node from a value.
template <typename T>
ExprNodePtr absl_nonnull Literal(T&& value) {
  return ExprNode::MakeLiteralNode(
      TypedValue::FromValue(std::forward<T>(value)));
}

// Creates a Literal node from a value.
template <typename T>
ExprNodePtr absl_nonnull Literal(const T& value) {
  return ExprNode::MakeLiteralNode(TypedValue::FromValue(value));
}

// Creates a Literal node from a value.
inline ExprNodePtr absl_nonnull Literal(const TypedValue& qvalue) {
  return ExprNode::MakeLiteralNode(TypedValue(qvalue));
}

// Creates a Literal node from a value.
inline ExprNodePtr absl_nonnull Literal(TypedValue& qvalue) {
  return ExprNode::MakeLiteralNode(TypedValue(qvalue));
}

// Creates a Literal node from a value.
inline ExprNodePtr absl_nonnull Literal(TypedValue&& qvalue) {
  return ExprNode::MakeLiteralNode(std::move(qvalue));
}

// Returns a Leaf with a given leaf key.
inline ExprNodePtr absl_nonnull Leaf(absl::string_view leaf_key) {
  return ExprNode::MakeLeafNode(leaf_key);
}

// Returns a Placeholder with a given placeholder key.
inline ExprNodePtr absl_nonnull Placeholder(absl::string_view placeholder_key) {
  return ExprNode::MakePlaceholderNode(placeholder_key);
}

// Binds the given operator with the arguments.
absl::StatusOr<ExprNodePtr absl_nonnull> BindOp(
    ExprOperatorPtr absl_nonnull op,
    absl::Span<const ExprNodePtr absl_nonnull> args,
    const absl::flat_hash_map<std::string, ExprNodePtr absl_nonnull>& kwargs);

// Finds an operator in ExprOperatorRegistry by name, and binds it with
// given arguments.
//
// This function parses the op_name string and returns an error if the parsing
// is not successful. Prefer using this version only with string constants.
absl::StatusOr<ExprNodePtr absl_nonnull> BindOp(
    absl::string_view op_name, absl::Span<const ExprNodePtr absl_nonnull> args,
    const absl::flat_hash_map<std::string, ExprNodePtr absl_nonnull>& kwargs);

// Like BindOp, but taking absl::StatusOr<ExprNodePtr absl_nonnull> instead of
// ExprNodePtr. Propagates errors upwards.
absl::StatusOr<ExprNodePtr absl_nonnull> CallOp(
    absl::StatusOr<ExprOperatorPtr absl_nonnull> status_or_op,
    std::initializer_list<absl::StatusOr<ExprNodePtr absl_nonnull>>
        status_or_args,
    std::initializer_list<
        std::pair<std::string, absl::StatusOr<ExprNodePtr absl_nonnull>>>
        status_or_kwargs = {});

absl::StatusOr<ExprNodePtr absl_nonnull> CallOp(
    absl::StatusOr<ExprOperatorPtr absl_nonnull> status_or_op,
    std::vector<absl::StatusOr<ExprNodePtr absl_nonnull>> status_or_args,
    absl::flat_hash_map<std::string, absl::StatusOr<ExprNodePtr absl_nonnull>>
        status_or_kwargs = {});

absl::StatusOr<ExprNodePtr absl_nonnull> CallOp(
    absl::string_view op_name,
    std::initializer_list<absl::StatusOr<ExprNodePtr absl_nonnull>>
        status_or_args,
    std::initializer_list<
        std::pair<std::string, absl::StatusOr<ExprNodePtr absl_nonnull>>>
        status_or_kwargs = {});

absl::StatusOr<ExprNodePtr absl_nonnull> CallOp(
    absl::string_view op_name,
    std::vector<absl::StatusOr<ExprNodePtr absl_nonnull>> status_or_args,
    absl::flat_hash_map<std::string, absl::StatusOr<ExprNodePtr absl_nonnull>>
        status_or_kwargs = {});

// Creates a node with given operator and dependencies.
//
// NOTE: This function expects that `deps` is appropriately aligned with
// operator's parameters. The provided dependencies will be attached to
// the new node AS-IS.
absl::StatusOr<ExprNodePtr absl_nonnull> MakeOpNode(
    ExprOperatorPtr absl_nonnull op,
    std::vector<ExprNodePtr absl_nonnull> deps);

// Creates a new node by cloning a given one and replacing the expr operator.
absl::StatusOr<ExprNodePtr absl_nonnull> WithNewOperator(  // clang-format hint
    const ExprNodePtr absl_nonnull& node, ExprOperatorPtr absl_nonnull op);

// Returns a new expression node that is same as a given one, but with new
// dependencies.
absl::StatusOr<ExprNodePtr absl_nonnull> WithNewDependencies(
    const ExprNodePtr absl_nonnull& node,
    std::vector<ExprNodePtr absl_nonnull> deps);

// Returns an ordered set of leaf keys from the expression.
std::vector<std::string> GetLeafKeys(const ExprNodePtr absl_nonnull& expr);

// Returns an ordered set of placeholder keys from the expression.
std::vector<std::string> GetPlaceholderKeys(  // clang-format hint
    const ExprNodePtr absl_nonnull& expr);

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EXPR_H_
