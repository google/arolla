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
#include "arolla/expr/expr.h"

#include <algorithm>
#include <cstddef>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

absl::StatusOr<ExprNodePtr absl_nonnull> ToLowerNode(  // clang-format hint
    const ExprNodePtr absl_nonnull& node) {
  const auto& op = node->op();
  if (op == nullptr) {
    return node;
  }
  ASSIGN_OR_RETURN(
      auto result, op->ToLowerLevel(node),
      WithNote(_, absl::StrCat("While lowering node ", GetDebugSnippet(node))));
  if (!node->attr().IsSubsetOf(result->attr())) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "expression %s attributes changed in ToLower from %s to "
        "%s; this indicates incorrect InferAttributes() or GetOutputType() "
        "of the operator %s",
        GetDebugSnippet(node), absl::FormatStreamed(node->attr()),
        absl::FormatStreamed(result->attr()), op->display_name()));
  }
  return result;
}

absl::StatusOr<ExprNodePtr absl_nonnull> ToLowest(  // clang-format hint
    const ExprNodePtr absl_nonnull& expr) {
  return DeepTransform(expr, &ToLowerNode);
}

namespace {

struct ExprNodeFormatter {
  void operator()(std::string* absl_nonnull out,
                  ExprNodePtr absl_nonnull node) const {
    absl::StrAppend(out, GetDebugSnippet(node));
  }
};

bool AreExprAttributesTheSame(
    absl::Span<const ExprNodePtr absl_nonnull> lexprs,
    absl::Span<const ExprNodePtr absl_nonnull> rexprs) {
  if (lexprs.size() != rexprs.size()) {
    return false;
  }
  for (size_t i = 0; i != lexprs.size(); ++i) {
    if (!lexprs[i]->attr().IsIdenticalTo(rexprs[i]->attr())) {
      return false;
    }
  }
  return true;
}

}  // namespace

absl::StatusOr<ExprNodePtr absl_nonnull> MakeOpNode(
    ExprOperatorPtr absl_nonnull op,
    std::vector<ExprNodePtr absl_nonnull> deps) {
  ASSIGN_OR_RETURN(
      auto output_attr, op->InferAttributes(GetExprAttrs(deps)),
      WithNote(_, absl::StrCat("While constructing a node with operator ",
                               op->display_name(), " and dependencies {",
                               absl::StrJoin(deps, ", ", ExprNodeFormatter()),
                               "}")));
  return ExprNode::UnsafeMakeOperatorNode(std::move(op), std::move(deps),
                                          std::move(output_attr));
}

absl::StatusOr<ExprNodePtr absl_nonnull> BindOp(
    ExprOperatorPtr absl_nonnull op,
    absl::Span<const ExprNodePtr absl_nonnull> args,
    const absl::flat_hash_map<std::string, ExprNodePtr absl_nonnull>& kwargs) {
  ASSIGN_OR_RETURN(auto signature, op->GetSignature());
  ASSIGN_OR_RETURN(auto bound_args, BindArguments(signature, args, kwargs));
  return MakeOpNode(std::move(op), std::move(bound_args));
}

absl::StatusOr<ExprNodePtr absl_nonnull> BindOp(
    absl::string_view op_name, absl::Span<const ExprNodePtr absl_nonnull> args,
    const absl::flat_hash_map<std::string, ExprNodePtr absl_nonnull>& kwargs) {
  ASSIGN_OR_RETURN(auto op, LookupOperator(op_name));
  return BindOp(std::move(op), args, kwargs);
}

absl::StatusOr<ExprNodePtr absl_nonnull> WithNewOperator(  // clang-format hint
    const ExprNodePtr absl_nonnull& node, ExprOperatorPtr absl_nonnull op) {
  if (!node->is_op()) {
    return absl::InvalidArgumentError(
        "WithNewOperator works only with operator nodes");
  }
  return MakeOpNode(std::move(op), node->node_deps());
}

absl::StatusOr<ExprNodePtr absl_nonnull> WithNewDependencies(
    const ExprNodePtr absl_nonnull& node,
    std::vector<ExprNodePtr absl_nonnull> deps) {
  const auto& old_deps = node->node_deps();
  if (absl::c_equal(old_deps, deps, [](const auto& lhs, const auto& rhs) {
        return lhs->fingerprint() == rhs->fingerprint();
      })) {
    return node;
  }
  if (node->is_op()) {
    // Performance optimization in order to avoid attributes recomputation.
    if (AreExprAttributesTheSame(old_deps, deps)) {
      return ExprNode::UnsafeMakeOperatorNode(ExprOperatorPtr(node->op()),
                                              std::move(deps),
                                              ExprAttributes(node->attr()));
    } else {
      return MakeOpNode(node->op(), std::move(deps));
    }
  }
  if (!deps.empty()) {
    return absl::InvalidArgumentError(
        "only operator nodes can have dependencies");
  }
  return node;
}

namespace {

template <typename Strings>
std::vector<std::string> SortedStrings(const Strings& strings) {
  std::vector<std::string> result;
  result.reserve(strings.size());
  for (const auto& str : strings) {
    result.emplace_back(str);
  }
  std::sort(result.begin(), result.end());
  return result;
}

}  // namespace

std::vector<std::string> GetLeafKeys(const ExprNodePtr absl_nonnull& expr) {
  absl::flat_hash_set<absl::string_view> result;
  for (const auto& node : VisitorOrder(expr)) {
    if (node->is_leaf()) {
      result.emplace(node->leaf_key());
    }
  }
  return SortedStrings(result);
}

std::vector<std::string> GetPlaceholderKeys(  // clang-format hint
    const ExprNodePtr absl_nonnull& expr) {
  absl::flat_hash_set<absl::string_view> result;
  for (const auto& node : VisitorOrder(expr)) {
    if (node->is_placeholder()) {
      result.emplace(node->placeholder_key());
    }
  }
  return SortedStrings(result);
}

absl::StatusOr<ExprNodePtr absl_nonnull> CallOp(
    absl::StatusOr<ExprOperatorPtr absl_nonnull> status_or_op,
    std::initializer_list<absl::StatusOr<ExprNodePtr absl_nonnull>>
        status_or_args,
    std::initializer_list<
        std::pair<std::string, absl::StatusOr<ExprNodePtr absl_nonnull>>>
        status_or_kwargs) {
  ASSIGN_OR_RETURN(auto op, std::move(status_or_op));
  ASSIGN_OR_RETURN(
      std::vector<ExprNodePtr absl_nonnull> args,
      LiftStatusUp(absl::Span<const absl::StatusOr<ExprNodePtr absl_nonnull>>(
          status_or_args)));
  ASSIGN_OR_RETURN((absl::flat_hash_map<std::string, ExprNodePtr> kwargs),
                   LiftStatusUp(status_or_kwargs));
  return BindOp(op, args, kwargs);
}

absl::StatusOr<ExprNodePtr absl_nonnull> CallOp(
    absl::StatusOr<ExprOperatorPtr absl_nonnull> status_or_op,
    std::vector<absl::StatusOr<ExprNodePtr absl_nonnull>> status_or_args,
    absl::flat_hash_map<std::string, absl::StatusOr<ExprNodePtr absl_nonnull>>
        status_or_kwargs) {
  ASSIGN_OR_RETURN(auto op, std::move(status_or_op));
  ASSIGN_OR_RETURN(
      auto args,
      LiftStatusUp(absl::Span<const absl::StatusOr<ExprNodePtr absl_nonnull>>(
          status_or_args)));
  ASSIGN_OR_RETURN((absl::flat_hash_map<std::string, ExprNodePtr> kwargs),
                   LiftStatusUp(status_or_kwargs));
  return BindOp(op, args, kwargs);
}

absl::StatusOr<ExprNodePtr absl_nonnull> CallOp(
    absl::string_view op_name,
    std::initializer_list<absl::StatusOr<ExprNodePtr absl_nonnull>>
        status_or_args,
    std::initializer_list<
        std::pair<std::string, absl::StatusOr<ExprNodePtr absl_nonnull>>>
        status_or_kwargs) {
  ASSIGN_OR_RETURN(
      auto args,
      LiftStatusUp(absl::Span<const absl::StatusOr<ExprNodePtr absl_nonnull>>(
          status_or_args)));
  ASSIGN_OR_RETURN(auto kwargs, LiftStatusUp(status_or_kwargs));
  return BindOp(op_name, args, kwargs);
}

absl::StatusOr<ExprNodePtr absl_nonnull> CallOp(
    absl::string_view op_name,
    std::vector<absl::StatusOr<ExprNodePtr absl_nonnull>> status_or_args,
    absl::flat_hash_map<std::string, absl::StatusOr<ExprNodePtr absl_nonnull>>
        status_or_kwargs) {
  ASSIGN_OR_RETURN(
      auto args,
      LiftStatusUp(absl::Span<const absl::StatusOr<ExprNodePtr absl_nonnull>>(
          status_or_args)));
  ASSIGN_OR_RETURN(auto kwargs, LiftStatusUp(std::move(status_or_kwargs)));
  return BindOp(op_name, args, kwargs);
}

}  // namespace arolla::expr
