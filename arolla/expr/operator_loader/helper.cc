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
#include "arolla/expr/operator_loader/helper.h"

#include <cstddef>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::GetLeafKeys;
using ::arolla::expr::GetNthOperator;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::Placeholder;
using ::arolla::expr::PostOrder;
using ::arolla::expr::QTypeAnnotation;
using ::arolla::expr::RegisteredOperator;
using ::arolla::expr::Transform;

absl::Status CheckExpr(const ExprOperatorSignature& signature,
                       const PostOrder& post_order) {
  DCHECK_OK(ValidateSignature(signature));
  std::set<absl::string_view> unexpected_leaf_keys;
  std::set<absl::string_view> unexpected_placeholder_keys;
  for (const auto& node : post_order.nodes()) {
    if (node->is_leaf()) {
      unexpected_leaf_keys.insert(node->leaf_key());
    } else if (node->is_placeholder()) {
      unexpected_placeholder_keys.insert(node->placeholder_key());
    }
  }
  unexpected_leaf_keys.erase("input_qtype_sequence");
  if (!unexpected_leaf_keys.empty()) {
    std::ostringstream message;
    message << "expression contains unexpected leaves: ";
    bool first = true;
    for (const auto& key : unexpected_leaf_keys) {
      message << NonFirstComma(first) << ToDebugString(Leaf(key));
    }
    message << "; did you mean to use placeholders?";
    return absl::InvalidArgumentError(std::move(message).str());
  }
  for (const auto& param : signature.parameters) {
    unexpected_placeholder_keys.erase(param.name);
  }
  if (!unexpected_placeholder_keys.empty()) {
    std::ostringstream message;
    message << "expression contains unexpected placeholders: ";
    bool first = true;
    for (const auto& key : unexpected_placeholder_keys) {
      message << NonFirstComma(first) << ToDebugString(Placeholder(key));
    }
    return absl::InvalidArgumentError(std::move(message).str());
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<ResolvePlaceholdersResult> ResolvePlaceholders(
    const ExprOperatorSignature& signature,
    const ExprNodePtr absl_nonnull& expr,
    QTypePtr absl_nullable expected_output_qtype) {
  static const absl::NoDestructor kParseOp(std::make_shared<RegisteredOperator>(
      "qtype._parse_input_qtype_sequence"));
  const PostOrder post_order(expr);
  RETURN_IF_ERROR(CheckExpr(signature, post_order));
  absl::flat_hash_map<absl::string_view, ExprNodePtr> placeholders;
  for (const auto& node : post_order.nodes()) {
    if (node->is_placeholder()) {
      placeholders.emplace(node->placeholder_key(), nullptr);
    }
  }
  std::string signature_pattern;
  signature_pattern.reserve(signature.parameters.size());
  for (const auto& param : signature.parameters) {
    switch (param.kind) {
      case ExprOperatorSignature::Parameter::Kind::kPositionalOrKeyword:
        signature_pattern += placeholders.contains(param.name) ? 'P' : 'p';
        break;
      case ExprOperatorSignature::Parameter::Kind::kVariadicPositional:
        signature_pattern += placeholders.contains(param.name) ? 'V' : 'v';
        break;
    }
  }
  ASSIGN_OR_RETURN(auto input_qtype_sequence_expr,
                   MakeOpNode(QTypeAnnotation::Make(),
                              {Leaf("input_qtype_sequence"),
                               Literal(GetInputQTypeSequenceQType())}));
  ASSIGN_OR_RETURN(
      auto parsed_input_qtype_sequence_expr,
      MakeOpNode(*kParseOp, {Literal(Bytes(std::move(signature_pattern))),
                             input_qtype_sequence_expr}));
  size_t j = 1;
  for (size_t i = 0; i < signature.parameters.size(); ++i) {
    auto param_name = signature.parameters[i].name;
    auto it = placeholders.find(param_name);
    if (it == placeholders.end()) {
      continue;
    }
    ASSIGN_OR_RETURN(it->second,
                     MakeOpNode(std::make_shared<GetNthOperator>(j++),
                                {parsed_input_qtype_sequence_expr}));
  }
  ASSIGN_OR_RETURN(
      auto resolved_expr,
      TransformOnPostOrder(post_order, [&](ExprNodePtr node) -> ExprNodePtr {
        if (node->is_placeholder()) {
          auto it = placeholders.find(node->placeholder_key());
          // NOTE: Guaranteed by CheckExpr().
          DCHECK(it != placeholders.end());
          if (it == placeholders.end()) {
            return node;
          }
          return it->second;
        } else if (node->is_leaf()) {
          // NOTE: Guaranteed by CheckExpr().
          DCHECK_EQ(node->leaf_key(), "input_qtype_sequence");
          return input_qtype_sequence_expr;
        } else {
          return node;
        }
      }));

  if (expected_output_qtype != nullptr) {
    // NOTE: As all input qtypes are provided, the expression should know its
    // output qtype.
    DCHECK(expected_output_qtype != GetNothingQType());
    if (resolved_expr->qtype() == nullptr) {
      return absl::FailedPreconditionError(
          absl::StrCat("failed to infer output qtype, expected: ",
                       expected_output_qtype->name()));
    }
    if (resolved_expr->qtype() != expected_output_qtype) {
      return absl::InvalidArgumentError(
          absl::StrCat("expected output qtype ", expected_output_qtype->name(),
                       ", but got ", resolved_expr->qtype()->name()));
    }
  }
  ASSIGN_OR_RETURN(auto readiness_expr,
                   MakeOpNode(std::make_shared<GetNthOperator>(0),
                              {std::move(parsed_input_qtype_sequence_expr)}));

  return ResolvePlaceholdersResult{std::move(resolved_expr),
                                   std::move(readiness_expr)};
}

absl::StatusOr<Sequence> MakeInputQTypeSequence(
    absl::Span<const ExprAttributes> inputs) {
  const auto nothing_qtype = GetNothingQType();
  auto buffer = std::make_shared<QTypePtr[]>(inputs.size());
  for (size_t i = 0; i < inputs.size(); ++i) {
    if (inputs[i].qtype() == nothing_qtype) {
      return absl::InvalidArgumentError(
          "inputs of type NOTHING are unsupported");
    }
    buffer[i] =
        (inputs[i].qtype() == nullptr ? nothing_qtype : inputs[i].qtype());
  }
  return Sequence(GetQTypeQType(), inputs.size(), std::move(buffer));
}

absl::StatusOr<Sequence> MakeInputQTypeSequence(
    absl::Span<const ExprNodePtr> inputs) {
  const auto nothing_qtype = GetNothingQType();
  auto buffer = std::make_shared<QTypePtr[]>(inputs.size());
  for (size_t i = 0; i < inputs.size(); ++i) {
    if (inputs[i]->qtype() == nothing_qtype) {
      return absl::InvalidArgumentError(
          "inputs of type NOTHING are unsupported");
    }
    buffer[i] =
        (inputs[i]->qtype() == nullptr ? nothing_qtype : inputs[i]->qtype());
  }
  return Sequence(GetQTypeQType(), inputs.size(), std::move(buffer));
}

QTypePtr absl_nonnull GetInputQTypeSequenceQType() {
  static const auto result = GetSequenceQType(GetQTypeQType());
  return result;
}

absl::StatusOr<ExprNodePtr> ReplacePlaceholdersWithLeaves(
    const ExprNodePtr& expr) {
  const auto leaf_keys = GetLeafKeys(expr);
  if (!leaf_keys.empty()) {
    return absl::InvalidArgumentError("expected no leaf nodes, found: L." +
                                      absl::StrJoin(leaf_keys, ", L."));
  }
  return Transform(
      expr, [](expr::ExprNodePtr node) -> absl::StatusOr<expr::ExprNodePtr> {
        if (node->is_placeholder()) {
          return Leaf(node->placeholder_key());
        }
        return node;
      });
}

}  // namespace arolla::operator_loader
