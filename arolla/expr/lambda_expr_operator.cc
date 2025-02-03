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
#include "arolla/expr/lambda_expr_operator.h"

#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

constexpr absl::string_view kDefaultLambdaOperatorName = "anonymous.lambda";

absl::Status ValidateLambdaBody(const PostOrder& lambda_body_post_order) {
  for (const auto& node : lambda_body_post_order.nodes()) {
    // 1. Check that no leaves present within the lambda body.
    if (node->is_leaf()) {
      return absl::InvalidArgumentError(
          "leaf nodes are not permitted within the lambda body");
    }
  }
  // 2. Check that the placeholders within the lambda body have no
  // dependencies.
  for (const auto& node : lambda_body_post_order.nodes()) {
    if (node->is_placeholder() && !node->node_deps().empty()) {
      return absl::InvalidArgumentError(
          "no placeholder nodes with dependencies permitted within the "
          "lambda "
          "body");
    }
  }
  // 3. Validate corollary from (2): the visitor-order includes each
  // placeholder_key only once, because the key uniquely identifies the node.
  absl::flat_hash_set<absl::string_view> placeholder_keys;
  for (const auto& node : lambda_body_post_order.nodes()) {
    if (node->is_placeholder() &&
        !placeholder_keys.emplace(node->placeholder_key()).second) {
      return absl::InternalError(
          "placeholder's key must unique identify the node");
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<std::shared_ptr<LambdaOperator>> LambdaOperator::Make(
    ExprNodePtr lambda_body) {
  return LambdaOperator::Make(kDefaultLambdaOperatorName,
                              std::move(lambda_body));
}

absl::StatusOr<std::shared_ptr<LambdaOperator>> LambdaOperator::Make(
    absl::string_view operator_name, ExprNodePtr lambda_body) {
  auto placeholders = GetPlaceholderKeys(lambda_body);
  if (placeholders.empty()) {
    return absl::InvalidArgumentError(
        "exactly one placeholder expected, but none were found");
  } else if (placeholders.size() > 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "exactly one placeholder expected, but %d are found: P.%s",
        placeholders.size(), absl::StrJoin(placeholders, ", P.")));
  }
  return LambdaOperator::Make(operator_name,
                              ExprOperatorSignature{{placeholders[0]}},
                              std::move(lambda_body), "");
}

absl::StatusOr<std::shared_ptr<LambdaOperator>> LambdaOperator::Make(
    const ExprOperatorSignature& lambda_signature, ExprNodePtr lambda_body) {
  return LambdaOperator::Make(kDefaultLambdaOperatorName, lambda_signature,
                              std::move(lambda_body), "");
}

absl::StatusOr<std::shared_ptr<LambdaOperator>> LambdaOperator::Make(
    absl::string_view operator_name,
    const ExprOperatorSignature& lambda_signature, ExprNodePtr lambda_body) {
  return LambdaOperator::Make(operator_name, lambda_signature,
                              std::move(lambda_body), "");
}

absl::StatusOr<std::shared_ptr<LambdaOperator>> LambdaOperator::Make(
    absl::string_view operator_name,
    const ExprOperatorSignature& lambda_signature, ExprNodePtr lambda_body,
    absl::string_view doc) {
  RETURN_IF_ERROR(ValidateSignature(lambda_signature));
  auto lambda_body_post_order = PostOrder(lambda_body);
  RETURN_IF_ERROR(ValidateLambdaBody(lambda_body_post_order));

  // 1. Check that all placeholders from the lambda's body are present as
  // parameters.
  absl::flat_hash_map<absl::string_view, bool> lambda_param_used;
  for (const auto& param : lambda_signature.parameters) {
    lambda_param_used.emplace(param.name, false);
  }
  for (const auto& node : lambda_body_post_order.nodes()) {
    if (!node->is_placeholder()) {
      continue;
    }
    const auto it = lambda_param_used.find(node->placeholder_key());
    if (it == lambda_param_used.end()) {
      return absl::InvalidArgumentError(
          absl::StrCat("P.", node->placeholder_key(),
                       " is missing in the list of lambda parameters"));
    }
    it->second = true;
  }
  // 2. Check that all parameters are used.
  for (const auto& param : lambda_signature.parameters) {
    if (!(absl::StartsWith(param.name, "unused") ||
          absl::StartsWith(param.name, "_")) &&
        !lambda_param_used[param.name]) {
      // NOTE: If the parameter is intentionally unused and you don't want
      // to change the operator's signature, use `SuppressUnusedWarning`
      // (`arolla.optools.suppress_unused_parameter_warning` in Python).
      LOG(WARNING) << "Unused lambda parameter: '" << param.name << "' in "
                   << operator_name;
    }
  }
  // 3. Generate fingerprint.
  auto fingerprint = FingerprintHasher("arolla::expr::LambdaOperator")
                         .Combine(operator_name, lambda_signature,
                                  lambda_body->fingerprint(), doc)
                         .Finish();
  return std::make_shared<LambdaOperator>(
      PrivateConstrutorTag{}, operator_name, lambda_signature,
      std::move(lambda_body_post_order), doc, fingerprint);
}

LambdaOperator::LambdaOperator(PrivateConstrutorTag, absl::string_view name,
                               const ExprOperatorSignature& signature,
                               PostOrder lambda_body_post_order,
                               absl::string_view doc, Fingerprint fingerprint)
    : ExprOperatorWithFixedSignature(name, signature, doc, fingerprint),
      lambda_body_post_order_(std::move(lambda_body_post_order)) {
  absl::flat_hash_map<absl::string_view, size_t> sig_param_indices;
  sig_param_indices.reserve(signature.parameters.size());
  for (size_t i = 0; i < signature.parameters.size(); ++i) {
    sig_param_indices[signature.parameters[i].name] = i;
  }
  // Note: Initialize param_indices using an out-of-range value to distinguish
  // parameters never used within the lambda body.
  lambda_param_indices_.resize(signature.parameters.size(),
                               std::numeric_limits<size_t>::max());
  for (size_t i = 0; i < lambda_body_post_order_.nodes_size(); ++i) {
    const auto& node = lambda_body_post_order_.node(i);
    if (node->is_placeholder()) {
      lambda_param_indices_[sig_param_indices.at(node->placeholder_key())] = i;
    }
  }
}

namespace {

absl::StatusOr<ExprNodePtr> WrapAsTuple(absl::Span<const ExprNodePtr> fields) {
  return MakeOpNode(MakeTupleOperator::Make(),
                    std::vector<ExprNodePtr>(fields.begin(), fields.end()));
}

ExprAttributes WrapAsTuple(absl::Span<const ExprAttributes> field_attrs) {
  return MakeTupleOperator::StaticInferAttributes(field_attrs);
}

}  // namespace

absl::StatusOr<ExprNodePtr> LambdaOperator::ToLowerLevel(
    const ExprNodePtr& node) const {
  RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
  std::vector<ExprNodePtr> result(lambda_body_post_order_.nodes_size());
  if (!lambda_param_indices_.empty()) {
    const auto inputs = absl::MakeConstSpan(node->node_deps());
    for (size_t i = 0; i + 1 < lambda_param_indices_.size(); ++i) {
      if (lambda_param_indices_[i] != std::numeric_limits<size_t>::max()) {
        result[lambda_param_indices_[i]] = inputs[i];
      }
    }
    if (lambda_param_indices_.back() != std::numeric_limits<size_t>::max()) {
      if (HasVariadicParameter(signature())) {
        ASSIGN_OR_RETURN(
            result[lambda_param_indices_.back()],
            WrapAsTuple(inputs.subspan(lambda_param_indices_.size() - 1)));
      } else {
        result[lambda_param_indices_.back()] = inputs.back();
      }
    }
  }
  for (size_t i = 0; i < lambda_body_post_order_.nodes_size(); ++i) {
    const auto& original_node = lambda_body_post_order_.node(i);
    if (original_node->is_placeholder()) {
      continue;
    }
    if (original_node->is_literal()) {
      result[i] = original_node;
      continue;
    }
    DCHECK(original_node->is_op());
    const auto& dep_indices = lambda_body_post_order_.dep_indices(i);
    std::vector<ExprNodePtr> deps(dep_indices.size());
    for (size_t j = 0; j < dep_indices.size(); ++j) {
      deps[j] = result[dep_indices[j]];
    }
    if (i + 1 < lambda_body_post_order_.nodes_size() ||
        node->attr().IsEmpty()) {
      ASSIGN_OR_RETURN(result[i],
                       WithNewDependencies(original_node, std::move(deps)));
    } else {
      // As an optimization, if the topmost node in the lambda body is an
      // operator, we reuse the attributes from the original node instead
      // of recomputing them.
#ifndef NDEBUG
      auto attr = original_node->op()->InferAttributes(GetExprAttrs(deps));
      DCHECK(attr.ok() && attr->IsIdenticalTo(node->attr()));
#endif

      result[i] = ExprNode::UnsafeMakeOperatorNode(
          ExprOperatorPtr(original_node->op()), std::move(deps),
          ExprAttributes(node->attr()));
    }
  }
  return result.back();
}

absl::StatusOr<ExprAttributes> LambdaOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  std::vector<ExprAttributes> results(lambda_body_post_order_.nodes_size());
  if (!lambda_param_indices_.empty()) {
    for (size_t i = 0; i + 1 < lambda_param_indices_.size(); ++i) {
      if (lambda_param_indices_[i] != std::numeric_limits<size_t>::max()) {
        results[lambda_param_indices_[i]] = inputs[i];
      }
    }
    if (lambda_param_indices_.back() != std::numeric_limits<size_t>::max()) {
      if (HasVariadicParameter(signature())) {
        results[lambda_param_indices_.back()] =
            WrapAsTuple(inputs.subspan(lambda_param_indices_.size() - 1));
      } else {
        results[lambda_param_indices_.back()] = inputs.back();
      }
    }
  }
  std::vector<ExprAttributes> deps;
  for (size_t i = 0; i < lambda_body_post_order_.nodes_size(); ++i) {
    const auto& original_node = lambda_body_post_order_.node(i);
    if (original_node->is_placeholder()) {
      continue;
    }
    if (const auto& attr = original_node->attr(); attr.qvalue().has_value()) {
      results[i] = attr;
      continue;
    }
    DCHECK(original_node->is_op());
    const auto& dep_indices = lambda_body_post_order_.dep_indices(i);
    deps.resize(dep_indices.size());
    for (size_t j = 0; j < dep_indices.size(); ++j) {
      deps[j] = results[dep_indices[j]];
    }
    ASSIGN_OR_RETURN(results[i], original_node->op()->InferAttributes(deps),
                     _ << "while deducing output type for "
                       << GetDebugSnippet(original_node));
  }
  return results.back();
}

absl::string_view LambdaOperator::py_qvalue_specialization_key() const {
  return "::arolla::expr::LambdaOperator";
}

namespace {

// Returns a helper operator, that gets lowered to the first argument and
// drops the rest.
absl::StatusOr<ExprOperatorPtr> IgnoreUnusedParametersOp() {
  static const absl::NoDestructor<absl::StatusOr<ExprOperatorPtr>> result(
      MakeLambdaOperator("ignore_unused_parameters",
                         ExprOperatorSignature::Make("expr, *unused"),
                         Placeholder("expr")));
  return *result;
}

}  // namespace

absl::StatusOr<ExprNodePtr> SuppressUnusedWarning(
    absl::string_view unused_parameters, absl::StatusOr<ExprNodePtr> expr) {
  std::vector<absl::string_view> unused_parameter_names = absl::StrSplit(
      unused_parameters, absl::ByAnyChar(", "), absl::SkipEmpty());
  std::vector<absl::StatusOr<ExprNodePtr>> args;
  args.reserve(1 + unused_parameter_names.size());
  args.push_back(std::move(expr));
  for (absl::string_view name : unused_parameter_names) {
    args.push_back(Placeholder(name));
  }
  return CallOp(IgnoreUnusedParametersOp(), std::move(args));
}

}  // namespace arolla::expr
