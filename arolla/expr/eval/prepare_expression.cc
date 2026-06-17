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
#include "arolla/expr/eval/prepare_expression.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/casting.h"
#include "arolla/expr/eval/compile_where_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/expr_stack_trace.h"
#include "arolla/expr/eval/extensions.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/class_info.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status.h"
#include "arolla/util/string.h"

namespace arolla::expr::eval_internal {
namespace {

using Stage = DynamicEvaluationEngineOptions::PreparationStage;

class InternalRootOperatorImpl final : public ExprOperatorWithFixedSignature {
 public:
  InternalRootOperatorImpl()
      : ExprOperatorWithFixedSignature(
            "_internal_root_operator_",
            ExprOperatorSignature{{.name = "arg0"},
                                  {.name = "args",
                                   .kind = ExprOperatorSignature::Parameter::
                                       Kind::kVariadicPositional}},
            "Returns the first argument; it's a special internal operator for "
            "the root of the expression.",
            FingerprintHasher("::arolla::expr::InternalRootOperator").Finish(),
            ExprOperatorTags::kBuiltin) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    // Note: We don't want this operator to be replaced with a literal, so don't
    // propagate `args0.attr().qvalue()` even if it is present.
    return ExprAttributes(inputs[0].qtype());
  }
};

bool AllDepsAreLiterals(const ExprNodePtr& node) {
  for (const auto& d : node->node_deps()) {
    if (!d->qvalue()) {
      return false;
    }
  }
  return true;
}

absl::Status MissingInputTypesError(
    const absl::flat_hash_map<std::string, QTypePtr>& input_types,
    const ExprNodePtr& root) {
  std::set<std::string> missing_types;
  for (const auto& node : VisitorOrder(root)) {
    if (!node->is_op() || IsQTypeAnnotation(node)) {
      continue;
    }
    for (const auto& d : node->node_deps()) {
      if (d->is_leaf() && !input_types.contains(d->leaf_key())) {
        missing_types.insert(d->leaf_key());
      }
    }
  }
  if (root->is_leaf() && !input_types.contains(root->leaf_key())) {
    missing_types.insert(root->leaf_key());
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("missing QType information for inputs {%s}",
                      Truncate(absl::StrJoin(missing_types, ", "), 200)));
}

// Replaces a subexpression with a known literal value with an actual literal.
//
// NOTE: Having this transformation allows a "constexpr" operator (that
// implements its computation as attribute inference) to have no to-lower-node
// transformation or custom compiler support.
absl::StatusOr<ExprNodePtr> EmbedLiteralsTransformation(
    const DynamicEvaluationEngineOptions& /*options*/,
    const ExprNodePtr absl_nonnull& node,
    const ExprOperatorPtr absl_nullable& /*decayed_op*/) {
  if (!node->is_literal() && node->qvalue().has_value()) {
    return Literal(*node->qvalue());
  }
  return node;
}

// Looks up type for leaf in the map and annotates it. `root` is used to form
// a complete error message in case of the type is missing.
absl::StatusOr<ExprNodePtr> AnnotateLeafWithQType(
    ExprNodePtr leaf,
    const absl::flat_hash_map<std::string, QTypePtr>& input_types,
    const ExprNodePtr& root) {
  auto it = input_types.find(leaf->leaf_key());
  if (it == input_types.end()) {
    return MissingInputTypesError(input_types, root);
  }
  return ExprNode::UnsafeMakeOperatorNode(
      ExprOperatorPtr(QTypeAnnotation::Make()),
      {std::move(leaf), Literal(it->second)}, ExprAttributes(it->second));
}

// Node transformation that annotates all the input leaves with their QTypes and
// validates the leaves that are already annotated.
//
// NOTE: We do not define just L.x -> annotation.qtype(L.x, ...) transformation
// because DeepTransform would consider it as an infinite transformation.
//
// The `root` argument is used only to form a good error message in case of
// missing input type.
//
NodeTransformationFn PopulateQTypesTransformation(
    const absl::flat_hash_map<std::string, QTypePtr>& input_types,
    const ExprNodePtr& root) {
  return [&input_types, &root](const DynamicEvaluationEngineOptions&,
                               const ExprNodePtr absl_nonnull& node,
                               const ExprOperatorPtr absl_nullable& decayed_op)
             -> absl::StatusOr<ExprNodePtr absl_nonnull> {
    if (decayed_op == nullptr) {
      return node;
    }
    if (IsInstanceOf<QTypeAnnotation>(*decayed_op)) [[unlikely]] {
      const QType* annotated_qtype = ReadQTypeAnnotation(node);
      if (annotated_qtype == nullptr) [[unlikely]] {
        // It is uncommon for a qtype annotation to provide no qtype.
        // This transformation cannot handle this case; leave it to other
        // transformations.
        return node;
      }
      if (node->node_deps()[0]->is_leaf()) {
        auto it = input_types.find(node->node_deps()[0]->leaf_key());
        if (it != input_types.end() && it->second != annotated_qtype) {
          return absl::FailedPreconditionError(absl::StrFormat(
              "inconsistent qtype annotation and input qtype: %s",
              JoinTypeNames({annotated_qtype, it->second})));
        }
        return node;
      } else if (node->node_deps()[0]->qtype() != nullptr) {
        // QTypeAnnotation::InferAttributes has already validated QType
        // consistency, so we can just strip the annotation here.
        return node->node_deps()[0];
      }
    }
    // Annotate all leaf dependencies with their qtypes.
    bool has_leaf_dep = false;
    for (auto& dep : node->node_deps()) {
      if (dep->is_leaf()) {
        has_leaf_dep = true;
        break;
      }
    }
    if (!has_leaf_dep) [[likely]] {
      return node;
    }

    std::vector<ExprNodePtr> new_deps = node->node_deps();
    for (auto& d : new_deps) {
      if (d->is_leaf()) {
        ASSIGN_OR_RETURN(
            d, AnnotateLeafWithQType(std::move(d), input_types, root));
      }
    }
    // NOTE: Since at least one leaf was annotated, the expr attributes will
    // need to be recomputed. Therefore, using WithNewDependencies()
    // provides no benefit here.
    return MakeOpNode(node->op(), std::move(new_deps));
  };
}

// Precomputes parts of an expression that depend on literals only and replaces
// the corresponding nodes with literals.
//
// Note: This transformation is a more involved version of
// EmbedLiteralsTransformation, which computes the value of a "literal"
// subexpression even if attribute inference didn't infer it.
absl::StatusOr<ExprNodePtr absl_nonnull> LiteralFoldingTransformation(
    const DynamicEvaluationEngineOptions& options,
    const ExprNodePtr absl_nonnull& node,
    const ExprOperatorPtr absl_nullable& decayed_op) {
  if (decayed_op == nullptr || decayed_op == InternalRootOperator()) {
    return node;
  }
  if (!AllDepsAreLiterals(node)) {
    return node;
  }

  DynamicEvaluationEngineOptions invoke_options = options;
  // kPopulateQTypes is not needed for literal folding, and kLiteralFolding
  // would cause infinite recursion. kOptimization is not needed for a one-off
  // evaluation, and kWhereOperatorsTransformation is not needed when
  // optimizations are disabled.
  invoke_options.enabled_preparation_stages &=
      ~(Stage::kLiteralFolding | Stage::kPopulateQTypes | Stage::kOptimization |
        Stage::kWhereOperatorsTransformation);
  ASSIGN_OR_RETURN(auto result, Invoke(node, {}, std::move(invoke_options)));
  return Literal(std::move(result));
}

absl::StatusOr<ExprNodePtr absl_nonnull> ToLowerTransformation(
    const DynamicEvaluationEngineOptions&, const ExprNodePtr absl_nonnull& node,
    const ExprOperatorPtr absl_nullable& decayed_op) {
  if (decayed_op == nullptr) {
    return node;
  }
  ASSIGN_OR_RETURN(
      auto result, decayed_op->ToLowerLevel(node),
      WithNote(_, absl::StrCat("While lowering node ", GetDebugSnippet(node))));
  if (!node->attr().IsSubsetOf(result->attr())) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "expression %s attributes changed in ToLower from %v to "
        "%v; this indicates incorrect InferAttributes() or GetOutputType() "
        "of the operator %s",
        GetDebugSnippet(node), node->attr(), result->attr(),
        node->op()->display_name()));
  }
  return result;
}

absl::StatusOr<ExprNodePtr> StripAnnotationsTransformation(
    const DynamicEvaluationEngineOptions&, const ExprNodePtr absl_nonnull& node,
    const ExprOperatorPtr absl_nullable& decayed_op) {
  if (!HasAnnotationExprOperatorTag(decayed_op)) {
    return node;
  }
  if (node->node_deps().empty()) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "invalid annotation %s: expected at least 1 argument, got 0",
        GetDebugSnippet(node)));
  }
  // We keep the minimum number of QType annotations required for type
  // assertions until the very end, and remove all other annotations.
  if (node->node_deps()[0]->qtype() == nullptr &&
      IsInstanceOf<QTypeAnnotation>(decayed_op.get())) {
    return node;
  }
  return node->node_deps()[0];
}

absl::StatusOr<ExprNodePtr> ApplyNodeTransformations(
    const DynamicEvaluationEngineOptions& options, ExprNodePtr expr,
    absl::Span<const NodeTransformationFn> transformations,
    ExprStackTrace* absl_nullable stack_trace) {
  return DeepTransform(
      expr,
      [&options, transformations,
       stack_trace](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
        ASSIGN_OR_RETURN(auto decayed_op, DecayRegisteredOperator(node->op()));
        for (const auto& t : transformations) {
          auto result = t(options, node, decayed_op);
          if (!result.ok()) {
            return stack_trace != nullptr
                       ? stack_trace->AnnotateWithNodeSourceLocations(
                             std::move(result).status(), node)
                       : std::move(result).status();
          }
          if ((*result)->fingerprint() == node->fingerprint()) {
            continue;
          }
          if (!node->attr().IsSubsetOf((*result)->attr())) {
            return absl::FailedPreconditionError(absl::StrFormat(
                "expression %s attributes changed from %v to %v during "
                "compilation",
                GetDebugSnippet(node), node->attr(), (*result)->attr()));
          }
          // Postpone the remaining transformations to guarantee that the
          // transformations are applied sequentially (the further
          // transformations may assume that the former are fully applied).
          return *std::move(result);
        }
        return node;
      },
      /*log_fn=*/stack_trace != nullptr
          ? std::optional([stack_trace](const ExprNodePtr& node, const
                                        absl_nullable ExprNodePtr& prev_node) {
              if (prev_node == nullptr) {
                stack_trace->InitNode(node);
              } else {
                stack_trace->AddTrace(node, prev_node);
              }
            })
          : std::nullopt);
}

absl::StatusOr<ExprNodePtr> PrepareSingleLeafExpression(
    const ExprNodePtr& expr,
    const absl::flat_hash_map<std::string, QTypePtr>& input_types,
    const DynamicEvaluationEngineOptions& options) {
  if (options.enabled_preparation_stages & Stage::kPopulateQTypes) {
    return AnnotateLeafWithQType(expr, input_types, expr);
  } else {
    return expr;
  }
}

}  // namespace

absl::StatusOr<ExprNodePtr> PrepareExpression(
    const ExprNodePtr& expr,
    const absl::flat_hash_map<std::string, QTypePtr>& input_types,
    const DynamicEvaluationEngineOptions& options,
    ExprStackTrace* absl_nullable stack_trace) {
  // PopulateQTypesTransformation does not handle a single leaf correctly, but
  // there is nothing to "prepare" anyway.
  if (expr->is_leaf()) {
    return PrepareSingleLeafExpression(expr, input_types, options);
  }

  ExprNodePtr current_expr = expr;

  std::vector<NodeTransformationFn> transformations;
  if (options.enabled_preparation_stages & Stage::kEmbedLiterals) {
    transformations.emplace_back(EmbedLiteralsTransformation);
  }
  if (options.enabled_preparation_stages & Stage::kPopulateQTypes) {
    transformations.emplace_back(
        PopulateQTypesTransformation(input_types, expr));
  }
  if (options.enabled_preparation_stages & Stage::kLiteralFolding) {
    transformations.emplace_back(LiteralFoldingTransformation);
  }
  if (options.enabled_preparation_stages & Stage::kToLower) {
    transformations.emplace_back(ToLowerTransformation);
  }

  // The least frequent transformations go at the end, as they will be likely
  // no-op and processed only once.

  if (options.enabled_preparation_stages & Stage::kStripAnnotations) {
    transformations.emplace_back(StripAnnotationsTransformation);
  }

  // Casting must go after lowering because it assumes that the expression
  // contains only backend operators.
  // TODO(b/161214936) Consider adding a no-op transformation that validates it.
  if (options.enabled_preparation_stages &
      Stage::kBackendCompatibilityCasting) {
    transformations.emplace_back(CastingTransformation);
  }

  // Optimizations go after casting because they rely on compatible input types
  // for the backend operators.
  if (options.enabled_preparation_stages & Stage::kOptimization &&
      options.optimizer.has_value()) {
    transformations.emplace_back(
        [](const DynamicEvaluationEngineOptions& options,
           const ExprNodePtr absl_nonnull& expr,
           const ExprOperatorPtr absl_nullable& /*decayed_op*/) {
          return (*options.optimizer)(expr);
        });
  }

  if (options.enabled_preparation_stages & Stage::kExtensions) {
    transformations.emplace_back(CompilerExtensionRegistry::GetInstance()
                                     .GetCompilerExtensionSet()
                                     .node_transformation_fn);
  }

  ASSIGN_OR_RETURN(current_expr,
                   ApplyNodeTransformations(options, std::move(current_expr),
                                            transformations, stack_trace));

  if (options.enabled_preparation_stages &
      Stage::kWhereOperatorsTransformation) {
    ASSIGN_OR_RETURN(current_expr,
                     WhereOperatorGlobalTransformation(
                         options, std::move(current_expr), stack_trace));
  }

  return current_expr;
}

const ExprOperatorPtr& InternalRootOperator() {
  static const absl::NoDestructor<ExprOperatorPtr> result(
      std::make_shared<InternalRootOperatorImpl>());
  return *result;
}

absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>>
LookupNamedOutputTypes(
    const ExprNodePtr& prepared_expr,
    const std::vector<std::string>& side_output_names,
    const absl::flat_hash_map<Fingerprint, QTypePtr>& node_types) {
  absl::flat_hash_map<std::string, QTypePtr> named_output_types;
  if (!side_output_names.empty()) {
    const auto& root_deps = prepared_expr->node_deps();
    if (root_deps.size() != side_output_names.size() + 1) {
      return absl::InternalError("inconsistent side_output_names size");
    }
    named_output_types.reserve(side_output_names.size());
    for (size_t i = 0; i != side_output_names.size(); ++i) {
      const auto& name = side_output_names[i];
      if (auto it = node_types.find(root_deps[i + 1]->fingerprint());
          it != node_types.end()) {
        named_output_types.emplace(name, it->second);
      } else {
        return absl::FailedPreconditionError(
            absl::StrFormat("unable to deduce named output type for %s in "
                            "the expression %s.",
                            name, GetDebugSnippet(prepared_expr)));
      }
    }
  }
  return named_output_types;
}

namespace {

absl::Status CheckForTypeMismatchAndSetType(
    absl::flat_hash_map<Fingerprint, QTypePtr>& resulting_types,
    const ExprNodePtr& expr, QTypePtr absl_nullable qtype) {
  if (qtype == nullptr) {
    return absl::OkStatus();
  }
  auto& record = resulting_types[expr->fingerprint()];
  if (record == nullptr) {
    record = qtype;
  } else if (record != qtype) {
    return absl::FailedPreconditionError(
        absl::StrFormat("different QTypes found for the same Expr %s: %s vs %s",
                        GetDebugSnippet(expr), record->name(), qtype->name()));
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<ExprNodePtr> ExtractQTypesForCompilation(
    const ExprNodePtr& prepared_expr,
    absl::flat_hash_map<Fingerprint, QTypePtr>* resulting_types,
    ExprStackTrace* absl_nullable stack_trace) {
  return ExtractQTypesForCompilation(PostOrder(prepared_expr), resulting_types,
                                     stack_trace);
}

absl::StatusOr<ExprNodePtr> ExtractQTypesForCompilation(
    const PostOrder& prepared_post_order,
    absl::flat_hash_map<Fingerprint, QTypePtr>* resulting_types,
    ExprStackTrace* absl_nullable stack_trace) {
  std::vector<ExprNodePtr> results;
  results.reserve(prepared_post_order.nodes_size());
  auto add_to_results = [&](ExprNodePtr absl_nonnull node,
                            QTypePtr absl_nullable node_qtype) -> absl::Status {
    RETURN_IF_ERROR(
        CheckForTypeMismatchAndSetType(*resulting_types, node, node_qtype));
    results.emplace_back(std::move(node));
    return absl::OkStatus();
  };
  for (size_t i = 0; i < prepared_post_order.nodes_size(); ++i) {
    const auto& node = prepared_post_order.node(i);
    const auto* node_qtype = node->qtype();
    if (!node->is_op()) {
      RETURN_IF_ERROR(add_to_results(node, node_qtype));
      continue;
    }
    const auto dep_indices = prepared_post_order.dep_indices(i);
    if (IsAnnotation(node).value_or(false)) {
#ifndef NDEBUG
      if (!IsQTypeAnnotation(node)) {
        return absl::FailedPreconditionError(absl::StrFormat(
            "unexpected annotation in a prepared expression: %s",
            GetDebugSnippet(node)));
      }
#endif
      RETURN_IF_ERROR(add_to_results(results[dep_indices[0]], node_qtype));
      continue;
    }
    const auto& deps = node->node_deps();
    bool has_modified_dep = false;
    for (size_t j = 0; j < dep_indices.size(); ++j) {
      if (deps[j]->fingerprint() != results[dep_indices[j]]->fingerprint()) {
        has_modified_dep = true;
        break;
      }
    }
    if (!has_modified_dep) {
      RETURN_IF_ERROR(add_to_results(node, node_qtype));
      continue;
    }
    std::vector<ExprNodePtr> new_deps;
    new_deps.reserve(dep_indices.size());
    for (size_t j = 0; j < dep_indices.size(); ++j) {
      new_deps.emplace_back(results[dep_indices[j]]);
    }
    auto new_node = ExprNode::UnsafeMakeOperatorNode(
        ExprOperatorPtr(node->op()), std::move(new_deps), ExprAttributes{});
    if (stack_trace != nullptr) {
      stack_trace->AddTrace(new_node, node);
    }
    RETURN_IF_ERROR(add_to_results(std::move(new_node), node_qtype));
  }
  return std::move(results.back());
}

absl::StatusOr<QTypePtr> LookupQType(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, QTypePtr>& types) {
  if (auto it = types.find(node->fingerprint()); it != types.end()) {
    return it->second;
  }
  return absl::InternalError(
      absl::StrFormat("unknown QType for node %s", GetDebugSnippet(node)));
}

absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>> LookupLeafQTypes(
    absl::Span<const std::string> leaf_keys,
    const absl::flat_hash_map<Fingerprint, QTypePtr>& types) {
  absl::flat_hash_map<std::string, QTypePtr> result;
  for (const std::string& leaf_key : leaf_keys) {
    ASSIGN_OR_RETURN(result[leaf_key], LookupQType(Leaf(leaf_key), types));
  }
  return result;
}

}  // namespace arolla::expr::eval_internal
