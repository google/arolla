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
#include "arolla/expr/eval/prepare_expression.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/casting.h"
#include "arolla/expr/eval/compile_where_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/extensions.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_stack_trace.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

using Stage = DynamicEvaluationEngineOptions::PreparationStage;

class InternalRootOperatorImpl final : public BuiltinExprOperatorTag,
                                       public ExprOperatorWithFixedSignature {
 public:
  InternalRootOperatorImpl()
      : ExprOperatorWithFixedSignature(
            "_internal_root_operator_",
            ExprOperatorSignature{{.name = "arg0"},
                                  {.name = "args",
                                   .kind = ExprOperatorSignature::Parameter::
                                       Kind::kVariadicPositional}},
            "",  // TODO: doc-string
            FingerprintHasher("::arolla::expr::InternalRootOperator")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    return inputs[0];
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
  return CallOp(QTypeAnnotation::Make(),
                {std::move(leaf), Literal(it->second)});
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
  return
      [&input_types, &root](const DynamicEvaluationEngineOptions&,
                            ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
        if (!node->is_op()) {
          return node;
        }

        if (const QType* annotated_qtype = ReadQTypeAnnotation(node);
            annotated_qtype != nullptr) {
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

        bool has_leaf_dep = false;
        for (const auto& d : node->node_deps()) {
          if (d->is_leaf()) {
            has_leaf_dep = true;
          }
        }
        if (!has_leaf_dep) {
          return node;
        }

        std::vector<ExprNodePtr> new_deps = node->node_deps();
        for (auto& d : new_deps) {
          if (d->is_leaf()) {
            ASSIGN_OR_RETURN(
                d, AnnotateLeafWithQType(std::move(d), input_types, root));
          }
        }
        return WithNewDependencies(node, std::move(new_deps));
      };
}

// Precomputes parts of an expression that depend on literals only and replaces
// the corresponding nodes with literals.
absl::StatusOr<ExprNodePtr> LiteralFoldingTransformation(
    const DynamicEvaluationEngineOptions& options, ExprNodePtr node) {
  if (!node->is_op() || !AllDepsAreLiterals(node) ||
      node->op() == InternalRootOperator()) {
    return node;
  }
  if (node->qvalue()) {
    return Literal(*node->qvalue());
  }

  DynamicEvaluationEngineOptions invoke_options = options;
  // kPopulateQTypes is not needed for literal folding, and kLiteralFolding
  // would cause infinite recursion. kOptimization is not needed for a one-off
  // evaluation, and kWhereOperatorsTransformation is not needed when
  // optimizations are disabled.
  invoke_options.enabled_preparation_stages &=
      ~(Stage::kLiteralFolding | Stage::kPopulateQTypes | Stage::kOptimization |
        Stage::kWhereOperatorsTransformation);
  ASSIGN_OR_RETURN(auto result, Invoke(node, {}, invoke_options),
                   _ << "while doing literal folding");
  return Literal(result);
}

absl::StatusOr<ExprNodePtr> ToLowerTransformation(
    const DynamicEvaluationEngineOptions&, ExprNodePtr expr) {
  return ToLowerNode(expr);
}

absl::StatusOr<ExprNodePtr> StripAnnotationsTransformation(
    const DynamicEvaluationEngineOptions&, const ExprNodePtr& node) {
  ASSIGN_OR_RETURN(bool is_annotation, IsAnnotation(node));
  if (is_annotation && node->node_deps().empty()) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "invalid annotation %s: expected at least 1 argument, got 0",
        GetDebugSnippet(node)));
  }
  return (is_annotation &&
          !IsQTypeAnnotation(node)  // We keep QType annotations for type
                                    // assertions till the very end.
          )
             ? node->node_deps()[0]
             : node;
}

absl::Status CheckForTypeMismatchAndSetType(
    absl::flat_hash_map<Fingerprint, QTypePtr>* resulting_types,
    const ExprNodePtr& expr, QTypePtr qtype) {
  auto it = resulting_types->find(expr->fingerprint());
  if (it != resulting_types->end() && it->second != nullptr) {
    if (it->second != qtype) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "different QTypes found for the same Expr %s: %s vs %s",
          GetDebugSnippet(expr), it->second->name(), qtype->name()));
    }
  } else {
    (*resulting_types)[expr->fingerprint()] = qtype;
  }
  return absl::OkStatus();
}

absl::StatusOr<ExprNodePtr> ApplyNodeTransformations(
    const DynamicEvaluationEngineOptions& options, ExprNodePtr expr,
    absl::Span<const std::pair<TransformationType, NodeTransformationFn>>
        transformations,
    std::shared_ptr<ExprStackTrace> stack_trace) {
  return DeepTransform(
      expr,
      [&options, &transformations,
       &stack_trace](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
        for (const auto& t : transformations) {
          ASSIGN_OR_RETURN(auto result, t.second(options, node));
          if (result->fingerprint() == node->fingerprint()) {
            continue;
          }
          if (!node->attr().IsSubsetOf(result->attr())) {
            return absl::FailedPreconditionError(absl::StrFormat(
                "expression %s attributes changed from %s to %s during "
                "compilation",
                GetDebugSnippet(node), absl::FormatStreamed(node->attr()),
                absl::FormatStreamed(result->attr())));
          }
          if (stack_trace != nullptr) {
            stack_trace->AddTrace(result, node, t.first);
          }
          // Postpone the remaining transformations to guarantee that the
          // transformations are applied sequentially (the further
          // transformations may assume that the former are fully applied).
          return result;
        }
        return node;
      },
      /*new_deps_trace_logger=*/
      [&stack_trace](ExprNodePtr node, ExprNodePtr prev_node,
                     DeepTransformStage stage) {
        if (stack_trace != nullptr) {
          if (stage == DeepTransformStage::kWithNewDeps) {
            stack_trace->AddTrace(node, prev_node,
                                  TransformationType::kChildTransform);
          } else if (stage ==
                     DeepTransformStage::kNewChildAfterTransformation) {
            stack_trace->AddTrace(
                node, prev_node,
                TransformationType::kCausedByAncestorTransform);
          }
        }
      });
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
    std::shared_ptr<ExprStackTrace> stack_trace) {
  // PopulateQTypesTransformation does not handle a single leaf correctly, but
  // there is nothing to "prepare" anyway.
  if (expr->is_leaf()) {
    return PrepareSingleLeafExpression(expr, input_types, options);
  }

  ExprNodePtr current_expr = expr;

  std::vector<std::pair<TransformationType, NodeTransformationFn>>
      transformations;
  if (options.enabled_preparation_stages & Stage::kPopulateQTypes) {
    transformations.push_back(
        {TransformationType::kUntraced,
         PopulateQTypesTransformation(input_types, expr)});
  }
  if (options.enabled_preparation_stages & Stage::kLiteralFolding) {
    transformations.push_back(
        {TransformationType::kUntraced, LiteralFoldingTransformation});
  }
  if (options.enabled_preparation_stages & Stage::kToLower) {
    transformations.push_back(
        {TransformationType::kLowering, ToLowerTransformation});
  }

  // The least frequent transformations go at the end, as they will be likely
  // no-op and processed only once.
  if (options.enabled_preparation_stages & Stage::kStripAnnotations) {
    transformations.push_back(
        {TransformationType::kUntraced, StripAnnotationsTransformation});
  }

  // Casting must go after lowering because it assumes that the expression
  // contains only backend operators.
  // TODO(b/161214936) Consider adding a no-op transformation that validates it.
  if (options.enabled_preparation_stages &
      Stage::kBackendCompatibilityCasting) {
    transformations.push_back(
        {TransformationType::kUntraced, CastingTransformation});
  }

  // Optimizations go after casting because they rely on compatible input types
  // for the backend operators.
  if (options.enabled_preparation_stages & Stage::kOptimization &&
      options.optimizer.has_value()) {
    transformations.push_back(
        {TransformationType::kOptimization,
         [](const DynamicEvaluationEngineOptions& options, ExprNodePtr expr) {
           return (*options.optimizer)(std::move(expr));
         }});
  }

  if (options.enabled_preparation_stages & Stage::kExtensions) {
    transformations.push_back(
        {TransformationType::kUntraced, CompilerExtensionRegistry::GetInstance()
                                            .GetCompilerExtensionSet()
                                            .node_transformation_fn});
  }

  ASSIGN_OR_RETURN(current_expr,
                   ApplyNodeTransformations(options, current_expr,
                                            transformations, stack_trace));

  if (options.enabled_preparation_stages &
      Stage::kWhereOperatorsTransformation) {
    ASSIGN_OR_RETURN(current_expr,
                     WhereOperatorGlobalTransformation(options, current_expr));
  }

  return current_expr;
}

ExprOperatorPtr InternalRootOperator() {
  static absl::NoDestructor<ExprOperatorPtr> first_op(
      std::make_shared<InternalRootOperatorImpl>());
  return (*first_op);
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

absl::StatusOr<ExprNodePtr> ExtractQTypesForCompilation(
    const ExprNodePtr& expr,
    absl::flat_hash_map<Fingerprint, QTypePtr>* resulting_types,
    std::shared_ptr<ExprStackTrace> stack_trace) {
  return PostOrderTraverse(
      expr,
      [&resulting_types, &stack_trace](
          const ExprNodePtr& node, absl::Span<const ExprNodePtr* const> visits)
          -> absl::StatusOr<ExprNodePtr> {
        if (IsQTypeAnnotation(node) && !visits.empty()) {
          QTypePtr qtype = node->qtype();
          ExprNodePtr wrapped_node = *(visits[0]);
          RETURN_IF_ERROR(CheckForTypeMismatchAndSetType(resulting_types,
                                                         wrapped_node, qtype));
          ASSIGN_OR_RETURN(bool is_annotation, IsAnnotation(wrapped_node));
          // If there is an annotation stack with_qtype(anno1(anno2(x))), we
          // assign QType to all intermediate nodes as well.
          while (is_annotation && !wrapped_node->node_deps().empty()) {
            wrapped_node = wrapped_node->node_deps()[0];
            RETURN_IF_ERROR(CheckForTypeMismatchAndSetType(
                resulting_types, wrapped_node, qtype));
            ASSIGN_OR_RETURN(is_annotation, IsAnnotation(wrapped_node));
          }

          if (stack_trace != nullptr) {
            stack_trace->AddTrace(*(visits[0]), node,
                                  TransformationType::kUntraced);
          }

          return *(visits[0]);
        }

        std::vector<expr::ExprNodePtr> node_deps =
            DereferenceVisitPointers(visits);
        ASSIGN_OR_RETURN(auto new_node,
                         WithNewDependencies(node, std::move(node_deps)));
        RETURN_IF_ERROR(CheckForTypeMismatchAndSetType(
            resulting_types, new_node, node->qtype()));
        if (stack_trace != nullptr) {
          stack_trace->AddTrace(new_node, node, TransformationType::kUntraced);
        }
        return new_node;
      });
}

absl::StatusOr<QTypePtr> LookupQType(
    const ExprNodePtr node,
    const absl::flat_hash_map<Fingerprint, QTypePtr>& types) {
  if (auto it = types.find(node->fingerprint()); it != types.end()) {
    return it->second;
  }
  return absl::InternalError(
      absl::StrFormat("unknown QType for node %s", GetDebugSnippet(node)));
}

absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>> LookupLeafQTypes(
    const ExprNodePtr& expr,
    const absl::flat_hash_map<Fingerprint, QTypePtr>& types) {
  absl::flat_hash_map<std::string, QTypePtr> result;
  for (const auto& node : VisitorOrder(expr)) {
    if (node->is_leaf()) {
      ASSIGN_OR_RETURN(result[node->leaf_key()], LookupQType(node, types));
    }
  }
  return result;
}

}  // namespace arolla::expr::eval_internal
