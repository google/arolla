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
#include "arolla/qexpr/eval_extensions/prepare_core_map_operator.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/dynamic_compiled_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/extensions.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operators/map_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

// Wraps non-array arguments into a lambda, so they are not exposed out of the
// operator.
absl::StatusOr<ExprNodePtr> MoveNonArrayLiteralArgumentsIntoOp(
    ExprNodePtr node) {
  auto deps = absl::Span<const ExprNodePtr>(node->node_deps());
  const auto& op_node = deps[0];
  auto data_deps = deps.subspan(1);

  constexpr auto can_be_embedded = [](const auto& dep) {
    return dep->qvalue().has_value() && !IsArrayLikeQType(dep->qtype());
  };
  int num_deps_to_bind_to_op =
      std::count_if(data_deps.begin(), data_deps.end(), can_be_embedded);
  if (num_deps_to_bind_to_op == 0) {
    return node;
  }

  std::vector<ExprNodePtr> wrapped_op_deps;
  ExprOperatorSignature new_op_signature;
  std::vector<ExprNodePtr> new_deps;
  new_deps.reserve(deps.size() - num_deps_to_bind_to_op);
  new_op_signature.parameters.reserve(data_deps.size() -
                                      num_deps_to_bind_to_op);
  wrapped_op_deps.reserve(data_deps.size());
  new_deps.push_back(nullptr);  // for the new op
  for (const auto& dep : data_deps) {
    if (can_be_embedded(dep)) {
      wrapped_op_deps.emplace_back(Literal(*dep->qvalue()));
    } else {
      std::string param_name =
          absl::StrFormat("param_%d", new_op_signature.parameters.size());
      new_op_signature.parameters.push_back(
          ExprOperatorSignature::Parameter{param_name});
      wrapped_op_deps.push_back(Placeholder(param_name));
      new_deps.push_back(dep);
    }
  }
  if (!op_node->qvalue().has_value()) {
    return absl::InternalError("non-literal op in core.map operator");
  }
  ASSIGN_OR_RETURN(const ExprOperatorPtr& op,
                   op_node->qvalue()->As<ExprOperatorPtr>());
  ASSIGN_OR_RETURN(
      ExprOperatorPtr new_op,
      MakeLambdaOperator(absl::StrFormat("wrapped[%s]", op->display_name()),
                         new_op_signature, BindOp(op, wrapped_op_deps, {})));
  new_deps[0] = Literal(new_op);
  return WithNewDependencies(node, std::move(new_deps));
}

}  // namespace

PackedCoreMapOperator::PackedCoreMapOperator(DynamicCompiledOperator mapper,
                                             ExprAttributes attr)
    : ExprOperatorWithFixedSignature(
          absl::StrFormat("packed_core_map[%s]", mapper.display_name()),
          ExprOperatorSignature{
              {"first_arg"},  // op must accept at least one argument.
              {.name = "rest_args",
               .kind = ExprOperatorSignature::Parameter::Kind::
                   kVariadicPositional}},
          "Applies a QExpr operator pointwise to the *args.",
          FingerprintHasher(
              "::arolla::expr::eval_internal::PackedCoreMapOperator")
              .Combine(mapper.fingerprint(), attr)
              .Finish()),
      mapper_(std::move(mapper)),
      attr_(std::move(attr)) {}

absl::StatusOr<ExprAttributes> PackedCoreMapOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  // We rely on the fact that the expr qtypes are guaranteed not to change
  // during compilation.
  return attr_;
}

// expr/eval extension to preprocess core.map.
absl::StatusOr<ExprNodePtr> MapOperatorTransformation(
    const DynamicEvaluationEngineOptions& options, ExprNodePtr node) {
  if (!node->is_op()) {
    return node;
  }
  ASSIGN_OR_RETURN(auto op, DecayRegisteredOperator(node->op()));
  if (typeid(*op) != typeid(expr_operators::MapOperator)) {
    return node;
  }
  if (node->qtype() == nullptr) {
    // We defer the transformation if the map operator was unable to infer its
    // type.
    return node;
  }

  ASSIGN_OR_RETURN(node, MoveNonArrayLiteralArgumentsIntoOp(node));

  absl::Span<const ExprNodePtr> deps = node->node_deps();
  if (deps.size() < 2) {
    return absl::InternalError("too few deps for core.map operator");
  }
  const auto& evaluand_node = deps[0];
  auto data_deps = deps.subspan(1);
  const auto& evaluand_attr = evaluand_node->attr();
  if (!evaluand_attr.qvalue().has_value()) {
    return absl::InternalError("non-literal op in core.map operator");
  }

  // Converting Expr-level op to QExpr.
  ASSIGN_OR_RETURN(const ExprOperatorPtr& mapper,
                   evaluand_attr.qvalue()->As<ExprOperatorPtr>());
  std::vector<QTypePtr> mapper_input_qtypes;
  mapper_input_qtypes.reserve(data_deps.size());
  for (const auto& d : data_deps) {
    if (d->qtype() == nullptr) {
      return absl::InternalError(
          "unexpected behavior of MapOperator::InferAttributes");
    }
    ASSIGN_OR_RETURN(QTypePtr mapper_input_type,
                     IsArrayLikeQType(d->qtype())
                         ? ToOptionalQType(d->qtype()->value_qtype())
                         : d->qtype());
    // NOTE: we are not passing d->qvalue() here because all the literal
    // non-array deps are already embedded in
    // MoveNonArrayLiteralArgumentsIntoOp.
    mapper_input_qtypes.emplace_back(mapper_input_type);
  }

  DynamicEvaluationEngineOptions mapper_options(options);
  mapper_options.enabled_preparation_stages =
      DynamicEvaluationEngineOptions::PreparationStage::kAll;
  ASSIGN_OR_RETURN(auto precompiled_mapper,
                   DynamicCompiledOperator::Build(mapper_options, mapper,
                                                  mapper_input_qtypes));
  ExprOperatorPtr prepared_map_op = std::make_shared<PackedCoreMapOperator>(
      std::move(precompiled_mapper), node->attr());
  return MakeOpNode(
      std::move(prepared_map_op),
      std::vector<ExprNodePtr>(data_deps.begin(), data_deps.end()));
}

AROLLA_INITIALIZER(
        .reverse_deps =
            {
                ::arolla::initializer_dep::kOperators,
                ::arolla::initializer_dep::kQExprOperators,
            },
        .init_fn = [] {
          CompilerExtensionRegistry::GetInstance().RegisterNodeTransformationFn(
              MapOperatorTransformation);
        })

}  // namespace arolla::expr::eval_internal
