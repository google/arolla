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
#include "arolla/expr/eval/compile_where_operator.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/algorithm/control_flow_graph.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/dynamic_compiled_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/expr_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

using Stage = DynamicEvaluationEngineOptions::PreparationStage;

// Wrapper around DominatorTree that operates on `ExprNodePtr`s instead of node
// ids. The structure remains valid after expr modifications that do not affect
// its dominator tree, but the user must manually register such changes using
// AddNodeAlias().
class ExprDominatorTree {
 public:
  // Builds dominator tree for the given expression.
  static absl::StatusOr<ExprDominatorTree> Build(const ExprNodePtr& root) {
    auto node_order = expr::VisitorOrder(root);
    // AcyclicCFG requires the entry node's id to be 0, so we number them in
    // reversed visitor order.
    std::reverse(node_order.begin(), node_order.end());

    absl::flat_hash_map<Fingerprint, AcyclicCFG::NodeId> node_ids;
    node_ids.reserve(node_order.size());
    for (size_t i = 0; i < node_order.size(); ++i) {
      node_ids[node_order[i]->fingerprint()] = i;
    }

    // deps[i] contains all the inputs to the i-th node.
    std::vector<std::vector<AcyclicCFG::NodeId>> deps;
    deps.reserve(node_order.size());
    for (const auto& node : node_order) {
      deps.emplace_back();
      deps.back().reserve(node->node_deps().size());
      for (const auto& dep : node->node_deps()) {
        deps.back().push_back(node_ids.at(dep->fingerprint()));
      }
    }
    ASSIGN_OR_RETURN(auto graph, AcyclicCFG::Create(std::move(deps)));
    DominatorTree tree(*graph);
    return ExprDominatorTree(std::move(graph), std::move(tree),
                             std::move(node_ids));
  }

  // Check that `ancestor` dominates `descendant`, i.e. all the paths from the
  // root to `descendant` are going through `ancestor`. The function expects
  // that `ancestor` must be an ancestor of `descendant` in the original expr.
  bool StrictlyDominates(const ExprNodePtr& descendant,
                         const ExprNodePtr& ancestor) const {
    int64_t descendant_id = GetNodeId(descendant);
    int64_t ancestor_id = GetNodeId(ancestor);
    return tree_.depth(descendant_id) > tree_.depth(ancestor_id);
  }

  // Returns true if the node has exactly one direct parent in the expression.
  // For example, in `(a + b) - (a + b)` node `a` has just one direct parent `(a
  // + b)`, while `(a + b)` is considered to have two direct parents.
  bool HasSingleParentInExprDag(const ExprNodePtr& node) const {
    int64_t id = GetNodeId(node);
    return graph_->reverse_deps(id).size() == 1;
  }

  // Registers a node change after an expr modification that did not affect the
  // dominator tree structure.
  void AddNodeAlias(const ExprNodePtr& new_node, const ExprNodePtr& old_node) {
    node_ids_.emplace(new_node->fingerprint(), GetNodeId(old_node));
  }

 private:
  AcyclicCFG::NodeId GetNodeId(const ExprNodePtr& node) const {
    DCHECK(node_ids_.contains(node->fingerprint()))
        << "No node id registered for node " << GetDebugSnippet(node);
    return node_ids_.at(node->fingerprint());
  }

  ExprDominatorTree(
      std::unique_ptr<AcyclicCFG> graph, DominatorTree tree,
      absl::flat_hash_map<Fingerprint, AcyclicCFG::NodeId> node_ids)
      : graph_(std::move(graph)),
        tree_(std::move(tree)),
        node_ids_(std::move(node_ids)) {}

  std::unique_ptr<AcyclicCFG> graph_;
  DominatorTree tree_;
  absl::flat_hash_map<Fingerprint, AcyclicCFG::NodeId> node_ids_;
};

absl::Status VerifyArgQTypes(const QType* cond_qtype, const QType* true_qtype,
                             const QType* false_qtype) {
  if (cond_qtype == nullptr || true_qtype == nullptr ||
      false_qtype == nullptr) {
    return absl::InternalError(
        "all types must be known before core._short_circuit_where "
        "transformation");
  }
  if (cond_qtype != GetQType<OptionalUnit>()) {
    return absl::InternalError(
        absl::StrFormat("core._short_circuit_where operator supports only "
                        "OPTIONAL_UNIT conditions, got %s",
                        cond_qtype->name()));
  }
  if (true_qtype != false_qtype) {
    return absl::InternalError(
        absl::StrFormat("true and false branches of core._short_circuit_where "
                        "must have the same QType; got %s and %s",
                        true_qtype->name(), false_qtype->name()));
  }
  return absl::OkStatus();
}

absl::Status CheckTypesUnchangedOrStripped(
    absl::Span<const QTypePtr> expected,
    absl::Span<const ExprAttributes> given) {
  if (expected.size() != given.size()) {
    return absl::InternalError(
        "number of args for internal.packed_where operator changed during "
        "compilation");
  }
  for (size_t i = 0; i < expected.size(); ++i) {
    if (given[i].qtype() != nullptr && given[i].qtype() != expected[i]) {
      return absl::InternalError(
          "input types for internal.packed_where operator changed during "
          "compilation");
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<ExprOperatorPtr> PackedWhereOp::Create(
    DynamicCompiledOperator true_op, DynamicCompiledOperator false_op) {
  if (true_op.output_qtype() != false_op.output_qtype()) {
    return absl::InternalError(
        "inconsistent output types for internal.packed_where operator "
        "branches");
  }
  return std::make_shared<PackedWhereOp>(
      PrivateConstructorTag{}, std::move(true_op), std::move(false_op));
}

PackedWhereOp::PackedWhereOp(PrivateConstructorTag,
                             DynamicCompiledOperator true_op,
                             DynamicCompiledOperator false_op)
    : ExprOperatorWithFixedSignature(
          "internal.packed_where",
          ExprOperatorSignature{{.name = "condition"},
                                {.name = "_leaves",
                                 .kind = ExprOperatorSignature::Parameter::
                                     Kind::kVariadicPositional}},
          /*docstring=*/"(Internal) Stateful short circuit where operator.",
          FingerprintHasher("arolla::expr::PackedWhereOp")
              .Combine(true_op.fingerprint(), false_op.fingerprint())
              .Finish()),
      true_op_(std::move(true_op)),
      false_op_(std::move(false_op)) {}

absl::StatusOr<ExprAttributes> PackedWhereOp::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  size_t expected_arg_count =
      1 + true_op_.input_qtypes().size() + false_op_.input_qtypes().size();
  if (expected_arg_count != inputs.size()) {
    return absl::InternalError(
        "number of args for internal.packed_where operator changed during "
        "compilation");
  }
  auto true_inputs = inputs.subspan(1, true_op_.input_qtypes().size());
  RETURN_IF_ERROR(
      CheckTypesUnchangedOrStripped(true_op_.input_qtypes(), true_inputs));
  auto false_inputs = inputs.subspan(1 + true_op_.input_qtypes().size());
  RETURN_IF_ERROR(
      CheckTypesUnchangedOrStripped(false_op_.input_qtypes(), false_inputs));
  return ExprAttributes(true_op_.output_qtype());
}

absl::StatusOr<ExprNodePtr> WhereOperatorTransformationImpl(
    const DynamicEvaluationEngineOptions& options, ExprNodePtr node,
    const ExprDominatorTree& dominator_tree) {
  ASSIGN_OR_RETURN(auto op, DecayRegisteredOperator(node->op()));
  if (!IsBackendOperator(op, "core._short_circuit_where")) {
    return node;
  }
  const auto& deps = node->node_deps();
  if (deps.size() != 3) {
    return absl::InternalError(absl::StrFormat(
        "incorrect number of dependencies passed to an "
        "core._short_circuit_where operator node: expected 3 but got %d.",
        deps.size()));
  }
  const ExprNodePtr& condition_branch = deps[0];
  const ExprNodePtr& true_branch = deps[1];
  const ExprNodePtr& false_branch = deps[2];

  RETURN_IF_ERROR(VerifyArgQTypes(condition_branch->qtype(),
                                  true_branch->qtype(), false_branch->qtype()));

  // Filter for "if" branch subnodes that should be short circuited.
  auto must_be_short_circuited = [&](ExprNodePtr branch_root) {
    return [branch_root = std::move(branch_root),
            &dominator_tree](const ExprNodePtr& n) -> absl::StatusOr<bool> {
      // We don't split annotation chains.
      ASSIGN_OR_RETURN(auto annotationless_n, StripTopmostAnnotations(n));
      // We don't take leaves and literals into lambda.
      if (annotationless_n->is_leaf()) {
        return false;
      }
      if (annotationless_n.get() != n.get()) {
        return absl::InternalError(
            absl::StrFormat("WhereOperatorGlobalTransformation does not "
                            "support annotations except for leaves, got %s",
                            GetDebugSnippet(n)));
      }
      if (n->is_literal()) {
        return false;
      }
      // We take branch_root itself only if its only parent is the current
      // core._short_circuit_where node.
      if (n.get() == branch_root.get()) {
        return dominator_tree.HasSingleParentInExprDag(n);
      }
      // We take operators that are strictly dominated by branch_root.
      return dominator_tree.StrictlyDominates(annotationless_n, branch_root);
    };
  };

  // 1. Check if there is nothing to short circuit — we just fall back to the
  // normal core.where in this case.

  ASSIGN_OR_RETURN(bool true_branch_must_be_short_circuited,
                   must_be_short_circuited(true_branch)(true_branch));
  ASSIGN_OR_RETURN(bool false_branch_must_be_short_circuited,
                   must_be_short_circuited(false_branch)(false_branch));
  if (!true_branch_must_be_short_circuited &&
      !false_branch_must_be_short_circuited) {
    ASSIGN_OR_RETURN(ExprOperatorPtr core_where_op,
                     LookupOperator("core.where"));
    ASSIGN_OR_RETURN(core_where_op, DecayRegisteredOperator(core_where_op));
    // WhereOperatorGlobalTransformation runs outside of the main DeepTransform,
    // so we have to be sure that the operator we use is already at the lowest
    // level.
    if (!HasBackendExprOperatorTag(core_where_op)) {
      return absl::InternalError(
          "core.where operator must be a backend operator");
    }
    return MakeOpNode(core_where_op,
                      {condition_branch, true_branch, false_branch});
  }

  // 2. Extract the subexpressions to short circuit into lambdas and precompile
  // these lambdas.

  DynamicEvaluationEngineOptions subexpression_options(options);
  // We add new leaves, so need to populate QTypes for them. We also wrap
  // subexpressions into lambdas, so we add lowering. But all other stages
  // should be already done by this time.
  subexpression_options.enabled_preparation_stages =
      Stage::kPopulateQTypes | Stage::kToLower;
  // Overriding input slots may be not expected by the outer expression.
  subexpression_options.allow_overriding_input_slots = false;
  ASSIGN_OR_RETURN(
      ExprNodePtr true_lambda_expr,
      ExtractLambda(true_branch, must_be_short_circuited(true_branch)));
  ASSIGN_OR_RETURN(auto precompiled_true,
                   DynamicCompiledOperator::Build(
                       subexpression_options, true_lambda_expr->op(),
                       GetExprQTypes(true_lambda_expr->node_deps())));
  ASSIGN_OR_RETURN(
      ExprNodePtr false_lambda_expr,
      ExtractLambda(false_branch, must_be_short_circuited(false_branch)));
  ASSIGN_OR_RETURN(auto precompiled_false,
                   DynamicCompiledOperator::Build(
                       subexpression_options, false_lambda_expr->op(),
                       GetExprQTypes(false_lambda_expr->node_deps())));

  // 3. Encapsulate the precompiled expressions into PackedWhereOp.

  ASSIGN_OR_RETURN(ExprOperatorPtr packed_op,
                   PackedWhereOp::Create(std::move(precompiled_true),
                                         std::move(precompiled_false)));
  std::vector<ExprNodePtr> args = {condition_branch};
  args.insert(args.end(), true_lambda_expr->node_deps().begin(),
              true_lambda_expr->node_deps().end());
  args.insert(args.end(), false_lambda_expr->node_deps().begin(),
              false_lambda_expr->node_deps().end());
  return MakeOpNode(std::move(packed_op), std::move(args));
}

absl::StatusOr<ExprNodePtr> WhereOperatorGlobalTransformation(
    const DynamicEvaluationEngineOptions& options, ExprNodePtr node) {
  ASSIGN_OR_RETURN(auto dominator_tree, ExprDominatorTree::Build(node));
  // We do not use Transform in order to be able to add alias to the previous
  // node.
  return PostOrderTraverse(
      node,
      [&](const ExprNodePtr& node,
          absl::Span<const ExprNodePtr* const> arg_visits)
          -> absl::StatusOr<ExprNodePtr> {
        ASSIGN_OR_RETURN(
            auto transformed_node,
            WithNewDependencies(node, DereferenceVisitPointers(arg_visits)));
        // NOTE: We could AddNodeAlias for transformed_node here, but we don't
        // do it because WhereOperatorTransformationImpl does not rely on it
        // (and the alias it needs will be added below).
        ASSIGN_OR_RETURN(
            transformed_node,
            WhereOperatorTransformationImpl(
                options, std::move(transformed_node), dominator_tree));
        dominator_tree.AddNodeAlias(transformed_node, node);
        return transformed_node;
      });
}

absl::StatusOr<TypedSlot> CompileWhereOperator(
    const DynamicEvaluationEngineOptions& options,
    const PackedWhereOp& where_op, absl::Span<const TypedSlot> input_slots,
    TypedSlot output_slot,
    eval_internal::ExecutableBuilder* executable_builder) {
  size_t expected_arg_count = 1 + where_op.true_op().input_qtypes().size() +
                              where_op.false_op().input_qtypes().size();
  if (expected_arg_count != input_slots.size()) {
    return absl::InternalError(
        "incorrect number of input slots passed to internal.packed_where "
        "operator");
  }

  auto true_input_slots =
      input_slots.subspan(1, where_op.true_op().input_qtypes().size());
  auto before_true_branch = executable_builder->SkipEvalOp();
  RETURN_IF_ERROR(where_op.true_op().BindTo(*executable_builder,
                                            true_input_slots, output_slot));

  auto false_input_slots =
      input_slots.subspan(1 + where_op.true_op().input_qtypes().size());
  auto before_false_branch = executable_builder->SkipEvalOp();
  RETURN_IF_ERROR(where_op.false_op().BindTo(*executable_builder,
                                             false_input_slots, output_slot));

  if (input_slots[0].GetType() != GetQType<OptionalUnit>()) {
    return absl::InternalError(
        "unexpected condition slot type for internal.packed_where operator");
  }
  ASSIGN_OR_RETURN(auto cond_slot, input_slots[0].SubSlot(0).ToSlot<bool>());
  int64_t jump_to_false_branch = before_false_branch - before_true_branch;
  auto before_true_branch_op_name =
      absl::StrFormat("jump_if_not<%+d>", jump_to_false_branch);
  if (jump_to_false_branch == 0) {
    return absl::InternalError(
        "true branch of internal.packed_where compiled into no operators");
  }
  RETURN_IF_ERROR(executable_builder->SetEvalOp(
      before_true_branch,
      JumpIfNotBoundOperator(cond_slot, jump_to_false_branch),
      eval_internal::FormatOperatorCall(before_true_branch_op_name,
                                        {input_slots[0]}, {}),
      before_true_branch_op_name));
  int64_t jump_after_false_branch =
      executable_builder->current_eval_ops_size() - before_false_branch - 1;
  auto before_false_branch_op_name =
      absl::StrFormat("jump<%+d>", jump_after_false_branch);
  if (jump_after_false_branch == 0) {
    return absl::InternalError(
        "false branch of internal.packed_where compiled into no operators");
  }
  RETURN_IF_ERROR(executable_builder->SetEvalOp(
      before_false_branch, JumpBoundOperator(jump_after_false_branch),
      eval_internal::FormatOperatorCall(before_false_branch_op_name, {}, {}),
      before_false_branch_op_name));
  return output_slot;
}

}  // namespace arolla::expr::eval_internal
