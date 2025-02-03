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
#include "arolla/codegen/expr/codegen_operator.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/algorithm/control_flow_graph.h"
#include "arolla/codegen/expr/optimizations.h"
#include "arolla/codegen/expr/types.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/derived_qtype_cast_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/prepare_expression.h"
#include "arolla/expr/eval/side_output.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qexpr/operator_metadata.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/map.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

ABSL_FLAG(int64_t, arolla_codegen_min_local_variables_per_lambda, 50,
          R"""(
    Minimum number of local variables required in order to create lambda.
    There are several things to consider for tuning this parameter.
    1. maximum depth of braces is limited in C++, so we shouldn't create too
       deep structure.
    2. C++ compiler can be not good in optimizing too many lambda functions.
    3. On other hand smaller number can eliminate stack usage more.
    4. It is not clear whenever compiler can successfully reuse stack memory
       for several variables with the same type.
    )""");
ABSL_FLAG(int64_t, arolla_codegen_max_allowed_inline_depth, 50,
          R"""(
    Maximim depth in inlining function calls that used only once.
    There are several things to consider for tuning this parameter.
    1. Inlining may help compiler to optimize better and take advantage of
       temporary variables, save stack pressure.
    2. Inlining making code slightly more readable.
    3. maximum depth of braces is limited in C++, so we shouldn't create too
       deep structure.
    )""");

namespace arolla::codegen {

namespace codegen_impl {

bool IsInlinableLiteralType(const QType* /*nullable*/ qtype) {
  auto is_primitive_type = [](const QType* /*nullable*/ type) {
    return IsScalarQType(type) && type != GetQType<Text>() &&
           type != GetQType<Bytes>();
  };
  return qtype != nullptr && is_primitive_type(DecayOptionalQType(qtype));
}

}  // namespace codegen_impl

namespace {

using expr::DecayRegisteredOperator;
using expr::ExprNodePtr;
using expr::ExprNodeType;
using expr::ExprOperatorPtr;
using expr::ExprOperatorSignature;
using expr::HasBackendExprOperatorTag;
using expr::UnnamedExprOperator;
using expr::eval_internal::InternalRootOperator;

using NodeId = AcyclicCFG::NodeId;

// Operator with a single argument marking result to be exported.
// Operator has state with id of the named output to export.
class InternalNamedOutputExportOperator final : public UnnamedExprOperator {
 public:
  explicit InternalNamedOutputExportOperator(int64_t export_id)
      : UnnamedExprOperator(
            ExprOperatorSignature({{"x"}}),
            FingerprintHasher("codegen::InternalNamedOutputExportOperator")
                .Combine(export_id)
                .Finish()),
        export_id_(export_id) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    return input_qtypes[0];
  }

  int64_t ExportId() const { return export_id_; }

 private:
  int64_t export_id_;
};

std::optional<int64_t> MaybeGetExportId(const ExprNodePtr& node) {
  if (auto* export_op =
          fast_dynamic_downcast_final<const InternalNamedOutputExportOperator*>(
              node->op().get())) {
    return export_op->ExportId();
  }
  return std::nullopt;
}

absl::StatusOr<std::vector<QTypePtr>> DependencyTypes(
    const ExprNodePtr& node,
    std::function<absl::StatusOr<QTypePtr>(const ExprNodePtr&)>
        qtype_from_expr_fn) {
  std::vector<QTypePtr> result;
  result.reserve(node->node_deps().size());
  for (const ExprNodePtr& dep : node->node_deps()) {
    ASSIGN_OR_RETURN(result.emplace_back(), qtype_from_expr_fn(dep));
  }
  return result;
}

// Lookups metadata of the operator.
// Returns nullopt on known fake non backend operator.
// Returns error on other unexpected operators.
absl::StatusOr<std::optional<QExprOperatorMetadata>> GetOperatorMetadata(
    const QExprOperatorMetadataRegistry& op_registry, const ExprNodePtr& node,
    std::function<absl::StatusOr<QTypePtr>(const ExprNodePtr&)>
        qtype_from_expr_fn) {
  ASSIGN_OR_RETURN(auto op, DecayRegisteredOperator(node->op()));
  if (op == InternalRootOperator()) {
    return std::nullopt;
  }
  if (expr::IsQTypeAnnotation(node)) {
    return std::nullopt;
  }
  if (auto export_id_opt = MaybeGetExportId(node); export_id_opt.has_value()) {
    return std::nullopt;
  }
  if (typeid(*op) == typeid(expr::DerivedQTypeUpcastOperator) ||
      typeid(*op) == typeid(expr::DerivedQTypeDowncastOperator)) {
    return std::nullopt;
  }
  if (!HasBackendExprOperatorTag(op)) {
    return absl::InvalidArgumentError(absl::StrCat(
        node->op()->display_name(), " is not a backend ExprOperator"));
  }

  ASSIGN_OR_RETURN(auto dependency_types,
                   DependencyTypes(node, qtype_from_expr_fn));

  ASSIGN_OR_RETURN(
      auto metadata,
      op_registry.LookupOperatorMetadata(op->display_name(), dependency_types),
      _ << "while processing: " << expr::GetDebugSnippet(node));
  return {metadata};
}

// Returns AcyclicCFG and list of ExprNodePtr indexed by the graph's node ids.
absl::StatusOr<std::pair<std::unique_ptr<AcyclicCFG>, std::vector<ExprNodePtr>>>
BuildEvalCfg(const ExprNodePtr& entry_node) {
  auto nodes_order = expr::VisitorOrder(entry_node);
  std::reverse(nodes_order.begin(), nodes_order.end());

  absl::flat_hash_map<Fingerprint, NodeId> node_id;
  node_id.reserve(nodes_order.size());
  for (const auto& node : nodes_order) {
    NodeId id = node_id.size();
    node_id[node->fingerprint()] = id;
  }

  std::vector<std::vector<NodeId>> deps;
  deps.reserve(nodes_order.size());
  for (const auto& node : nodes_order) {
    std::vector<NodeId> cur_deps;
    cur_deps.reserve(node->node_deps().size());
    for (const auto& dep : node->node_deps()) {
      cur_deps.push_back(node_id[dep->fingerprint()]);
    }
    deps.push_back(std::move(cur_deps));
  }
  ASSIGN_OR_RETURN(auto graph, AcyclicCFG::Create(std::move(deps)));
  return {std::pair{std::move(graph), std::move(nodes_order)}};
}

// Returns true for all nodes used at most once, but guarantee
// maximum nesting depth <= FLAGS_arolla_codegen_max_allowed_inline_depth.
std::vector<bool> FindInlinableNodes(const AcyclicCFG& graph) {
  std::vector<bool> inlinable(graph.num_nodes(), false);

  std::vector<size_t> inline_depth(graph.num_nodes(), 0);
  for (NodeId node_id = graph.num_nodes() - 1; node_id > 0; --node_id) {
    bool used_once = graph.reverse_deps(node_id).size() == 1;
    if (used_once) {
      size_t max_inline_depth = 0;
      for (NodeId dep : graph.deps(node_id)) {
        max_inline_depth = std::max(max_inline_depth, inline_depth[dep]);
      }
      if (max_inline_depth <
          absl::GetFlag(FLAGS_arolla_codegen_max_allowed_inline_depth)) {
        inlinable[node_id] = true;
        inline_depth[node_id] = max_inline_depth + 1;
      }
    }
  }
  inlinable[0] = true;  // root is used once by the main function

  return inlinable;
}

class Codegen {
 public:
  Codegen(const QExprOperatorMetadataRegistry& op_registry,
          const AcyclicCFG& graph, std::vector<ExprNodePtr> exprs,
          absl::flat_hash_map<Fingerprint, QTypePtr> node_qtypes,
          std::vector<std::string> side_output_names,
          // If true inputs considered to be stored in global context
          // (e.g., Frame).
          // If false inputs are considered to be expensive
          // to compute and need to be stored to local variable.
          bool inputs_are_cheap_to_read)
      : op_registry_(op_registry),
        graph_(graph),
        dominator_tree_(graph_),
        exprs_(std::move(exprs)),
        node_qtypes_(std::move(node_qtypes)),
        side_output_names_(std::move(side_output_names)),
        inputs_are_cheap_to_read_(inputs_are_cheap_to_read) {}

  absl::StatusOr<OperatorCodegenData> Process() {
    std::vector<bool> inlinable = FindInlinableNodes(graph_);
    OperatorCodegenData data;
    data.side_outputs.reserve(side_output_names_.size());
    for (const auto& name : side_output_names_) {
      data.side_outputs.emplace_back(name, -1);
    }
    for (NodeId node_id = graph_.num_nodes() - 1; node_id >= 0; --node_id) {
      RETURN_IF_ERROR(ProcessSingleNode(node_id, inlinable[node_id], &data));
    }
    for (const auto& [name, assignment_id] : data.side_outputs) {
      if (assignment_id == -1) {
        return absl::InternalError(absl::StrFormat(
            "named output `%s` is lost in transformations", name));
      }
    }
    ASSIGN_OR_RETURN(data.functions, SplitOnFunctions(data));
    FilterArgumentsAsFunction(data);
    // Note that LambdifyFunctions expects all assignments including inlinable
    // be listed in the function.
    LambdifyFunctions(data);

    ComputeLocalExprStatus(data);

    data.output_id = ToAssignmentId(0);
    return data;
  }

 private:
  absl::StatusOr<QTypePtr> QTypeFromExpr(const ExprNodePtr& node) const {
    DCHECK(node_qtypes_.contains(node->fingerprint()));
    auto qtype = node_qtypes_.at(node->fingerprint());
    if (qtype == nullptr) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "unable to deduce QType for %s", expr::ToDebugString(node)));
    }
    return qtype;
  }

  // Assignments are ordered in reverse order compare to nodes in a graph
  LValueId ToAssignmentId(NodeId node_id) const {
    return graph_.num_nodes() - node_id - 1;
  }

  // Assignments are ordered in reverse order compare to nodes in a graph
  NodeId ToNodeId(LValueId assignment_id) const {
    return graph_.num_nodes() - assignment_id - 1;
  }

  bool IsLiteralNode(NodeId node_id) const {
    return exprs_[node_id]->is_literal();
  }

  bool IsLeafNode(NodeId node_id) const { return exprs_[node_id]->is_leaf(); }

  // Returns true for nodes, which can be separated.
  // I. e. no intermediate results are used outside of its deps.
  // If inputs_are_cheap_to_read is true, leaves are marked as separable.
  // All literals are marked as not separable.
  absl::StatusOr<std::vector<bool>> FindSeparableNodes() const {
    int64_t n = graph_.num_nodes();
    // literals are always global
    // leaves are always global if inputs_are_cheap_to_read_
    absl::flat_hash_set<NodeId> global_nodes;
    for (int64_t node_id = 0; node_id != n; ++node_id) {
      if (IsLiteralNode(node_id) ||
          (inputs_are_cheap_to_read_ && IsLeafNode(node_id))) {
        global_nodes.insert(node_id);
      }
    }
    ASSIGN_OR_RETURN(auto externalized_graph,
                     ExternalizeNodes(graph_, dominator_tree_, global_nodes));
    // all nodes with empty frontier can be placed to the function
    auto is_separable = FindVerticesWithEmptyDominanceFrontier(
        *externalized_graph, dominator_tree_);
    // Do not separate literals and leaves.
    // There is no good reason to create extra indirection for them.
    for (NodeId node_id = 0; node_id != n; ++node_id) {
      if (IsLiteralNode(node_id) || IsLeafNode(node_id)) {
        is_separable[node_id] = false;
      }
    }
    return is_separable;
  }

  // Finds out what assignments could be placed into the separate functions.
  // Graph node corresponding to the output_id will be dominator of all nodes
  // correspond to the assignment_ids.
  absl::StatusOr<std::vector<Function>> SplitOnFunctions(
      OperatorCodegenData& data) const {
    int64_t n = graph_.num_nodes();
    ASSIGN_OR_RETURN(auto is_separable, FindSeparableNodes());
    CHECK(is_separable[0] || IsLiteralNode(0) || IsLeafNode(0))
        << "InternalError: entry node should be always separable";

    // Assign function id to the function root assignment.
    // Initialize all functions output_id's.
    std::vector<Function> functions;
    constexpr int64_t kUndefined = -1;
    std::vector<int64_t> function_id(n, kUndefined);
    for (NodeId node_id = n - 1; node_id >= 0; --node_id) {
      if (is_separable[node_id]) {
        function_id[node_id] = functions.size();
        Function new_fn;
        new_fn.output_id = ToAssignmentId(node_id);
        new_fn.is_result_status_or = data.assignments[new_fn.output_id]
                                         .lvalue()
                                         .is_entire_expr_status_or;
        functions.push_back(std::move(new_fn));
      }
    }
    CHECK((function_id[0] != kUndefined) || IsLiteralNode(0) || IsLeafNode(0))
        << "InternalError: entry node should be assigned to the function";

    // Propagate function ids to the dependencies.
    for (NodeId node_id = 0; node_id != n; ++node_id) {
      for (NodeId dep : graph_.deps(node_id)) {
        if (function_id[dep] == kUndefined) {
          function_id[dep] = function_id[node_id];
        }
      }
    }

    // Initialize function assignment ids.
    for (NodeId node_id = n - 1; node_id >= 0; --node_id) {
      LValueId assignment_id = ToAssignmentId(node_id);
      int64_t cur_function_id = function_id[node_id];
      // Literals are global.
      if (IsLiteralNode(node_id)) {
        continue;
      }
      // Leaves are global iff inputs_are_cheap_to_read_ is true
      // (or if entire expr is a leaf).
      if ((inputs_are_cheap_to_read_ || node_id == 0) && IsLeafNode(node_id)) {
        continue;
      }
      // Add assignment to the current function.
      // Output assignment is not added, it is stored separately in output_id.
      if (!is_separable[node_id]) {
        functions[cur_function_id].assignment_ids.push_back(assignment_id);
        for (NodeId rdep : graph_.reverse_deps(node_id)) {
          CHECK_EQ(function_id[rdep], cur_function_id)
              << "InternalError: only separable nodes can be used by other "
                 "functions";
        }
        continue;
      }
      // Current assignment is output_id of the current function.
      // Add function output_id to the unique reverse dependency.
      int64_t rdep_function = kUndefined;
      for (NodeId rdep : graph_.reverse_deps(node_id)) {
        if (function_id[rdep] != cur_function_id) {
          // some other function depend on this node
          if (rdep_function == kUndefined) {
            rdep_function = function_id[rdep];
            functions[rdep_function].assignment_ids.push_back(assignment_id);
          } else {
            CHECK_EQ(rdep_function, function_id[rdep])
                << "InternalError: non leaf function node must be used by not "
                   "more than one other function";
          }
        }
      }
    }

    return functions;
  }

  // Finds assignments that can be placed to the lambdas capturing everything
  // that defined before.
  // Include information to data.lambdas.
  // Updates data.functions[*].assignment_ids to contain only actual local
  // assignments. Inlined and within lambda assignments are removed.
  void LambdifyFunctions(OperatorCodegenData& data) const {
    for (Function& function : data.functions) {
      LambdifyFunction(data, function);
    }
  }

  // Finds for each assignment whenever it will be producing StatusOr locally.
  void ComputeLocalExprStatus(OperatorCodegenData& data) const {
    absl::flat_hash_map<LValueId, int64_t> id2lambda;
    for (int64_t i = 0; i < data.lambdas.size(); ++i) {
      id2lambda.emplace(data.lambdas[i].output_id, i);
    }
    absl::flat_hash_map<LValueId, int64_t> id2function;
    for (int64_t i = 0; i < data.functions.size(); ++i) {
      id2function.emplace(data.functions[i].output_id, i);
    }

    for (LValueId assignment_id = 0; assignment_id != data.assignments.size();
         ++assignment_id) {
      auto& assignment = data.assignments[assignment_id];
      bool is_local_expr_status_or =
          assignment.rvalue().operator_returns_status_or;
      if (id2function.contains(assignment_id)) {
        // Function calls are producing StatusOr in case function is producing
        // StatusOr.
        is_local_expr_status_or =
            data.functions[id2function[assignment_id]].is_result_status_or;
      } else {
        // Regular assignment produces StatusOr in case there are any
        // inlinable argument producing StatusOr.
        std::vector<LValueId> output_assignments =
            DependencyArgs(ToNodeId(assignment_id));
        for (LValueId dep_id : output_assignments) {
          is_local_expr_status_or =
              is_local_expr_status_or ||
              (data.assignments[dep_id].is_inlinable() &&
               data.assignments[dep_id].lvalue().is_local_expr_status_or);
        }
        if (id2lambda.contains(assignment_id)) {
          // Lambda needs to produce StatusOr also in case there are
          // any intermediate assignment producing StatusOr.
          Function& lambda = data.lambdas[id2lambda[assignment_id]];
          for (LValueId assignment_id : lambda.assignment_ids) {
            is_local_expr_status_or |= data.assignments[assignment_id]
                                           .lvalue()
                                           .is_local_expr_status_or;
          }
          lambda.is_result_status_or = is_local_expr_status_or;
        }
      }

      assignment.lvalue().is_local_expr_status_or = is_local_expr_status_or;
    }
  }

  // Filter arguments that are supported to be passed as function, but
  // there is not benefit to do so.
  // Not overusing that will help to make code more compact, readable and
  // faster compilable.
  void FilterArgumentsAsFunction(OperatorCodegenData& data) const {
    for (Assignment& assignment : data.assignments) {
      RValue& rvalue = assignment.rvalue();
      if (rvalue.kind != RValueKind::kFunctionCall &&
          rvalue.kind != RValueKind::kFunctionWithContextCall) {
        continue;
      }
      if (rvalue.argument_as_function_offsets.empty()) {
        continue;
      }
      auto new_end = std::remove_if(
          rvalue.argument_as_function_offsets.begin(),
          rvalue.argument_as_function_offsets.end(), [&](int offset) {
            const Assignment& cur_assignment =
                data.assignments[rvalue.argument_ids[offset]];
            return !cur_assignment.is_inlinable() ||
                   cur_assignment.lvalue().kind == LValueKind::kLiteral;
          });
      rvalue.argument_as_function_offsets.erase(
          new_end, rvalue.argument_as_function_offsets.end());
    }
  }

  // Returns true iff assignment_id is inlinable argument that can be
  // passed as function.
  bool IsInlinableAsFunctionArgument(LValueId assignment_id,
                                     const OperatorCodegenData& data) const {
    auto& cur_assignment = data.assignments[assignment_id];
    // No need to pass literal as a function.
    if (cur_assignment.lvalue().kind == LValueKind::kLiteral) {
      return false;
    }
    // No need to pass local variables as a function.
    if (!cur_assignment.is_inlinable()) {
      return false;
    }
    NodeId dominator_node_id = dominator_tree_.parent(ToNodeId(assignment_id));
    LValueId dominator_assignment_id = ToAssignmentId(dominator_node_id);
    // for inlinable node, dominator is the only parent
    auto& parent_assignment = data.assignments[dominator_assignment_id];
    const std::vector<LValueId>& parent_arg_ids =
        parent_assignment.rvalue().argument_ids;
    int arg_in_parent_id =
        std::find(parent_arg_ids.begin(), parent_arg_ids.end(), assignment_id) -
        parent_arg_ids.begin();
    const std::vector<int>& argument_as_function_offsets =
        parent_assignment.rvalue().argument_as_function_offsets;
    return std::count(argument_as_function_offsets.begin(),
                      argument_as_function_offsets.end(),
                      arg_in_parent_id) != 0;
  }

  // Perform operation described in LambdifyFunctions for single function.
  void LambdifyFunction(OperatorCodegenData& data, Function& function) const {
    absl::flat_hash_map<int64_t, std::vector<LValueId>>
        lambda_local_assignments;
    for (LValueId assignment_id : function.assignment_ids) {
      auto& cur_assignment = data.assignments[assignment_id];
      NodeId node_id = ToNodeId(assignment_id);
      NodeId dominator_node_id = dominator_tree_.parent(node_id);
      LValueId dominator_assignment_id = ToAssignmentId(dominator_node_id);

      auto cur_lambda_assignments =
          std::move(lambda_local_assignments[assignment_id]);
      auto& dominator_assignments =
          lambda_local_assignments[dominator_assignment_id];
      bool enough_assignments_for_lambda =
          cur_lambda_assignments.size() >
          absl::GetFlag(FLAGS_arolla_codegen_min_local_variables_per_lambda);
      bool as_function_argument =
          IsInlinableAsFunctionArgument(assignment_id, data);
      if (enough_assignments_for_lambda ||
          (as_function_argument && !cur_lambda_assignments.empty())) {
        // Lambda is created, assignments are not propagated further.
        // They will be defined in the newly created lambda.
        // Assignments could refer to the assignments outside of the function,
        // but they will be defined earlier and captured by lambda.
        data.lambdas.push_back(
            Function{.assignment_ids = std::move(cur_lambda_assignments),
                     .output_id = assignment_id,
                     .is_result_status_or = false});
        // we inline lambdas only in case it is as function argument.
        // Otherwise they are creating too deep nesting level of braces,
        // which is not well supported in C++.
        cur_assignment.set_inlinable(as_function_argument);
      } else {
        // No lambda is created. Moving all assignments to the dominator.
        // Defining assignments in the dominator will guarantee that all
        // usages will be after the definition.
        // Note that order of assignments is always increasing.
        dominator_assignments.insert(dominator_assignments.end(),
                                     cur_lambda_assignments.begin(),
                                     cur_lambda_assignments.end());
      }
      // Assignment belong to its dominator regardless creation of the lambda.
      if (!cur_assignment.is_inlinable()) {
        dominator_assignments.push_back(assignment_id);
      }
    }
    // New assignments are the leftovers from the previous process.
    function.assignment_ids =
        std::move(lambda_local_assignments[function.output_id]);
  }

  std::vector<LValueId> DependencyArgs(NodeId node_id) const {
    const auto deps = graph_.deps(node_id);
    std::vector<LValueId> deps_vector = std::vector(deps.begin(), deps.end());
    for (NodeId& id : deps_vector) {
      id = ToAssignmentId(id);
    }
    return deps_vector;
  }

  auto DependencyTypes(NodeId node_id,
                       const OperatorCodegenData& out_data) const {
    std::vector<QTypePtr> result;
    result.reserve(graph_.deps(node_id).size());
    for (NodeId dep : DependencyArgs(node_id)) {
      result.push_back(out_data.assignments[dep].lvalue().qtype);
    }
    return result;
  }

  absl::Status ProcessInternalRootOperator(NodeId node_id, bool inlinable,
                                           OperatorCodegenData* out_data) {
    if (node_id != 0) {
      return absl::InternalError(
          "InternalRootOperator can be only in the first node");
    }
    const auto& node = exprs_[node_id];
    ASSIGN_OR_RETURN(QTypePtr qtype, QTypeFromExpr(node));
    std::string type_name = CppTypeName(qtype).value_or("auto");

    std::vector<LValueId> output_assignments = DependencyArgs(node_id);
    bool is_entire_expr_status_or = false;
    for (LValueId dep_id : output_assignments) {
      is_entire_expr_status_or =
          is_entire_expr_status_or ||
          out_data->assignments[dep_id].lvalue().is_entire_expr_status_or;
    }
    out_data->assignments.push_back(
        Assignment{LValue{.type_name = type_name,
                          .is_entire_expr_status_or = is_entire_expr_status_or,
                          .qtype = qtype,
                          .kind = LValueKind::kLocal},
                   RValue{.kind = RValueKind::kFirst,
                          .operator_returns_status_or = false,
                          .code = "",
                          .argument_ids = output_assignments},
                   inlinable});
    if (output_assignments.size() < 2) {
      return absl::InternalError(
          absl::StrFormat("InternalRootOperator must have at least 2 arguments"
                          ", found: %d",
                          output_assignments.size()));
    }
    return absl::OkStatus();
  }

  absl::Status ProcessInternalNamedOutputExportOperator(
      NodeId node_id, int64_t export_id, bool inlinable,
      OperatorCodegenData* out_data) {
    const auto& node = exprs_[node_id];
    ASSIGN_OR_RETURN(QTypePtr qtype, QTypeFromExpr(node));
    std::string type_name = CppTypeName(qtype).value_or("auto");

    std::vector<LValueId> output_assignments = DependencyArgs(node_id);
    if (output_assignments.size() != 1) {
      return absl::InternalError(
          "InternalNamedOutputExportOperator must have 1 argument");
    }

    out_data->assignments.push_back(
        Assignment{LValue{.type_name = type_name,
                          .is_entire_expr_status_or =
                              out_data->assignments[output_assignments[0]]
                                  .lvalue()
                                  .is_entire_expr_status_or,
                          .qtype = qtype,
                          .kind = LValueKind::kLocal},
                   RValue{.kind = RValueKind::kOutput,
                          .operator_returns_status_or = false,
                          .code = std::to_string(export_id),
                          .argument_ids = output_assignments},
                   inlinable});

    if (export_id < 0 || export_id >= side_output_names_.size()) {
      return absl::InternalError(
          absl::StrFormat("export_id is out of range: %d", export_id));
    }
    out_data->side_outputs[export_id].second = ToAssignmentId(node_id);
    return absl::OkStatus();
  }

  absl::Status ProcessDerivedQTypeCastOperator(NodeId node_id, bool inlinable,
                                               OperatorCodegenData* out_data) {
    const auto& node = exprs_[node_id];
    ASSIGN_OR_RETURN(QTypePtr qtype, QTypeFromExpr(node));
    qtype = DecayDerivedQType(qtype);
    std::string type_name = CppTypeName(qtype).value_or("auto");

    std::vector<LValueId> output_assignments = DependencyArgs(node_id);
    if (output_assignments.size() != 1) {
      return absl::InternalError(
          "DerivedQTypeCastOperator must have 1 argument");
    }
    out_data->assignments.push_back(
        Assignment{LValue{.type_name = type_name,
                          .is_entire_expr_status_or =
                              out_data->assignments[output_assignments[0]]
                                  .lvalue()
                                  .is_entire_expr_status_or,
                          .qtype = qtype,
                          .kind = LValueKind::kLocal},
                   RValue{.kind = RValueKind::kFirst,
                          .operator_returns_status_or = false,
                          .code = "",
                          .argument_ids = output_assignments},
                   inlinable});
    return absl::OkStatus();
  }

  absl::Status ProcessSingleNode(NodeId node_id, bool inlinable,
                                 OperatorCodegenData* out_data) {
    const auto& node = exprs_[node_id];
    ASSIGN_OR_RETURN(QTypePtr qtype, QTypeFromExpr(node));
    std::string type_name = CppTypeName(qtype).value_or("auto");
    switch (node->type()) {
      case ExprNodeType::kLeaf: {
        if (type_name == "auto") {
          return absl::FailedPreconditionError(
              absl::StrFormat("CppTypeName must be implemented for all inputs. "
                              "Leaf: %s; QType: %s",
                              node->leaf_key(), qtype->name()));
        }
        out_data->inputs[node->leaf_key()] = ToAssignmentId(node_id);
        out_data->assignments.push_back(
            Assignment{LValue{.type_name = type_name,
                              .is_entire_expr_status_or = false,
                              .qtype = qtype,
                              .kind = LValueKind::kInput},
                       RValue::CreateInput(),
                       /*inlinable=*/inputs_are_cheap_to_read_ || inlinable});
        return absl::OkStatus();
      }
      case ExprNodeType::kPlaceholder: {
        return absl::FailedPreconditionError(
            absl::StrFormat("operator generation doesn't support placeholders: "
                            "P.%s found",
                            node->placeholder_key()));
      }
      case ExprNodeType::kLiteral: {
        auto value = node->qvalue();
        DCHECK(value);
        ASSIGN_OR_RETURN(std::string value_repr, CppLiteralRepr(*value));
        out_data->assignments.push_back(
            Assignment{LValue{.type_name = type_name,
                              .is_entire_expr_status_or = false,
                              .qtype = qtype,
                              .kind = LValueKind::kLiteral},
                       RValue::CreateLiteral(value_repr),
                       /*inlinable=*/
                       codegen_impl::IsInlinableLiteralType(value->GetType())});
        return absl::OkStatus();
      }
      case ExprNodeType::kOperator: {
        ASSIGN_OR_RETURN(auto op, DecayRegisteredOperator(node->op()));
        if (op == InternalRootOperator()) {
          return ProcessInternalRootOperator(node_id, inlinable, out_data);
        }
        if (auto export_id_opt = MaybeGetExportId(node);
            export_id_opt.has_value()) {
          return ProcessInternalNamedOutputExportOperator(
              node_id, *export_id_opt, inlinable, out_data);
        }
        if (typeid(*op) == typeid(expr::DerivedQTypeUpcastOperator) ||
            typeid(*op) == typeid(expr::DerivedQTypeDowncastOperator)) {
          return ProcessDerivedQTypeCastOperator(node_id, inlinable, out_data);
        }
        if (!HasBackendExprOperatorTag(op)) {
          return absl::InvalidArgumentError(absl::StrCat(
              node->op()->display_name(), " is not a backend ExprOperator"));
        }
        std::string type_name = CppTypeName(qtype).value_or("auto");

        ASSIGN_OR_RETURN(std::optional<QExprOperatorMetadata> op_metadata,
                         GetOperatorMetadata(op_registry_, node,
                                             [&](const ExprNodePtr& node) {
                                               return this->QTypeFromExpr(node);
                                             }));
        if (!op_metadata.has_value()) {
          return absl::InternalError(absl::StrCat(node->op()->display_name(),
                                                  " metadata is not found"));
        }
        const BuildDetails& build_details = op_metadata->build_details;
        out_data->headers.insert(build_details.hdrs.begin(),
                                 build_details.hdrs.end());
        out_data->deps.insert(build_details.deps.begin(),
                              build_details.deps.end());
        const std::optional<OpClassDetails>& op_class_details =
            build_details.op_class_details;
        if (!op_class_details.has_value()) {
          return absl::FailedPreconditionError(absl::StrFormat(
              "codegen doesn't work with operator without OpClassDetails: %s",
              op->display_name()));
        }
        std::vector<LValueId> dependency_args = DependencyArgs(node_id);
        bool is_entire_expr_status_or = op_class_details->returns_status_or;
        for (LValueId dep_id : dependency_args) {
          const Assignment& assignment = out_data->assignments[dep_id];
          is_entire_expr_status_or =
              is_entire_expr_status_or ||
              assignment.lvalue().is_entire_expr_status_or;
        }
        std::string op_class = build_details.op_class;
        RValueKind function_kind = op_class_details->accepts_context
                                       ? RValueKind::kFunctionWithContextCall
                                       : RValueKind::kFunctionCall;
        out_data->assignments.push_back(Assignment{
            LValue{.type_name = type_name,
                   .is_entire_expr_status_or = is_entire_expr_status_or,
                   .qtype = qtype,
                   .kind = LValueKind::kLocal},
            RValue{.kind = function_kind,
                   .operator_returns_status_or =
                       op_class_details->returns_status_or,
                   .code = op_class + "{}",
                   .argument_ids = dependency_args,
                   .argument_as_function_offsets =
                       op_class_details->arg_as_function_ids,
                   .comment = node->op() != nullptr
                                  ? std::string(node->op()->display_name())
                                  : ""},
            inlinable});
        return absl::OkStatus();
      }
    }
    return absl::InternalError(absl::StrFormat("unexpected AstNodeType: %d",
                                               static_cast<int>(node->type())));
  }

 private:
  const QExprOperatorMetadataRegistry& op_registry_;
  // Acyclic control-flow graph. Each node correspond to the single assignment.
  // Larger NodeId need to be evaluated first.
  const AcyclicCFG& graph_;
  // Dominator tree for the graph_.
  DominatorTree dominator_tree_;
  // Intermediate expressions indexed by the graph's node ids.
  std::vector<ExprNodePtr> exprs_;
  absl::flat_hash_map<Fingerprint, QTypePtr> node_qtypes_;
  // Sorted list of side output names.
  // If not empty, we expect the root operator to be
  // InternalRootOperator with `side_output_names_.size() + 1`
  // arguments: main output and side outputs.
  std::vector<std::string> side_output_names_;
  bool inputs_are_cheap_to_read_;
};

// Wraps nodes in `export_ids_map` keys with `InternalNamedOutputExportOperator`
// with corresponding export ids.
absl::StatusOr<ExprNodePtr> AttachExportOperators(
    const ExprNodePtr& expr,
    const absl::flat_hash_map<Fingerprint, std::vector<int64_t>>&
        export_ids_map) {
  return PostOrderTraverse(
      expr,
      [&](const ExprNodePtr& node, absl::Span<const ExprNodePtr* const> visits)
          -> absl::StatusOr<ExprNodePtr> {
        ASSIGN_OR_RETURN(
            auto new_node,
            WithNewDependencies(node, DereferenceVisitPointers(visits)));
        if (auto it = export_ids_map.find(node->fingerprint());
            it != export_ids_map.end()) {
          std::vector<int64_t> export_ids = it->second;
          std::sort(export_ids.begin(), export_ids.end());
          for (int64_t export_id : export_ids) {
            ASSIGN_OR_RETURN(
                new_node,
                expr::CallOp(
                    std::make_shared<InternalNamedOutputExportOperator>(
                        export_id),
                    {new_node}));
          }
        }
        return new_node;
      });
}

// Find all export ids in InternalNamedOutputExportOperator's that evaluated
// unconditionally, i. e. there is a path from root to them without
// using arguments that listed in `arg_as_function_ids` of operator metadata.
absl::StatusOr<absl::flat_hash_set<int64_t>> FindUnconditionalExportIds(
    const QExprOperatorMetadataRegistry& op_registry, const ExprNodePtr& expr) {
  absl::flat_hash_set<int64_t> res;
  std::vector<ExprNodePtr> visit_order = expr::VisitorOrder(expr);
  if (visit_order.empty()) {
    return absl::InternalError("visitor order is empty");
  }
  absl::flat_hash_set<Fingerprint> unconditional_nodes;
  unconditional_nodes.insert(visit_order.back()->fingerprint());
  for (int64_t node_id = static_cast<int64_t>(visit_order.size()) - 1;
       node_id > 0; --node_id) {
    const auto& node = visit_order[node_id];
    if (!unconditional_nodes.contains(node->fingerprint()) || !node->is_op()) {
      continue;
    }
    ASSIGN_OR_RETURN(
        std::optional<QExprOperatorMetadata> op_metadata,
        GetOperatorMetadata(op_registry, node,
                            [](const auto& node) { return node->qtype(); }));
    std::vector<int> arg_as_function_ids;
    // known non backend operators are unconditional (e.g., annotation.qtype)
    if (op_metadata.has_value()) {
      const BuildDetails& build_details = op_metadata->build_details;
      const std::optional<OpClassDetails>& op_class_details =
          build_details.op_class_details;
      if (!op_class_details.has_value()) {
        return absl::FailedPreconditionError(absl::StrFormat(
            "codegen doesn't work with operator without OpClassDetails: %s",
            node->op()->display_name()));
      }
      arg_as_function_ids = op_class_details->arg_as_function_ids;
    }
    for (int64_t arg_id = 0; arg_id != node->node_deps().size(); ++arg_id) {
      if (std::count(arg_as_function_ids.begin(), arg_as_function_ids.end(),
                     arg_id) == 0) {
        unconditional_nodes.insert(node->node_deps()[arg_id]->fingerprint());
      }
    }
  }
  for (const auto& node : visit_order) {
    if (!unconditional_nodes.contains(node->fingerprint())) {
      continue;
    }
    if (auto export_id_opt = MaybeGetExportId(node);
        export_id_opt.has_value()) {
      res.emplace(*export_id_opt);
    }
  }
  return res;
}

// Attaches InternalNamedOutputExportOperator to the exported nodes.
// This function is supposed to be used after all transformation and ToLower.
// The root operator of provided EXPRession must be
// InternalRootOperator with `side_output_names.size() + 1`
// arguments: main output and side outputs.
// Root of the result can be InternalRootOperator iff there are
// exported nodes not used for the computation of the main output.
absl::StatusOr<ExprNodePtr> AttachExportOperators(
    const QExprOperatorMetadataRegistry& op_registry, ExprNodePtr expr) {
  if (ExprOperatorPtr op = expr->op(); op != InternalRootOperator()) {
    return absl::InternalError(
        "expected InternalRootOperator in AttachExportOperators");
  }
  if (expr->node_deps().empty()) {
    return absl::InternalError(
        "empty argument list for InternalRootOperator in "
        "AttachExportOperators");
  }
  auto main_output_expr = expr->node_deps()[0];
  auto named_output_exprs =
      absl::MakeConstSpan(expr->node_deps()).subspan(1);  // Remove main output.

  // Attach InternalNamedOutputExportOperator for all nodes to export.
  absl::flat_hash_map<Fingerprint, std::vector<int64_t>> export_ids;
  for (int64_t export_id = 0; export_id != named_output_exprs.size();
       ++export_id) {
    export_ids[named_output_exprs[export_id]->fingerprint()].emplace_back(
        export_id);
  }

  // Wrap nodes with InternalNamedOutputExportOperator.
  ASSIGN_OR_RETURN(expr, AttachExportOperators(expr, export_ids));

  // Recreate main output and named output expressions.
  main_output_expr = expr->node_deps()[0];
  named_output_exprs =
      absl::MakeConstSpan(expr->node_deps()).subspan(1);  // Remove main output.

  // Keep only export nodes not reached unconditionally in the main_output_expr
  // or other export nodes.

  // Nodes which are unconditionally evaluated as a dependency of either the
  // main output expression or another export node.
  ASSIGN_OR_RETURN(absl::flat_hash_set<int64_t> inner_export_ids,
                   FindUnconditionalExportIds(op_registry, main_output_expr));
  for (int64_t export_id = 0; export_id != named_output_exprs.size();
       ++export_id) {
    if (inner_export_ids.contains(export_id)) {
      // This serves two purpose:
      // 1. prevent adding all duplicated exports into inner_export_ids
      // 2. performance optimization
      continue;
    }
    ASSIGN_OR_RETURN(
        absl::flat_hash_set<int64_t> new_export_ids,
        FindUnconditionalExportIds(op_registry, named_output_exprs[export_id]));
    new_export_ids.erase(export_id);
    inner_export_ids.insert(new_export_ids.begin(), new_export_ids.end());
  }
  // Set of root nodes to be evaluated.
  std::vector<ExprNodePtr> top_output_exprs = {main_output_expr};
  for (int64_t export_id = 0; export_id != named_output_exprs.size();
       ++export_id) {
    if (!inner_export_ids.contains(export_id)) {
      top_output_exprs.push_back(named_output_exprs[export_id]);
    }
  }
  if (top_output_exprs.size() == 1) {
    // all named outputs within the main output
    return top_output_exprs[0];
  }
  return BindOp(InternalRootOperator(), top_output_exprs, {});
}

struct NodeWithSideOutputNames {
  ExprNodePtr node;
  std::vector<std::string> side_output_names;
};

// Returns node and sorted side output names vector.
// All named output nodes are wrapped with
// `InternalNamedOutputExportOperator(export_id)`, where export_id is an index
// in `side_output_names`.
// If any named output is not used for the final result computation,
// the root operator will be InternalRootOperator with `K + 1`
// arguments: main output and side outputs not used for main output computation.
absl::StatusOr<NodeWithSideOutputNames> Preprocess(
    const QExprOperatorMetadataRegistry& op_registry, const ExprNodePtr& expr) {
  ASSIGN_OR_RETURN((auto [stripped_expr, side_outputs]),
                   ExtractSideOutputs(expr));

  ExprNodePtr new_expr = stripped_expr;

  std::vector<std::string> side_output_names;
  if (!side_outputs.empty()) {
    side_output_names.reserve(side_outputs.size());
    std::vector<ExprNodePtr> exprs = {new_expr};
    exprs.reserve(side_outputs.size() + 1);
    for (const auto& name : SortedMapKeys(side_outputs)) {
      side_output_names.push_back(name);
      exprs.push_back(side_outputs.at(name));
    }
    ASSIGN_OR_RETURN(new_expr, BindOp(InternalRootOperator(), exprs, {}));
  }

  ASSIGN_OR_RETURN(
      auto optimizer,
      GetOptimizer(absl::GetFlag(FLAGS_arolla_codegen_optimizer_name)));
  ASSIGN_OR_RETURN(
      new_expr,
      expr::eval_internal::PrepareExpression(
          new_expr, {},
          expr::DynamicEvaluationEngineOptions{.optimizer = optimizer}));
  if (!side_outputs.empty()) {
    ASSIGN_OR_RETURN(new_expr, AttachExportOperators(op_registry, new_expr));
  }
  return NodeWithSideOutputNames{std::move(new_expr),
                                 std::move(side_output_names)};
}

}  // namespace

absl::StatusOr<std::string> LValue::QTypeConstruction() const {
  return CppQTypeConstruction(qtype);
}

absl::StatusOr<OperatorCodegenData> GenerateOperatorCode(
    ExprNodePtr expr, bool inputs_are_cheap_to_read) {
  const QExprOperatorMetadataRegistry& op_registry =
      QExprOperatorMetadataRegistry::GetInstance();
  ASSIGN_OR_RETURN((auto [new_expr, side_output_names]),
                   Preprocess(op_registry, expr));
  absl::flat_hash_map<Fingerprint, QTypePtr> node_qtypes;
  ASSIGN_OR_RETURN(new_expr, expr::eval_internal::ExtractQTypesForCompilation(
                                 new_expr, &node_qtypes));
  ASSIGN_OR_RETURN(auto graph_exprs, BuildEvalCfg(new_expr));
  const auto& [graph, exprs] = graph_exprs;
  Codegen codegen(op_registry, *graph, exprs, node_qtypes, side_output_names,
                  /*inputs_are_cheap_to_read=*/inputs_are_cheap_to_read);
  return codegen.Process();
}

}  // namespace arolla::codegen
