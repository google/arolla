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
#include "arolla/expr/optimization/pattern_based_optimization.h"

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/base/nullability.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace pattern_based_optimization_internal {

absl::StatusOr<ExprNodePtr absl_nonnull> SubstituteNodes(
    const PostOrder& post_order,
    absl::Span<const std::pair<size_t, size_t>> post_order_mem_mapping,
    absl::Span<const ExprNode* const> memory) {
  absl::InlinedVector<ExprNodePtr, 8> results(post_order.nodes_size());
  for (const auto& [to_index, memory_index] : post_order_mem_mapping) {
    results[to_index] = ExprNodePtr::NewRef(memory[memory_index]);
  }
  for (size_t i = 0; i < post_order.nodes_size(); ++i) {
    if (results[i] != nullptr) {
      continue;
    }
    const auto& node = post_order.node(i);
    const auto& dep_indices = post_order.dep_indices(i);
    bool has_modified_dep = absl::c_any_of(
        dep_indices, [&](size_t k) { return results[k] != nullptr; });
    if (!has_modified_dep) {
      continue;
    }
    const auto& deps = node->node_deps();
    std::vector<ExprNodePtr> new_deps(dep_indices.size());
    for (size_t j = 0; j < dep_indices.size(); ++j) {
      const size_t k = dep_indices[j];
      if (results[k] != nullptr) {
        new_deps[j] = results[k];
      } else {
        new_deps[j] = deps[j];
      }
    }
    ASSIGN_OR_RETURN(results[i], MakeOpNode(node->op(), std::move(new_deps)));
  }
  if (results.back() == nullptr) {
    return post_order.nodes().back();
  }
  return results.back();
}

absl::StatusOr<bool> MatchPattern(
    const PatternPlan& pattern, const ExprNode& root,
    absl::Span<const ExprNode*> /*mutable*/ memory) {
  if (memory.size() < pattern.capacity) {
    return absl::InvalidArgumentError(
        absl::StrFormat("memory.size() == %d, less than pattern capacity %d",
                        memory.size(), pattern.capacity));
  }
  memory[0] = &root;
  size_t next_mem_index = 1;
  for (const auto& action : pattern.unfold_actions) {
    const auto& node = *memory[action.mem_index];
    const auto& op = node.op();
    const auto& node_deps = node.node_deps();
    if (op == nullptr || node_deps.size() != action.expected_arity ||
        op->display_name() != action.expected_op_name) {
      return false;
    }
#ifndef NDEBUG
    ASSIGN_OR_RETURN(auto decayed_op, DecayRegisteredOperator(op));
    if (!HasBackendExprOperatorTag(decayed_op) &&
        !HasBuiltinExprOperatorTag(decayed_op)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "tried applying a peephole optimization to operator %s."
          " which is neither backend nor builtin. Is"
          " your peephole optimization correct?",
          op->display_name()));
    }
#endif
    for (size_t i = 0; i < node_deps.size(); ++i) {
      memory[next_mem_index + i] = node_deps[i].get();
    }
    next_mem_index += node_deps.size();
  }
  for (const auto& action : pattern.check_fixed_fingerprint_actions) {
    const auto& node = *memory[action.mem_index];
    if (node.fingerprint() != action.expected_fingerprint) {
      return false;
    }
  }
  for (const auto& action : pattern.check_same_fingerprint_as_actions) {
    const auto& node = *memory[action.mem_index];
    const auto& other_node = *memory[action.other_mem_index];
    if (node.fingerprint() != other_node.fingerprint()) {
      return false;
    }
  }
  for (const auto& action : pattern.check_predicates) {
    // TODO: Change NodeMatcher to take the node by reference.
    const auto* node = memory[action.mem_index];
    if (!action.predicate(ExprNodePtr::NewRef(node))) {
      return false;
    }
  }
  return true;
}

}  // namespace pattern_based_optimization_internal
}  // namespace arolla::expr
