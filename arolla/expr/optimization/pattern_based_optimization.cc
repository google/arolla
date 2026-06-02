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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

class CompiledPatternOptimization final : public PeepholeOptimization {
 public:
  CompiledPatternOptimization(
      pattern_based_optimization_internal::PatternPlan pattern_plan,
      PostOrder to_post_order,
      std::vector<std::pair<size_t, size_t>> to_post_order_mem_mapping,
      PatternKey key)
      : pattern_plan_(std::move(pattern_plan)),
        to_post_order_(std::move(to_post_order)),
        to_post_order_mem_mapping_(std::move(to_post_order_mem_mapping)),
        key_(std::move(key)) {}

  std::optional<PatternKey> GetKey() const final { return key_; }

  absl::StatusOr<ExprNodePtr> ApplyToRoot(const ExprNodePtr& root) const final {
    absl::InlinedVector<const ExprNode*, 8> memory;
    memory.resize(pattern_plan_.capacity);
    ASSIGN_OR_RETURN(bool matched, MatchPattern(pattern_plan_, *root,
                                                absl::MakeSpan(memory)));
    if (!matched) {
      return root;
    }
    return pattern_based_optimization_internal::SubstituteNodes(
        to_post_order_, to_post_order_mem_mapping_, memory);
  }

 private:
  pattern_based_optimization_internal::PatternPlan pattern_plan_;
  PostOrder to_post_order_;
  std::vector<std::pair<size_t, size_t>> to_post_order_mem_mapping_;
  PatternKey key_;
};

}  // namespace

absl::StatusOr<std::unique_ptr<PeepholeOptimization> absl_nonnull>
CreatePatternBasedOptimization(
    ExprNodePtr absl_nonnull from, ExprNodePtr absl_nonnull to,
    absl::flat_hash_map<std::string,
                        PeepholeOptimization::NodeMatcher absl_nonnull>
        placeholder_matchers) {
  auto key = PeepholeOptimization::PatternKey(from);
  ASSIGN_OR_RETURN(
      auto prepared_pattern,
      pattern_based_optimization_internal::CompilePattern(
          std::move(from), std::move(to), std::move(placeholder_matchers)));
  return std::make_unique<CompiledPatternOptimization>(
      std::move(prepared_pattern.pattern_plan),
      std::move(prepared_pattern.to_post_order),
      std::move(prepared_pattern.to_post_order_mem_mapping), key);
}

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
    const auto& node = *memory[action.mem_index];
    if (!action.predicate(node)) {
      return false;
    }
  }
  return true;
}

namespace {

// Does basic verification of the pattern.
absl::Status VerifyPattern(
    const ExprNodePtr& from, const ExprNodePtr& to,
    const absl::flat_hash_map<std::string, PeepholeOptimization::NodeMatcher>&
        placeholder_matchers) {
  if (from->is_placeholder()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("from EXPRession is placeholder, which would match "
                        "everything: %s -> %s",
                        ToDebugString(from), ToDebugString(to)));
  }
  if (!GetLeafKeys(from).empty() || !GetLeafKeys(to).empty()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("leaves are not allowed in optimizations: %s -> %s",
                        ToDebugString(from), ToDebugString(to)));
  }
  absl::flat_hash_set<std::string> from_keys_set;
  for (const auto& key : GetPlaceholderKeys(from)) {
    from_keys_set.insert(key);
  }
  std::vector<std::string> unknown_to_keys;
  for (const auto& key : GetPlaceholderKeys(to)) {
    if (!from_keys_set.contains(key)) {
      unknown_to_keys.push_back(key);
    }
  }
  if (!unknown_to_keys.empty()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("unknown placeholder keys in to expression: %s, %s->%s",
                        absl::StrJoin(unknown_to_keys, ","),
                        ToDebugString(from), ToDebugString(to)));
  }
  std::vector<std::string> unknown_matcher_keys;
  for (const auto& [key, _] : placeholder_matchers) {
    if (!from_keys_set.contains(key)) {
      unknown_matcher_keys.push_back(key);
    }
  }
  if (!unknown_matcher_keys.empty()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("unknown placeholder matcher keys: %s, %s->%s",
                        absl::StrJoin(unknown_matcher_keys, ","),
                        ToDebugString(from), ToDebugString(to)));
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<CompilePatternResult> CompilePattern(
    ExprNodePtr absl_nonnull from, ExprNodePtr absl_nonnull to,
    absl::flat_hash_map<std::string,
                        PeepholeOptimization::NodeMatcher absl_nonnull>
        placeholder_matchers) {
  RETURN_IF_ERROR(VerifyPattern(from, to, placeholder_matchers));

  CompilePatternResult result;

  // Map to track fingerprint to mem_index.
  absl::flat_hash_map<Fingerprint, size_t> fingerprint_to_mem_index;

  // Emulates the execution, treating the memory buffer as a "queue".
  std::vector<ExprNodePtr> memory = {from};
  for (size_t mem_index = 0; mem_index < memory.size(); ++mem_index) {
    const auto& node = memory[mem_index];

    Fingerprint fp = node->fingerprint();
    if (auto [it, inserted] = fingerprint_to_mem_index.emplace(fp, mem_index);
        !inserted) {
      // Node already seen in the pattern; add a check that the corresponding
      // subexpressions are the same.
      result.pattern_plan.check_same_fingerprint_as_actions.push_back({
          .mem_index = mem_index,
          .other_mem_index = it->second,
      });
      continue;
    }

    if (node->is_literal()) {
      result.pattern_plan.check_fixed_fingerprint_actions.push_back({
          .mem_index = mem_index,
          .expected_fingerprint = fp,
      });
    } else if (node->is_placeholder()) {
      // Add a predicate check only if a matcher is provided.
      if (auto matcher_it = placeholder_matchers.find(node->placeholder_key());
          matcher_it != placeholder_matchers.end()) {
        result.pattern_plan.check_predicates.push_back({
            .mem_index = mem_index,
            .predicate = std::move(matcher_it->second),
        });
      }
    } else if (node->is_op()) {
      result.pattern_plan.unfold_actions.push_back({
          .mem_index = mem_index,
          .expected_arity = node->node_deps().size(),
          .expected_op_name = std::string(node->op()->display_name()),
      });
      for (const auto& dep : node->node_deps()) {
        memory.push_back(dep);
      }
    } else if (node->is_leaf()) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "leaves are not allowed in optimizations: %s", ToDebugString(node)));
    } else {
      return absl::InvalidArgumentError(absl::StrFormat(
          "unexpected node type in pattern: %s", ToDebugString(node)));
    }
  }

  ASSIGN_OR_RETURN(to, ResolveReferenceToRegisteredOperators(to));
  result.to_post_order = PostOrder(to);

  for (size_t i = 0; i < result.to_post_order.nodes_size(); ++i) {
    const auto& node = result.to_post_order.node(i);
    if (node->is_placeholder()) {
      auto it = fingerprint_to_mem_index.find(node->fingerprint());
      if (it == fingerprint_to_mem_index.end()) {
        return absl::FailedPreconditionError(
            absl::StrFormat("unknown placeholder key in `to` expression: %s",
                            node->placeholder_key()));
      }
      result.to_post_order_mem_mapping.emplace_back(i, it->second);
    }
  }

  result.pattern_plan.capacity = memory.size();
  return result;
}

}  // namespace pattern_based_optimization_internal
}  // namespace arolla::expr
