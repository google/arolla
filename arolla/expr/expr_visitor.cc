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
#include "arolla/expr/expr_visitor.h"

#include <cstddef>
#include <limits>
#include <optional>
#include <stack>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

template <class PrevisitFn, class PostVisitFn>
void VisitorOrderImpl(const ExprNodePtr& root, PrevisitFn previsit_fn,
                      PostVisitFn postvisit_fn) {
  struct Frame {
    const ExprNodePtr& node;
    size_t processed_deps_count = 0;
  };
  absl::flat_hash_set<Fingerprint> visited = {root->fingerprint()};
  std::vector<Frame> stack = {Frame{root}};
  while (!stack.empty()) {
    auto& frame = stack.back();
    if (frame.processed_deps_count == 0) {
      previsit_fn(frame.node);
    }
    const auto& node_deps = frame.node->node_deps();
    if (frame.processed_deps_count == node_deps.size()) {
      postvisit_fn(frame.node);
      stack.pop_back();
      continue;
    }
    const auto& dep = node_deps[frame.processed_deps_count++];
    if (visited.insert(dep->fingerprint()).second) {
      stack.push_back(Frame{dep});
    }
  }
}

}  // namespace

std::vector<ExprNodePtr> VisitorOrder(ExprNodePtr root) {
  std::vector<ExprNodePtr> res_visits;
  VisitorOrderImpl(
      root, [](auto) {},
      [&res_visits](const auto& node) { res_visits.push_back(node); });
  return res_visits;
}

std::vector<std::pair<bool, ExprNodePtr>> PreAndPostVisitorOrder(
    ExprNodePtr root) {
  std::vector<std::pair<bool, ExprNodePtr>> res_visits;
  VisitorOrderImpl(
      root,
      [&res_visits](const auto& node) { res_visits.emplace_back(true, node); },
      [&res_visits](const auto& node) {
        res_visits.emplace_back(false, node);
      });
  return res_visits;
}

PostOrder::PostOrder(const ExprNodePtr& root) {
  struct Frame {
    const ExprNodePtr& node;
    size_t dep_idx = 0;
  };
  absl::flat_hash_map<Fingerprint, size_t> node_indices;
  {  // Initialize nodes_.
    std::vector<Frame> stack;
    stack.push_back(Frame{root});
    while (!stack.empty()) {
      auto& frame = stack.back();
      const auto& deps = frame.node->node_deps();
      while (frame.dep_idx < deps.size() &&
             node_indices.contains(deps[frame.dep_idx]->fingerprint())) {
        ++frame.dep_idx;
      }
      if (frame.dep_idx < deps.size()) {
        stack.push_back(Frame{deps[frame.dep_idx++]});
      } else {
        node_indices.emplace(frame.node->fingerprint(), nodes_.size());
        nodes_.push_back(frame.node);
        stack.pop_back();
      }
    }
  }
  {  // Initialize adjacency_array_.
    size_t total_arc_count = 0;
    for (const auto& node : nodes_) {
      total_arc_count += node->node_deps().size();
    }
    adjacency_array_.resize(nodes_.size() + 1 + total_arc_count);
    size_t i = 0;
    size_t j = nodes_.size() + 1;
    while (i < nodes_.size()) {
      adjacency_array_[i] = j;
      for (const auto& dep : nodes_[i++]->node_deps()) {
        adjacency_array_[j++] = node_indices.at(dep->fingerprint());
      }
    }
    adjacency_array_[nodes_.size()] = j;
  }
}

absl::StatusOr<ExprNodePtr> DeepTransform(
    const ExprNodePtr& root,
    absl::FunctionRef<absl::StatusOr<ExprNodePtr>(ExprNodePtr)> transform_fn,
    std::optional<LogTransformationFn> log_transformation_fn,
    size_t processed_node_limit) {
  // This function implements a non-recursive version of the following
  // algorithm:
  //
  //   def deep_transform_impl(node, transform_fn, cache, original_node=None,
  //                           log_transformation_fn=None):
  //     # First stage.
  //     for dep in node.deps:
  //       if dep.fingerprint not in cache:
  //         if original_node is not None:
  //           log_transformation_fn(node, original_node,
  //                                 kNewChildAfterTransformation)
  //         cache[dep.fingerprint] = None
  //         # Recursive call (A).
  //         deep_transform_impl(dep, transform_fn, cache,
  //                             original_node=original_node)
  //     new_deps = [cache[dep.fingerprint] for dep in cache.deps]
  //     assert all(new_deps)
  //     new_node = with_new_dependencies(node, new_deps)
  //     log_transformation_fn(new_node, node, kWithNewDeps)
  //     if (new_node.fingerprint != node.fingerprint
  //         and new_node.fingerprint in cache):
  //       # Return statement (1).
  //       assert cache[new_node.fingerprint] is not None
  //       cache[node.fingerprint] = cache[new_node.fingerprint]
  //       return
  //     transformed_new_node = transform_fn(new_node)
  //     if transformed_new_node.fingerprint == new_node.fingerprint:
  //       # Return statement (2).
  //       cache[node.fingerprint] = new_node
  //       cache[new_node.fingerprint] = new_node
  //       return
  //     if transformed_new_node.fingerprint not in cache:
  //       cache[transformed_new_node.fingerprint] = None
  //       # Recursive call (B).
  //       deep_transform_impl(transformed_new_node, transform_fn, cache,
  //                           original_node=new_node)
  //     # Second stage.
  //     # Return statement (3).
  //     assert cache[transformed_new_node.fingerprint] is not None
  //     cache[node.fingerprint] = cache[transformed_new_node.fingerprint]
  //     cache[new_node.fingerprint] = cache[transformed_new_node.fingerprint]
  //     return

  constexpr size_t kSkipFirstStage = std::numeric_limits<size_t>::max();

  constexpr auto infinite_loop_error = [](const ExprNodePtr& node) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "infinite loop of node transformations containing node %s",
        GetDebugSnippet(node)));
  };

  struct Frame {
    ExprNodePtr node;
    size_t dep_idx = 0;
    Fingerprint new_node_fingerprint;
    Fingerprint transformed_new_node_fingerprint;
    // The closest transformed node on the current node's ancestor path.
    std::optional<ExprNodePtr> original_node = std::nullopt;
  };
  absl::flat_hash_map<Fingerprint, ExprNodePtr> cache;
  std::stack<Frame> stack;
  cache.emplace(root->fingerprint(), nullptr);
  stack.emplace(Frame{.node = root});
  while (!stack.empty()) {
    auto& frame = stack.top();
    if (cache.size() > processed_node_limit) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "too many processed nodes (%i), this probably means an infinite "
          "transformation. Possibly caused by node %s",
          cache.size(), GetDebugSnippet(frame.node)));
    }
    if (frame.dep_idx != kSkipFirstStage) {
      // First stage.
      const auto& deps = frame.node->node_deps();
      while (
          frame.dep_idx < deps.size() &&
          !cache.emplace(deps[frame.dep_idx]->fingerprint(), nullptr).second) {
        ++frame.dep_idx;
      }
      if (frame.dep_idx < deps.size()) {
        // Recursive call (A).
        if (log_transformation_fn.has_value() &&
            frame.original_node != std::nullopt) {
          (*log_transformation_fn)(
              deps[frame.dep_idx], frame.node,
              DeepTransformStage::kNewChildAfterTransformation);
        }
        stack.emplace(Frame{.node = deps[frame.dep_idx++],
                            .original_node = frame.original_node});
        continue;
      }
      std::vector<ExprNodePtr> new_deps(deps.size());
      for (size_t i = 0; i < deps.size(); ++i) {
        new_deps[i] = cache[deps[i]->fingerprint()];
        if (new_deps[i] == nullptr) {
          return infinite_loop_error(frame.node);
        }
      }
      ASSIGN_OR_RETURN(auto new_node,
                       WithNewDependencies(frame.node, std::move(new_deps)));
      if (log_transformation_fn.has_value()) {
        (*log_transformation_fn)(new_node, frame.node,
                                 DeepTransformStage::kWithNewDeps);
      }
      if (new_node->fingerprint() != frame.node->fingerprint()) {
        if (auto [it, miss] = cache.emplace(new_node->fingerprint(), nullptr);
            !miss) {
          // Return statement (1).
          if (it->second == nullptr) {
            return infinite_loop_error(frame.node);
          }
          cache[frame.node->fingerprint()] = it->second;
          stack.pop();
          continue;
        }
      }
      ASSIGN_OR_RETURN(auto transformed_new_node, transform_fn(new_node),
                       WithNote(_, absl::StrCat("While transforming ",
                                                GetDebugSnippet(frame.node))));
      DCHECK_NE(transformed_new_node, nullptr);
      if (transformed_new_node->fingerprint() == new_node->fingerprint()) {
        // Return statement (2).
        cache[frame.node->fingerprint()] = std::move(transformed_new_node);
        if (new_node->fingerprint() != frame.node->fingerprint()) {
          cache[new_node->fingerprint()] = std::move(new_node);
        }
        stack.pop();
        continue;
      }
      if (auto [it, miss] =
              cache.emplace(transformed_new_node->fingerprint(), nullptr);
          !miss) {
        // The early case of return statement (3), when transformed_new_node is
        // already in the cache, and no recursive call (B) needed.
        if (it->second == nullptr) {
          return infinite_loop_error(frame.node);
        }
        cache[frame.node->fingerprint()] = it->second;
        if (new_node->fingerprint() != frame.node->fingerprint()) {
          cache[new_node->fingerprint()] = it->second;
        }
        stack.pop();
        continue;
      }
      frame.dep_idx = kSkipFirstStage;
      frame.new_node_fingerprint = new_node->fingerprint();
      frame.transformed_new_node_fingerprint =
          transformed_new_node->fingerprint();
      // Recursive call (B).
      stack.emplace(Frame{.node = transformed_new_node,
                          .original_node = transformed_new_node});
      continue;
    }
    // Second stage.
    // Return case (3), after the recursive call (B).
    const auto& node_result = cache.at(frame.transformed_new_node_fingerprint);
    DCHECK_NE(node_result, nullptr);
    cache[frame.node->fingerprint()] = node_result;
    if (frame.new_node_fingerprint != frame.node->fingerprint()) {
      cache[frame.new_node_fingerprint] = node_result;
    }
    stack.pop();
  }
  auto& root_result = cache.at(root->fingerprint());
  DCHECK_NE(root_result, nullptr);
  return std::move(root_result);
}

}  // namespace arolla::expr
