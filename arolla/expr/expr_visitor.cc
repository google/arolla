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

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status.h"

namespace arolla::expr {
namespace {

template <class PrevisitFn, class PostVisitFn>
void VisitorOrderImpl(const ExprNodePtr absl_nonnull& root,
                      PrevisitFn previsit_fn, PostVisitFn postvisit_fn) {
  constexpr size_t kReservedSize = 32;
  struct Frame {
    const ExprNodePtr absl_nonnull& node;
    size_t processed_deps_count = 0;
  };
  absl::flat_hash_set<Fingerprint> visited;
  visited.reserve(kReservedSize);
  visited.insert(root->fingerprint());
  std::vector<Frame> stack;
  stack.reserve(kReservedSize);
  stack.push_back(Frame{root});
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

std::vector<ExprNodePtr absl_nonnull> VisitorOrder(  // clang-format hint
    ExprNodePtr absl_nonnull root) {
  std::vector<ExprNodePtr> res_visits;
  VisitorOrderImpl(
      root, [](auto) {},
      [&res_visits](const ExprNodePtr absl_nonnull& node) {
        res_visits.push_back(node);
      });
  return res_visits;
}

std::vector<std::pair<bool, ExprNodePtr>> PreAndPostVisitorOrder(
    ExprNodePtr absl_nonnull root) {
  std::vector<std::pair<bool, ExprNodePtr>> res_visits;
  VisitorOrderImpl(
      root,
      [&res_visits](const ExprNodePtr absl_nonnull& node) {
        res_visits.emplace_back(true, node);
      },
      [&res_visits](const ExprNodePtr absl_nonnull& node) {
        res_visits.emplace_back(false, node);
      });
  return res_visits;
}

PostOrder::PostOrder(const ExprNodePtr absl_nonnull& root) {
  constexpr size_t kReservedSize = 32;
  std::vector<const ExprNode* absl_nonnull> nodes;
  absl::flat_hash_map<Fingerprint, size_t> node_indices;
  nodes.reserve(kReservedSize);
  node_indices.reserve(kReservedSize);

  size_t total_arc_count = 0;

  {  // Initialize nodes.
    struct Frame {
      const ExprNode* absl_nonnull node;
      size_t dep_idx;
    };
    std::vector<Frame> stack;
    stack.reserve(kReservedSize);
    Frame* absl_nonnull frame = &stack.emplace_back(Frame{root.get()});
    size_t frame_dep_idx = 0;
    for (;;) {
      auto deps = absl::MakeConstSpan(frame->node->node_deps());
      while (frame_dep_idx < deps.size() &&
             node_indices.contains(deps[frame_dep_idx]->fingerprint())) {
        ++frame_dep_idx;
      }
      if (frame_dep_idx < deps.size()) {
        // Recursive call to process a dependency of the current node.
        frame->dep_idx = frame_dep_idx + 1;
        frame = &stack.emplace_back(Frame{deps[frame_dep_idx].get()});
        frame_dep_idx = 0;
        continue;
      }
      // Finish recursive calls, and return to the caller.
      total_arc_count += deps.size();
      node_indices.emplace(frame->node->fingerprint(), nodes.size());
      nodes.push_back(frame->node);
      stack.pop_back();
      if (stack.empty()) [[unlikely]] {
        break;
      }
      frame = &stack.back();
      frame_dep_idx = frame->dep_idx;
    }
  }

  auto adjacency_array =
      std::make_unique<size_t[]>(nodes.size() + 1 + total_arc_count);
  {  // Initialize adjacency_array.
    size_t i = 0;
    size_t j = nodes.size() + 1;
    while (i < nodes.size()) {
      adjacency_array[i] = j;
      for (auto& dep : nodes[i++]->node_deps()) {
        adjacency_array[j++] = node_indices.find(dep->fingerprint())->second;
      }
    }
    adjacency_array[nodes.size()] = j;
  }

  root_ = root;
  nodes_holder_ = std::move(nodes);
  static_assert(sizeof(ExprNodePtr) == sizeof(const ExprNode*));
  static_assert(alignof(ExprNodePtr) == alignof(const ExprNode*));
  nodes_ =
      absl::MakeSpan(reinterpret_cast<const ExprNodePtr*>(nodes_holder_.data()),
                     nodes_holder_.size());
  adjacency_array_ = std::move(adjacency_array);
  node_indices_ = std::move(node_indices);
}

absl::StatusOr<ExprNodePtr> DeepTransform(
    const ExprNodePtr absl_nonnull& root,
    absl::FunctionRef<absl::StatusOr<ExprNodePtr>(ExprNodePtr)> transform_fn,
    std::optional<
        absl::FunctionRef<void(const ExprNodePtr absl_nonnull& new_node,
                               const ExprNodePtr absl_nullable& old_node)>>
        log_fn,
    size_t processed_node_limit) {
  // This function implements a non-recursive version of the following
  // algorithm:
  //
  //   def deep_transform_impl(node, transform_fn, cache, original_node=None,
  //                           log_fn=None):
  //     log_fn(node, None)
  //
  //     # First stage.
  //     for dep in node.deps:
  //       if dep.fingerprint not in cache:
  //         if original_node is not None:
  //           log_fn(node, original_node)
  //         cache[dep.fingerprint] = None
  //         # Recursive call (A).
  //         deep_transform_impl(dep, transform_fn, cache,
  //                             original_node=original_node)
  //     new_deps = [cache[dep.fingerprint] for dep in cache.deps]
  //     assert all(new_deps)
  //     new_node = with_new_dependencies(node, new_deps)
  //     log_fn(new_node, node)
  //     if (new_node.fingerprint != node.fingerprint
  //         and new_node.fingerprint in cache):
  //       # Return statement (1).
  //       assert cache[new_node.fingerprint] is not None
  //       cache[node.fingerprint] = cache[new_node.fingerprint]
  //       return
  //     transformed_new_node = transform_fn(new_node)
  //     log_fn(transformed_new_node, new_node)
  //     if transformed_new_node.fingerprint == new_node.fingerprint:
  //       # Return statement (2).
  //       cache[node.fingerprint] = new_node
  //       cache[new_node.fingerprint] = new_node
  //       return
  //     if transformed_new_node.fingerprint not in cache:
  //       cache[transformed_new_node.fingerprint] = None
  //       # Recursive call (B).
  //       deep_transform_impl(transformed_new_node, transform_fn, cache,
  //                           original_node=node)
  //     # Second stage.
  //     # Return statement (3).
  //     assert cache[transformed_new_node.fingerprint] is not None
  //     cache[node.fingerprint] = cache[transformed_new_node.fingerprint]
  //     cache[new_node.fingerprint] = cache[transformed_new_node.fingerprint]
  //     return

  constexpr size_t kSkipFirstStage = std::numeric_limits<size_t>::max();

  constexpr auto infinite_loop_error =  // clang-format hint
      [](const ExprNodePtr absl_nonnull& node) {
        return absl::FailedPreconditionError(absl::StrFormat(
            "infinite loop of node transformations containing node %s",
            GetDebugSnippet(node)));
      };

  struct Frame {
    ExprNodePtr absl_nonnull node;
    size_t dep_idx = 0;
    Fingerprint new_node_fingerprint;
    Fingerprint transformed_new_node_fingerprint;
    // The closest transformed node on the current node's ancestor path.
    ExprNodePtr absl_nullable original_node = nullptr;
  };
  absl::flat_hash_map<Fingerprint, ExprNodePtr absl_nullable> cache;
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
      if (frame.dep_idx == 0 && log_fn.has_value()) {
        (*log_fn)(frame.node, nullptr);
      }

      // First stage.
      const auto& deps = frame.node->node_deps();
      while (
          frame.dep_idx < deps.size() &&
          !cache.emplace(deps[frame.dep_idx]->fingerprint(), nullptr).second) {
        ++frame.dep_idx;
      }
      if (frame.dep_idx < deps.size()) {
        // Recursive call (A).
        if (log_fn.has_value() && frame.original_node != nullptr) {
          (*log_fn)(deps[frame.dep_idx], frame.original_node);
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
      if (log_fn.has_value()) {
        (*log_fn)(new_node, frame.node);
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
      if (log_fn.has_value()) {
        (*log_fn)(transformed_new_node, new_node);
      }
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
      stack.emplace(Frame{.node = std::move(transformed_new_node),
                          .original_node = frame.node});
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

std::vector<std::string> GetLeafKeys(const ExprNodePtr absl_nonnull& expr) {
  std::vector<std::string> result;
  VisitorOrderImpl(
      expr,
      [&](const ExprNodePtr absl_nonnull& node) {
        if (node->is_leaf()) {
          result.push_back(node->leaf_key());
        }
      },
      [](const ExprNodePtr absl_nonnull& node) {});
  std::sort(result.begin(), result.end());
  return result;
}

std::vector<std::string> GetPlaceholderKeys(  // clang-format hint
    const ExprNodePtr absl_nonnull& expr) {
  std::vector<std::string> result;
  VisitorOrderImpl(
      expr,
      [&](const ExprNodePtr absl_nonnull& node) {
        if (node->is_placeholder()) {
          result.push_back(node->placeholder_key());
        }
      },
      [](const ExprNodePtr absl_nonnull& node) {});
  std::sort(result.begin(), result.end());
  return result;
}

std::vector<std::string> GetLeafKeys(const PostOrder& post_order) {
  std::vector<std::string> result;
  for (const auto& node : post_order.nodes()) {
    if (node->is_leaf()) {
      result.push_back(node->leaf_key());
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

std::vector<std::string> GetPlaceholderKeys(const PostOrder& post_order) {
  std::vector<std::string> result;
  for (const auto& node : post_order.nodes()) {
    if (node->is_placeholder()) {
      result.push_back(node->placeholder_key());
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

}  // namespace arolla::expr
