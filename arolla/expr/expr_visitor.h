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
#ifndef AROLLA_EXPR_EXPR_VISITOR_H_
#define AROLLA_EXPR_EXPR_VISITOR_H_

#include <cstddef>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/base/attributes.h"
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

// This class represents a list of nodes in the reversed topological order
// and an index of dependencies them.
//
// Note: Only one occurrence of each subexpression is visited. For example, in
// the expression `L.x + L.x`, the node `L.x` is presented in the resulting
// sequence once.
//
class PostOrder {
 public:
  PostOrder() = default;
  explicit PostOrder(const ExprNodePtr& root);

  // Returns nodes in the reversed topological order.
  absl::Span<const ExprNodePtr> nodes() const { return nodes_; }

  // Returns the number of nodes.
  size_t nodes_size() const { return nodes_.size(); }

  // Returns a node by its index.
  const ExprNodePtr& node(size_t node_index) const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return nodes_[node_index];
  }

  // Returns indices of the node dependencies.
  absl::Span<const size_t> dep_indices(size_t node_index) const {
    DCHECK(node_index < nodes_.size());
    return absl::Span<const size_t>(
        adjacency_array_.data() + adjacency_array_[node_index],
        adjacency_array_[node_index + 1] - adjacency_array_[node_index]);
  }

 private:
  // AST nodes in the reversed topological order.
  std::vector<ExprNodePtr> nodes_;

  // Adjacency array for the expression.
  //
  // Let `n` be the number of nodes; the first `n+1` elements of the array store
  // ascending offsets defining `n` slices in the remaining part of the array:
  //
  //   adjacency_array_[adjacency_array_[i] : adjacency_array_[i+1]]
  //   for 0 <= i < n
  //
  // Each slice stores indices of the nodes upon which the given node depends.
  //
  // NOTE: You can also think of adjacency_array_ as a concatenation of
  // adjacency_list_offsets and adjacency_lists.
  std::vector<size_t> adjacency_array_;
};

// Creates a queue for visiting all expression nodes bottom-up.
//
// Note: exact duplicates are ignored. For the expression L.x + L.x the node
// L.x will present in VisitorOrder only once.
//
std::vector<ExprNodePtr> VisitorOrder(ExprNodePtr root);

// Creates a queue for visiting all expression nodes in
// DFS order.
// Each node will be listed twice:
//   * first for previsit with True
//   * second for postvisit with False
// Note: exact duplicates are ignored. For the expression L.x + L.x the node L.x
// will be present in PreAndPostVisitorOrder only twice.
std::vector<std::pair<bool, ExprNodePtr>> PreAndPostVisitorOrder(
    ExprNodePtr root);

template <typename VisitorResultType>
struct ExprVisitorResultTraits;

// Visits an expression tree bottom-up, applying visitor to each node.
//
// Visitor should be a callable type with the following signature:
//
//   (ExprNodePtr node, Span<const T* const> visits) -> T (or StatusOr<T>)
//
// Visitor takes a ExprNodePtr and results of its node_deps (after applying
// Visitor) as inputs and returns a result of type T or StatusOr<T>.
//
// Note: exact duplicates are ignored. For the expression L.x + L.x the node L.x
// will be visited only once.
template <typename Visitor>
auto PostOrderTraverse(const PostOrder& post_order, Visitor visitor) {
  using Traits = ExprVisitorResultTraits<
      typename meta::function_traits<Visitor>::return_type>;
  using T = typename Traits::ResultType;
  static_assert(std::is_invocable_r_v<absl::StatusOr<T>, Visitor, ExprNodePtr,
                                      absl::Span<const T* const>>,
                "Visitor has an unexpected signature.");
  // Cannot use T because std::vector<T> has different behaviour for T=bool.
  struct WrappedT {
    T value;
    WrappedT(T&& value) : value(std::move(value)) {}
    WrappedT(WrappedT&&) = default;
    WrappedT& operator=(WrappedT&&) = default;
  };
  std::vector<WrappedT> results;
  results.reserve(post_order.nodes_size());
  std::vector<const T*> args;
  const auto invoke_visitor = [&](size_t node_index) {
    const auto dep_indices = post_order.dep_indices(node_index);
    args.resize(dep_indices.size());
    for (size_t j = 0; j < dep_indices.size(); ++j) {
      args[j] = &results[dep_indices[j]].value;
    }
    return visitor(post_order.node(node_index), absl::MakeConstSpan(args));
  };
  for (size_t i = 0; i + 1 < post_order.nodes_size(); ++i) {
    auto visit_result = invoke_visitor(i);
    if (!Traits::ok(visit_result)) {
      return visit_result;
    }
    results.emplace_back(Traits::value(std::move(visit_result)));
  }
  return invoke_visitor(post_order.nodes_size() - 1);
}

template <typename Visitor>
auto PostOrderTraverse(const ExprNodePtr& root, Visitor visitor) {
  return PostOrderTraverse(PostOrder(root), visitor);
}

// Transforms the expression by applying transform_fn to each expression node.
//
// The nodes are processed in post order, for each call of transform_fn(node) it
// is guaranteed that all the node's deps are already processed and replaced
// with transformed versions.
//
// transform_fn must have one of the following signatures:
//   ExprNodePtr(ExprNodePtr node)
//   absl::StatusOr<ExprNodePtr>(ExprNodePtr node)
// Prefer accepting `node` by value as it can be moved outside when no
// transformation is needed.
//
template <typename TransformFn>
absl::StatusOr<ExprNodePtr> Transform(const ExprNodePtr& root,
                                      TransformFn transform_fn) {
  return TransformOnPostOrder(PostOrder(root), std::move(transform_fn));
}

// Transforms the expression by applying transform_fn to each expression node.
//
// `post_order` must contain nodes of an expression in post-order.
//
// The nodes are processed in post order, for each call of transform_fn(node) it
// is guaranteed that all the node's deps are already processed and replaced
// with transformed versions.
//
// transform_fn must have one of the following signatures:
//   ExprNodePtr(ExprNodePtr node)
//   absl::StatusOr<ExprNodePtr>(ExprNodePtr node)
//
template <typename TransformFn>
absl::StatusOr<ExprNodePtr> TransformOnPostOrder(const PostOrder& post_order,
                                                 TransformFn transform_fn) {
  using Traits = ExprVisitorResultTraits<
      typename meta::function_traits<TransformFn>::return_type>;
  static_assert(std::is_invocable_r_v<absl::StatusOr<ExprNodePtr>, TransformFn,
                                      ExprNodePtr>,
                "TransformFn has an unexpected signature.");
  std::vector<ExprNodePtr> results(post_order.nodes_size());
  for (size_t i = 0; i < post_order.nodes_size(); ++i) {
    const auto& node = post_order.node(i);
    const auto& dep_indices = post_order.dep_indices(i);
    bool has_modified_dep =
        node->is_op() && absl::c_any_of(dep_indices, [&](size_t k) {
          return results[k] != nullptr;
        });
    ExprNodePtr transform_fn_input_node;
    if (has_modified_dep) {
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
      ASSIGN_OR_RETURN(transform_fn_input_node,
                       MakeOpNode(node->op(), std::move(new_deps)),
                       WithNote(_, absl::StrCat("While processing ",
                                                GetDebugSnippet(node), ".")));
    } else {
      transform_fn_input_node = node;
    }
    auto transform_fn_result = transform_fn(std::move(transform_fn_input_node));
    if (!Traits::ok(transform_fn_result)) {
      return transform_fn_result;
    }
    auto new_node = Traits::value(std::move(transform_fn_result));
    if (new_node->fingerprint() != node->fingerprint()) {
      results[i] = Traits::value(std::move(new_node));
    }
  }
  if (results.back() != nullptr) {
    return std::move(results.back());
  } else {
    return post_order.nodes().back();
  }
}

// Called on a pair of nodes when DeepTransform modifies the node invisibly to
// transform_fn.
using LogTransformationFn = absl::FunctionRef<void(
    const ExprNodePtr& new_node, const ExprNodePtr& old_node)>;

// Transforms the expression by applying transform_fn to each expression node
// and each node (including new node_deps) created by transform_fn calls.
//
// Note transform_fn returns a single node which itself can contain new
// node_deps.
//
// The nodes are processed in post order, for each call of transform_fn(node) it
// is guaranteed that all the node's deps are already processed and replaced
// with transformed versions. Next node is processed after the current node
// (including new node_deps) is fully transformed.
//
// For example, here is a sequence of transformations. Note that it shows how
// the algorithm works conceptually but the actual implementation uses two-stage
// processing (see NodeState.ProcessingStage).
//
//          a                       a                       a
//       /  |  \    (b->b1)      /  |  \   (b1->b2)      /  |  \      (c->c1)
//      b   c   d               b1  c   d               b2  c   d
//
//          a                       a                       a
//       /  |  \    (d->d1)      /  |  \   (e->e1)       /  |   \     (d1->d2)
//      b2  c1  d               b2  c1  d1              b2  c1   d1
//                                     /  \                     /  \.
//                                    e    f                   e1   f
//
//          a                       a1
//       /  |  \    (a->a1)      /  |  \.
//      b2  c1  d2              b2  c1  d2
//             / \                     / \.
//            e1  f                   e1  f
//
//
// transform_fn must not cause an infinite chain of transformations (e.g. a ->
// b, b -> c, c -> a) otherwise an error will be returned.
//
// TODO: put the limit back to 1'000'000
absl::StatusOr<ExprNodePtr> DeepTransform(
    const ExprNodePtr& root,
    absl::FunctionRef<absl::StatusOr<ExprNodePtr>(ExprNodePtr)> transform_fn,
    std::optional<LogTransformationFn> log_transformation_fn = std::nullopt,
    size_t processed_node_limit = 10'000'000);

template <typename VisitorResultType>
struct ExprVisitorResultTraits {
  using ResultType = VisitorResultType;
  static constexpr bool ok(const VisitorResultType&) { return true; }
  static constexpr ResultType value(VisitorResultType&& input) { return input; }
};

template <typename T>
struct ExprVisitorResultTraits<absl::StatusOr<T>> {
  using ResultType = T;
  static bool ok(const absl::StatusOr<T>& input) { return input.ok(); }
  static ResultType value(absl::StatusOr<T>&& input) {
    return *std::move(input);
  }
};

// Converts Span<const T* const> to std::vector<T>.
// Visitor function accepts span of pointers in second argument, but for some
// visitors it is more convenient to work with std::vector<T>.
template <typename T>
std::vector<T> DereferenceVisitPointers(absl::Span<const T* const> visits) {
  std::vector<T> res;
  res.reserve(visits.size());
  for (const T* ptr : visits) {
    res.push_back(*ptr);
  }
  return res;
}

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EXPR_VISITOR_H_
