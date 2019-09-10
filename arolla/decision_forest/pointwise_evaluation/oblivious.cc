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
#include "arolla/decision_forest/pointwise_evaluation/oblivious.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/split_condition.h"

namespace arolla {

namespace {

bool IsPowerOf2(size_t x) { return (x & (x - 1)) == 0; }

struct StackEntry {
  DecisionTreeNodeId node_id;
  int depth;
};

// Traverse tree in order node, child_if_true, child_if_false and call
// `callback`.
template <typename CallbackFn>
bool TraverseTree(const DecisionTree& tree, CallbackFn callback) {
  std::vector<StackEntry> stack;
  stack.reserve(32);
  stack.push_back(StackEntry{GetTreeRootId(tree), 0});
  while (!stack.empty()) {
    auto [node_id, depth] = stack.back();
    stack.pop_back();
    if (!callback(node_id, depth)) {
      return false;
    }
    if (!node_id.is_leaf()) {
      const auto& node = tree.split_nodes[node_id.split_node_index()];
      stack.push_back(StackEntry{node.child_if_true, depth + 1});
      stack.push_back(StackEntry{node.child_if_false, depth + 1});
    }
  }
  return true;
}

}  // namespace

std::optional<ObliviousDecisionTree> ToObliviousTree(const DecisionTree& tree) {
  size_t region_count = tree.adjustments.size();
  if (!IsPowerOf2(region_count)) {
    return std::nullopt;
  }
  size_t depth = region_count ? __builtin_ctz(region_count) : 0;
  std::vector<std::shared_ptr<const SplitCondition>> layer_splits;
  layer_splits.reserve(depth);
  std::vector<float> adjustments;
  adjustments.reserve(region_count);

  auto process_node = [&](DecisionTreeNodeId node_id, int current_depth) {
    if (node_id.is_leaf()) {
      if (current_depth != depth) {
        return false;  // not balanced
      }
      adjustments.push_back(tree.adjustments[node_id.adjustment_index()] *
                            tree.weight);
    } else {
      if (current_depth >= depth) {
        return false;  // not balanced
      }
      const auto& node = tree.split_nodes[node_id.split_node_index()];
      if (layer_splits.size() == current_depth) {
        layer_splits.push_back(node.condition);
      } else {
        DCHECK_LT(current_depth, layer_splits.size());
        if (*layer_splits[current_depth] != *node.condition) {
          return false;  // different splits in one layer
        }
      }
    }
    return true;
  };

  if (!TraverseTree(tree, process_node)) {
    return std::nullopt;
  }

  return ObliviousDecisionTree{
    tree.tag, std::move(layer_splits), std::move(adjustments)};
}

}  // namespace arolla
