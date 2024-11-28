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
#include "arolla/algorithm/control_flow_graph.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl//container/flat_hash_set.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_format.h"
#include "absl//types/span.h"

namespace arolla {

using NodeId = AcyclicCFG::NodeId;

absl::StatusOr<std::unique_ptr<AcyclicCFG>> AcyclicCFG::Create(
    std::vector<std::vector<NodeId>> deps) {
  const int64_t n = deps.size();
  if (n == 0) {
    return absl::FailedPreconditionError("at least one node is expected");
  }
  std::vector<std::vector<int64_t>> reverse_deps(n);
  for (NodeId node = 0; node != n; ++node) {
    for (int64_t dep : deps[node]) {
      if (dep <= node) {
        return absl::FailedPreconditionError(absl::StrFormat(
            "all edges must be directed to the larger index, found %d -> %d",
            node, dep));
      }
      if (dep >= n) {
        return absl::FailedPreconditionError(
            absl::StrFormat("vertex id %d is out of range", dep));
      }
      reverse_deps[dep].push_back(node);
    }
  }
  for (NodeId node = 1; node != n; ++node) {
    if (reverse_deps[node].empty()) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "all vertices must be reachable from root, %d has no reverse deps",
          node));
    }
  }
  return std::unique_ptr<AcyclicCFG>(
      new AcyclicCFG(std::move(deps), std::move(reverse_deps)));
}

DominatorTree::DominatorTree(const AcyclicCFG& graph)
    : infos_(graph.num_nodes()) {
  const int64_t n = graph.num_nodes();
  infos_[0].parent = 0;
  infos_[0].depth = 0;
  for (NodeId node = 1; node != n; ++node) {
    auto& info = infos_[node];
    info.parent = Lca(graph.reverse_deps(node));
    info.depth = depth(info.parent) + 1;
    infos_[info.parent].children.push_back(node);
  }
}

NodeId DominatorTree::Lca(NodeId a, NodeId b) {
  if (depth(a) < depth(b)) {
    using std::swap;  // go/using-std-swap
    swap(a, b);
  }
  while (depth(a) > depth(b)) {
    a = parent(a);
  }
  while (a != b) {
    a = parent(a);
    b = parent(b);
  }
  return a;
}

NodeId DominatorTree::Lca(absl::Span<const NodeId> nodes) {
  auto res = nodes[0];
  for (const auto& node : nodes) {
    res = Lca(res, node);
  }
  return res;
}

absl::StatusOr<std::unique_ptr<AcyclicCFG>> ExternalizeNodes(
    const AcyclicCFG& graph, const DominatorTree& tree,
    const absl::flat_hash_set<NodeId>& global_nodes) {
  const int64_t n = graph.num_nodes();
  std::vector<std::vector<NodeId>> deps(n);
  for (NodeId node_id = 0; node_id < n; ++node_id) {
    if (global_nodes.contains(node_id) && node_id != 0) {
      deps[tree.parent(node_id)].push_back(node_id);
    }
    deps[node_id].reserve(graph.deps(node_id).size());
    for (NodeId dep : graph.deps(node_id)) {
      if (!global_nodes.contains(dep)) {
        deps[node_id].push_back(dep);
      }
    }
  }
  return AcyclicCFG::Create(std::move(deps));
}

std::vector<bool> FindVerticesWithEmptyDominanceFrontier(
    const AcyclicCFG& graph, const DominatorTree& tree) {
  int64_t n = graph.num_nodes();
  // Minimum depth of the dominator over all transitive dependencies.
  // Equal to the depth of the node for the leaves.
  std::vector<int64_t> min_over_deps_dominator_depth(n);
  std::vector<bool> empty_frontier(n);
  for (NodeId node_id = n - 1; node_id >= 0; --node_id) {
    int64_t min_depth = tree.depth(node_id);
    for (NodeId dep : graph.deps(node_id)) {
      min_depth =
          std::min(min_depth, std::min(min_over_deps_dominator_depth[dep],
                                       // depth of a parent
                                       tree.depth(dep) - 1));
    }
    empty_frontier[node_id] = (min_depth == tree.depth(node_id));
    min_over_deps_dominator_depth[node_id] = min_depth;
  }
  return empty_frontier;
}

}  // namespace arolla
