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
#ifndef AROLLA_ALGORITHM_CONTROL_FLOW_GRAPH_H_
#define AROLLA_ALGORITHM_CONTROL_FLOW_GRAPH_H_

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl//container/flat_hash_set.h"
#include "absl//status/statusor.h"
#include "absl//types/span.h"

namespace arolla {

// Control-flow graph without cycles.
// https://en.wikipedia.org/wiki/Control-flow_graph
// Entry node id is equal to 0.
// All edges directed to the larger node id, i.e. for edge A->B, A < B.
// All nodes are reachable from the entry node.
class AcyclicCFG {
 public:
  // Type for graph node id index.
  using NodeId = int64_t;

  // Constructs graph from dependencies. Returns error if the graph is not
  // CFG.
  // Order of deps is preserved.
  static absl::StatusOr<std::unique_ptr<AcyclicCFG>> Create(
      std::vector<std::vector<int64_t>> deps);

  // Returns number of vertices in the graph.
  int64_t num_nodes() const { return deps_.size(); }

  // Returns list of dependencies for the node in original order.
  absl::Span<const NodeId> deps(NodeId id) const { return deps_[id]; }

  // Returns list of reverse dependencies for the node in the increasing order.
  // Node can be listed multiple times in case it has more than one edge to
  // the given node.
  absl::Span<const NodeId> reverse_deps(NodeId id) const {
    return reverse_deps_[id];
  }

 private:
  explicit AcyclicCFG(std::vector<std::vector<NodeId>> deps,
                      std::vector<std::vector<NodeId>> reverse_deps)
      : deps_(std::move(deps)), reverse_deps_(std::move(reverse_deps)) {}

  std::vector<std::vector<NodeId>> deps_;
  std::vector<std::vector<NodeId>> reverse_deps_;
};

// Dominator tree for the given DAG.
// https://en.wikipedia.org/wiki/Dominator_(graph_theory)
class DominatorTree {
 public:
  // Type for graph node id index.
  using NodeId = AcyclicCFG::NodeId;

  explicit DominatorTree(const AcyclicCFG& graph);

  // Returns number of nodes in the tree (and underlying graph).
  int64_t num_nodes() const { return infos_.size(); }

  // Returns dominator for the given node. parent(0) == 0.
  NodeId parent(NodeId node_id) const { return infos_[node_id].parent; }

  // Returns all parents as array.
  std::vector<NodeId> parents() const {
    std::vector<NodeId> result(num_nodes());
    for (NodeId node_id = 0; node_id != num_nodes(); ++node_id) {
      result[node_id] = infos_[node_id].parent;
    }
    return result;
  }

  // Returns distance from 0 to the given id.
  int64_t depth(NodeId node_id) const { return infos_[node_id].depth; }

  // Returns children in the tree in the increasing order.
  // For all vertices in the returned list `node_id` is a dominator.
  absl::Span<const NodeId> children(NodeId node_id) const {
    return infos_[node_id].children;
  }

 private:
  struct Info {
    NodeId parent;
    int64_t depth;
    std::vector<NodeId> children;
  };

  // Finds least common ancestor for the vertices.
  NodeId Lca(NodeId a, NodeId b);
  NodeId Lca(absl::Span<const NodeId> nodes);

  std::vector<Info> infos_;
};

// Transform a graph in the following way:
// 1. Remove all incoming edges to the global nodes
// 2. For each global node add an incoming edge from its dominator.
// Note that dominator tree is not changed by this transformation.
absl::StatusOr<std::unique_ptr<AcyclicCFG>> ExternalizeNodes(
    const AcyclicCFG& graph, const DominatorTree& tree,
    const absl::flat_hash_set<AcyclicCFG::NodeId>& global_nodes = {});

// For each node resulting vector contains true iff node dominance frontier is
// empty. The dominance frontier of a node d is the set of all nodes n such that
// d dominates an immediate predecessor of n, but d does not strictly dominate
// n. It is the set of nodes where d's dominance stops.
// https://en.wikipedia.org/wiki/Dominator_(graph_theory)
// If the dominance frontier of a node d is empty, then all nodes
// reachable from d are only reachable through d.
std::vector<bool> FindVerticesWithEmptyDominanceFrontier(
    const AcyclicCFG& graph, const DominatorTree& tree);

}  // namespace arolla

#endif  // AROLLA_ALGORITHM_CONTROL_FLOW_GRAPH_H_
