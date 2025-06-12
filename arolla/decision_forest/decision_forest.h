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
#ifndef AROLLA_DECISION_FOREST_DECISION_FOREST_H_
#define AROLLA_DECISION_FOREST_DECISION_FOREST_H_

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

// This file describes internal representation of decision forest.
//
// DecisionTree is a binary tree that has float adjustment in each leaf and
// a split condition in each inner node. Depending on the result of
// the split condition we go either to "false" (child_if_false) or to "true"
// (child_if_true) branch. Each child is specified by DecisionTreeNodeId.
// DecisionTreeNodeId contains index of the next split node or (if the next node
// is leaf) index of adjustment. For more details see
// DecisionForestNaiveEvaluation in decision_forest.cc as an example.
//
// SplitNode and DecisionTree are passive data structures.
// DecisionForest is an immutable object that contains vector of decision trees
// and does validation checks during construction.
// DecisionForest can be serialized/deserialized to a proto or compiled for
// fast evaluation (see decision_forest/evaluation/).

// Contains either split node id or adjustment (leaf) id.
class DecisionTreeNodeId {
 public:
  static DecisionTreeNodeId SplitNodeId(int64_t split_node_index) {
    DecisionTreeNodeId n;
    n.val_ = split_node_index;
    return n;
  }

  static DecisionTreeNodeId AdjustmentId(int64_t adjustment_index) {
    DecisionTreeNodeId n;
    n.val_ = -adjustment_index - 1;
    return n;
  }

  bool is_leaf() const { return val_ < 0; }

  int64_t split_node_index() const {
    DCHECK(!is_leaf());
    return val_;
  }

  int64_t adjustment_index() const {
    DCHECK(is_leaf());
    return -val_ - 1;
  }

  int64_t raw_index() const { return val_; }

  bool operator==(const DecisionTreeNodeId& other) const {
    return val_ == other.val_;
  }

  bool operator!=(const DecisionTreeNodeId& other) const {
    return val_ != other.val_;
  }

 private:
  int64_t val_ = 0;
};

struct SplitNode {
  DecisionTreeNodeId child_if_false;
  DecisionTreeNodeId child_if_true;
  // SplitCondition is immutable, so it is safe to use shared_ptr.
  std::shared_ptr<const SplitCondition> condition;
};

struct DecisionTree {
  // split_node_index in each node should be within [1, split_nodes.size())
  // (id=0 is root node that doesn't have a parent).
  // adjustment_index in each node should be within [0, adjustments.size()).
  std::vector<SplitNode> split_nodes;
  std::vector<float> adjustments;
  float weight = 1.0f;

  // Optional tag. Filtering by this tag allows to select a subset of trees
  // in DecisionForest. Both step and submodel_id shouldn't be negative.
  struct Tag {
    int step = 0;
    int submodel_id = 0;
  };
  Tag tag;
};

inline DecisionTreeNodeId GetTreeRootId(const DecisionTree& tree) {
  return tree.split_nodes.empty() ? DecisionTreeNodeId::AdjustmentId(0)
                                  : DecisionTreeNodeId::SplitNodeId(0);
}

// Filter is used if partial evaluation of DecisionForest (i.e. only subset
// of trees) is required.
// Default-constructed filter accepts all trees.
struct TreeFilter {
  // Returns true if the tree should be evaluated.
  bool operator()(const DecisionTree::Tag& tree) const {
    return tree.step >= step_range_from &&
           (step_range_to == -1 || tree.step < step_range_to) &&
           (submodels.empty() || submodels.contains(tree.submodel_id));
  }

  int step_range_from = 0;
  int step_range_to = -1;  // -1 means unlimited.
  // Empty set is a special case that means "all submodels".
  absl::flat_hash_set<int> submodels;
};

class DecisionForest {
 public:
  static absl::StatusOr<std::unique_ptr<DecisionForest>> FromTrees(
      std::vector<DecisionTree>&& trees);

  absl::Status ValidateInputSlots(
      absl::Span<const TypedSlot> input_slots) const;

  const absl::flat_hash_map<int, QTypePtr>& GetRequiredQTypes() const {
    return required_qtypes_;
  }

  absl::Span<const DecisionTree> GetTrees() const { return trees_; }
  std::vector<DecisionTree> TreesCopy() const { return trees_; }

  // Returns the number of submodels in the forest
  // (max DecisionTree::Tag::submodel_id + 1).
  int submodel_count() const { return submodel_count_; }

  // Returns the number of steps in the forest
  // (max DecisionTree::Tag::step + 1).
  int step_count() const { return step_count_; }

  Fingerprint fingerprint() const { return fingerprint_; }

 private:
  explicit DecisionForest(std::vector<DecisionTree>&& trees)
      : trees_(std::move(trees)) {}
  absl::Status Initialize();

  std::vector<DecisionTree> trees_;
  absl::flat_hash_map<int, QTypePtr> required_qtypes_;
  Fingerprint fingerprint_;
  int submodel_count_;
  int step_count_;
};

std::string ToDebugString(const DecisionTree& tree);
std::string ToDebugString(const DecisionForest& forest);

// It is a reference non-optimized implementation.
// Shouldn't be used in production. Use ForestEvaluator instead.
float DecisionForestNaiveEvaluation(const DecisionForest& forest,
                                    const ConstFramePtr ctx,
                                    absl::Span<const TypedSlot> inputs,
                                    const TreeFilter& filter = {});

using DecisionForestPtr = std::shared_ptr<const DecisionForest>;

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(SplitNode);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(TreeFilter);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(DecisionForestPtr);

AROLLA_DECLARE_QTYPE(DecisionForestPtr);
AROLLA_DECLARE_QTYPE(TreeFilter);

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_DECISION_FOREST_H_
