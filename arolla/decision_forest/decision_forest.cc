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
#include "arolla/decision_forest/decision_forest.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/log/check.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

using NodeId = DecisionTreeNodeId;

float DecisionForestNaiveEvaluation(const DecisionForest& forest,
                                    const ConstFramePtr ctx,
                                    absl::Span<const TypedSlot> inputs,
                                    const TreeFilter& filter) {
  DCHECK_OK(forest.ValidateInputSlots(inputs));
  double res = 0;
  for (const auto& tree : forest.GetTrees()) {
    if (!filter(tree.tag)) continue;
    NodeId node_id = GetTreeRootId(tree);
    while (!node_id.is_leaf()) {
      DCHECK(node_id.split_node_index() >= 0 &&
             node_id.split_node_index() < tree.split_nodes.size());
      const auto& node = tree.split_nodes[node_id.split_node_index()];
      if (node.condition->EvaluateCondition(ctx, inputs)) {
        node_id = node.child_if_true;
      } else {
        node_id = node.child_if_false;
      }
    }
    DCHECK(node_id.adjustment_index() >= 0 &&
           node_id.adjustment_index() < tree.adjustments.size());
    res += tree.adjustments[node_id.adjustment_index()] * tree.weight;
  }
  return res;
}

namespace {

std::string NodeIdToString(DecisionTreeNodeId id) {
  if (id.is_leaf()) {
    return absl::StrFormat("adjustments[%d]", id.adjustment_index());
  } else {
    return absl::StrFormat("goto %d", id.split_node_index());
  }
}

}  // namespace

std::string ToDebugString(const DecisionTree& tree) {
  std::string res = "  DecisionTree {\n";
  absl::StrAppendFormat(&res, "    tag { step: %d   submodel_id: %d }\n",
                        tree.tag.step, tree.tag.submodel_id);
  absl::StrAppendFormat(&res, "    weight: %f\n", tree.weight);
  absl::StrAppend(&res, "    split_nodes {\n");
  for (size_t i = 0; i < tree.split_nodes.size(); ++i) {
    const SplitNode& node = tree.split_nodes[i];
    absl::StrAppendFormat(&res, "      %d: IF %s THEN %s ELSE %s\n", i,
                          node.condition->ToString(),
                          NodeIdToString(node.child_if_true),
                          NodeIdToString(node.child_if_false));
  }
  absl::StrAppend(&res, "    }\n");
  absl::StrAppend(&res, "    adjustments:");
  for (float adj : tree.adjustments) {
    absl::StrAppendFormat(&res, " %f", adj);
  }
  absl::StrAppend(&res, "\n  }");
  return res;
}

std::string ToDebugString(const DecisionForest& forest) {
  std::string res = "DecisionForest {\n";
  auto required_qtypes = forest.GetRequiredQTypes();
  for (const auto& [k, v] : std::map<int, QTypePtr>(required_qtypes.begin(),
                                                    required_qtypes.end())) {
    absl::StrAppendFormat(&res, "  input #%d: %s\n", k, v->name());
  }
  for (const auto& tree : forest.GetTrees()) {
    absl::StrAppend(&res, ToDebugString(tree), "\n");
  }
  absl::StrAppend(&res, "}");
  return res;
}

absl::StatusOr<std::unique_ptr<DecisionForest>> DecisionForest::FromTrees(
    std::vector<DecisionTree>&& trees) {
  auto forest = absl::WrapUnique(new DecisionForest(std::move(trees)));
  RETURN_IF_ERROR(forest->Initialize());
  return forest;
}

absl::Status DecisionForest::ValidateInputSlots(
    absl::Span<const TypedSlot> input_slots) const {
  for (const auto& kv : required_qtypes_) {
    if (kv.first >= input_slots.size()) {
      return absl::InvalidArgumentError("not enough arguments");
    }
    if (input_slots[kv.first].GetType() != kv.second) {
      return absl::InvalidArgumentError("type mismatch");
    }
  }
  return absl::OkStatus();
}

absl::Status DecisionForest::Initialize() {
  FingerprintHasher hasher("::arolla::DecisionForest");
  hasher.Combine(trees_.size());
  submodel_count_ = 0;
  step_count_ = 0;
  for (const auto& tree : trees_) {
    hasher.CombineSpan(tree.split_nodes)
        .CombineSpan(tree.adjustments)
        .Combine(tree.weight, tree.tag.step, tree.tag.submodel_id);
    if (tree.tag.submodel_id < 0) {
      return absl::InvalidArgumentError("submodel_id can not be negative");
    }
    if (tree.tag.step < 0) {
      return absl::InvalidArgumentError("step can not be negative");
    }
    submodel_count_ = std::max(submodel_count_, tree.tag.submodel_id + 1);
    step_count_ = std::max(step_count_, tree.tag.step + 1);
    if (tree.split_nodes.size() + 1 != tree.adjustments.size()) {
      return absl::InvalidArgumentError("incorrect number of regions");
    }
    for (const auto& node : tree.split_nodes) {
      bool is_correct = true;
      DecisionTreeNodeId child = node.child_if_false;
      if (child.is_leaf()) {
        is_correct &= child.adjustment_index() < tree.adjustments.size();
      } else {
        is_correct &= child.split_node_index() < tree.split_nodes.size();
      }
      child = node.child_if_true;
      if (child.is_leaf()) {
        is_correct &= child.adjustment_index() < tree.adjustments.size();
      } else {
        is_correct &= child.split_node_index() < tree.split_nodes.size();
      }
      if (!is_correct)
        return absl::InvalidArgumentError("incorrect split node");
      for (auto id_type : node.condition->GetInputSignatures()) {
        auto it = required_qtypes_.emplace(id_type.id, id_type.type);
        if (it.first->second != id_type.type) {
          return absl::InvalidArgumentError(
              "types mismatch in decision forest");
        }
      }
    }
  }
  // required_qtypes_ is not used for fingerprint because it is calculated
  // from trees_ during initialization.
  fingerprint_ = std::move(hasher).Finish();
  return absl::OkStatus();
}

void FingerprintHasherTraits<SplitNode>::operator()(
    FingerprintHasher* hasher, const SplitNode& value) const {
  hasher->Combine(value.child_if_false.raw_index(),
                  value.child_if_true.raw_index());
  value.condition->CombineToFingerprintHasher(hasher);
}

void FingerprintHasherTraits<TreeFilter>::operator()(
    FingerprintHasher* hasher, const TreeFilter& value) const {
  std::vector<int> submodels(value.submodels.begin(), value.submodels.end());
  absl::c_sort(submodels);
  hasher->Combine(value.step_range_from, value.step_range_to)
      .CombineSpan(submodels);
}

void FingerprintHasherTraits<DecisionForestPtr>::operator()(
    FingerprintHasher* hasher, const DecisionForestPtr& value) const {
  hasher->Combine(value->fingerprint());
}

AROLLA_DEFINE_SIMPLE_QTYPE(DECISION_FOREST, DecisionForestPtr);
AROLLA_DEFINE_SIMPLE_QTYPE(TREE_FILTER, TreeFilter);

}  // namespace arolla
