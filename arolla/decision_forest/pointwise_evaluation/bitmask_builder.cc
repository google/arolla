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
#include "arolla/decision_forest/pointwise_evaluation/bitmask_builder.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/pointwise_evaluation/bitmask_eval.h"
#include "arolla/decision_forest/pointwise_evaluation/oblivious.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace {

template <class T>
std::vector<T> PermuteVector(const std::vector<int>& ids,
                             const std::vector<T>& v) {
  std::vector<T> result(ids.size());
  for (int i = 0; i < ids.size(); ++i) {
    DCHECK_LT(ids[i], v.size());
    result[i] = v[ids[i]];
  }
  return result;
}

template <typename SplitMeta>
void SortAndDeduplicate(bool increasing, std::vector<SplitMeta>& metas,
                        std::vector<float>& values) {
  DCHECK_EQ(values.size(), metas.size());
  if (values.size() < 2 || values.size() != metas.size()) {
    return;
  }
  std::vector<int> ids(values.size());
  for (int i = 0; i < ids.size(); ++i) ids[i] = i;
  auto key = [&](int id) {
    if (increasing) {
      return std::make_tuple(values[id], metas[id].tree_id);
    } else {
      return std::make_tuple(-values[id], metas[id].tree_id);
    }
  };
  std::sort(ids.begin(), ids.end(),
            [&](int i, int j) { return key(i) < key(j); });
  // Deduplicate equal splits in the same tree.
  size_t out_pos = 0;
  for (size_t i = 1; i < ids.size(); ++i) {
    if (key(ids[out_pos]) == key(ids[i])) {
      // Drop duplicate split in one tree and union their masks.
      metas[ids[out_pos]].mask |= metas[ids[i]].mask;
    } else {
      ids[++out_pos] = ids[i];
    }
  }
  ids.erase(ids.begin() + out_pos + 1, ids.end());
  values = PermuteVector(ids, values);
  metas = PermuteVector(ids, metas);
}

template <typename EqSplits>
void FillValue2Range(EqSplits* eq_splits) {
  if (!eq_splits->values.empty()) {
    int last_pos = 0;
    for (size_t i = 0; i != eq_splits->values.size(); ++i) {
      if (eq_splits->values[i] != eq_splits->values[last_pos]) {
        eq_splits->value2range[eq_splits->values[last_pos]] = {last_pos, i};
        last_pos = i;
      }
    }
    eq_splits->value2range[eq_splits->values[last_pos]] = {
        last_pos, eq_splits->values.size()};
  }
}

template <typename IntervalSplits>
void SortSplits(IntervalSplits* splits) {
  // Left and right splits
  SortAndDeduplicate(false, splits->left_splits.metas,
                     splits->left_splits.thresholds);
  SortAndDeduplicate(true, splits->right_splits.metas,
                     splits->right_splits.thresholds);

  // Equality splits
  SortAndDeduplicate(true, splits->eq_splits.metas, splits->eq_splits.values);
  FillValue2Range(&splits->eq_splits);

  // Range splits
  absl::c_sort(splits->range_splits.range_splits,
               [](const auto& a, const auto& b) { return a.left < b.left; });
}

}  // namespace

bool BitmaskBuilder::IsSplitNodeSupported(const SplitNode& node) {
  return (
      fast_dynamic_downcast_final<const IntervalSplitCondition*>(
          node.condition.get()) ||
      fast_dynamic_downcast_final<const SetOfValuesSplitCondition<int64_t>*>(
          node.condition.get()));
}

void BitmaskBuilder::AddSmallTree(const DecisionTree& tree, int group_id) {
  combined_adjustments_size_ += tree.adjustments.size();
  // It never happens because we check it in forest_evaluator.cc.
  DCHECK_LE(tree.adjustments.size(), kMaxRegionsForBitmask);
  if (tree.adjustments.size() > 32) mask_type_ = MASK64;
  MaskedTree masked_tree{.group_id = group_id, .tag = tree.tag};
  masked_tree.splits.reserve(tree.split_nodes.size());
  masked_tree.adjustments.reserve(tree.adjustments.size());

  // Recursive conversion to MaskedTree.
  // It saves adjustment (if leaf) or MaskedSplit (if split node) for
  // the current node to masked_tree and returns mask of the whole subtree.
  auto dfs_fn = [&tree, &masked_tree](DecisionTreeNodeId node_id,
                                      const auto& dfs_inner) {
    if (node_id.is_leaf()) {
      masked_tree.adjustments.push_back(
          tree.adjustments[node_id.adjustment_index()] * tree.weight);
      return WideMask(1) << (masked_tree.adjustments.size() - 1);
    } else {
      const auto& node = tree.split_nodes[node_id.split_node_index()];
      auto mask_false = dfs_inner(node.child_if_false, dfs_inner);
      auto mask_true = dfs_inner(node.child_if_true, dfs_inner);
      masked_tree.splits.push_back({mask_false, node.condition});
      return mask_false | mask_true;
    }
  };
  auto full_tree_mask = dfs_fn(GetTreeRootId(tree), dfs_fn);
  if (tree.adjustments.size() < sizeof(0ull) * 8) {
    DCHECK_EQ(full_tree_mask, ~(~0ull << tree.adjustments.size()));
  } else {  // Needed because for (x << 64) doesn't work for int64_t.
    DCHECK_EQ(full_tree_mask, ~0ull);
  }
  DCHECK_EQ(tree.adjustments.size(), masked_tree.adjustments.size());
  DCHECK_EQ(tree.split_nodes.size(), masked_tree.splits.size());

  masked_trees_.push_back(std::move(masked_tree));
}

void BitmaskBuilder::AddObliviousTree(ObliviousDecisionTree&& tree,
                                      int group_id) {
  if (tree.layer_splits.size() > 32) mask_type_ = MASK64;
  combined_adjustments_size_ += tree.adjustments.size();
  oblivious_trees_.push_back({group_id, tree.tag, std::move(tree)});
}

template <typename Mask>
struct BitmaskBuilder::SplitsBuildingData {
  using EvalImpl = BitmaskEvalImpl<Mask>;

  // Interval splits grouped by input for convenient construction of SplitsData.
  struct IntervalSplitsGroupedByInput {
    explicit IntervalSplitsGroupedByInput(
        FrameLayout::Slot<OptionalValue<float>> slot)
        : left_splits{.slot = slot},
          right_splits{.slot = slot},
          eq_splits{.slot = slot},
          range_splits{.slot = slot} {}

    typename EvalImpl::LeftOrRightSplits
        left_splits;  // sorted by decreasing right
    typename EvalImpl::LeftOrRightSplits
        right_splits;                             // sorted by increasing left
    typename EvalImpl::EqSplits eq_splits;        // sorted by value
    typename EvalImpl::RangeSplits range_splits;  // sorted by increasing left
  };

  const BitmaskBuilder* builder;
  absl::flat_hash_map<int, IntervalSplitsGroupedByInput>
      interval_splits_grouped_by_input;
  absl::flat_hash_map<int,
                      typename EvalImpl::template SetOfValuesSplits<int64_t>>
      set_of_values_int64_grouped_by_input;

  absl::StatusOr<IntervalSplitsGroupedByInput*> GetIntervalSplitsForInput(
      int input_id) {
    auto& splits_map = interval_splits_grouped_by_input;
    auto it = splits_map.find(input_id);
    if (it != splits_map.end()) return &it->second;
    ASSIGN_OR_RETURN(
        auto slot,
        builder->input_slots_[input_id].ToSlot<OptionalValue<float>>());
    auto new_it =
        splits_map.insert({input_id, IntervalSplitsGroupedByInput(slot)}).first;
    return &new_it->second;
  }

  absl::StatusOr<typename EvalImpl::template SetOfValuesSplits<int64_t>*>
  GetSetOfValuesI64SplitsForInput(int input_id) {
    auto& splits_map = set_of_values_int64_grouped_by_input;
    auto it = splits_map.find(input_id);
    if (it != splits_map.end()) return &it->second;
    ASSIGN_OR_RETURN(
        auto slot,
        builder->input_slots_[input_id].ToSlot<OptionalValue<int64_t>>());
    auto new_it = splits_map.insert({input_id, {.slot = slot}}).first;
    return &new_it->second;
  }

  absl::Status AddSplit(const MaskedSplit& split, int tree_id) {
    typename EvalImpl::SplitMeta split_meta{
        static_cast<Mask>(split.false_branch_mask), tree_id};

    auto interval_split =
        fast_dynamic_downcast_final<const IntervalSplitCondition*>(
            split.condition.get());
    if (interval_split) {
      ASSIGN_OR_RETURN(auto* splits,
                       GetIntervalSplitsForInput(interval_split->input_id()));
      const float inf = std::numeric_limits<float>::infinity();

      // Reserving to save allocations at the beginning of the vector.
      constexpr size_t kReserveSize = 8;

      if (interval_split->left() == -inf) {
        splits->left_splits.Reserve(kReserveSize);
        splits->left_splits.metas.push_back(split_meta);
        splits->left_splits.thresholds.push_back(interval_split->right());
      } else if (interval_split->right() == +inf) {
        splits->right_splits.Reserve(kReserveSize);
        splits->right_splits.metas.push_back(split_meta);
        splits->right_splits.thresholds.push_back(interval_split->left());
      } else if (interval_split->left() == interval_split->right()) {
        splits->eq_splits.Reserve(kReserveSize);
        splits->eq_splits.metas.push_back(split_meta);
        splits->eq_splits.values.push_back(interval_split->left());
      } else {
        splits->range_splits.Reserve(kReserveSize);
        splits->range_splits.range_splits.push_back(
            {split_meta, interval_split->left(), interval_split->right()});
      }
      return absl::OkStatus();
    }

    auto set_of_values_int64 =
        fast_dynamic_downcast_final<const SetOfValuesSplitCondition<int64_t>*>(
            split.condition.get());
    if (set_of_values_int64) {
      ASSIGN_OR_RETURN(auto* splits, GetSetOfValuesI64SplitsForInput(
                                         set_of_values_int64->input_id()));
      for (int64_t v : set_of_values_int64->values()) {
        splits->metas[v].push_back(split_meta);
      }
      if (set_of_values_int64->GetDefaultResultForMissedInput()) {
        splits->metas_with_default_true.push_back(split_meta);
      }
      return absl::OkStatus();
    }

    return absl::InvalidArgumentError(absl::StrCat(
        "SplitCondition not supported: ", split.condition->ToString()));
  }
};

template <typename Mask>
absl::Status BitmaskBuilder::Build_MaskedTree(const MaskedTree& tree,
                                              SplitsBuildingData<Mask>* splits,
                                              BitmaskEvalImpl<Mask>* data) {
  auto tree_id = data->trees_metadata_.size();
  for (const MaskedSplit& split : tree.splits) {
    RETURN_IF_ERROR(splits->AddSplit(split, tree_id));
  }
  data->trees_metadata_.push_back(
      {tree.tag.submodel_id, data->adjustments_.size()});
  data->adjustments_.insert(data->adjustments_.end(), tree.adjustments.begin(),
                            tree.adjustments.end());
  return absl::OkStatus();
}

template <typename Mask>
absl::Status BitmaskBuilder::Build_ObliviousTree(
    const ObliviousWithGroupId& tree, SplitsBuildingData<Mask>* splits,
    BitmaskEvalImpl<Mask>* data) {
  auto tree_id = data->trees_metadata_.size();
  // The tree is oblivious, so adjustments.size() is 2**depth.
  Mask mask = tree.tree.adjustments.size();
  for (const auto& condition : tree.tree.layer_splits) {
    mask >>= 1;
    RETURN_IF_ERROR(splits->AddSplit({mask, condition}, tree_id));
  }
  data->trees_metadata_.push_back(
      {tree.tree.tag.submodel_id, data->adjustments_.size()});
  data->adjustments_.insert(data->adjustments_.end(),
                            tree.tree.adjustments.begin(),
                            tree.tree.adjustments.end());
  return absl::OkStatus();
}

void BitmaskBuilder::SortTreesByGroupAndSubmodel() {
  auto comp_fn = [](const auto& a, const auto& b) {
    if (a.group_id == b.group_id) {
      return a.tag.submodel_id < b.tag.submodel_id;
    } else {
      return a.group_id < b.group_id;
    }
  };
  absl::c_sort(masked_trees_, comp_fn);
  absl::c_sort(oblivious_trees_, comp_fn);
}

template <typename Mask>
absl::StatusOr<std::unique_ptr<BitmaskEval>> BitmaskBuilder::BuildImpl() {
  SplitsBuildingData<Mask> splits{this};

  auto data = absl::WrapUnique(new BitmaskEvalImpl<Mask>());
  data->trees_metadata_.reserve(masked_trees_.size() + oblivious_trees_.size());
  data->adjustments_.reserve(combined_adjustments_size_);

  SortTreesByGroupAndSubmodel();
  auto masked_iter = masked_trees_.begin();
  auto oblivious_iter = oblivious_trees_.begin();
  for (int group_id = 0; group_id < output_slots_.size(); ++group_id) {
    typename BitmaskEvalImpl<Mask>::GroupMetadata group{
        .output_slot = output_slots_[group_id]};
    group.regular_tree_range.first = data->trees_metadata_.size();
    while (masked_iter != masked_trees_.end() &&
           masked_iter->group_id == group_id) {
      RETURN_IF_ERROR(Build_MaskedTree(*masked_iter++, &splits, data.get()));
    }
    group.regular_tree_range.second = data->trees_metadata_.size();
    group.oblivious_tree_range.first = data->trees_metadata_.size();
    while (oblivious_iter != oblivious_trees_.end() &&
           oblivious_iter->group_id == group_id) {
      RETURN_IF_ERROR(
          Build_ObliviousTree(*oblivious_iter++, &splits, data.get()));
    }
    group.oblivious_tree_range.second = data->trees_metadata_.size();
    data->groups_.push_back(group);
  }

  for (auto& [_, splits_per_input] : splits.interval_splits_grouped_by_input) {
    SortSplits(&splits_per_input);
    if (!splits_per_input.left_splits.thresholds.empty()) {
      data->splits_.left_splits_grouped_by_input.push_back(
          std::move(splits_per_input.left_splits));
    }
    if (!splits_per_input.right_splits.thresholds.empty()) {
      data->splits_.right_splits_grouped_by_input.push_back(
          std::move(splits_per_input.right_splits));
    }
    if (!splits_per_input.eq_splits.value2range.empty()) {
      data->splits_.eq_splits_grouped_by_input.push_back(
          std::move(splits_per_input.eq_splits));
    }
    if (!splits_per_input.range_splits.range_splits.empty()) {
      data->splits_.range_splits_grouped_by_input.push_back(
          std::move(splits_per_input.range_splits));
    }
  }
  for (auto& [_, splits_per_input] :
       splits.set_of_values_int64_grouped_by_input) {
    data->splits_.set_of_values_int64_grouped_by_input.push_back(
        std::move(splits_per_input));
  }

  return std::unique_ptr<BitmaskEval>(std::move(data));
}

absl::StatusOr<std::unique_ptr<BitmaskEval>> BitmaskBuilder::Build() && {
  if (masked_trees_.empty() && oblivious_trees_.empty()) {
    return nullptr;
  } else if (mask_type_ == MASK32) {
    return BuildImpl<uint32_t>();
  } else {
    return BuildImpl<uint64_t>();
  }
}

}  // namespace arolla
