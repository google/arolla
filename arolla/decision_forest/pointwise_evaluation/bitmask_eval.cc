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
#include "arolla/decision_forest/pointwise_evaluation/bitmask_eval.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "absl//base/optimization.h"
#include "absl//container/fixed_array.h"
#include "absl//log/check.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bits.h"

namespace arolla {

namespace {

template <typename Iter, typename TreeMask>
void ApplyMaskForRange(Iter from, Iter to,
                       absl::FixedArray<TreeMask>& tree_masks) {
  // It is performance critical place.
  // Loop with iterators is 30% faster than iterating by index.
  //
  // Manually hoisting the base of the tree_masks array avoids redundant
  // memory loads in this loop, improving performance by ~33%.
  TreeMask* base = tree_masks.data();
  for (Iter it = from; it != to; ++it) {
    auto [it_mask, it_tree_id] = it->GetFieldsWithMinimalLoadInstructions();
    base[it_tree_id] |= it_mask;
  }
}

}  // namespace

template <typename TreeMask>
void BitmaskEvalImpl<TreeMask>::ProcessLeftSplits(
    const LeftOrRightSplits& splits, const ConstFramePtr ctx,
    absl::FixedArray<TreeMask>& tree_masks) const {
  OptionalValue<float> v = ctx.Get(splits.slot);
  if (!v.present || std::isnan(v.value)) return;
  auto new_end =
      std::upper_bound(splits.thresholds.begin(), splits.thresholds.end(),
                       v.value, std::greater<float>());
  ApplyMaskForRange(
      splits.metas.begin(),
      splits.metas.begin() + (new_end - splits.thresholds.begin()), tree_masks);
}

template <typename TreeMask>
void BitmaskEvalImpl<TreeMask>::ProcessRightSplits(
    const LeftOrRightSplits& splits, const ConstFramePtr ctx,
    absl::FixedArray<TreeMask>& tree_masks) const {
  OptionalValue<float> v = ctx.Get(splits.slot);
  if (!v.present || std::isnan(v.value)) return;
  auto new_end =
      std::upper_bound(splits.thresholds.begin(), splits.thresholds.end(),
                       v.value, std::less<float>());
  ApplyMaskForRange(
      splits.metas.begin(),
      splits.metas.begin() + (new_end - splits.thresholds.begin()), tree_masks);
}

template <typename TreeMask>
void BitmaskEvalImpl<TreeMask>::ProcessEqSplits(
    const EqSplits& eq_splits, const ConstFramePtr ctx,
    absl::FixedArray<TreeMask>& tree_masks) const {
  OptionalValue<float> v = ctx.Get(eq_splits.slot);
  if (!v.present || std::isnan(v.value)) return;
  auto range_it = eq_splits.value2range.find(v.value);
  if (range_it != eq_splits.value2range.end()) {
    auto beg_end = range_it->second;
    ApplyMaskForRange(eq_splits.metas.begin() + beg_end.first,
                      eq_splits.metas.begin() + beg_end.second, tree_masks);
  }
}

template <typename TreeMask>
void BitmaskEvalImpl<TreeMask>::ProcessRangeSplits(
    const RangeSplits& splits, const ConstFramePtr ctx,
    absl::FixedArray<TreeMask>& tree_masks) const {
  OptionalValue<float> v = ctx.Get(splits.slot);
  if (!v.present || std::isnan(v.value)) return;
  for (const auto& range_split : splits.range_splits) {
    if (range_split.left > v.value) break;
    if (ABSL_PREDICT_TRUE(v.value <= range_split.right)) {
      tree_masks[range_split.meta.tree_id] |= range_split.meta.mask;
    }
  }
}

template <typename TreeMask>
template <typename T>
void BitmaskEvalImpl<TreeMask>::ProcessSetOfValuesSplits(
    const std::vector<SetOfValuesSplits<T>>& splits, const ConstFramePtr ctx,
    absl::FixedArray<TreeMask>& tree_masks) const {
  for (const auto& splits_grouped_by_input : splits) {
    OptionalValue<T> v = ctx.Get(splits_grouped_by_input.slot);
    if (v.present) {
      auto it = splits_grouped_by_input.metas.find(v.value);
      if (it == splits_grouped_by_input.metas.end()) continue;
      for (const SplitMeta& split : it->second) {
        tree_masks[split.tree_id] |= split.mask;
      }
    } else {
      for (const SplitMeta& split :
           splits_grouped_by_input.metas_with_default_true) {
        tree_masks[split.tree_id] |= split.mask;
      }
    }
  }
}

template <typename TreeMask>
absl::FixedArray<TreeMask> BitmaskEvalImpl<TreeMask>::FindTreeMasks(
    const ConstFramePtr ctx) const {
  absl::FixedArray<TreeMask> tree_masks(trees_metadata_.size(), 0);
  for (const auto& left_splits : splits_.left_splits_grouped_by_input) {
    ProcessLeftSplits(left_splits, ctx, tree_masks);
  }
  for (const auto& right_splits : splits_.right_splits_grouped_by_input) {
    ProcessRightSplits(right_splits, ctx, tree_masks);
  }
  for (const auto& eq_splits : splits_.eq_splits_grouped_by_input) {
    ProcessEqSplits(eq_splits, ctx, tree_masks);
  }
  for (const auto& range_splits : splits_.range_splits_grouped_by_input) {
    ProcessRangeSplits(range_splits, ctx, tree_masks);
  }
  ProcessSetOfValuesSplits<int64_t>(
      splits_.set_of_values_int64_grouped_by_input, ctx, tree_masks);
  return tree_masks;
}

template <typename TreeMask>
template <typename ProcessFn>
void BitmaskEvalImpl<TreeMask>::InternalEval(const ConstFramePtr input_ctx,
                                             FramePtr output_ctx,
                                             ProcessFn process_fn) const {
  absl::FixedArray<TreeMask> tree_masks = FindTreeMasks(input_ctx);
  for (const auto& group : groups_) {
    *output_ctx.GetMutable(group.output_slot) +=
        process_fn(tree_masks, group.regular_tree_range,
                   [](TreeMask mask) {
                     DCHECK_NE(~mask, 0);
                     return FindLSBSetNonZero(~mask);
                   }) +
        process_fn(tree_masks, group.oblivious_tree_range,
                   [](TreeMask mask) { return mask; });
  }
}

template <typename TreeMask>
void BitmaskEvalImpl<TreeMask>::IncrementalEval(const ConstFramePtr input_ctx,
                                                FramePtr output_ctx) const {
  auto process_fn = [&](const absl::FixedArray<TreeMask>& tree_masks,
                        std::pair<int, int> range, auto leaf_id_fn) {
    const auto LoopIter = [&](size_t tree_id, double& accumulator) {
      const auto& tree = trees_metadata_[tree_id];
      auto leaf_id = leaf_id_fn(tree_masks[tree_id]);
      accumulator += adjustments_[tree.adjustments_offset + leaf_id];
    };

    // Accumulate the results into two separate counters. This avoids
    // register data dependencies between the two calculations in the
    // unrolled loop and allows for instruction-level parallelism.
    //
    // NOTE: Even though we are summing floats and the eventual result is a
    // float, accumulating into doubles avoids accumulation of floating point
    // errors.
    double res[2] = {0, 0};
    size_t tree_id = range.first;
    // If we are iterating over an odd number of trees, do the first
    // iteration here, so that we can do the rest unrolled-by-two.
    if ((range.second - range.first) % 2 == 1) {
      LoopIter(tree_id++, res[1]);
    }
    // Manually unroll by two, summing into separate accumulators.
    while (tree_id != range.second) {
      LoopIter(tree_id++, res[0]);
      LoopIter(tree_id++, res[1]);
    }
    return res[0] + res[1];
  };
  InternalEval(input_ctx, output_ctx, process_fn);
}

template class BitmaskEvalImpl<uint32_t>;
template class BitmaskEvalImpl<uint64_t>;

}  // namespace arolla
