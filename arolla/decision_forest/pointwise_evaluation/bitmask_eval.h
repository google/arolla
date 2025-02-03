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
#ifndef AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_BITMASK_EVAL_H_
#define AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_BITMASK_EVAL_H_

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/container/fixed_array.h"
#include "absl/container/flat_hash_map.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"

namespace arolla {

// In bit mask forest evaluation tree is represented as collection of splits.
// We enumerate leaves of the tree by "In-order", where left child is false.
// For each split (internal node) we precompute bit mask of leaves in
// false branch.
// Also see http://dl.acm.org/citation.cfm?id=2766462.2767733&coll=DL&dl=ACM
class BitmaskEval {
 public:
  virtual ~BitmaskEval() = default;

  // Evaluates trees separately for each group and adds the result
  // to corresponding slots in output_ctx.
  virtual void IncrementalEval(ConstFramePtr input_ctx,
                               FramePtr output_ctx) const = 0;
};

template <typename TreeMask>
class BitmaskEvalImpl : public BitmaskEval {
 public:
  static constexpr size_t kMaxRegionsForBitmask = sizeof(TreeMask) * 8;

  // Evaluates trees separately for each group and adds the result
  // to corresponding slots in output_ctx.
  void IncrementalEval(ConstFramePtr input_ctx,
                       FramePtr output_ctx) const final;

 private:
  BitmaskEvalImpl() = default;
  friend class BitmaskBuilder;

  struct SplitMeta {
    // Mask corresponding to the current split.
    // All masks on true condition splits need to be |-ed to identify
    // resulting leaf of the tree.
    // In small depth/regular trees corresponds to mask of leaves in false
    // branch, for deep oblivious trees equals to 2 in a power of layer id.
    TreeMask mask;
    int tree_id;

    // This function implements a microarchitectural optimization targeting
    // x86_64 processors (eg Haswell, Sky Lake, Rome) for the common case when
    // TreeMask is 32-bit and hence SplitMeta is 64-bit.
    //
    // On these processors, there is a "speed limit" of 2 loads and 1 store per
    // clock cycle, regardless of the sizes of those loads. In the hot loop of
    // ApplyMaskForRange(), we need to load both fields of SplitMeta, and the
    // compiler does this by emitting a separate 32-bit load for each field into
    // registers. The loop, however, is bottlenecked by the above speed limit,
    // so we get better performance by forcing a single 64-bit load, copying the
    // result to a second register, and bit-shifting to extract the relevant
    // fields. This results in an extra instruction but drops one load, which
    // results in a net performance improvement from 1.5 cycles/leaf to 1.125
    // cycles/leaf.
    std::pair<TreeMask, int> GetFieldsWithMinimalLoadInstructions() const {
#ifdef __x86_64__
      static constexpr bool kEnable = sizeof(*this) == sizeof(uint64_t);
#else
      static constexpr bool kEnable = false;
#endif
      if constexpr (kEnable) {
        uint64_t mem;
        memcpy(&mem, this, sizeof(mem));
        static_assert(offsetof(SplitMeta, mask) == 0);
        return {static_cast<TreeMask>(mem),
                mem >> (offsetof(SplitMeta, tree_id) * 8)};
      } else {
        return {mask, tree_id};
      }
    }
  };

  struct LeftOrRightSplits {
    FrameLayout::Slot<OptionalValue<float>> slot;
    std::vector<SplitMeta> metas;
    std::vector<float> thresholds;

    // Reserve memory for n elements.
    void Reserve(size_t n) {
      metas.reserve(n);
      thresholds.reserve(n);
    }
  };

  struct EqSplits {
    FrameLayout::Slot<OptionalValue<float>> slot;
    std::vector<SplitMeta> metas;
    std::vector<float> values;
    // Map from split value to range of metas in splits.metas.
    absl::flat_hash_map<float, std::pair<int, int>> value2range;

    // Reserve memory for n elements.
    void Reserve(size_t n) {
      metas.reserve(n);
      values.reserve(n);
    }
  };

  struct RangeSplit {
    SplitMeta meta;  // Meta information of splits
    float left, right;
  };

  struct RangeSplits {
    FrameLayout::Slot<OptionalValue<float>> slot;
    std::vector<RangeSplit> range_splits;  // sorted by increasing left

    // Reserve memory for n elements.
    void Reserve(size_t n) { range_splits.reserve(n); }
  };

  template <typename T>
  struct SetOfValuesSplits {
    FrameLayout::Slot<OptionalValue<T>> slot;
    absl::flat_hash_map<T, std::vector<SplitMeta>> metas;
    std::vector<SplitMeta> metas_with_default_true;
  };

  struct SplitsData {
    std::vector<LeftOrRightSplits> left_splits_grouped_by_input;
    std::vector<LeftOrRightSplits> right_splits_grouped_by_input;
    std::vector<EqSplits> eq_splits_grouped_by_input;
    std::vector<RangeSplits> range_splits_grouped_by_input;
    std::vector<SetOfValuesSplits<int64_t>>
        set_of_values_int64_grouped_by_input;
  };

  struct TreeMetadata {
    int submodel_id;
    size_t adjustments_offset;
  };

  // Ranges of tree ids in trees_metadata_ that belong to a specific group.
  struct GroupMetadata {
    FrameLayout::Slot<float> output_slot;
    std::pair<int, int> regular_tree_range;
    std::pair<int, int> oblivious_tree_range;
  };

  absl::FixedArray<TreeMask> FindTreeMasks(ConstFramePtr ctx) const;

  void ProcessLeftSplits(const LeftOrRightSplits& splits, ConstFramePtr ctx,
                         absl::FixedArray<TreeMask>& tree_masks) const;
  void ProcessRightSplits(const LeftOrRightSplits& splits, ConstFramePtr ctx,
                          absl::FixedArray<TreeMask>& tree_masks) const;
  void ProcessEqSplits(const EqSplits& eq_splits, ConstFramePtr ctx,
                       absl::FixedArray<TreeMask>& tree_masks) const;
  void ProcessRangeSplits(const RangeSplits& splits, ConstFramePtr ctx,
                          absl::FixedArray<TreeMask>& tree_masks) const;

  template <typename T>
  void ProcessSetOfValuesSplits(const std::vector<SetOfValuesSplits<T>>& splits,
                                ConstFramePtr ctx,
                                absl::FixedArray<TreeMask>& tree_masks) const;

  template <typename ProcessFn>
  void InternalEval(ConstFramePtr input_ctx, FramePtr output_ctx,
                    ProcessFn process_fn) const;

  std::vector<TreeMetadata> trees_metadata_;
  std::vector<GroupMetadata> groups_;
  std::vector<float> adjustments_;

  SplitsData splits_;
};

extern template class BitmaskEvalImpl<uint32_t>;
extern template class BitmaskEvalImpl<uint64_t>;

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_BITMASK_EVAL_H_
