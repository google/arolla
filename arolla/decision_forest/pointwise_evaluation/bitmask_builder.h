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
#ifndef AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_BITMASK_BUILDER_H_
#define AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_BITMASK_BUILDER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/pointwise_evaluation/bitmask_eval.h"
#include "arolla/decision_forest/pointwise_evaluation/oblivious.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {

// Used to construct BitmaskEval.
class BitmaskBuilder {
 private:
  using WideMask = uint64_t;

 public:
  static constexpr size_t kMaxRegionsForBitmask = sizeof(WideMask) * 8;

  explicit BitmaskBuilder(
      absl::Span<const TypedSlot> input_slots,
      absl::Span<const FrameLayout::Slot<float>> output_slots)
      : input_slots_(input_slots.begin(), input_slots.end()),
        output_slots_(output_slots.begin(), output_slots.end()) {}

  static bool IsSplitNodeSupported(const SplitNode& node);

  void AddSmallTree(const DecisionTree& tree, int group_id);
  void AddObliviousTree(ObliviousDecisionTree&& tree, int group_id);

  absl::StatusOr<std::unique_ptr<BitmaskEval>> Build() &&;

 private:
  struct MaskedSplit {
    WideMask false_branch_mask;
    std::shared_ptr<const SplitCondition> condition;
  };

  struct MaskedTree {
    int group_id;
    DecisionTree::Tag tag;
    std::vector<MaskedSplit> splits;
    std::vector<float> adjustments;
  };

  struct ObliviousWithGroupId {
    int group_id;
    DecisionTree::Tag tag;
    ObliviousDecisionTree tree;
  };

  template <typename Mask>
  struct SplitsBuildingData;

  template <typename Mask>
  absl::Status Build_MaskedTree(const MaskedTree& tree,
                                SplitsBuildingData<Mask>* splits,
                                BitmaskEvalImpl<Mask>* data);
  template <typename Mask>
  absl::Status Build_ObliviousTree(const ObliviousWithGroupId& tree,
                                   SplitsBuildingData<Mask>* splits,
                                   BitmaskEvalImpl<Mask>* data);

  void SortTreesByGroupAndSubmodel();

  template <typename TreeMask>
  absl::StatusOr<std::unique_ptr<BitmaskEval>> BuildImpl();

  size_t combined_adjustments_size_ = 0;
  std::vector<TypedSlot> input_slots_;
  std::vector<FrameLayout::Slot<float>> output_slots_;
  std::vector<MaskedTree> masked_trees_;
  std::vector<ObliviousWithGroupId> oblivious_trees_;

  enum { MASK32, MASK64 } mask_type_ = MASK32;
};

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_BITMASK_BUILDER_H_
