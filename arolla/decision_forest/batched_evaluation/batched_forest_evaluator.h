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
#ifndef AROLLA_DECISION_FOREST_BATCHED_EVALUATION_BATCHED_FOREST_EVALUATOR_H_
#define AROLLA_DECISION_FOREST_BATCHED_EVALUATION_BATCHED_FOREST_EVALUATOR_H_

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/pointwise_evaluation/forest_evaluator.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/threading.h"

namespace arolla {

// Optimized batched evaluator for decision forest.
class BatchedForestEvaluator {
 public:
  // Intended for benchmarks and tests to force using a specific algorithm.
  // In all other cases the default value 'CompilationParams()' should be used.
  struct CompilationParams {
    static constexpr CompilationParams Default() { return {}; }

    // If the total count of split nodes in a forest exceeds this number,
    // BatchedForesetEvaluator splits the forest and uses several pointwise
    // evaluators. Important for performance if the forest doesn't fit into
    // processor cache in one piece.
    int64_t optimal_splits_per_evaluator = 500000;
  };

  struct SlotMapping {
    int input_index;
    TypedSlot pointwise_slot;
  };

  // Creates BatchedForestEvaluator.
  // The "groups" argument allows to calculate results separately for different
  // groups of trees. TreeFilters of different groups shouldn't intersect.
  // The default value groups={{}} is a single group that includes all trees.
  static absl::StatusOr<std::unique_ptr<BatchedForestEvaluator>> Compile(
      const DecisionForest& decision_forest,
      absl::Span<const TreeFilter> groups = {{}},
      const CompilationParams& params = CompilationParams::Default());

  // Evaluates decision forest on a set of arrays.
  // All input_slots should store arrays of the same size.
  // Types of input_slots should correspond to required types of the decision.
  // Sizes of arrays in input_slots should correspond to row_count. The default
  // missed value row_count means that it should be taken from the input arrays.
  absl::Status EvalBatch(absl::Span<const TypedSlot> input_slots,
                         absl::Span<const TypedSlot> output_slots,
                         FramePtr frame,
                         RawBufferFactory* = GetHeapBufferFactory(),
                         std::optional<int64_t> row_count = {}) const;

  static void SetThreading(std::unique_ptr<ThreadingInterface> threading,
                           int64_t min_rows_per_thread = 128) {
    *threading_ = std::move(threading);
    min_rows_per_thread_ = min_rows_per_thread;
  }

 private:
  static absl::NoDestructor<std::unique_ptr<ThreadingInterface>> threading_;
  static int64_t min_rows_per_thread_;

  BatchedForestEvaluator(FrameLayout&& pointwise_layout,
                         std::vector<SlotMapping>&& input_mapping,
                         std::vector<TypedSlot>&& output_pointwise_slots,
                         std::vector<ForestEvaluator>&& pointwise_evaluators)
      : pointwise_layout_(std::move(pointwise_layout)),
        input_mapping_(std::move(input_mapping)),
        output_pointwise_slots_(output_pointwise_slots),
        pointwise_evaluators_(std::move(pointwise_evaluators)) {
    input_pointwise_slots_.reserve(input_mapping_.size());
    input_count_ = 0;
    for (const auto& m : input_mapping_) {
      input_pointwise_slots_.push_back(m.pointwise_slot);
      input_count_ = std::max(input_count_, m.input_index + 1);
    }
  }

  // Gets values from input_slots and remaps it according to input_mapping_.
  absl::Status GetInputsFromSlots(absl::Span<const TypedSlot> input_slots,
                                  ConstFramePtr frame,
                                  std::vector<TypedRef>* input_arrays) const;

  FrameLayout pointwise_layout_;
  std::vector<SlotMapping> input_mapping_;
  std::vector<TypedSlot> input_pointwise_slots_;
  std::vector<TypedSlot> output_pointwise_slots_;
  int input_count_;
  std::vector<ForestEvaluator> pointwise_evaluators_;
};

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_BATCHED_EVALUATION_BATCHED_FOREST_EVALUATOR_H_
