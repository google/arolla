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
#include "arolla/decision_forest/batched_evaluation/batched_forest_evaluator.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/pointwise_evaluation/forest_evaluator.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/array_like/frame_iter.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/threading.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace {

absl::StatusOr<TypedValue> AddFullFloatArrays(TypedRef a, TypedRef b) {
  if (a.GetType() == GetDenseArrayQType<float>() &&
      b.GetType() == GetDenseArrayQType<float>()) {
    const auto& va = a.UnsafeAs<DenseArray<float>>();
    const auto& vb = b.UnsafeAs<DenseArray<float>>();
    DCHECK_EQ(va.size(), vb.size());
    DCHECK(va.IsFull() && vb.IsFull());
    Buffer<float>::Builder bldr(va.size());
    auto sa = va.values.span();
    auto sb = vb.values.span();
    auto sr = bldr.GetMutableSpan();
    for (int64_t i = 0; i < va.size(); ++i) {
      sr[i] = sa[i] + sb[i];
    }
    return TypedValue::FromValue(DenseArray<float>{std::move(bldr).Build()});
  } else if (a.GetType() == GetArrayQType<float>() &&
             b.GetType() == GetArrayQType<float>()) {
    const auto& va = a.UnsafeAs<Array<float>>();
    const auto& vb = b.UnsafeAs<Array<float>>();
    DCHECK_EQ(va.size(), vb.size());
    DCHECK(va.IsFullForm() && vb.IsFullForm());
    Buffer<float>::Builder bldr(va.size());
    auto sa = va.dense_data().values.span();
    auto sb = vb.dense_data().values.span();
    auto sr = bldr.GetMutableSpan();
    for (int64_t i = 0; i < va.size(); ++i) {
      sr[i] = sa[i] + sb[i];
    }
    return TypedValue::FromValue(Array<float>{std::move(bldr).Build()});
  } else {
    return absl::InternalError("Invalid type in BatchedForestEvaluator/Add");
  }
}

absl::StatusOr<std::vector<ForestEvaluator>> CreatePointwiseEvaluators(
    const BatchedForestEvaluator::CompilationParams& params,
    const DecisionForest& decision_forest, const std::vector<TypedSlot>& inputs,
    const std::vector<ForestEvaluator::Output>& outputs) {
  int64_t split_count = 0;
  for (const auto& tree : decision_forest.GetTrees()) {
    split_count += tree.split_nodes.size();
  }
  int64_t evaluator_count = std::max<int64_t>(
      1, (split_count + params.optimal_splits_per_evaluator - 1) /
             params.optimal_splits_per_evaluator);

  std::vector<ForestEvaluator> evaluators;
  evaluators.reserve(evaluator_count);

  if (evaluator_count == 1) {
    ASSIGN_OR_RETURN(auto evaluator, ForestEvaluator::Compile(decision_forest,
                                                              inputs, outputs));
    evaluators.push_back(std::move(evaluator));
    return evaluators;
  }

  int64_t splits_per_evaluator =
      (split_count + evaluator_count - 1) / evaluator_count;
  int64_t estimated_trees_per_evaluator =
      (decision_forest.GetTrees().size() + evaluator_count - 1) /
      evaluator_count;
  std::vector<DecisionTree> trees;
  trees.reserve(estimated_trees_per_evaluator);
  int64_t current_split_count = 0;
  for (const auto& tree : decision_forest.GetTrees()) {
    trees.push_back(tree);
    current_split_count += tree.split_nodes.size();
    if (current_split_count >= splits_per_evaluator) {
      ASSIGN_OR_RETURN(auto partial_forest,
                       DecisionForest::FromTrees(std::move(trees)));
      ASSIGN_OR_RETURN(auto evaluator, ForestEvaluator::Compile(
                                           *partial_forest, inputs, outputs));
      evaluators.push_back(std::move(evaluator));
      trees.clear();
      trees.reserve(estimated_trees_per_evaluator);
      current_split_count = 0;
    }
  }
  if (!trees.empty()) {
    ASSIGN_OR_RETURN(auto partial_forest,
                     DecisionForest::FromTrees(std::move(trees)));
    ASSIGN_OR_RETURN(auto evaluator, ForestEvaluator::Compile(*partial_forest,
                                                              inputs, outputs));
    evaluators.push_back(std::move(evaluator));
  }
  return evaluators;
}

}  // namespace

Indestructible<std::unique_ptr<ThreadingInterface>>
    BatchedForestEvaluator::threading_;
int64_t BatchedForestEvaluator::min_rows_per_thread_;

absl::StatusOr<std::unique_ptr<BatchedForestEvaluator>>
BatchedForestEvaluator::Compile(const DecisionForest& decision_forest,
                                absl::Span<const TreeFilter> groups,
                                const CompilationParams& params) {
  // Construct pointwise_layout
  FrameLayout::Builder bldr;

  std::vector<SlotMapping> input_slots_mapping;
  TypedSlot placeholder =
      TypedSlot::FromSlot(FrameLayout::Slot<float>::UnsafeUninitializedSlot());
  std::vector<TypedSlot> input_pointwise_slots;
  for (const auto& kv : decision_forest.GetRequiredQTypes()) {
    TypedSlot pointwise_slot = AddSlot(kv.second, &bldr);
    while (input_pointwise_slots.size() <= kv.first) {
      input_pointwise_slots.push_back(placeholder);
    }
    input_pointwise_slots[kv.first] = pointwise_slot;
    input_slots_mapping.push_back({kv.first, pointwise_slot});
  }

  std::vector<ForestEvaluator::Output> pointwise_outputs;
  std::vector<TypedSlot> output_pointwise_slots;
  pointwise_outputs.reserve(groups.size());
  output_pointwise_slots.reserve(groups.size());
  for (const TreeFilter& filter : groups) {
    auto slot = bldr.AddSlot<float>();
    pointwise_outputs.push_back({filter, slot});
    output_pointwise_slots.push_back(TypedSlot::FromSlot(slot));
  }

  auto pointwise_layout = std::move(bldr).Build();

  // Create evaluator
  ASSIGN_OR_RETURN(
      std::vector<ForestEvaluator> pointwise_evaluators,
      CreatePointwiseEvaluators(params, decision_forest, input_pointwise_slots,
                                pointwise_outputs));
  return absl::WrapUnique(new BatchedForestEvaluator(
      std::move(pointwise_layout), std::move(input_slots_mapping),
      std::move(output_pointwise_slots), std::move(pointwise_evaluators)));
}

absl::Status BatchedForestEvaluator::GetInputsFromSlots(
    absl::Span<const TypedSlot> input_slots, ConstFramePtr frame,
    std::vector<TypedRef>* input_arrays) const {
  if (input_slots.size() < input_count_) {
    return absl::InvalidArgumentError(
        absl::StrFormat("not enough inputs: at least %d expected, %d found",
                        input_count_, input_slots.size()));
  }
  for (auto m : input_mapping_) {
    input_arrays->push_back(
        TypedRef::FromSlot(input_slots[m.input_index], frame));
  }
  return absl::OkStatus();
}

absl::Status BatchedForestEvaluator::EvalBatch(
    absl::Span<const TypedSlot> input_slots,
    absl::Span<const TypedSlot> output_slots, FramePtr frame,
    RawBufferFactory* buffer_factory, std::optional<int64_t> row_count) const {
  // TODO: Try also the non-pointwise algorithm:
  //     Iterate through split nodes in the outer loop, and iterate through rows
  //     in the inner loop.

  std::vector<TypedRef> input_arrays;
  input_arrays.reserve(input_mapping_.size());
  RETURN_IF_ERROR(GetInputsFromSlots(input_slots, frame, &input_arrays));

  if (!row_count.has_value()) {
    if (!input_arrays.empty()) {
      ASSIGN_OR_RETURN(row_count, GetArraySize(input_arrays[0]));
    } else if (!input_slots.empty()) {
      ASSIGN_OR_RETURN(row_count,
                       GetArraySize(TypedRef::FromSlot(input_slots[0], frame)));
    }
  }

  int thread_count = 1;

  // TODO: The parallel implementation work slower than single
  // threaded one (it wasn't this way when the algorithm was implemented,
  // probably became slower after some infrastructure changes), so we disable it
  // for now. Need to implement a different algorithm.
  /*if (*threading_ && row_count.has_value()) {
    thread_count = std::clamp<int>(
        (*row_count + min_rows_per_thread_ - 1) / min_rows_per_thread_, 1,
        (*threading_)->GetRecommendedThreadCount());
  }*/

  // Runs given evaluator and stores the results to `frame`.
  auto run_evaluator = [&](const ForestEvaluator& eval) -> absl::Status {
    ASSIGN_OR_RETURN(
        auto frame_iterator,
        FrameIterator::Create(
            input_arrays, {input_pointwise_slots_.data(), input_arrays.size()},
            output_slots, output_pointwise_slots_, &pointwise_layout_,
            FrameIterator::Options{.row_count = row_count,
                                   .frame_buffer_count = 64 * thread_count,
                                   .buffer_factory = buffer_factory}));

    if (thread_count > 1) {
      frame_iterator.ForEachFrame([&eval](FramePtr f) { eval.Eval(f, f); },
                                  **threading_, thread_count);
    } else {
      frame_iterator.ForEachFrame([&eval](FramePtr f) { eval.Eval(f, f); });
    }
    return frame_iterator.StoreOutput(frame);
  };

  if (pointwise_evaluators_.size() == 1) {
    return run_evaluator(pointwise_evaluators_.front());
  } else {
    std::vector<TypedValue> res_sum;
    res_sum.reserve(output_slots.size());
    RETURN_IF_ERROR(run_evaluator(pointwise_evaluators_.front()));
    for (const auto& s : output_slots) {
      res_sum.push_back(TypedValue::FromSlot(s, frame));
    }
    for (int eval_id = 1; eval_id < pointwise_evaluators_.size() - 1;
         ++eval_id) {
      RETURN_IF_ERROR(run_evaluator(pointwise_evaluators_[eval_id]));
      for (int i = 0; i < output_slots.size(); ++i) {
        ASSIGN_OR_RETURN(
            res_sum[i],
            AddFullFloatArrays(res_sum[i].AsRef(),
                               TypedRef::FromSlot(output_slots[i], frame)));
      }
    }
    RETURN_IF_ERROR(run_evaluator(pointwise_evaluators_.back()));
    for (int i = 0; i < output_slots.size(); ++i) {
      ASSIGN_OR_RETURN(
          TypedValue full_sum,
          AddFullFloatArrays(res_sum[i].AsRef(),
                             TypedRef::FromSlot(output_slots[i], frame)));
      RETURN_IF_ERROR(full_sum.CopyToSlot(output_slots[i], frame));
    }
    return absl::OkStatus();
  }
}

}  // namespace arolla
