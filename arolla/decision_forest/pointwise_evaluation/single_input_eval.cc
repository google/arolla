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
#include "arolla/decision_forest/pointwise_evaluation/single_input_eval.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/pointwise_evaluation/forest_evaluator.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace single_input_eval_internal {

namespace {

template <typename T>
absl::Status AddSplitPoints(const SplitCondition* cond,
                            std::vector<T>* split_points) {
  if (auto* set_of_values_cond =
          fast_dynamic_downcast_final<const SetOfValuesSplitCondition<T>*>(
              cond)) {
    split_points->insert(split_points->end(),
                         set_of_values_cond->values().begin(),
                         set_of_values_cond->values().end());
    return absl::OkStatus();
  }
  if (auto* interval_cond =
          fast_dynamic_downcast_final<const IntervalSplitCondition*>(cond)) {
    if constexpr (std::is_same<T, float>()) {
      constexpr float inf = std::numeric_limits<float>::infinity();
      if (interval_cond->left() != -inf) {
        split_points->push_back(interval_cond->left());
      }
      if (interval_cond->right() != inf &&
          interval_cond->right() != interval_cond->left()) {
        split_points->push_back(interval_cond->right());
      }
      return absl::OkStatus();
    }
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "split condition is not supported: %s", cond->ToString()));
}

template <typename T>
absl::StatusOr<std::vector<T>> GetSplitPoints(
    absl::Span<const DecisionTree> trees) {
  std::vector<T> split_points;
  for (const DecisionTree& tree : trees) {
    for (const SplitNode& node : tree.split_nodes) {
      RETURN_IF_ERROR(AddSplitPoints(node.condition.get(), &split_points));
    }
  }
  absl::c_sort(split_points);
  split_points.erase(std::unique(split_points.begin(), split_points.end()),
                     split_points.end());
  return split_points;
}

template <typename T>
bool is_nan(T v ABSL_ATTRIBUTE_UNUSED) {
  if constexpr (std::numeric_limits<T>::has_quiet_NaN) {
    return std::isnan(v);
  } else {
    return false;
  }
}

// Used only on compilation stage.
template <typename T>
class InternalEvaluator {
 public:
  InternalEvaluator(SimpleForestEvaluator eval,
                    FrameLayout::Slot<OptionalValue<T>> slot,
                    FrameLayout layout)
      : eval_(std::move(eval)),
        slot_(slot),
        layout_(std::make_unique<FrameLayout>(std::move(layout))),
        alloc_(std::make_unique<MemoryAllocation>(layout_.get())),
        frame_(alloc_->frame()) {}

  static absl::StatusOr<InternalEvaluator> Create(const DecisionForest& forest,
                                                  int input_id) {
    FrameLayout::Builder bldr;
    auto slot = bldr.AddSlot<OptionalValue<T>>();
    auto layout = std::move(bldr).Build();
    std::vector<TypedSlot> inputs(input_id + 1, TypedSlot::FromSlot(slot));

    ASSIGN_OR_RETURN(auto eval, SimpleForestEvaluator::Compile(
                                    forest, inputs,
                                    {.enable_regular_eval = true,
                                     .enable_bitmask_eval = true,
                                     .enable_single_input_eval = false}));

    return InternalEvaluator(std::move(eval), slot, std::move(layout));
  }

  float Eval(OptionalValue<T> v) {
    frame_.Set(slot_, v);
    return eval_.Eval(frame_);
  }

 private:
  SimpleForestEvaluator eval_;
  FrameLayout::Slot<OptionalValue<T>> slot_;
  std::unique_ptr<FrameLayout> layout_;
  std::unique_ptr<MemoryAllocation> alloc_;
  FramePtr frame_;
};

}  // namespace

template <typename T>
absl::StatusOr<PiecewiseConstantEvaluator<T>>
PiecewiseConstantCompiler<T>::Build(
    int input_id, FrameLayout::Slot<OptionalValue<T>> input_slot) && {
  ASSIGN_OR_RETURN(std::vector<T> split_points, GetSplitPoints<T>(trees_));

  ASSIGN_OR_RETURN(auto forest, DecisionForest::FromTrees(std::move(trees_)));
  ASSIGN_OR_RETURN(auto evaluator,
                   InternalEvaluator<T>::Create(*forest, input_id));

  std::vector<float> point_values;
  point_values.reserve(split_points.size());
  for (T v : split_points) {
    point_values.push_back(evaluator.Eval(v));
  }

  std::vector<float> middle_values;
  middle_values.reserve(split_points.size() + 1);
  if (split_points.empty()) {
    middle_values.push_back(evaluator.Eval(static_cast<T>(0)));
  } else {
    middle_values.push_back(evaluator.Eval(std::numeric_limits<T>::lowest()));
    for (int i = 1; i < split_points.size(); ++i) {
      T left = split_points[i - 1];
      T right = split_points[i];
      middle_values.push_back(evaluator.Eval((left + right) / 2));
    }
    middle_values.push_back(evaluator.Eval(std::numeric_limits<T>::max()));
  }

  return PiecewiseConstantEvaluator<T>(
      input_slot, std::move(split_points), std::move(point_values),
      std::move(middle_values), evaluator.Eval(std::nullopt));
}

template <typename T>
PiecewiseConstantEvaluator<T>::PiecewiseConstantEvaluator(
    FrameLayout::Slot<OptionalValue<T>> slot, std::vector<T> split_points,
    std::vector<float> point_values, std::vector<float> middle_values,
    float result_if_value_is_missed)
    : input_slot_(slot),
      split_points_(std::move(split_points)),
      point_values_(std::move(point_values)),
      middle_values_(std::move(middle_values)),
      result_if_value_is_missed_(result_if_value_is_missed) {}

template <typename T>
float PiecewiseConstantEvaluator<T>::Eval(const ConstFramePtr ctx) const {
  OptionalValue<T> opt_value = ctx.Get(input_slot_);
  if (!opt_value.present || is_nan(opt_value.value)) {
    return result_if_value_is_missed_;
  }
  T value = opt_value.value;

  int split_point_id =
      std::lower_bound(split_points_.begin(), split_points_.end(), value) -
      split_points_.begin();

  if (split_point_id < split_points_.size()) {
    return split_points_[split_point_id] == value
               ? point_values_[split_point_id]
               : middle_values_[split_point_id];
  } else {
    return middle_values_.back();
  }
}

void PiecewiseConstantEvaluators::IncrementalEval(ConstFramePtr input_ctx,
                                                  FramePtr output_ctx) const {
  double res = 0;
  for (const auto& predictor : float_predictors_) {
    res += predictor.Eval(input_ctx);
  }
  for (const auto& predictor : int64_predictors_) {
    res += predictor.Eval(input_ctx);
  }
  *output_ctx.GetMutable(output_slot_) += res;
}

}  // namespace single_input_eval_internal

void SingleInputEval::IncrementalEval(ConstFramePtr input_ctx,
                                      FramePtr output_ctx) const {
  for (const auto& evaluator : evaluators_) {
    evaluator.IncrementalEval(input_ctx, output_ctx);
  }
}

absl::Status SingleInputBuilder::AddTree(
    const DecisionTree& tree, SplitCondition::InputSignature input_signature,
    int group_id) {
  if (group_id < 0 || group_id >= compilers_.size()) {
    return absl::InvalidArgumentError("group_id is out of range");
  }
  if (input_signature.type == GetOptionalQType<float>()) {
    compilers_[group_id].float_per_input_compilers[input_signature.id].AddTree(
        tree);
  } else if (input_signature.type == GetOptionalQType<int64_t>()) {
    compilers_[group_id].int64_per_input_compilers[input_signature.id].AddTree(
        tree);
  } else {
    return absl::InvalidArgumentError(absl::StrFormat(
        "QType not supported: %s", input_signature.type->name()));
  }
  return absl::OkStatus();
}

template <typename T>
absl::StatusOr<SingleInputBuilder::EvaluatorsVector<T>>
SingleInputBuilder::BuildEvaluatorsVector(CompilersMap<T>* compilers) {
  EvaluatorsVector<T> res;
  res.reserve(compilers->size());
  for (auto& [input_id, compiler] : *compilers) {
    if (input_id < 0 || input_id >= input_slots_.size()) {
      return absl::InvalidArgumentError("input_id is out of range");
    }
    ASSIGN_OR_RETURN(
        auto slot, input_slots_[input_id].template ToSlot<OptionalValue<T>>());
    ASSIGN_OR_RETURN(auto evaluator, std::move(compiler).Build(input_id, slot));
    res.push_back(std::move(evaluator));
  }
  return res;
}

absl::StatusOr<SingleInputEval> SingleInputBuilder::Build() && {
  SingleInputEval res;
  for (PerGroupCompilers& group : compilers_) {
    ASSIGN_OR_RETURN(auto float_predictors,
                     BuildEvaluatorsVector(&group.float_per_input_compilers));
    ASSIGN_OR_RETURN(auto int64_predictors,
                     BuildEvaluatorsVector(&group.int64_per_input_compilers));
    res.evaluators_.push_back(
        single_input_eval_internal::PiecewiseConstantEvaluators(
            group.output_slot, std::move(float_predictors),
            std::move(int64_predictors)));
  }
  return res;
}

}  // namespace arolla
