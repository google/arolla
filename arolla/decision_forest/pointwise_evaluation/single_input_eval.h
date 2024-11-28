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
#ifndef AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_SINGLE_INPUT_EVAL_H_
#define AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_SINGLE_INPUT_EVAL_H_

#include <cstdint>
#include <utility>
#include <vector>

#include "absl//container/flat_hash_map.h"
#include "absl//container/inlined_vector.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

class SingleInputBuilder;

namespace single_input_eval_internal {

// Represents a piecewise constant function of a single numeric argument.
// Input is OptionalValue<T> (stored in frame). Output is always a float.
// point_values are values in split points (size should be split_points.size()).
// middle_values are values between (and also before and after) split points
// (size should be split_points.size() + 1).
template <typename T>
class PiecewiseConstantEvaluator {
 public:
  explicit PiecewiseConstantEvaluator(FrameLayout::Slot<OptionalValue<T>> slot,
                                      std::vector<T> split_points,
                                      std::vector<float> point_values,
                                      std::vector<float> middle_values,
                                      float result_if_value_is_missed);

  float Eval(ConstFramePtr ctx) const;

 private:
  FrameLayout::Slot<OptionalValue<T>> input_slot_;
  std::vector<T> split_points_;
  std::vector<float> point_values_;
  std::vector<float> middle_values_;
  float result_if_value_is_missed_;
};

// Constructs PiecewiseConstantEvaluator from set of decision trees. All trees
// should use a single input (the same for each tree) of type OptionalValue<T>.
template <typename T>
class PiecewiseConstantCompiler {
 public:
  void AddTree(const DecisionTree& tree) { trees_.push_back(tree); }

  absl::StatusOr<PiecewiseConstantEvaluator<T>> Build(
      int input_id, FrameLayout::Slot<OptionalValue<T>> input_slot) &&;

 private:
  std::vector<DecisionTree> trees_;
};

// Holds PiecewiseConstantEvaluator for every input. Only input types
// OptionalValue<float> and OptionalValue<int64_t> are supported.
class PiecewiseConstantEvaluators {
 public:
  void IncrementalEval(ConstFramePtr input_ctx, FramePtr output_ctx) const;

 private:
  explicit PiecewiseConstantEvaluators(
      FrameLayout::Slot<float> output_slot,
      std::vector<PiecewiseConstantEvaluator<float>> float_predictors,
      std::vector<PiecewiseConstantEvaluator<int64_t>> int64_predictors)
      : output_slot_(output_slot),
        float_predictors_(std::move(float_predictors)),
        int64_predictors_(std::move(int64_predictors)) {}
  friend class ::arolla::SingleInputBuilder;

  FrameLayout::Slot<float> output_slot_;
  std::vector<PiecewiseConstantEvaluator<float>> float_predictors_;
  std::vector<PiecewiseConstantEvaluator<int64_t>> int64_predictors_;
};

}  // namespace single_input_eval_internal

// SingleInputEval is an optimized evaluator for trees without interaction.
// I.e. each tree should use only one input, but different trees can use
// different inputs. It uses PiecewiseConstantEvaluator to evaluate all trees
// related to one input at the same time.
class SingleInputEval {
 public:
  // Evaluates trees separately for each group and adds the result
  // to corresponding slots in output_ctx.
  void IncrementalEval(ConstFramePtr input_ctx, FramePtr output_ctx) const;

 private:
  SingleInputEval() = default;
  friend class SingleInputBuilder;

  absl::InlinedVector<single_input_eval_internal::PiecewiseConstantEvaluators,
                      2>
      evaluators_;
};

class SingleInputBuilder {
 public:
  explicit SingleInputBuilder(
      absl::Span<const TypedSlot> input_slots,
      absl::Span<const FrameLayout::Slot<float>> output_slots)
      : input_slots_(input_slots.begin(), input_slots.end()) {
    compilers_.reserve(output_slots.size());
    for (const auto slot : output_slots) {
      compilers_.push_back(PerGroupCompilers{.output_slot = slot});
    }
  }

  static bool IsInputTypeSupported(QTypePtr type) {
    return type == GetOptionalQType<float>() ||
           type == GetOptionalQType<int64_t>();
  }

  // Adds a tree to the evaluator. Returns InvalidArgumentError in type of input
  // is not supported (only OPTIONAL_FLOAT32 and OPTIONAL_INT64 are supported).
  absl::Status AddTree(const DecisionTree& tree,
                       SplitCondition::InputSignature input_signature,
                       int group_id);

  absl::StatusOr<SingleInputEval> Build() &&;

 private:
  template <typename T>
  using EvaluatorsVector =
      std::vector<single_input_eval_internal::PiecewiseConstantEvaluator<T>>;
  template <typename T>
  using CompilersMap = absl::flat_hash_map<
      int, single_input_eval_internal::PiecewiseConstantCompiler<T>>;

  template <typename T>
  absl::StatusOr<EvaluatorsVector<T>> BuildEvaluatorsVector(
      CompilersMap<T>* compilers);

  struct PerGroupCompilers {
    FrameLayout::Slot<float> output_slot;
    CompilersMap<float> float_per_input_compilers;
    CompilersMap<int64_t> int64_per_input_compilers;
  };

  std::vector<PerGroupCompilers> compilers_;
  std::vector<TypedSlot> input_slots_;
};

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_SINGLE_INPUT_EVAL_H_
