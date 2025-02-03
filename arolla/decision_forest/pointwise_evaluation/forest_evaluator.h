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
#ifndef AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_FOREST_EVALUATOR_H_
#define AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_FOREST_EVALUATOR_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/pointwise_evaluation/bitmask_eval.h"
#include "arolla/decision_forest/pointwise_evaluation/bound_split_conditions.h"
#include "arolla/decision_forest/pointwise_evaluation/pointwise.h"
#include "arolla/decision_forest/pointwise_evaluation/single_input_eval.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

class ForestEvaluator {
 public:
  // Intended for benchmarks and tests to force using a specific algorithm.
  struct CompilationParams {
    static constexpr CompilationParams Default() { return {}; }
    bool enable_regular_eval = true;
    bool enable_bitmask_eval = true;
    bool enable_single_input_eval = true;
  };

  // The "outputs" argument in Compile allows to calculate results separately
  // for different groups of trees. Each Output specifies a TreeFilter for
  // a group and a slot in which the result should be stored to.
  // Groups shouldn't intersect.
  // To include all trees in a single Output use empty filter: Output{{}, slot}.
  struct Output {
    TreeFilter filter;
    FrameLayout::Slot<float> slot;
  };

  static absl::StatusOr<ForestEvaluator> Compile(
      const DecisionForest& decision_forest,
      absl::Span<const TypedSlot> input_slots, absl::Span<const Output> outputs,
      CompilationParams params = CompilationParams::Default());

  // Evaluates the whole forest.
  void Eval(ConstFramePtr input_ctx, FramePtr output_ctx) const;

 private:
  template <class T>
  using Predictor = BoostedPredictor<float, T, std::plus<double>, int>;
  template <class T>
  using PredictorCompiler =
      BoostedPredictorCompiler<float, T, std::plus<double>, int>;

  using UniversalBoundCondition =
      VariantBoundCondition<IntervalBoundCondition,
                            SetOfValuesBoundCondition<int64_t>,
                            VirtualBoundCondition>;

  struct RegularPredictors {
    Predictor<UniversalBoundCondition> universal_predictor;
    Predictor<IntervalBoundCondition> interval_splits_predictor;

    float Predict(ConstFramePtr input_ctx) const {
      return universal_predictor.Predict(input_ctx, 0.0) +
             interval_splits_predictor.Predict(input_ctx, 0.0);
    }
  };
  using RegularPredictorsList = absl::InlinedVector<RegularPredictors, 2>;
  class RegularPredictorsBuilder;

  explicit ForestEvaluator(std::vector<FrameLayout::Slot<float>>&& output_slots,
                           RegularPredictorsList&& predictors,
                           std::unique_ptr<BitmaskEval>&& bitmask_predictor,
                           SingleInputEval&& single_input_predictor)
      : output_slots_(std::move(output_slots)),
        regular_predictors_(std::move(predictors)),
        bitmask_predictor_(std::move(bitmask_predictor)),
        single_input_predictor_(std::move(single_input_predictor)) {}

  std::vector<FrameLayout::Slot<float>> output_slots_;
  RegularPredictorsList regular_predictors_;
  std::unique_ptr<BitmaskEval> bitmask_predictor_;
  SingleInputEval single_input_predictor_;
};

// A convenience wrapper for ForestEvaluator. Doesn't support multiple outputs
// and partial evaluation. Uses ForestEvaluator inside.
class SimpleForestEvaluator {
 public:
  static absl::StatusOr<SimpleForestEvaluator> Compile(
      const DecisionForest& decision_forest,
      absl::Span<const TypedSlot> input_slots,
      ForestEvaluator::CompilationParams params =
          ForestEvaluator::CompilationParams::Default()) {
    DCHECK(GetQType<float>()->type_layout().HasField(0, typeid(float)));
    ForestEvaluator::Output output{
        .filter = {},
        // We create a slot to use it with GetQType<float>()->type_layout().
        .slot = FrameLayout::Slot<float>::UnsafeSlotFromOffset(0)};
    ASSIGN_OR_RETURN(auto evaluator,
                     ForestEvaluator::Compile(decision_forest, input_slots,
                                              {output}, params));
    return SimpleForestEvaluator(std::move(evaluator));
  }

  float Eval(ConstFramePtr ctx) const {
    float res;
    FramePtr output_ctx(&res, &GetQType<float>()->type_layout());
    evaluator_.Eval(ctx, output_ctx);
    return res;
  }

 private:
  explicit SimpleForestEvaluator(ForestEvaluator&& evaluator)
      : evaluator_(std::move(evaluator)) {}

  ForestEvaluator evaluator_;
};

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_FOREST_EVALUATOR_H_
