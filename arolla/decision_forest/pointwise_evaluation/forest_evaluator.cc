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
#include "arolla/decision_forest/pointwise_evaluation/forest_evaluator.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/pointwise_evaluation/bitmask_builder.h"
#include "arolla/decision_forest/pointwise_evaluation/bound_split_conditions.h"
#include "arolla/decision_forest/pointwise_evaluation/oblivious.h"
#include "arolla/decision_forest/pointwise_evaluation/single_input_eval.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace {

bool HasOnlyIntervalSplitConditions(const DecisionTree& tree) {
  for (const auto& split_node : tree.split_nodes) {
    if (!fast_dynamic_downcast_final<const IntervalSplitCondition*>(
            split_node.condition.get()))
      return false;
  }
  return true;
}

absl::StatusOr<std::vector<int>> SplitTreesByGroups(
    absl::Span<const DecisionTree> trees,
    absl::Span<const ForestEvaluator::Output> outputs) {
  if (outputs.empty()) {
    return absl::InvalidArgumentError("at least one output is expected");
  }
  std::vector<int> tree2group(trees.size(), -1);
  for (int group_id = 0; group_id < outputs.size(); ++group_id) {
    for (int tree_id = 0; tree_id < trees.size(); ++tree_id) {
      if (!outputs[group_id].filter(trees[tree_id].tag)) continue;
      if (tree2group[tree_id] != -1) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "intersection of groups for outputs #%d and #%d is not empty",
            tree2group[tree_id], group_id));
      }
      tree2group[tree_id] = group_id;
    }
  }
  return tree2group;
}

std::optional<SplitCondition::InputSignature> GetSingleInputSignature(
    const DecisionTree& tree) {
  std::optional<SplitCondition::InputSignature> input_signature;
  for (const auto& node : tree.split_nodes) {
    auto signatures = node.condition->GetInputSignatures();
    if (signatures.size() != 1 ||
        (input_signature && input_signature->id != signatures[0].id)) {
      return std::nullopt;
    }
    input_signature = signatures[0];
  }
  return input_signature;
}

}  // namespace

class ForestEvaluator::RegularPredictorsBuilder {
 public:
  RegularPredictorsBuilder(int group_count,
                           absl::Span<const TypedSlot> input_slots)
      : group_count_(group_count),
        input_slots_(input_slots.begin(), input_slots.end()),
        universal_compilers_(group_count),
        interval_splits_compilers_(group_count) {}

  absl::Status AddTree(const DecisionTree& tree, int group_id) {
    if (HasOnlyIntervalSplitConditions(tree)) {
      return AddTreeToRegularForestCompiler(
          tree,
          [this](const std::shared_ptr<const SplitCondition>& cond) {
            auto interval_cond =
                std::static_pointer_cast<const IntervalSplitCondition>(cond);
            return IntervalBoundCondition::Create(interval_cond, input_slots_);
          },
          &interval_splits_compilers_[group_id]);
    } else {
      return AddTreeToRegularForestCompiler(
          tree,
          [this](const std::shared_ptr<const SplitCondition>& cond) {
            return UniversalBoundCondition::Create(cond, input_slots_);
          },
          &universal_compilers_[group_id]);
    }
  }

  absl::StatusOr<RegularPredictorsList> Build() && {
    RegularPredictorsList res;
    res.reserve(group_count_);
    for (int i = 0; i < group_count_; ++i) {
      ASSIGN_OR_RETURN(auto universal_predictor,
                       universal_compilers_[i].Compile());
      ASSIGN_OR_RETURN(auto interval_splits_predictor,
                       interval_splits_compilers_[i].Compile());
      res.push_back({std::move(universal_predictor),
                     std::move(interval_splits_predictor)});
    }
    return res;
  }

 private:
  template <typename ForestCompiler, typename CreateConditionFunc>
  absl::Status AddTreeToRegularForestCompiler(const DecisionTree& tree,
                                              CreateConditionFunc create_cond,
                                              ForestCompiler* forest_compiler) {
    auto tree_compiler = forest_compiler->AddTree(
        tree.split_nodes.size() + tree.adjustments.size(),
        tree.tag.submodel_id);
    for (int64_t id = 0; id < tree.split_nodes.size(); ++id) {
      const auto& split_node = tree.split_nodes[id];
      auto child_if_false = split_node.child_if_false.is_leaf()
                                ? split_node.child_if_false.adjustment_index() +
                                      tree.split_nodes.size()
                                : split_node.child_if_false.split_node_index();
      auto child_if_true = split_node.child_if_true.is_leaf()
                               ? split_node.child_if_true.adjustment_index() +
                                     tree.split_nodes.size()
                               : split_node.child_if_true.split_node_index();
      ASSIGN_OR_RETURN(auto cond, create_cond(split_node.condition));
      RETURN_IF_ERROR(
          tree_compiler.SetNode(id, child_if_true, child_if_false, cond));
    }
    for (int64_t i = 0; i < tree.adjustments.size(); ++i) {
      RETURN_IF_ERROR(tree_compiler.SetLeaf(i + tree.split_nodes.size(),
                                            tree.adjustments[i] * tree.weight));
    }
    return absl::OkStatus();
  }

  int group_count_;
  std::vector<TypedSlot> input_slots_;
  std::vector<PredictorCompiler<UniversalBoundCondition>> universal_compilers_;
  std::vector<PredictorCompiler<IntervalBoundCondition>>
      interval_splits_compilers_;
};

absl::StatusOr<ForestEvaluator> ForestEvaluator::Compile(
    const DecisionForest& decision_forest,
    absl::Span<const TypedSlot> input_slots, absl::Span<const Output> outputs,
    CompilationParams params) {
  ASSIGN_OR_RETURN(auto tree2group,
                   SplitTreesByGroups(decision_forest.GetTrees(), outputs));
  std::vector<FrameLayout::Slot<float>> output_slots;
  output_slots.reserve(outputs.size());
  for (const auto& output : outputs) {
    output_slots.push_back(output.slot);
  }
  RegularPredictorsBuilder regular_builder(outputs.size(), input_slots);
  BitmaskBuilder bitmask_builder(input_slots, output_slots);
  SingleInputBuilder single_input_builder(input_slots, output_slots);
  std::vector<std::map<int, double>> consts(outputs.size());
  if (tree2group.size() != decision_forest.GetTrees().size()) {
    return absl::InternalError("size of tree2group doesn't match trees");
  }
  for (size_t i = 0; i < decision_forest.GetTrees().size(); ++i) {
    if (tree2group[i] == -1) {
      continue;  // tree is not used
    }
    if (tree2group[i] < 0 || tree2group[i] >= outputs.size()) {
      return absl::InternalError("invalid tree2group mapping");
    }
    const DecisionTree& tree = decision_forest.GetTrees()[i];

    if (params.enable_regular_eval && tree.split_nodes.empty()) {
      // Consts are merged together and added to regular_builder.
      consts[tree2group[i]][tree.tag.submodel_id] +=
          tree.adjustments[0] * tree.weight;
      continue;
    }

    if (params.enable_single_input_eval) {
      if (std::optional<SplitCondition::InputSignature> input_signature =
              GetSingleInputSignature(tree)) {
        if (single_input_builder.IsInputTypeSupported(input_signature->type)) {
          RETURN_IF_ERROR(single_input_builder.AddTree(tree, *input_signature,
                                                       tree2group[i]));
          continue;
        }
      }
    }

    if (params.enable_bitmask_eval &&
        std::all_of(tree.split_nodes.begin(), tree.split_nodes.end(),
                    BitmaskBuilder::IsSplitNodeSupported)) {
      auto oblivious = ToObliviousTree(tree);
      if (oblivious.has_value() && (oblivious->layer_splits.size() <=
                                    BitmaskBuilder::kMaxRegionsForBitmask)) {
        bitmask_builder.AddObliviousTree(*std::move(oblivious), tree2group[i]);
        continue;
      }
      if (tree.adjustments.size() <= BitmaskBuilder::kMaxRegionsForBitmask) {
        bitmask_builder.AddSmallTree(tree, tree2group[i]);
        continue;
      }
    }

    if (params.enable_regular_eval) {
      RETURN_IF_ERROR(regular_builder.AddTree(tree, tree2group[i]));
    } else {
      return absl::InvalidArgumentError(
          "No suitable evaluator. Use enable_regular_eval=true.");
    }
  }
  for (int group_id = 0; group_id < consts.size(); ++group_id) {
    for (const auto& [submodel_id, value] : consts[group_id]) {
      DecisionTree tree;
      tree.adjustments = {static_cast<float>(value)};
      tree.tag.submodel_id = submodel_id;
      RETURN_IF_ERROR(regular_builder.AddTree(tree, group_id));
    }
  }

  ASSIGN_OR_RETURN(auto regular_predictors, std::move(regular_builder).Build());
  ASSIGN_OR_RETURN(auto bitmask_predictor, std::move(bitmask_builder).Build());
  ASSIGN_OR_RETURN(auto single_input_predictor,
                   std::move(single_input_builder).Build());
  return ForestEvaluator(std::move(output_slots), std::move(regular_predictors),
                         std::move(bitmask_predictor),
                         std::move(single_input_predictor));
}

void ForestEvaluator::Eval(const ConstFramePtr input_ctx,
                           FramePtr output_ctx) const {
  for (size_t i = 0; i < output_slots_.size(); ++i) {
    *output_ctx.GetMutable(output_slots_[i]) =
        regular_predictors_[i].Predict(input_ctx);
  }
  if (bitmask_predictor_) {
    bitmask_predictor_->IncrementalEval(input_ctx, output_ctx);
  }
  single_input_predictor_.IncrementalEval(input_ctx, output_ctx);
}

}  // namespace arolla
