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
#ifndef AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_OBLIVIOUS_H_
#define AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_OBLIVIOUS_H_

#include <memory>
#include <optional>
#include <vector>

#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/split_condition.h"

namespace arolla {

// Represents oblivious decision tree (all splits are equal on each layer).
// Decision tree is always full and balanced.
struct ObliviousDecisionTree {
  DecisionTree::Tag tag;
  // Split per layer. Each node in a layer has exactly the same split.
  std::vector<std::shared_ptr<const SplitCondition>> layer_splits;
  // Adjustment per leaf. Leaves are numbered from the 'false branch' side
  // to the 'true branch' side. E.g., 0-th leaf corresponds
  // for all false conditions, last for all true conditions.
  // 1-st leaf corresponds to all except last conditions being false.
  std::vector<float> adjustments;
};

// Returns oblivious tree representation or nullopt if not possible.
std::optional<ObliviousDecisionTree> ToObliviousTree(const DecisionTree& tree);

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_OBLIVIOUS_H_
