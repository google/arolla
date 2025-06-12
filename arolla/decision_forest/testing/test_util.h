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
#ifndef AROLLA_DECISION_FOREST_TEST_UTIL_TEST_UTIL_H_
#define AROLLA_DECISION_FOREST_TEST_UTIL_TEST_UTIL_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// Fills typed slot with random value. For optional float values are
// in range [0, 1]. For optional int64_t - in range [0, 1000]. Other types are
// not supported. missed_prob is a probability of missed value.
absl::Status FillWithRandomValue(TypedSlot tslot, FramePtr ctx,
                                 absl::BitGen* rnd, double missed_prob = 0);

// Fills DenseArray typed slot with random values. For float arrays values are
// in range [0, 1]. For int64_t arrays - in range [0, 1000]. Other types are
// not supported. missed_prob is a probability of missed value.
// This function doesn't use RandomDenseArray (dense_array/testing/utils.h)
// because the behaviour should be consistent with FillWithRandomValue.
absl::Status FillArrayWithRandomValues(int64_t size, TypedSlot tslot,
                                       FramePtr ctx, absl::BitGen* rnd,
                                       double missed_prob = 0);

// Creates input slots for given decision forest.
void CreateSlotsForForest(const DecisionForest& forest,
                          FrameLayout::Builder* layout_builder,
                          std::vector<TypedSlot>* slots);

// Creates input slots for batched evaluation of the given decision forest.
absl::Status CreateArraySlotsForForest(const DecisionForest& forest,
                                       FrameLayout::Builder* layout_builder,
                                       std::vector<TypedSlot>* slots);

// Creates balanced random tree with
// IntervalSplitCondition and split points in [0, 1).
// interactions = false means that only one (randomly chosen) feature is used.
// range_split_prob > 0 for generating range splits.
DecisionTree CreateRandomFloatTree(absl::BitGen* rnd, int num_features,
                                   bool interactions, int num_splits,
                                   double range_split_prob = 0.0,
                                   double equality_split_prob = 0.0);

// Creates balanced random forest with
// IntervalSplitCondition and split points in [0, 1).
// interactions = false means that only one feature per tree is used.
std::unique_ptr<const DecisionForest> CreateRandomFloatForest(
    absl::BitGen* rnd, int num_features, bool interactions, int min_num_splits,
    int max_num_splits, int num_trees);

// Creates balanced random tree with different types of split conditions.
// The tree will expect features with types specified in features_types.
// If type for some feature used by the tree is not specified (nullptr) then
// it will be chosen randomly and stored to feature_types.
// interactions = false means that only one (randomly chosen) feature is used.
DecisionTree CreateRandomTree(absl::BitGen* rnd, bool interactions,
                              int num_splits,
                              std::vector<QTypePtr>* feature_types);

// Creates random oblivious tree with different types of split conditions.
// The tree will expect features with types specified in features_types.
// If type for some feature used by the tree is not specified (nullptr) then
// it will be chosen randomly and stored to feature_types.
DecisionTree CreateRandomObliviousTree(absl::BitGen* rnd, int depth,
                                       std::vector<QTypePtr>* feature_types);

// Creates balanced random forest with different types of split conditions.
// interactions = false means that only one feature per tree is used.
std::unique_ptr<const DecisionForest> CreateRandomForest(
    absl::BitGen* rnd, int num_features, bool interactions, int min_num_splits,
    int max_num_splits, int num_trees,
    absl::Span<const QTypePtr> feature_types = {});

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_TEST_UTIL_TEST_UTIL_H_
