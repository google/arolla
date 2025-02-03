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
#include "arolla/decision_forest/testing/test_util.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

constexpr int kSetOfValuesSize = 10;

// Creates a tree layer by layer. ConditionFactoryFn is called in the order
// of node creation, once for every node.
template <typename ConditionFactoryFn>
DecisionTree CreateRandomTreeImpl(absl::BitGen* rnd, int num_features,
                                  bool interactions, int num_splits,
                                  ConditionFactoryFn condition_factory) {
  DecisionTree tree;
  tree.adjustments.resize(num_splits + 1);
  for (float& val : tree.adjustments) {
    val = absl::Uniform<uint8_t>(*rnd);
  }
  int single_feature_id = absl::Uniform<int32_t>(*rnd, 0, num_features);
  for (int i = 0; i < num_splits; ++i) {
    auto child1 =
        i * 2 + 1 < num_splits
            ? DecisionTreeNodeId::SplitNodeId(i * 2 + 1)
            : DecisionTreeNodeId::AdjustmentId(i * 2 + 1 - num_splits);
    auto child2 =
        i * 2 + 2 < num_splits
            ? DecisionTreeNodeId::SplitNodeId(i * 2 + 2)
            : DecisionTreeNodeId::AdjustmentId(i * 2 + 2 - num_splits);
    int feature_id;
    if (interactions) {
      feature_id = absl::Uniform<int32_t>(*rnd, 0, num_features);
    } else {
      feature_id = single_feature_id;
    }
    tree.split_nodes.push_back({child1, child2, condition_factory(feature_id)});
  }
  return tree;
}

}  // namespace

absl::Status FillWithRandomValue(TypedSlot tslot, FramePtr ctx,
                                 absl::BitGen* rnd, double missed_prob) {
  if (tslot.byte_offset() == FrameLayout::Slot<float>::kUninitializedOffset) {
    return absl::OkStatus();
  }
  bool missed = (absl::Uniform<float>(*rnd, 0, 1) < missed_prob);
  if (tslot.GetType() == GetOptionalQType<float>()) {
    auto slot = tslot.ToSlot<OptionalValue<float>>().value();
    auto val = OptionalValue<float>(absl::Uniform<float>(*rnd, 0, 1));
    ctx.Set(slot, missed ? OptionalValue<float>{} : val);
  } else if (tslot.GetType() == GetOptionalQType<int64_t>()) {
    auto slot = tslot.ToSlot<OptionalValue<int64_t>>().value();
    auto val = OptionalValue<int64_t>(absl::Uniform<int64_t>(*rnd, 0, 1000));
    ctx.Set(slot, missed ? OptionalValue<int64_t>{} : val);
  } else {
    return absl::UnimplementedError(std::string("Unimplemented for type: ") +
                                    std::string(tslot.GetType()->name()));
  }
  return absl::OkStatus();
}

absl::Status FillArrayWithRandomValues(int64_t size, TypedSlot tslot,
                                       FramePtr ctx, absl::BitGen* rnd,
                                       double missed_prob) {
  if (tslot.byte_offset() == FrameLayout::Slot<float>::kUninitializedOffset) {
    return absl::OkStatus();
  }
  if (tslot.GetType() == GetDenseArrayQType<float>()) {
    auto slot = tslot.UnsafeToSlot<DenseArray<float>>();
    DenseArrayBuilder<float> bldr(size);
    for (int64_t i = 0; i < size; ++i) {
      bool missed = (absl::Uniform<float>(*rnd, 0, 1) < missed_prob);
      if (!missed) {
        bldr.Set(i, absl::Uniform<float>(*rnd, 0, 1));
      }
    }
    ctx.Set(slot, std::move(bldr).Build());
  } else if (tslot.GetType() == GetDenseArrayQType<int64_t>()) {
    auto slot = tslot.UnsafeToSlot<DenseArray<int64_t>>();
    DenseArrayBuilder<int64_t> bldr(size);
    for (int64_t i = 0; i < size; ++i) {
      bool missed = (absl::Uniform<float>(*rnd, 0, 1) < missed_prob);
      if (!missed) {
        bldr.Set(i, absl::Uniform<int64_t>(*rnd, 0, 1000));
      }
    }
    ctx.Set(slot, std::move(bldr).Build());
  } else {
    return absl::UnimplementedError(std::string("Unimplemented for type: ") +
                                    std::string(tslot.GetType()->name()));
  }
  return absl::OkStatus();
}

void CreateSlotsForForest(const DecisionForest& forest,
                          FrameLayout::Builder* layout_builder,
                          std::vector<TypedSlot>* slots) {
  auto placeholder =
      TypedSlot::FromSlot(FrameLayout::Slot<float>::UnsafeUninitializedSlot());
  for (auto id_qtype : forest.GetRequiredQTypes()) {
    while (slots->size() <= id_qtype.first) {
      slots->push_back(placeholder);
    }
    QTypePtr qtype = id_qtype.second;
    (*slots)[id_qtype.first] = AddSlot(qtype, layout_builder);
  }
}

absl::Status CreateArraySlotsForForest(const DecisionForest& forest,
                                       FrameLayout::Builder* layout_builder,
                                       std::vector<TypedSlot>* slots) {
  auto placeholder =
      TypedSlot::FromSlot(FrameLayout::Slot<float>::UnsafeUninitializedSlot());
  for (auto id_qtype : forest.GetRequiredQTypes()) {
    while (slots->size() <= id_qtype.first) {
      slots->push_back(placeholder);
    }
    QTypePtr qtype = id_qtype.second;
    if (qtype == GetOptionalQType<float>()) {
      (*slots)[id_qtype.first] =
          TypedSlot::FromSlot(layout_builder->AddSlot<DenseArray<float>>());
    } else if (qtype == GetOptionalQType<int64_t>()) {
      (*slots)[id_qtype.first] =
          TypedSlot::FromSlot(layout_builder->AddSlot<DenseArray<int64_t>>());
    } else {
      return absl::UnimplementedError(std::string("Unimplemented for type: ") +
                                      std::string(qtype->name()));
    }
  }
  if (slots->empty()) {
    // For batched evaluation decision forest should have at least one argument.
    // Otherwise we don't know the size of the batch.
    slots->push_back(
        TypedSlot::FromSlot(layout_builder->AddSlot<DenseArray<float>>()));
  }
  return absl::OkStatus();
}

DecisionTree CreateRandomFloatTree(absl::BitGen* rnd, int num_features,
                                   bool interactions, int num_splits,
                                   double range_split_prob,
                                   double equality_split_prob) {
  return CreateRandomTreeImpl(
      rnd, num_features, interactions, num_splits, [&](int feature_id) {
        float split_type_rnd = absl::Uniform<float>(*rnd, 0, 1);
        if (split_type_rnd < range_split_prob + equality_split_prob) {
          float sp0 = absl::Uniform<uint8_t>(*rnd) / 256.0;
          float sp1 = split_type_rnd < range_split_prob
                          ? absl::Uniform<uint8_t>(*rnd) / 256.0
                          : sp0;
          return IntervalSplit(feature_id, std::min(sp0, sp1),
                               std::max(sp0, sp1));
        } else {
          float split_point = absl::Uniform<uint8_t>(*rnd) / 256.0;
          if (absl::Bernoulli(*rnd, 0.5)) {
            return IntervalSplit(feature_id, -INFINITY, split_point);
          } else {
            return IntervalSplit(feature_id, split_point, +INFINITY);
          }
        }
      });
}

std::unique_ptr<const DecisionForest> CreateRandomFloatForest(
    absl::BitGen* rnd, int num_features, bool interactions, int min_num_splits,
    int max_num_splits, int num_trees) {
  std::vector<DecisionTree> trees;
  trees.reserve(num_trees);
  for (int i = 0; i < num_trees; ++i) {
    int num_splits =
        absl::Uniform<int32_t>(*rnd, min_num_splits, max_num_splits);
    trees.push_back(
        CreateRandomFloatTree(rnd, num_features, interactions, num_splits));
  }
  return DecisionForest::FromTrees(std::move(trees)).value();
}

DecisionTree CreateRandomTree(absl::BitGen* rnd, bool interactions,
                              int num_splits,
                              std::vector<QTypePtr>* feature_types) {
  const float inf = std::numeric_limits<float>::infinity();
  return CreateRandomTreeImpl(
      rnd, feature_types->size(), interactions, num_splits,
      [&](int feature_id) -> std::shared_ptr<SplitCondition> {
        QTypePtr& type = (*feature_types)[feature_id];
        if (!type) {
          type = absl::Bernoulli(*rnd, 0.5) ? GetOptionalQType<float>()
                                            : GetOptionalQType<int64_t>();
        }
        if (type == GetOptionalQType<float>()) {
          float split_point = absl::Uniform<uint8_t>(*rnd) / 256.0;
          if (absl::Bernoulli(*rnd, 0.5)) {
            return IntervalSplit(feature_id, -inf, split_point);
          } else {
            return IntervalSplit(feature_id, split_point, +inf);
          }
        } else {
          absl::flat_hash_set<int64_t> values;
          for (int i = 0; i < kSetOfValuesSize; ++i) {
            values.insert(absl::Uniform<int64_t>(*rnd, 0, 1000));
          }
          return SetOfValuesSplit<int64_t>(feature_id, values,
                                           absl::Bernoulli(*rnd, 0.5));
        }
      });
}

DecisionTree CreateRandomObliviousTree(absl::BitGen* rnd, int depth,
                                       std::vector<QTypePtr>* feature_types) {
  const float inf = std::numeric_limits<float>::infinity();
  std::vector<std::shared_ptr<SplitCondition>> conditions(depth);
  for (int i = 0; i < depth; ++i) {
    int feature_id = absl::Uniform<int32_t>(*rnd, 0, feature_types->size());
    QTypePtr& type = (*feature_types)[feature_id];
    if (!type) {
      type = absl::Bernoulli(*rnd, 0.5) ? GetOptionalQType<float>()
                                        : GetOptionalQType<int64_t>();
    }
    if (type == GetOptionalQType<float>()) {
      float split_point = absl::Uniform<uint8_t>(*rnd) / 256.0;
      if (absl::Bernoulli(*rnd, 0.5)) {
        conditions[i] = IntervalSplit(feature_id, -inf, split_point);
      } else {
        conditions[i] = IntervalSplit(feature_id, split_point, +inf);
      }
    } else {
      absl::flat_hash_set<int64_t> values;
      for (int i = 0; i < kSetOfValuesSize; ++i) {
        values.insert(absl::Uniform<int64_t>(*rnd, 0, 1000));
      }
      conditions[i] = SetOfValuesSplit<int64_t>(feature_id, values,
                                                absl::Bernoulli(*rnd, 0.5));
    }
  }
  int cond_id = 0;
  int node_id = 0;
  return CreateRandomTreeImpl(rnd, feature_types->size(), false,
                              (1 << depth) - 1, [&](int) {
                                node_id++;
                                bool last_in_the_row = node_id & (node_id + 1);
                                if (last_in_the_row) {
                                  return conditions[cond_id];
                                } else {
                                  return conditions[cond_id++];
                                }
                              });
}

// Creates balanced random forest with different types of split conditions.
std::unique_ptr<const DecisionForest> CreateRandomForest(
    absl::BitGen* rnd, int num_features, bool interactions, int min_num_splits,
    int max_num_splits, int num_trees,
    absl::Span<const QTypePtr> feature_types) {
  std::vector<QTypePtr> types;
  for (int feature_id = 0; feature_id < num_features; feature_id++) {
    if (feature_id < feature_types.size() && feature_types[feature_id]) {
      types.push_back(feature_types[feature_id]);
    } else {
      types.push_back(nullptr);  // can be changed by CreateRandomTrees
    }
  }
  std::vector<DecisionTree> trees;
  trees.reserve(num_trees);
  for (int i = 0; i < num_trees; ++i) {
    int num_splits =
        absl::Uniform<int32_t>(*rnd, min_num_splits, max_num_splits);
    trees.push_back(CreateRandomTree(rnd, interactions, num_splits, &types));
  }
  return DecisionForest::FromTrees(std::move(trees)).value();
}

}  // namespace arolla
