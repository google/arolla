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

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//random/distributions.h"
#include "absl//random/random.h"
#include "absl//status/statusor.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/decision_forest/testing/test_util.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/threading.h"

namespace arolla {
namespace {

absl::StatusOr<DecisionForestPtr> CreateTestForest() {
  constexpr float kInf = std::numeric_limits<float>::infinity();
  constexpr auto S = DecisionTreeNodeId::SplitNodeId;
  constexpr auto A = DecisionTreeNodeId::AdjustmentId;
  std::vector<DecisionTree> trees(2);
  trees[0].tag = {.submodel_id = 0};
  trees[0].adjustments = {0.5, 1.5, 2.5, 3.5};
  trees[0].split_nodes = {
      {S(1), S(2), IntervalSplit(0, 1.5, kInf)},
      {A(0), A(2), SetOfValuesSplit<int64_t>(1, {1, 2}, false)},
      {A(1), A(3), IntervalSplit(0, -kInf, 10)}};
  trees[1].tag = {.submodel_id = 1};
  trees[1].adjustments = {-1.0, 1.0};
  trees[1].split_nodes = {{A(0), A(1), IntervalSplit(0, 1, 5)}};
  return DecisionForest::FromTrees(std::move(trees));
}

TEST(BatchedForestEvaluator, EvalBatch) {
  ASSERT_OK_AND_ASSIGN(auto forest, CreateTestForest());
  std::vector<TreeFilter> groups{{.submodels = {0}}, {.submodels = {1}}};
  ASSERT_OK_AND_ASSIGN(auto eval,
                       BatchedForestEvaluator::Compile(*forest, groups));

  FrameLayout::Builder bldr;
  auto in1_slot = bldr.AddSlot<DenseArray<float>>();
  auto in2_slot = bldr.AddSlot<DenseArray<int64_t>>();
  auto out1_slot = bldr.AddSlot<DenseArray<float>>();
  auto out2_slot = bldr.AddSlot<DenseArray<float>>();
  FrameLayout layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  frame.Set(in1_slot,
            CreateDenseArray<float>({0, 0, 1.2, 1.6, 7.0, 13.5, NAN}));
  frame.Set(in2_slot, CreateDenseArray<int64_t>({3, 1, 1, 1, 1, 1, {}}));

  {  // multithreading disabled
    ASSERT_OK(eval->EvalBatch(
        {TypedSlot::FromSlot(in1_slot), TypedSlot::FromSlot(in2_slot)},
        {TypedSlot::FromSlot(out1_slot), TypedSlot::FromSlot(out2_slot)},
        frame));
    EXPECT_THAT(frame.Get(out1_slot),
                ::testing::ElementsAre(0.5, 2.5, 2.5, 3.5, 3.5, 1.5, 0.5));
    EXPECT_THAT(frame.Get(out2_slot),
                ::testing::ElementsAre(-1, -1, 1, 1, -1, -1, -1));
  }

  frame.Set(out1_slot, DenseArray<float>());
  frame.Set(out2_slot, DenseArray<float>());

  {  // multithreading enabled
     // (but only 1 thread is used because row_count is small).
    BatchedForestEvaluator::SetThreading(std::make_unique<StdThreading>(2));
    ASSERT_OK(eval->EvalBatch(
        {TypedSlot::FromSlot(in1_slot), TypedSlot::FromSlot(in2_slot)},
        {TypedSlot::FromSlot(out1_slot), TypedSlot::FromSlot(out2_slot)},
        frame));
    EXPECT_THAT(frame.Get(out1_slot),
                ::testing::ElementsAre(0.5, 2.5, 2.5, 3.5, 3.5, 1.5, 0.5));
    EXPECT_THAT(frame.Get(out2_slot),
                ::testing::ElementsAre(-1, -1, 1, 1, -1, -1, -1));
    BatchedForestEvaluator::SetThreading(nullptr);
  }

  frame.Set(out1_slot, DenseArray<float>());
  frame.Set(out2_slot, DenseArray<float>());

  {  // multithreading enabled, 2 threads are used.
    BatchedForestEvaluator::SetThreading(std::make_unique<StdThreading>(2),
                                         /*min_rows_per_thread=*/1);
    ASSERT_OK(eval->EvalBatch(
        {TypedSlot::FromSlot(in1_slot), TypedSlot::FromSlot(in2_slot)},
        {TypedSlot::FromSlot(out1_slot), TypedSlot::FromSlot(out2_slot)},
        frame));
    EXPECT_THAT(frame.Get(out1_slot),
                ::testing::ElementsAre(0.5, 2.5, 2.5, 3.5, 3.5, 1.5, 0.5));
    EXPECT_THAT(frame.Get(out2_slot),
                ::testing::ElementsAre(-1, -1, 1, 1, -1, -1, -1));
    BatchedForestEvaluator::SetThreading(nullptr);
  }
}

TEST(BatchedForestEvaluator, UnusedInputs) {
  constexpr auto A = DecisionTreeNodeId::AdjustmentId;
  DecisionTree tree;
  tree.adjustments = {-1, 1};
  tree.split_nodes = {{A(0), A(1), IntervalSplit(/*input_id=*/2, 0, 1)}};
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({tree}));
  ASSERT_OK_AND_ASSIGN(auto eval, BatchedForestEvaluator::Compile(*forest));

  FrameLayout::Builder bldr;
  auto unused1_slot = bldr.AddSlot<DenseArray<int64_t>>();
  auto unused2_slot = bldr.AddSlot<DenseArray<int64_t>>();
  auto in_slot = bldr.AddSlot<DenseArray<float>>();
  auto out_slot = bldr.AddSlot<DenseArray<float>>();
  FrameLayout layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  frame.Set(in_slot, CreateDenseArray<float>({-1, 0.5, 2}));

  ASSERT_OK(eval->EvalBatch(
      {TypedSlot::FromSlot(unused1_slot), TypedSlot::FromSlot(unused2_slot),
       TypedSlot::FromSlot(in_slot)},
      {TypedSlot::FromSlot(out_slot)}, frame));

  EXPECT_THAT(frame.Get(out_slot), ::testing::ElementsAre(-1, 1, -1));
}

TEST(BatchedForestEvaluator, AllInputUnused) {
  std::vector<DecisionTree> trees(1);
  trees[0].adjustments = {1.5};
  ASSERT_OK_AND_ASSIGN(DecisionForestPtr forest,
                       DecisionForest::FromTrees(std::move(trees)));

  std::vector<TreeFilter> groups{{.submodels = {0}}};
  ASSERT_OK_AND_ASSIGN(auto eval,
                       BatchedForestEvaluator::Compile(*forest, groups));

  FrameLayout::Builder bldr;
  auto in1_slot = bldr.AddSlot<DenseArray<float>>();
  auto in2_slot = bldr.AddSlot<DenseArray<int64_t>>();
  auto out_slot = bldr.AddSlot<DenseArray<float>>();
  FrameLayout layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  frame.Set(in1_slot,
            CreateDenseArray<float>({0, 0, 1.2, 1.6, 7.0, 13.5, NAN}));
  frame.Set(in2_slot, CreateDenseArray<int64_t>({3, 1, 1, 1, 1, 1, {}}));

  ASSERT_OK(eval->EvalBatch(
      {TypedSlot::FromSlot(in1_slot), TypedSlot::FromSlot(in2_slot)},
      {TypedSlot::FromSlot(out_slot)}, frame));
  EXPECT_THAT(frame.Get(out_slot),
              ::testing::ElementsAre(1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5));
}

TEST(BatchedForestEvaluator, SplitCountPerEvaluator) {
  constexpr int64_t min_num_splits = 10;
  constexpr int64_t max_num_splits = 30;
  constexpr int64_t num_trees = 100;
  constexpr int64_t batch_size = 10;

  absl::BitGen rnd;
  constexpr int64_t min_total_split_count = num_trees * min_num_splits;
  int64_t split_count_per_evaluator = absl::Uniform<int64_t>(
      rnd, min_total_split_count / 5, min_total_split_count * 4 / 5);

  // Create evaluators
  auto forest =
      CreateRandomFloatForest(&rnd, /*num_features=*/10, /*interactions=*/true,
                              min_num_splits, max_num_splits, num_trees);
  ASSERT_OK_AND_ASSIGN(auto evaluator,
                       BatchedForestEvaluator::Compile(*forest));
  ASSERT_OK_AND_ASSIGN(
      // Splits forests into a sum of several smaller forests
      // to reduce cache usage. The result of evaluation should be the same.
      auto subdivided_evaluator,
      BatchedForestEvaluator::Compile(*forest, {TreeFilter()},
                                      {split_count_per_evaluator}));

  // Create memory layout and slots
  std::vector<TypedSlot> slots;
  FrameLayout::Builder layout_builder;
  ASSERT_OK(CreateArraySlotsForForest(*forest, &layout_builder, &slots));
  auto dense_array_output_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto array_output_slot = layout_builder.AddSlot<Array<float>>();
  FrameLayout layout = std::move(layout_builder).Build();

  // Prepare input data
  MemoryAllocation ctx(&layout);
  FramePtr frame = ctx.frame();
  for (auto slot : slots) {
    ASSERT_OK(FillArrayWithRandomValues(batch_size, slot, frame, &rnd));
  }

  // Run
  ASSERT_OK(evaluator->EvalBatch(slots,
                                 {TypedSlot::FromSlot(dense_array_output_slot)},
                                 frame, nullptr, batch_size));
  ASSERT_OK(evaluator->EvalBatch(slots,
                                 {TypedSlot::FromSlot(array_output_slot)},
                                 frame, nullptr, batch_size));

  DenseArray<float> dense_array1 = frame.Get(dense_array_output_slot);
  Array<float> array1 = frame.Get(array_output_slot);
  frame.Set(dense_array_output_slot, DenseArray<float>());
  frame.Set(array_output_slot, Array<float>());

  ASSERT_OK(subdivided_evaluator->EvalBatch(
      slots, {TypedSlot::FromSlot(dense_array_output_slot)}, frame, nullptr,
      batch_size));
  ASSERT_OK(subdivided_evaluator->EvalBatch(
      slots, {TypedSlot::FromSlot(array_output_slot)}, frame, nullptr,
      batch_size));
  DenseArray<float> dense_array2 = frame.Get(dense_array_output_slot);
  Array<float> array2 = frame.Get(array_output_slot);

  ASSERT_EQ(dense_array1.size(), batch_size);
  ASSERT_EQ(array1.size(), batch_size);
  ASSERT_EQ(dense_array2.size(), batch_size);
  ASSERT_EQ(array2.size(), batch_size);
  for (int64_t i = 0; i < batch_size; ++i) {
    bool present = array1[i].present;
    EXPECT_EQ(array2[i].present, present);
    EXPECT_EQ(dense_array1[i].present, present);
    EXPECT_EQ(dense_array2[i].present, present);
    if (present) {
      float value = array1[i].value;
      EXPECT_FLOAT_EQ(array2[i].value, value);
      EXPECT_FLOAT_EQ(dense_array1[i].value, value);
      EXPECT_FLOAT_EQ(dense_array2[i].value, value);
    }
  }
}

}  // namespace
}  // namespace arolla
