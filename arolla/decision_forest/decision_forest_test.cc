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
#include "arolla/decision_forest/decision_forest.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::testing {
namespace {

using ::absl_testing::StatusIs;
using ::testing::HasSubstr;
using ::testing::Test;

constexpr float inf = std::numeric_limits<float>::infinity();
constexpr auto S = DecisionTreeNodeId::SplitNodeId;
constexpr auto A = DecisionTreeNodeId::AdjustmentId;

TEST(DecisionForestTest, ForestValidation) {
  DecisionTree tree1;
  tree1.adjustments = {0.5, 1.5, 2.5, 3.5};
  tree1.split_nodes = {{S(1), S(2), IntervalSplit(0, 1.5, inf)},
                       {A(0), A(1), SetOfValuesSplit<int64_t>(1, {5}, false)},
                       {A(2), A(3), IntervalSplit(0, -inf, 10)}};

  DecisionTree tree2;
  tree2.adjustments = {1., 2.};
  tree2.split_nodes = {{A(0), A(1), IntervalSplit(0, 1.5, inf)}};

  DecisionTree tree3;
  tree3.adjustments = {1., 2., 3.};  // Incorrect number of regions.
  tree3.split_nodes = {{A(0), A(1), IntervalSplit(0, 1.5, inf)}};

  DecisionTree tree4;
  tree4.adjustments = {1., 2.};
  tree4.split_nodes = {// Type of input #1 mismatch in tree1 and tree4.
                       {A(0), A(1), IntervalSplit(1, 1.5, inf)}};

  EXPECT_OK(DecisionForest::FromTrees({tree1, tree2}));
  EXPECT_THAT(DecisionForest::FromTrees({tree1, tree3}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("incorrect number of regions")));
  EXPECT_THAT(DecisionForest::FromTrees({tree1, tree4}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("types mismatch in decision forest")));
}

TEST(DecisionForestTest, Fingerprint) {
  DecisionTree tree;
  tree.adjustments = {0.5, 1.5, 2.5, 3.5};
  tree.split_nodes = {{S(1), S(2), IntervalSplit(0, 1.5, inf)},
                      {A(0), A(1), SetOfValuesSplit<int64_t>(1, {5}, false)},
                      {A(2), A(3), IntervalSplit(0, -inf, 10)}};

  ASSERT_OK_AND_ASSIGN(auto forest1, DecisionForest::FromTrees({tree}));
  ASSERT_OK_AND_ASSIGN(auto forest2, DecisionForest::FromTrees({tree}));
  tree.adjustments[1] += 0.1;
  ASSERT_OK_AND_ASSIGN(auto forest3, DecisionForest::FromTrees({tree}));

  EXPECT_EQ(forest1->fingerprint(), forest2->fingerprint());
  EXPECT_NE(forest1->fingerprint(), forest3->fingerprint());
}

TEST(DecisionForestTest, ToDebugString) {
  std::vector<DecisionTree> trees(2);
  trees[0].adjustments = {0.5, 1.5, 2.5, 3.5};
  trees[0].split_nodes = {
      {S(1), S(2), IntervalSplit(0, 1.5, inf)},
      {A(0), A(1), SetOfValuesSplit<int64_t>(1, {5}, false)},
      {A(2), A(3), IntervalSplit(0, -inf, 10)}};
  trees[1].adjustments = {5};
  trees[1].tag.step = 1;
  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees(std::move(trees)));
  EXPECT_EQ(ToDebugString(*forest),
            "DecisionForest {\n"
            "  input #0: OPTIONAL_FLOAT32\n"
            "  input #1: OPTIONAL_INT64\n"
            "  DecisionTree {\n"
            "    tag { step: 0   submodel_id: 0 }\n"
            "    weight: 1.000000\n"
            "    split_nodes {\n"
            "      0: IF #0 in range [1.500000 inf] THEN goto 2 ELSE goto 1\n"
            "      1: IF #1 in set [5] "
            "THEN adjustments[1] ELSE adjustments[0]\n"
            "      2: IF #0 in range [-inf 10.000000] "
            "THEN adjustments[3] ELSE adjustments[2]\n"
            "    }\n"
            "    adjustments: 0.500000 1.500000 2.500000 3.500000\n"
            "  }\n"
            "  DecisionTree {\n"
            "    tag { step: 1   submodel_id: 0 }\n"
            "    weight: 1.000000\n"
            "    split_nodes {\n"
            "    }\n"
            "    adjustments: 5.000000\n"
            "  }\n"
            "}");
}

TEST(DecisionForestTest, InputsValidation) {
  std::vector<DecisionTree> trees(1);
  DecisionTree& tree = trees[0];
  tree.adjustments = {0.5, 1.5, 2.5, 3.5};
  tree.split_nodes = {{S(1), S(2), IntervalSplit(0, 1.5, inf)},
                      {A(0), A(1), SetOfValuesSplit<int64_t>(1, {5}, false)},
                      {A(2), A(3), IntervalSplit(0, -inf, 10)}};

  FrameLayout::Builder bldr;
  auto slot_float = bldr.AddSlot<OptionalValue<float>>();
  auto slot_int64 = bldr.AddSlot<OptionalValue<int64_t>>();
  FrameLayout layout = std::move(bldr).Build();

  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees(std::move(trees)));
  EXPECT_OK(forest->ValidateInputSlots(
      {TypedSlot::FromSlot(slot_float), TypedSlot::FromSlot(slot_int64)}));
  EXPECT_THAT(forest->ValidateInputSlots({}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("not enough arguments")));
  EXPECT_THAT(
      forest->ValidateInputSlots(
          {TypedSlot::FromSlot(slot_float), TypedSlot::FromSlot(slot_float)}),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("type mismatch")));
}

TEST(DecisionForestTest, TreeFilter) {
  DecisionTree::Tag t0{.step = 0, .submodel_id = 0};
  DecisionTree::Tag t1{.step = 1, .submodel_id = 1};
  DecisionTree::Tag t2{.step = 2, .submodel_id = 0};

  TreeFilter f0{};
  TreeFilter f1{.submodels = {0}};
  TreeFilter f2{.submodels = {1}};
  TreeFilter f3{.submodels = {0, 1}};
  TreeFilter f4{.step_range_from = 1};
  TreeFilter f5{.step_range_to = 2};
  TreeFilter f6{.step_range_from = 1, .step_range_to = 2, .submodels = {0}};

  EXPECT_EQ((std::vector<bool>{f0(t0), f0(t1), f0(t2)}),
            (std::vector<bool>{true, true, true}));
  EXPECT_EQ((std::vector<bool>{f1(t0), f1(t1), f1(t2)}),
            (std::vector<bool>{true, false, true}));
  EXPECT_EQ((std::vector<bool>{f2(t0), f2(t1), f2(t2)}),
            (std::vector<bool>{false, true, false}));
  EXPECT_EQ((std::vector<bool>{f3(t0), f3(t1), f3(t2)}),
            (std::vector<bool>{true, true, true}));
  EXPECT_EQ((std::vector<bool>{f4(t0), f4(t1), f4(t2)}),
            (std::vector<bool>{false, true, true}));
  EXPECT_EQ((std::vector<bool>{f5(t0), f5(t1), f5(t2)}),
            (std::vector<bool>{true, true, false}));
  EXPECT_EQ((std::vector<bool>{f6(t0), f6(t1), f6(t2)}),
            (std::vector<bool>{false, false, false}));
}

TEST(DecisionForestTest, GetTreeRootId) {
  DecisionTree tree1;
  tree1.adjustments = {1.0};
  EXPECT_TRUE(GetTreeRootId(tree1).is_leaf());

  DecisionTree tree2;
  tree2.split_nodes = {{A(0), A(1), IntervalSplit(0, 1, 2)}};
  tree2.adjustments = {1.0, 2.0};
  EXPECT_FALSE(GetTreeRootId(tree2).is_leaf());
}

TEST(DecisionForestTest, NaiveEvaluation) {
  std::vector<DecisionTree> trees(3);
  trees[0].adjustments = {0.5, 1.5, 2.5, 3.5};
  trees[0].split_nodes = {
      {S(1), S(2), IntervalSplit(0, 1.5, inf)},
      {A(0), A(1), SetOfValuesSplit<int64_t>(1, {5}, false)},
      {A(2), A(3), IntervalSplit(0, -inf, 10)}};
  // constant trees
  trees[1].adjustments = {5.0};
  trees[1].tag = {1, 1};

  trees[2].adjustments = {2.0};
  trees[2].tag = {2, 0};

  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees(std::move(trees)));
  EXPECT_EQ(forest->step_count(), 3);
  EXPECT_EQ(forest->submodel_count(), 2);

  FrameLayout::Builder bldr;
  auto input1_slot = bldr.AddSlot<OptionalValue<float>>();
  auto input2_slot = bldr.AddSlot<OptionalValue<int64_t>>();
  std::vector<TypedSlot> slots = {TypedSlot::FromSlot(input1_slot),
                                  TypedSlot::FromSlot(input2_slot)};

  FrameLayout layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  frame.Set(input1_slot, 1.0f);
  frame.Set(input2_slot, 5);
  EXPECT_EQ(DecisionForestNaiveEvaluation(*forest, frame, slots), 8.5);

  frame.Set(input1_slot, NAN);
  frame.Set(input2_slot, {});
  EXPECT_EQ(DecisionForestNaiveEvaluation(*forest, frame, slots), 7.5);

  frame.Set(input1_slot, 2.0f);
  frame.Set(input2_slot, 4);
  EXPECT_EQ(DecisionForestNaiveEvaluation(*forest, frame, slots), 10.5);

  // filter by submodel
  EXPECT_EQ(DecisionForestNaiveEvaluation(*forest, frame, slots,
                                          TreeFilter{.submodels = {0}}),
            5.5);
  EXPECT_EQ(DecisionForestNaiveEvaluation(*forest, frame, slots,
                                          TreeFilter{.submodels = {1}}),
            5.0);
  EXPECT_EQ(DecisionForestNaiveEvaluation(*forest, frame, slots,
                                          TreeFilter{.submodels = {0, 1}}),
            10.5);

  // filter by step
  EXPECT_EQ(DecisionForestNaiveEvaluation(*forest, frame, slots,
                                          TreeFilter{.step_range_from = 1}),
            7.0);
  EXPECT_EQ(DecisionForestNaiveEvaluation(*forest, frame, slots,
                                          TreeFilter{.step_range_to = 2}),
            8.5);
}

}  // namespace
}  // namespace arolla::testing
