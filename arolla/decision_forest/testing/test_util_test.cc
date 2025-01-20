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

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//log/check.h"
#include "absl//random/random.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

TEST(TestUtilTest, FillWithRandomValue) {
  absl::BitGen rnd;
  FrameLayout::Builder bldr;
  auto opt_float_slot = bldr.AddSlot<OptionalValue<float>>();
  auto opt_int64_slot = bldr.AddSlot<OptionalValue<int64_t>>();
  auto layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);

  alloc.frame().Set(opt_float_slot, OptionalValue<float>(-1.0));
  alloc.frame().Set(opt_int64_slot, OptionalValue<int64_t>(-1));
  ASSERT_OK(FillWithRandomValue(TypedSlot::FromSlot(opt_float_slot),
                                alloc.frame(), &rnd));
  ASSERT_OK(FillWithRandomValue(TypedSlot::FromSlot(opt_int64_slot),
                                alloc.frame(), &rnd));
  EXPECT_NE(OptionalValue<float>(-1.0), alloc.frame().Get(opt_float_slot));
  EXPECT_NE(OptionalValue<int64_t>(-1), alloc.frame().Get(opt_int64_slot));
}

TEST(TestUtilTest, CreateSlotsForForest) {
  absl::BitGen rnd;
  auto forest = CreateRandomForest(&rnd, 5, true, 1, 64, 16);
  FrameLayout::Builder bldr;
  std::vector<TypedSlot> slots;
  CreateSlotsForForest(*forest, &bldr, &slots);
  EXPECT_OK(forest->ValidateInputSlots(slots));
}

TEST(TestUtilTest, CreateRandomFloatTree) {
  absl::BitGen rnd;
  for (size_t depth = 0; depth <= 15; ++depth) {
    auto tree = CreateRandomFloatTree(&rnd, 5, true, (1 << depth) - 1);
    EXPECT_EQ(tree.adjustments.size(), 1 << depth);
    EXPECT_EQ(tree.split_nodes.size(), (1 << depth) - 1);
  }
}

TEST(TestUtilTest, CreateRandomFloatForest) {
  absl::BitGen rnd;
  auto forest = CreateRandomFloatForest(&rnd, 5, true, 1, 64, 16);
  EXPECT_EQ(forest->GetTrees().size(), 16);
  EXPECT_GE(forest->GetRequiredQTypes().size(), 1);
  EXPECT_LE(forest->GetRequiredQTypes().size(), 5);
  for (const DecisionTree& tree : forest->GetTrees()) {
    EXPECT_LE(tree.split_nodes.size(), 64);
  }
}

TEST(TestUtilTest, CreateRandomForest) {
  absl::BitGen rnd;
  auto forest = CreateRandomForest(&rnd, 5, true, 1, 64, 16);
  EXPECT_EQ(forest->GetTrees().size(), 16);
  EXPECT_GE(forest->GetRequiredQTypes().size(), 1);
  EXPECT_LE(forest->GetRequiredQTypes().size(), 5);
  for (const DecisionTree& tree : forest->GetTrees()) {
    EXPECT_LE(tree.split_nodes.size(), 64);
  }
}

TEST(TestUtilTest, CreateRandomObliviousTree) {
  absl::BitGen rnd;
  std::vector<QTypePtr> types(10);
  auto tree = CreateRandomObliviousTree(&rnd, 3, &types);
  ASSERT_EQ(tree.split_nodes.size(), 7);
  EXPECT_EQ(tree.split_nodes[1].condition, tree.split_nodes[2].condition);
  EXPECT_EQ(tree.split_nodes[3].condition, tree.split_nodes[4].condition);
  EXPECT_EQ(tree.split_nodes[4].condition, tree.split_nodes[5].condition);
  EXPECT_EQ(tree.split_nodes[5].condition, tree.split_nodes[6].condition);
}

TEST(TestUtilTest, CreateRandomForestWithoutInteractions) {
  absl::BitGen rnd;
  auto forest = CreateRandomForest(&rnd, 5, false, 512, 512, 1);
  EXPECT_EQ(forest->GetTrees().size(), 1);
  EXPECT_EQ(forest->GetRequiredQTypes().size(), 1);
}

}  // namespace
}  // namespace arolla
