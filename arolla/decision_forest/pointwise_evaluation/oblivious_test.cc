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
#include "arolla/decision_forest/pointwise_evaluation/oblivious.h"

#include <limits>
#include <memory>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;

constexpr auto S = DecisionTreeNodeId::SplitNodeId;
constexpr auto A = DecisionTreeNodeId::AdjustmentId;
constexpr float inf = std::numeric_limits<float>::infinity();
std::shared_ptr<SplitCondition> Cond(int input_id, float left, float right) {
  return std::make_shared<IntervalSplitCondition>(input_id, left, right);
}

TEST(ObliviousTest, Errors) {
  {  // not power of 2 vertices
    DecisionTree tree;
    tree.split_nodes = {{A(0), S(1), Cond(0, -inf, 1.0)},
                        {A(1), A(2), Cond(0, -1.0, inf)}};
    tree.adjustments = {0.0, 1.0, 2.0};
    EXPECT_EQ(ToObliviousTree(tree), std::nullopt);
  }
  {  // not balanced tree
    DecisionTree tree;
    tree.split_nodes = {{A(0), S(1), Cond(0, -inf, 1.0)},
                        {S(2), A(2), Cond(0, -1.0, inf)},
                        {A(1), A(3), Cond(0, -1.0, inf)}};
    tree.adjustments = {0.0, 1.0, 2.0, 3.0};
    EXPECT_EQ(ToObliviousTree(tree), std::nullopt);
  }
  {  // different splits on one layer
    DecisionTree tree;
    tree.split_nodes = {{S(2), S(1), Cond(0, -inf, 1.0)},
                        {A(1), A(2), Cond(0, -1.0, inf)},
                        {A(0), A(3), Cond(0, 1.0, inf)}};
    tree.adjustments = {0.0, 1.0, 2.0, 3.0};
    EXPECT_EQ(ToObliviousTree(tree), std::nullopt);
  }
}

TEST(ObliviousTest, Ok) {
  {  // depth 0 with weight
    DecisionTree tree;
    tree.adjustments = {2.0};
    tree.weight = 0.5;
    auto oblivious_tree = ToObliviousTree(tree);
    ASSERT_TRUE(oblivious_tree.has_value());
    EXPECT_THAT(oblivious_tree->layer_splits, ElementsAre());
    EXPECT_THAT(oblivious_tree->adjustments, ElementsAre(1.0));
  }
  {  // depth 1 with weight
    DecisionTree tree;
    tree.split_nodes = {{A(0), A(1), Cond(0, -inf, 1.0)}};
    tree.adjustments = {7.0, 3.0};
    tree.weight = 2.0;
    auto oblivious_tree = ToObliviousTree(tree);
    ASSERT_TRUE(oblivious_tree.has_value());
    EXPECT_EQ(oblivious_tree->layer_splits.size(), 1);
    EXPECT_EQ(*oblivious_tree->layer_splits[0], *Cond(0, -inf, 1.0));
    EXPECT_THAT(oblivious_tree->adjustments, ElementsAre(14.0, 6.0));
  }
  {  // depth 2
    DecisionTree tree;
    tree.split_nodes = {{S(2), S(1), Cond(0, -inf, 1.0)},
                        {A(1), A(2), Cond(0, -1.0, inf)},
                        {A(0), A(3), Cond(0, -1.0, inf)}};
    tree.adjustments = {0.0, 1.0, 2.0, 3.0};
    auto oblivious_tree = ToObliviousTree(tree);
    ASSERT_TRUE(oblivious_tree.has_value());
    EXPECT_EQ(oblivious_tree->layer_splits.size(), 2);
    EXPECT_EQ(*oblivious_tree->layer_splits[0], *Cond(0, -inf, 1.0));
    EXPECT_EQ(*oblivious_tree->layer_splits[1], *Cond(0, -1.0, inf));
    EXPECT_THAT(oblivious_tree->adjustments, ElementsAre(0.0, 3.0, 1.0, 2.0));
  }
}

}  // namespace
}  // namespace arolla
