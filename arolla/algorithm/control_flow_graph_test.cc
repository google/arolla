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
#include "arolla/algorithm/control_flow_graph.h"

#include <cstddef>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/status_macros_backport.h"  // IWYU pragma: keep, macro definition

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;

TEST(AcyclicCFG, Empty) {
  ASSERT_OK_AND_ASSIGN(auto g, AcyclicCFG::Create({{}}));
  EXPECT_EQ(g->num_nodes(), 1);
  EXPECT_THAT(g->deps(0), IsEmpty());
  EXPECT_THAT(g->reverse_deps(0), IsEmpty());
}

TEST(AcyclicCFG, Simple) {
  ASSERT_OK_AND_ASSIGN(auto g, AcyclicCFG::Create({
                                   {1, 3},  // 0
                                   {2, 3},  // 1
                                   {},      // 2
                                   {},      // 3
                               }));
  EXPECT_EQ(g->num_nodes(), 4);
  EXPECT_THAT(g->deps(0), ElementsAre(1, 3));
  EXPECT_THAT(g->deps(1), ElementsAre(2, 3));
  EXPECT_THAT(g->deps(2), IsEmpty());
  EXPECT_THAT(g->deps(3), IsEmpty());
  EXPECT_THAT(g->reverse_deps(0), IsEmpty());
  EXPECT_THAT(g->reverse_deps(1), ElementsAre(0));
  EXPECT_THAT(g->reverse_deps(2), ElementsAre(1));
  EXPECT_THAT(g->reverse_deps(3), ElementsAre(0, 1));
}

TEST(AcyclicCFG, Errors) {
  EXPECT_THAT(
      AcyclicCFG::Create({{0}}).status(),
      StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("directed")));
  EXPECT_THAT(AcyclicCFG::Create({{1}}).status(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("out of range")));
  EXPECT_THAT(
      AcyclicCFG::Create({{1}, {0}}).status(),
      StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("directed")));
  EXPECT_THAT(
      AcyclicCFG::Create({{1}, {}, {}}).status(),
      StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("reachable")));
}

TEST(DominatorTree, Chain) {
  ASSERT_OK_AND_ASSIGN(auto graph, AcyclicCFG::Create({{1}, {2}, {3}, {}}));
  DominatorTree tree(*graph);
  auto empty_frontier = FindVerticesWithEmptyDominanceFrontier(*graph, tree);
  for (size_t i = 0; i != graph->num_nodes(); ++i) {
    EXPECT_EQ(tree.depth(i), i);
    EXPECT_EQ(tree.parent(i), i == 0 ? i : i - 1) << i;
    if (i + 1 != graph->num_nodes()) {
      EXPECT_THAT(tree.children(i), ElementsAre(i + 1));
    } else {
      EXPECT_THAT(tree.children(i), IsEmpty()) << i;
    }
    EXPECT_TRUE(empty_frontier[i]) << i;
  }
}

// Similar to https://en.wikipedia.org/wiki/Dominator_(graph_theory), but
// without cycles.
TEST(DominatorTree, WikiTest) {
  ASSERT_OK_AND_ASSIGN(auto graph, AcyclicCFG::Create({
                                       {1},        // 0
                                       {2, 3, 5},  // 1
                                       {4},        // 2
                                       {4},        // 3
                                       {},         // 4
                                       {}          // 5
                                   }));
  DominatorTree tree(*graph);
  auto empty_frontier = FindVerticesWithEmptyDominanceFrontier(*graph, tree);
  EXPECT_EQ(tree.depth(0), 0);
  EXPECT_EQ(tree.parent(0), 0);
  EXPECT_THAT(tree.children(0), ElementsAre(1));
  EXPECT_TRUE(empty_frontier[0]);

  EXPECT_EQ(tree.depth(1), 1);
  EXPECT_EQ(tree.parent(1), 0);
  EXPECT_THAT(tree.children(1), ElementsAre(2, 3, 4, 5));
  EXPECT_TRUE(empty_frontier[1]);

  for (size_t i = 2; i != 6; ++i) {
    EXPECT_EQ(tree.depth(i), 2) << i;
    EXPECT_EQ(tree.parent(i), 1) << i;
    EXPECT_THAT(tree.children(i), IsEmpty()) << i;
  }
  EXPECT_FALSE(empty_frontier[2]);
  EXPECT_FALSE(empty_frontier[3]);
  EXPECT_TRUE(empty_frontier[4]);
  EXPECT_TRUE(empty_frontier[5]);
}

TEST(DominatorTree, TwoChainsConnectedNearTheMiddle) {
  /*
  digraph {
    # blue is original graph
    0 -> 1 -> 2 -> 3 -> 4 -> 5 [style=bold,color=blue]
    1 -> 6 -> 7 -> 8 -> 9 [style=bold,color=blue]
    4 -> 7 [style=bold,color=blue]
    5 -> 8 [style=bold,color=blue]

    # red is dominator tree
    0 -> 1 -> 2 -> 3 -> 4 -> 5 [style=bold,color=red]
    1 -> 6  [style=bold,color=red]
    1 -> 7  [style=bold,color=red]
    1 -> 8 -> 9  [style=bold,color=red]
  }
  */
  ASSERT_OK_AND_ASSIGN(auto graph, AcyclicCFG::Create({
                                       {1},     // 0
                                       {2, 6},  // 1
                                       {3},     // 2
                                       {4},     // 3
                                       {5, 7},  // 4
                                       {8},     // 5
                                       {7},     // 6
                                       {8},     // 7
                                       {9},     // 8
                                       {},      // 9
                                   }));
  DominatorTree tree(*graph);
  auto empty_frontier = FindVerticesWithEmptyDominanceFrontier(*graph, tree);

  EXPECT_THAT(tree.parents(), ElementsAre(0, 0, 1, 2, 3, 4, 1, 1, 1, 8));
  EXPECT_THAT(empty_frontier,
              // 0, 1, 8, 9 have empty frontier
              ElementsAre(true, true, false, false, false, false, false, false,
                          true, true));
}

TEST(FindVerticesWithEmptyDominanceFrontier, ExternalizeLeaves) {
  /*
  digraph {
    3 [style=dotted]
    # blue is original graph
    0 -> 1  [style=bold,color=blue]
    0 -> 2  [style=bold,color=blue]
    1 -> 3  [style=dotted,color=blue]
    2 -> 3  [style=dotted,color=blue]

    # new edge added after transformation
    0 -> 3  [style=dashed,color=black]

    # red is dominator tree
    0 -> 1 [style=bold,color=red]
    0 -> 2 [style=bold,color=red]
    0 -> 3 [style=bold,color=red]
  }
  */
  ASSERT_OK_AND_ASSIGN(auto graph, AcyclicCFG::Create({
                                       {1, 2},  // 0
                                       {3},     // 1
                                       {3},     // 2
                                       {},      // 3
                                   }));
  DominatorTree tree(*graph);
  ASSERT_OK_AND_ASSIGN(auto extern3_graph, ExternalizeNodes(*graph, tree, {3}));
  EXPECT_THAT(extern3_graph->deps(0), ElementsAre(1, 2, 3));
  EXPECT_THAT(extern3_graph->deps(1), IsEmpty());
  EXPECT_THAT(extern3_graph->deps(2), IsEmpty());
  EXPECT_THAT(extern3_graph->deps(3), IsEmpty());

  EXPECT_THAT(tree.parents(), ElementsAre(0, 0, 0, 0));
  // without node #3 everything have empty frontier
  EXPECT_THAT(FindVerticesWithEmptyDominanceFrontier(*extern3_graph, tree),
              ElementsAre(true, true, true, true));
}

TEST(FindVerticesWithEmptyDominanceFrontier, ExternalizeInternalNode) {
  // without node #3, node #2 still have node #4 in its frontier
  /*
  digraph {
    # blue is original graph
    3 [style=dotted]
    0 -> 1  [style=bold,color=blue]
    1 -> 3  [style=dotted,color=blue]
    0 -> 2  [style=bold,color=blue]
    2 -> 3  [style=dotted,color=blue]
    3 -> 4  [style=bold,color=blue]

    # new edge added after transformation
    0 -> 3  [style=dashed,color=black]

    # red is dominator tree
    0 -> 1 [style=bold,color=red]
    0 -> 2 [style=bold,color=red]
    0 -> 3 [style=bold,color=red]
    3 -> 4 [style=bold,color=red]
  }
  */
  ASSERT_OK_AND_ASSIGN(auto graph, AcyclicCFG::Create({
                                       {1, 2},  // 0
                                       {3},     // 1
                                       {3},     // 2
                                       {4},     // 3
                                       {},      // 4
                                   }));
  DominatorTree tree(*graph);

  EXPECT_THAT(tree.parents(), ElementsAre(0, 0, 0, 0, 3));
  ASSERT_OK_AND_ASSIGN(auto extern3_graph, ExternalizeNodes(*graph, tree, {3}));
  EXPECT_THAT(extern3_graph->deps(0), ElementsAre(1, 2, 3));
  EXPECT_THAT(extern3_graph->deps(1), IsEmpty());
  EXPECT_THAT(extern3_graph->deps(2), IsEmpty());
  EXPECT_THAT(extern3_graph->deps(3), ElementsAre(4));
  EXPECT_THAT(extern3_graph->deps(4), IsEmpty());
  EXPECT_THAT(FindVerticesWithEmptyDominanceFrontier(*extern3_graph, tree),
              ElementsAre(true, true, true, true, true));
}

}  // namespace
}  // namespace arolla
