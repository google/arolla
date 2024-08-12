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
#include "arolla/expr/optimization/peephole_optimizations/reduce.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::EqualsExpr;

class ReduceOptimizationsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitArolla();
    ASSERT_OK_AND_ASSIGN(optimizer_,
                         CreatePeepholeOptimizer({ReduceOptimizations}));
  }

  std::unique_ptr<PeepholeOptimizer> optimizer_;
};

size_t CountNodes(const ExprNodePtr& expr) {
  size_t result = 0;
  return PostOrderTraverse(
      expr,
      [&](const ExprNodePtr& /*node*/,
          absl::Span<const size_t* const> /*visits*/) { return ++result; });
}

TEST_F(ReduceOptimizationsTest, SingleSubstitution) {
  auto a = Leaf("l1");
  auto b = Leaf("l2");
  auto c = Leaf("l3");
  auto d = Leaf("l4");
  ASSERT_OK_AND_ASSIGN(auto ab, CallOp("math.add", {a, b}));
  ASSERT_OK_AND_ASSIGN(auto cd, CallOp("math.add", {c, d}));
  ASSERT_OK_AND_ASSIGN(auto abcd_balanced, CallOp("math.add", {ab, cd}));
  ASSERT_OK_AND_ASSIGN(auto abcd_linear,
                       CallOp("math.add", {CallOp("math.add", {ab, c}), d}));
  ASSERT_OK_AND_ASSIGN(auto abcd_reversed,
                       CallOp("math.add", {a, CallOp("math.add", {b, cd})}));
  ASSERT_OK_AND_ASSIGN(auto abcd_optimized, CallOp("math._add4", {a, b, c, d}));
  EXPECT_THAT(optimizer_->Apply(abcd_balanced),
              IsOkAndHolds(EqualsExpr(abcd_optimized)));
  EXPECT_THAT(optimizer_->Apply(abcd_linear),
              IsOkAndHolds(EqualsExpr(abcd_optimized)));
  EXPECT_THAT(optimizer_->Apply(abcd_reversed),
              IsOkAndHolds(EqualsExpr(abcd_optimized)));
}

TEST_F(ReduceOptimizationsTest, BalancedTree) {
  const int leaf_count = 128;
  std::vector<ExprNodePtr> nodes;
  nodes.reserve(leaf_count);
  for (int i = 0; i < leaf_count; ++i) {
    nodes.push_back(Leaf(absl::StrFormat("l%d", i)));
  }
  while (nodes.size() > 1) {
    for (int64_t i = 0; i < nodes.size() / 2; ++i) {
      nodes[i] = *CallOp("math.add", {nodes[i * 2], nodes[i * 2 + 1]});
    }
    if (nodes.size() % 2 == 1) {
      nodes[nodes.size() / 2] = nodes.back();
    }
    nodes.resize((nodes.size() + 1) / 2);
  }
  ExprNodePtr expr = nodes[0];

  EXPECT_EQ(CountNodes(expr), leaf_count + 127);
  ASSERT_OK_AND_ASSIGN(auto res, optimizer_->Apply(expr));
  EXPECT_EQ(CountNodes(res), leaf_count + 43);
}

TEST_F(ReduceOptimizationsTest, LinearTree) {
  const int leaf_count = 128;
  ExprNodePtr expr = Leaf("l0");
  for (int i = 1; i < leaf_count; ++i) {
    expr = *CallOp("math.add", {expr, Leaf(absl::StrFormat("l%d", i))});
  }

  EXPECT_EQ(CountNodes(expr), leaf_count + 127);
  ASSERT_OK_AND_ASSIGN(auto res, optimizer_->Apply(expr));
  EXPECT_EQ(CountNodes(res), leaf_count + 43);
}

TEST_F(ReduceOptimizationsTest, ReversedLinearTree) {
  const int leaf_count = 128;
  ExprNodePtr expr = Leaf("l0");
  for (int i = 1; i < leaf_count; ++i) {
    expr = *CallOp("math.add", {Leaf(absl::StrFormat("l%d", i)), expr});
  }

  EXPECT_EQ(CountNodes(expr), leaf_count + 127);
  ASSERT_OK_AND_ASSIGN(auto res, optimizer_->Apply(expr));
  EXPECT_EQ(CountNodes(res), leaf_count + 43);
}

}  // namespace
}  // namespace arolla::expr
