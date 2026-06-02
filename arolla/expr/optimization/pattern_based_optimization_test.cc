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
#include "arolla/expr/optimization/pattern_based_optimization.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/testing/testing.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

using ::arolla::expr::pattern_based_optimization_internal::MatchPattern;
using ::arolla::expr::pattern_based_optimization_internal::PatternPlan;
using ::arolla::expr::pattern_based_optimization_internal::SubstituteNodes;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::MockExprOperator;

TEST(PatternBasedOptimizationTest, MatchPattern_MemoryTooSmall) {
  ExprNodePtr leaf = Leaf("x");
  PatternPlan plan;
  plan.capacity = 10;
  std::vector<const ExprNode*> memory(5, nullptr);
  EXPECT_THAT(MatchPattern(plan, *leaf, absl::MakeSpan(memory)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("less than pattern capacity")));
}

TEST(PatternBasedOptimizationTest, MatchPattern_UnfoldAction_Success) {
  auto add_op = MockExprOperator::MakeNice({
      .name = "mock.add",
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr root,
                       CallOp(add_op, {Leaf("a"), Leaf("b")}));

  PatternPlan plan;
  plan.capacity = 3;
  plan.unfold_actions.push_back({
      .mem_index = 0,
      .expected_arity = 2,
      .expected_op_name = "mock.add",
  });

  std::vector<const ExprNode*> memory(3, nullptr);
  EXPECT_THAT(MatchPattern(plan, *root, absl::MakeSpan(memory)),
              IsOkAndHolds(true));
  EXPECT_THAT(ExprNodePtr::NewRef(memory[0]), EqualsExpr(root));
  EXPECT_THAT(ExprNodePtr::NewRef(memory[1]), EqualsExpr(Leaf("a")));
  EXPECT_THAT(ExprNodePtr::NewRef(memory[2]), EqualsExpr(Leaf("b")));
}

TEST(PatternBasedOptimizationTest, MatchPattern_UnfoldAction_MismatchOpName) {
  auto sub_op = MockExprOperator::MakeNice({
      .name = "mock.subtract",
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr root,
                       CallOp(sub_op, {Leaf("a"), Leaf("b")}));

  PatternPlan plan;
  plan.capacity = 3;
  plan.unfold_actions.push_back({
      .mem_index = 0,
      .expected_arity = 2,
      .expected_op_name = "mock.add",
  });

  std::vector<const ExprNode*> memory(3, nullptr);
  EXPECT_THAT(MatchPattern(plan, *root, absl::MakeSpan(memory)),
              IsOkAndHolds(false));
}

TEST(PatternBasedOptimizationTest, MatchPattern_UnfoldAction_MismatchArity) {
  auto add_op = MockExprOperator::MakeNice({
      .name = "mock.add",
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr root,
                       CallOp(add_op, {Leaf("a"), Leaf("b")}));

  PatternPlan plan;
  plan.capacity = 3;
  plan.unfold_actions.push_back({
      .mem_index = 0,
      .expected_arity = 3,
      .expected_op_name = "mock.add",
  });

  std::vector<const ExprNode*> memory(3, nullptr);
  EXPECT_THAT(MatchPattern(plan, *root, absl::MakeSpan(memory)),
              IsOkAndHolds(false));
}

TEST(PatternBasedOptimizationTest, MatchPattern_UnfoldAction_NotOp) {
  ExprNodePtr root = Leaf("a");

  PatternPlan plan;
  plan.capacity = 3;
  plan.unfold_actions.push_back({
      .mem_index = 0,
      .expected_arity = 2,
      .expected_op_name = "mock.add",
  });

  std::vector<const ExprNode*> memory(3, nullptr);
  EXPECT_THAT(MatchPattern(plan, *root, absl::MakeSpan(memory)),
              IsOkAndHolds(false));
}

TEST(PatternBasedOptimizationTest, MatchPattern_CheckFixedFingerprint) {
  ExprNodePtr a = Leaf("a");
  ExprNodePtr b = Leaf("b");

  PatternPlan plan;
  plan.capacity = 1;
  plan.check_fixed_fingerprint_actions.push_back({
      .mem_index = 0,
      .expected_fingerprint = a->fingerprint(),
  });

  std::vector<const ExprNode*> memory(1, nullptr);
  EXPECT_THAT(MatchPattern(plan, *a, absl::MakeSpan(memory)),
              IsOkAndHolds(true));
  EXPECT_THAT(MatchPattern(plan, *b, absl::MakeSpan(memory)),
              IsOkAndHolds(false));
}

TEST(PatternBasedOptimizationTest, MatchPattern_CheckSameFingerprintAs) {
  auto add_op = MockExprOperator::MakeNice({
      .name = "mock.add",
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr root_same,
                       CallOp(add_op, {Leaf("a"), Leaf("a")}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr root_diff,
                       CallOp(add_op, {Leaf("a"), Leaf("b")}));

  PatternPlan plan;
  plan.capacity = 3;
  plan.unfold_actions.push_back({
      .mem_index = 0,
      .expected_arity = 2,
      .expected_op_name = "mock.add",
  });
  plan.check_same_fingerprint_as_actions.push_back({
      .mem_index = 1,
      .other_mem_index = 2,
  });

  std::vector<const ExprNode*> memory(3, nullptr);
  EXPECT_THAT(MatchPattern(plan, *root_same, absl::MakeSpan(memory)),
              IsOkAndHolds(true));
  EXPECT_THAT(MatchPattern(plan, *root_diff, absl::MakeSpan(memory)),
              IsOkAndHolds(false));
}

TEST(PatternBasedOptimizationTest, MatchPattern_CheckPredicate) {
  ExprNodePtr literal_node = Literal(1.0f);
  ExprNodePtr leaf_node = Leaf("x");

  PatternPlan plan;
  plan.capacity = 1;
  plan.check_predicates.push_back({
      .mem_index = 0,
      .predicate = [](const ExprNodePtr& node) { return node->is_literal(); },
  });

  std::vector<const ExprNode*> memory(1, nullptr);
  EXPECT_THAT(MatchPattern(plan, *literal_node, absl::MakeSpan(memory)),
              IsOkAndHolds(true));
  EXPECT_THAT(MatchPattern(plan, *leaf_node, absl::MakeSpan(memory)),
              IsOkAndHolds(false));
}

TEST(PatternBasedOptimizationTest, SubstituteNodes_NoModifications) {
  auto add_op = MockExprOperator::MakeNice({
      .name = "mock.add",
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                       CallOp(add_op, {Leaf("a"), Leaf("b")}));

  PostOrder post_order(expr);
  std::vector<std::pair<size_t, size_t>> mapping;
  std::vector<const ExprNode*> memory;

  EXPECT_THAT(SubstituteNodes(post_order, mapping, memory),
              IsOkAndHolds(EqualsExpr(expr)));
}

TEST(PatternBasedOptimizationTest, SubstituteNodes_WithModifications) {
  auto sub_op = MockExprOperator::MakeNice({
      .name = "mock.subtract",
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr target_expr,
                       CallOp(sub_op, {Leaf("x"), Leaf("y")}));

  PostOrder post_order(target_expr);

  ExprNodePtr a = Leaf("a");
  ExprNodePtr b = Leaf("b");
  std::vector<const ExprNode*> memory = {nullptr, a.get(), b.get()};

  // Replace Leaf("x") (index 0 in post_order) with memory[1] (Leaf("a"))
  // Replace Leaf("y") (index 1 in post_order) with memory[2] (Leaf("b"))
  std::vector<std::pair<size_t, size_t>> mapping = {{0, 1}, {1, 2}};

  ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr,
                       CallOp(sub_op, {Leaf("a"), Leaf("b")}));

  EXPECT_THAT(SubstituteNodes(post_order, mapping, memory),
              IsOkAndHolds(EqualsExpr(expected_expr)));
}

TEST(PatternBasedOptimizationTest, SubstituteNodes_PartialModification) {
  auto sub_op = MockExprOperator::MakeNice({
      .name = "mock.subtract",
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr target_expr,
                       CallOp(sub_op, {Leaf("x"), Leaf("y")}));

  PostOrder post_order(target_expr);

  ExprNodePtr a = Leaf("a");
  std::vector<const ExprNode*> memory = {nullptr, a.get()};

  // Replace Leaf("x") (index 0 in post_order) with memory[1] (Leaf("a"))
  // Leave Leaf("y") (index 1 in post_order) unchanged
  std::vector<std::pair<size_t, size_t>> mapping = {{0, 1}};

  ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr,
                       CallOp(sub_op, {Leaf("a"), Leaf("y")}));

  EXPECT_THAT(SubstituteNodes(post_order, mapping, memory),
              IsOkAndHolds(EqualsExpr(expected_expr)));
}

TEST(PatternBasedOptimizationTest, CreatePatternBasedOptimization_Errors) {
  EXPECT_THAT(
      CreatePatternBasedOptimization(Placeholder("x"), Placeholder("x")),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("from EXPRession is placeholder")));

  EXPECT_THAT(CreatePatternBasedOptimization(Leaf("x"), Leaf("x")),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("leaves are not allowed")));

  auto add_op = MockExprOperator::MakeNice({
      .name = "mock.add",
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr from_with_leaf,
                       CallOp(add_op, {Placeholder("x"), Leaf("y")}));
  EXPECT_THAT(CreatePatternBasedOptimization(from_with_leaf, Placeholder("x")),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("leaves are not allowed")));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr from_add,
                       CallOp(add_op, {Placeholder("x"), Placeholder("x")}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr to_unknown,
                       CallOp(add_op, {Placeholder("x"), Placeholder("y")}));
  EXPECT_THAT(CreatePatternBasedOptimization(from_add, to_unknown),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("unknown placeholder keys in to expression")));

  EXPECT_THAT(
      CreatePatternBasedOptimization(from_add, Placeholder("x"),
                                     {{"y", [](auto) { return true; }}}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("unknown placeholder matcher keys")));
}

TEST(PatternBasedOptimizationTest,
     CreatePatternBasedOptimization_ApplyToRoot_Success) {
  auto add_op = MockExprOperator::MakeNice({
      .name = "mock.add",
      .tags = ExprOperatorTags::kBackend,
  });
  auto sub_op = MockExprOperator::MakeNice({
      .name = "mock.subtract",
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr from,
                       CallOp(add_op, {Placeholder("a"), Placeholder("b")}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr to,
                       CallOp(sub_op, {Placeholder("a"), Placeholder("b")}));

  ASSERT_OK_AND_ASSIGN(auto optimization,
                       CreatePatternBasedOptimization(from, to));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr root,
                       CallOp(add_op, {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expected,
                       CallOp(sub_op, {Leaf("x"), Leaf("y")}));

  EXPECT_THAT(optimization->ApplyToRoot(root),
              IsOkAndHolds(EqualsExpr(expected)));
}

TEST(PatternBasedOptimizationTest,
     CreatePatternBasedOptimization_ApplyToRoot_NoMatch) {
  auto add_op = MockExprOperator::MakeNice({
      .name = "mock.add",
      .tags = ExprOperatorTags::kBackend,
  });
  auto sub_op = MockExprOperator::MakeNice({
      .name = "mock.subtract",
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr from,
                       CallOp(add_op, {Placeholder("a"), Placeholder("b")}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr to,
                       CallOp(sub_op, {Placeholder("a"), Placeholder("b")}));

  ASSERT_OK_AND_ASSIGN(auto optimization,
                       CreatePatternBasedOptimization(from, to));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr root,
                       CallOp(sub_op, {Leaf("x"), Leaf("y")}));

  EXPECT_THAT(optimization->ApplyToRoot(root), IsOkAndHolds(EqualsExpr(root)));
}

TEST(PatternBasedOptimizationTest,
     CreatePatternBasedOptimization_SamePlaceholder_ApplyToRoot) {
  auto add_op = MockExprOperator::MakeNice({
      .name = "mock.add",
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr from,
                       CallOp(add_op, {Placeholder("a"), Placeholder("a")}));
  ExprNodePtr to = Placeholder("a");

  ASSERT_OK_AND_ASSIGN(auto optimization,
                       CreatePatternBasedOptimization(from, to));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr root_same,
                       CallOp(add_op, {Leaf("x"), Leaf("x")}));
  EXPECT_THAT(optimization->ApplyToRoot(root_same),
              IsOkAndHolds(EqualsExpr(Leaf("x"))));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr root_diff,
                       CallOp(add_op, {Leaf("x"), Leaf("y")}));
  EXPECT_THAT(optimization->ApplyToRoot(root_diff),
              IsOkAndHolds(EqualsExpr(root_diff)));
}

TEST(PatternBasedOptimizationTest,
     CreatePatternBasedOptimization_WithPredicate) {
  auto add_op = MockExprOperator::MakeNice({
      .name = "mock.add",
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr from,
                       CallOp(add_op, {Placeholder("a"), Placeholder("b")}));
  ExprNodePtr to = Placeholder("a");

  absl::flat_hash_map<std::string, PeepholeOptimization::NodeMatcher> matchers;
  matchers["a"] = [](const ExprNodePtr& node) { return node->is_literal(); };

  ASSERT_OK_AND_ASSIGN(auto optimization, CreatePatternBasedOptimization(
                                              from, to, std::move(matchers)));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr root_match,
                       CallOp(add_op, {Literal(1.0f), Leaf("y")}));
  EXPECT_THAT(optimization->ApplyToRoot(root_match),
              IsOkAndHolds(EqualsExpr(Literal(1.0f))));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr root_mismatch,
                       CallOp(add_op, {Leaf("x"), Leaf("y")}));
  EXPECT_THAT(optimization->ApplyToRoot(root_mismatch),
              IsOkAndHolds(EqualsExpr(root_mismatch)));
}

TEST(PatternBasedOptimizationTest, CreatePatternBasedOptimization_GetKey) {
  auto add_op = MockExprOperator::MakeNice({
      .name = "mock.add",
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(ExprNodePtr from,
                       CallOp(add_op, {Placeholder("a"), Placeholder("b")}));
  ExprNodePtr to = Placeholder("a");

  ASSERT_OK_AND_ASSIGN(auto optimization,
                       CreatePatternBasedOptimization(from, to));

  EXPECT_EQ(optimization->GetKey(), PeepholeOptimization::PatternKey(from));
}

}  // namespace
}  // namespace arolla::expr
