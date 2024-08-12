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
#include "arolla/expr/visitors/substitution.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithNameAnnotation;

class SubstitutionTest : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

TEST_F(SubstitutionTest, SubsByName) {
  ASSERT_OK_AND_ASSIGN(auto x, WithNameAnnotation(Leaf("x"), "lx"));
  ASSERT_OK_AND_ASSIGN(auto y, WithNameAnnotation(Leaf("y"), "ly"));
  ASSERT_OK_AND_ASSIGN(auto z, WithNameAnnotation(Leaf("z"), "lz"));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr, CallOp("math.add", {x, z}));
  EXPECT_THAT(SubstituteByName(expr, {{"ly", z}}),
              IsOkAndHolds(EqualsExpr(expected_expr)));
}

TEST_F(SubstitutionTest, SubstituteLeavesByName) {
  ASSERT_OK_AND_ASSIGN(auto x, WithNameAnnotation(Leaf("x"), "lx"));
  ASSERT_OK_AND_ASSIGN(auto y, WithNameAnnotation(Leaf("y"), "ly"));
  EXPECT_THAT(SubstituteByName(x, {{"lx", y}}), IsOkAndHolds(EqualsExpr(y)));
}

TEST_F(SubstitutionTest, SubstitutePlaceholdersByName) {
  ASSERT_OK_AND_ASSIGN(auto x, WithNameAnnotation(Placeholder("x"), "px"));
  ASSERT_OK_AND_ASSIGN(auto y, WithNameAnnotation(Placeholder("y"), "py"));
  EXPECT_THAT(SubstituteByName(x, {{"px", y}}), IsOkAndHolds(EqualsExpr(y)));
  // Passing leaf key instead of name does not work.
  EXPECT_THAT(SubstituteByName(x, {{"x", y}}), IsOkAndHolds(EqualsExpr(x)));
}

TEST_F(SubstitutionTest, SubstitutePlaceholders) {
  auto px = Placeholder("x");
  auto py = Placeholder("y");
  ASSERT_OK_AND_ASSIGN(auto x, WithNameAnnotation(px, "name"));
  ASSERT_OK_AND_ASSIGN(auto y, WithNameAnnotation(py, "name"));
  EXPECT_THAT(SubstitutePlaceholders(x, {{"x", py}}),
              IsOkAndHolds(EqualsExpr(y)));
  // Passing name instead of placeholder key does not work.
  EXPECT_THAT(SubstitutePlaceholders(x, {{"name", py}}),
              IsOkAndHolds(EqualsExpr(x)));
}

TEST_F(SubstitutionTest, SubstituteLeaves) {
  auto lx = Leaf("x");
  auto ly = Leaf("y");
  ASSERT_OK_AND_ASSIGN(auto x, WithNameAnnotation(lx, "name"));
  ASSERT_OK_AND_ASSIGN(auto y, WithNameAnnotation(ly, "name"));
  EXPECT_THAT(SubstituteLeaves(x, {{"x", ly}}), IsOkAndHolds(EqualsExpr(y)));
  // Passing name instead of leaf key does not work.
  EXPECT_THAT(SubstituteLeaves(x, {{"name", ly}}), IsOkAndHolds(EqualsExpr(x)));
}

TEST_F(SubstitutionTest, SubsByFingerprint) {
  ASSERT_OK_AND_ASSIGN(auto x, WithNameAnnotation(Leaf("x"), "lx"));
  ASSERT_OK_AND_ASSIGN(auto y, WithNameAnnotation(Leaf("y"), "lx"));
  ASSERT_OK_AND_ASSIGN(auto z, WithNameAnnotation(Leaf("z"), "lz"));
  ASSERT_OK_AND_ASSIGN(auto x_add_expr, CallOp("math.add", {x, x}));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x_add_expr, y}));
  absl::flat_hash_map<Fingerprint, ExprNodePtr> subs = {
      {x->fingerprint(), y},
      {x_add_expr->fingerprint(), z},
      {y->fingerprint(), x}};
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_expr, CallOp("math.add", {z, x}));
  EXPECT_THAT(SubstituteByFingerprint(expr, subs),
              IsOkAndHolds(EqualsExpr(expected_expr)));
}

}  // namespace
}  // namespace arolla::expr
