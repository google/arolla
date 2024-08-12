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
#include "arolla/expr/eval/side_output.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithExportAnnotation;
using ::arolla::testing::WithExportValueAnnotation;
using ::testing::Field;
using ::testing::MatchesRegex;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

class SideOutputTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(SideOutputTest, ExtractSideOutputs) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add",
             {WithExportAnnotation(
                  CallOp("math.add", {WithExportValueAnnotation(
                                          Leaf("x"), "out_z", Leaf("z")),
                                      Leaf("y")}),
                  "out_xpy"),
              Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(
      auto expected_expr,
      CallOp("math.add",
             {CallOp("math.add", {Leaf("x"), Leaf("y")}), Leaf("y")}));
  auto expected_out_z = Leaf("z");
  ASSERT_OK_AND_ASSIGN(auto expected_out_xpy,
                       CallOp("math.add", {Leaf("x"), Leaf("y")}));
  EXPECT_THAT(ExtractSideOutputs(expr),
              IsOkAndHolds(AllOf(
                  Field(&ExprWithSideOutputs::expr, EqualsExpr(expected_expr)),
                  Field(&ExprWithSideOutputs::side_outputs,
                        UnorderedElementsAre(
                            Pair("out_z", EqualsExpr(expected_out_z)),
                            Pair("out_xpy", EqualsExpr(expected_out_xpy)))))));
}

TEST_F(SideOutputTest, ExtractSideOutputsExportValueDuplicateNamesError) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add",
             {WithExportValueAnnotation(Leaf("x"), "out_z", Leaf("z")),
              WithExportValueAnnotation(Leaf("y"), "out_z", Leaf("x"))}));
  EXPECT_THAT(ExtractSideOutputs(expr),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex("duplicated export name.*out_z.*")));
}

TEST_F(SideOutputTest, ExtractSideOutputsExportDuplicateNamesError) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add", {WithExportAnnotation(Leaf("x"), "out_z"),
                          WithExportAnnotation(Leaf("y"), "out_z")}));
  EXPECT_THAT(ExtractSideOutputs(expr),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex("duplicated export name.*out_z.*")));
}

TEST_F(SideOutputTest,
       ExtractSideOutputsExportVsExportValueDuplicateNamesError) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add",
             {WithExportValueAnnotation(Leaf("x"), "out_z", Leaf("z")),
              WithExportAnnotation(Leaf("y"), "out_z")}));
  EXPECT_THAT(ExtractSideOutputs(expr),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex("duplicated export name.*out_z.*")));
}

TEST_F(SideOutputTest,
       ExtractSideOutputsExportVsExportValueDuplicateNamesSameExprError) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add",
             {WithExportValueAnnotation(Leaf("x"), "out_z", Leaf("z")),
              WithExportAnnotation(Leaf("z"), "out_z")}));
  ASSERT_OK_AND_ASSIGN(auto expected_expr,
                       CallOp("math.add", {Leaf("x"), Leaf("z")}));
  EXPECT_THAT(ExtractSideOutputs(expr),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex("duplicated export name.*out_z.*")));
}

}  // namespace
}  // namespace arolla::expr
