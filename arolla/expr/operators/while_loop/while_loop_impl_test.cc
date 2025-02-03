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
#include "arolla/expr/operators/while_loop/while_loop_impl.h"

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_format.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr_operators::while_loop_impl {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::testing::EqualsExpr;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

TEST(WhileLoopImplTest, ExtractImmutables) {
  absl::flat_hash_map<Fingerprint, std::string> immutable_names;
  auto immutable_naming_function = [&](const ExprNodePtr& node) -> std::string {
    if (auto it = immutable_names.find(node->fingerprint());
        it != immutable_names.end()) {
      return it->second;
    }
    std::string name = absl::StrFormat("_immutable_%d", immutable_names.size());
    immutable_names.emplace(node->fingerprint(), name);
    return name;
  };

  {
    // Literal are immutable exracted.
    auto expr = Literal(int64_t{1});
    EXPECT_THAT(ExtractImmutables(expr, immutable_naming_function),
                IsOkAndHolds(Pair(
                    EqualsExpr(Placeholder("_immutable_0")),
                    UnorderedElementsAre(Pair(
                        "_immutable_0", EqualsExpr(Literal<int64_t>(1)))))));
  }
  {
    // Leaves are considered immutable within a loop and exracted.
    auto expr = Leaf("fifty");
    EXPECT_THAT(
        ExtractImmutables(expr, immutable_naming_function),
        IsOkAndHolds(Pair(EqualsExpr(Placeholder("_immutable_1")),
                          UnorderedElementsAre(Pair(
                              "_immutable_1", EqualsExpr(Leaf("fifty")))))));
  }
  {
    // Placeholders are considered mutable and not exracted.
    auto expr = Placeholder("seven");
    EXPECT_THAT(ExtractImmutables(expr, immutable_naming_function),
                IsOkAndHolds(Pair(EqualsExpr(expr), IsEmpty())));
  }
  {
    // Leaves in subexpressions are exracted.
    ASSERT_OK_AND_ASSIGN(
        auto expr,
        CallOp("math.add",
               {Leaf("two"),
                CallOp("math.add", {Placeholder("fifty"), Leaf("seven")})}));
    EXPECT_THAT(ExtractImmutables(expr, immutable_naming_function),
                IsOkAndHolds(Pair(
                    EqualsExpr(CallOp(
                        "math.add",
                        {Placeholder("_immutable_3"),
                         CallOp("math.add", {Placeholder("fifty"),
                                             Placeholder("_immutable_2")})})),
                    UnorderedElementsAre(
                        Pair("_immutable_3", EqualsExpr(Leaf("two"))),
                        Pair("_immutable_2", EqualsExpr(Leaf("seven")))))));
  }
  {
    // Literals in subexpressions are NOT exracted.
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {Placeholder("fifty"),
                                                        Literal<int64_t>(7)}));
    EXPECT_THAT(
        ExtractImmutables(expr, immutable_naming_function),
        IsOkAndHolds(Pair(EqualsExpr(CallOp("math.add", {Placeholder("fifty"),
                                                         Literal<int64_t>(7)})),
                          IsEmpty())));
  }
  {
    // If subexpression contains a leaf, literals are extracted as well.
    ASSERT_OK_AND_ASSIGN(
        auto expr57, CallOp("math.add", {Leaf("fifty"), Literal<int64_t>(7)}));
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp("math.add", {expr57, Placeholder("two")}));
    EXPECT_THAT(
        ExtractImmutables(expr, immutable_naming_function),
        IsOkAndHolds(Pair(
            EqualsExpr(CallOp(
                "math.add", {Placeholder("_immutable_4"), Placeholder("two")})),
            UnorderedElementsAre(Pair("_immutable_4", EqualsExpr(expr57))))));
  }
  {
    // Similar subexpressions are merged.
    ASSERT_OK_AND_ASSIGN(
        auto expr,
        CallOp("math.add",
               {CallOp("math.add", {Placeholder("fifty"), Leaf("seven")}),
                Leaf("seven")}));
    EXPECT_THAT(
        ExtractImmutables(expr, immutable_naming_function),
        IsOkAndHolds(Pair(
            EqualsExpr(CallOp(
                "math.add", {CallOp("math.add", {Placeholder("fifty"),
                                                 Placeholder("_immutable_2")}),
                             Placeholder("_immutable_2")})),
            UnorderedElementsAre(
                Pair("_immutable_2", EqualsExpr(Leaf("seven")))))));
  }
  {
    // Parts of non-trivial subexpressions are not extracted, just the
    // subexpression altogeather.
    ASSERT_OK_AND_ASSIGN(
        auto expr,
        CallOp("math.add",
               {CallOp("math.add", {Literal<int64_t>(1), Leaf("fifty")}),
                Placeholder("seven")}));
    EXPECT_THAT(ExtractImmutables(expr, immutable_naming_function),
                IsOkAndHolds(Pair(
                    EqualsExpr(CallOp("math.add", {Placeholder("_immutable_5"),
                                                   Placeholder("seven")})),
                    UnorderedElementsAre(Pair(
                        "_immutable_5",
                        EqualsExpr(CallOp("math.add", {Literal<int64_t>(1),
                                                       Leaf("fifty")})))))));
  }
}

}  // namespace
}  // namespace arolla::expr_operators::while_loop_impl
