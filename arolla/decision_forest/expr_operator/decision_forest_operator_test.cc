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
#include "arolla/decision_forest/expr_operator/decision_forest_operator.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsExpr;
using ::testing::HasSubstr;

constexpr float inf = std::numeric_limits<float>::infinity();
constexpr auto S = DecisionTreeNodeId::SplitNodeId;
constexpr auto A = DecisionTreeNodeId::AdjustmentId;

absl::StatusOr<DecisionForestPtr> CreateForest() {
  std::vector<DecisionTree> trees(2);
  trees[0].adjustments = {0.5, 1.5, 2.5, 3.5};
  trees[0].tag.submodel_id = 0;
  trees[0].split_nodes = {
      {S(1), S(2), IntervalSplit(0, 1.5, inf)},
      {A(0), A(1), SetOfValuesSplit<int64_t>(1, {5}, false)},
      {A(2), A(3), IntervalSplit(0, -inf, 10)}};
  trees[1].adjustments = {5};
  trees[1].tag.submodel_id = 1;
  return DecisionForest::FromTrees(std::move(trees));
}

TEST(DecisionForestOperatorTest, GetOutputQType) {
  ASSERT_OK_AND_ASSIGN(const DecisionForestPtr forest, CreateForest());

  // No tree filters.
  {
    auto forest_op = std::make_shared<DecisionForestOperator>(
        forest, std::vector<TreeFilter>{});
    EXPECT_THAT(forest_op->GetOutputQType({GetQType<float>()}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("not enough arguments for the decision "
                                   "forest: expected at least 2, got 1")));
    EXPECT_THAT(
        forest_op->GetOutputQType(
            {GetQType<float>(), GetDenseArrayQType<float>()}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("either all forest inputs must be scalars or all "
                           "forest inputs must be arrays, but arg[0] is "
                           "FLOAT32 and arg[1] is DENSE_ARRAY_FLOAT32")));
    EXPECT_THAT(
        forest_op->GetOutputQType({GetQType<float>(), GetQType<int64_t>()}),
        IsOkAndHolds(MakeTupleQType({})));
  }
  // Two tree filters.
  {
    auto forest_op = std::make_shared<DecisionForestOperator>(
        forest, std::vector<TreeFilter>{TreeFilter{.submodels = {0}},
                                        TreeFilter{.submodels = {1, 2}}});
    EXPECT_THAT(forest_op->GetOutputQType({GetQType<float>()}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("not enough arguments for the decision "
                                   "forest: expected at least 2, got 1")));
    EXPECT_THAT(
        forest_op->GetOutputQType(
            {GetQType<float>(), GetDenseArrayQType<float>()}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("either all forest inputs must be scalars or all "
                           "forest inputs must be arrays, but arg[0] is "
                           "FLOAT32 and arg[1] is DENSE_ARRAY_FLOAT32")));
    EXPECT_THAT(
        forest_op->GetOutputQType({GetQType<float>(), GetQType<int64_t>()}),
        IsOkAndHolds(MakeTupleQType({GetQType<float>(), GetQType<float>()})));
  }
}

TEST(DecisionForestOperatorTest, GetOutputQTypeValidatesInputTypes) {
  ASSERT_OK_AND_ASSIGN(const DecisionForestPtr forest, CreateForest());
  auto forest_op = std::make_shared<DecisionForestOperator>(
      forest, std::vector<TreeFilter>{TreeFilter{}});
  // Compatible types.
  EXPECT_THAT(forest_op->GetOutputQType(
                  {GetOptionalQType<float>(), GetOptionalQType<int64_t>()}),
              IsOk());
  EXPECT_THAT(
      forest_op->GetOutputQType({GetQType<int32_t>(), GetQType<int64_t>()}),
      IsOk());
  EXPECT_THAT(forest_op->GetOutputQType(
                  {GetQType<double>(), GetOptionalQType<int64_t>()}),
              IsOk());
  EXPECT_THAT(forest_op->GetOutputQType({GetDenseArrayQType<int32_t>(),
                                         GetDenseArrayQType<int64_t>()}),
              IsOk());
  // Incompatible types.
  EXPECT_THAT(
      forest_op->GetOutputQType({GetQType<float>(), GetQType<float>()}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("value type of input #1 doesn't match: expected to be "
                         "compatible with OPTIONAL_INT64, got FLOAT32")));
  EXPECT_THAT(
      forest_op->GetOutputQType({GetQType<Text>(), GetQType<int64_t>()}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("value type of input #0 doesn't match: expected to be "
                         "compatible with OPTIONAL_FLOAT32, got TEXT")));
}

TEST(DecisionForestOperatorTest, ToLowerLevel) {
  ASSERT_OK_AND_ASSIGN(const DecisionForestPtr forest, CreateForest());
  auto forest_op = std::make_shared<DecisionForestOperator>(
      forest, std::vector<TreeFilter>{TreeFilter{}});
  {  // Types are not known; no conversions.
    ASSERT_OK_AND_ASSIGN(
        auto node,
        expr::CallOp(forest_op, {expr::Leaf("x"), expr::Leaf("y")}));
    EXPECT_THAT(expr::ToLowerNode(node), IsOkAndHolds(EqualsExpr(node)));
  }
  {  // Types exactly match; no conversions.
    auto x = expr::Literal(OptionalValue<float>(1.0f));
    auto y = expr::Literal(OptionalValue<int64_t>(5));
    ASSERT_OK_AND_ASSIGN(auto node, expr::CallOp(forest_op, {x, y}));
    EXPECT_THAT(expr::ToLowerNode(node), IsOkAndHolds(EqualsExpr(node)));
  }
  {  // Some types are not known; conversions are added where possible.
    auto x = expr::Literal<int32_t>(1);
    auto y = expr::Leaf("y");
    ASSERT_OK_AND_ASSIGN(auto node, expr::CallOp(forest_op, {x, y}));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        expr::CallOp(forest_op,
                     {expr::CallOp("core.to_optional",
                                   {expr::CallOp("core.to_float32", {x})}),
                      y}));
    EXPECT_THAT(expr::ToLowerNode(node), IsOkAndHolds(EqualsExpr(expected)));
  }
  {  // Scalars.
    auto x = expr::Literal<double>(1.0);
    auto y = expr::Literal<int64_t>(5);
    ASSERT_OK_AND_ASSIGN(auto node, expr::CallOp(forest_op, {x, y}));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        expr::CallOp(forest_op,
                     {expr::CallOp("core.to_optional",
                                   {expr::CallOp("core.to_float32", {x})}),
                      expr::CallOp("core.to_optional", {y})}));
    EXPECT_THAT(expr::ToLowerNode(node), IsOkAndHolds(EqualsExpr(expected)));
  }
  {  // Arrays.
    auto x = expr::Leaf("x");
    auto y = expr::Leaf("y");
    ASSERT_OK_AND_ASSIGN(
        auto node,
        expr::CallOp(forest_op,
                     {expr::CallOp("annotation.qtype",
                                   {x, expr::Literal(
                                           GetDenseArrayQType<int32_t>())}),
                      expr::CallOp("annotation.qtype",
                                   {y, expr::Literal(
                                           GetDenseArrayQType<int64_t>())})}));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        expr::CallOp(forest_op, {expr::CallOp("core.to_float32",
                                              {node->node_deps()[0]}),
                                 node->node_deps()[1]}));
    EXPECT_THAT(expr::ToLowerNode(node), IsOkAndHolds(EqualsExpr(expected)));
  }
}

TEST(DecisionForestOperatorTest, ExtendedRequiredInputIds) {
  // The forest uses only input #0.
  std::vector<DecisionTree> trees(1);
  trees[0].adjustments = {0.5, 1.5};
  trees[0].split_nodes = {{A(0), A(1), IntervalSplit(0, 1.5, inf)}};
  ASSERT_OK_AND_ASSIGN(const DecisionForestPtr forest,
                       DecisionForest::FromTrees(std::move(trees)));
  // Input #1 is required by the "original" forest. Input #0 is required by
  // `forest` itself, even though it is not listed.
  auto forest_op = std::make_shared<DecisionForestOperator>(
      forest, std::vector<TreeFilter>{TreeFilter{}}, std::vector<int>{1});
  EXPECT_THAT(forest_op->GetOutputQType({GetQType<float>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("not enough arguments for the decision "
                                 "forest: expected at least 2, got 1")));
  EXPECT_THAT(forest_op->GetOutputQType(
                  {GetQType<float>(), GetDenseArrayQType<int64_t>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("either all forest inputs must be scalars or "
                                 "all forest inputs must be arrays")));
  // Value type of input #1 is neither validated nor converted, since the input
  // is not used by `forest`.
  auto x = expr::Literal<int32_t>(1);
  auto y = expr::Literal(Text("abc"));
  ASSERT_OK_AND_ASSIGN(auto node, expr::CallOp(forest_op, {x, y}));
  ASSERT_OK_AND_ASSIGN(
      auto expected,
      expr::CallOp(forest_op,
                   {expr::CallOp("core.to_optional",
                                 {expr::CallOp("core.to_float32", {x})}),
                    y}));
  EXPECT_THAT(expr::ToLowerNode(node), IsOkAndHolds(EqualsExpr(expected)));
}

TEST(DecisionForestOperatorTest, Fingerprint) {
  ASSERT_OK_AND_ASSIGN(const DecisionForestPtr forest, CreateForest());
  std::vector<TreeFilter> filters{TreeFilter{}};
  // `forest` uses inputs #0 and #1.
  auto op = std::make_shared<DecisionForestOperator>(forest, filters);
  auto op_same = std::make_shared<DecisionForestOperator>(
      forest, filters, std::vector<int>{1, 0, 1});
  auto op_extended = std::make_shared<DecisionForestOperator>(
      forest, filters, std::vector<int>{2});
  EXPECT_EQ(op->fingerprint(), op_same->fingerprint());
  EXPECT_NE(op->fingerprint(), op_extended->fingerprint());
}

}  // namespace
}  // namespace arolla
