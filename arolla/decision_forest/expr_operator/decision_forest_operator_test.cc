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
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/init_arolla.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
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

class DecisionForestOperatorTest : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

TEST_F(DecisionForestOperatorTest, GetOutputQType) {
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
        forest_op->GetOutputQType({GetQType<float>(), GetQType<float>()}),
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
        forest_op->GetOutputQType({GetQType<float>(), GetQType<float>()}),
        IsOkAndHolds(MakeTupleQType({GetQType<float>(), GetQType<float>()})));
  }
}

}  // namespace
}  // namespace arolla
