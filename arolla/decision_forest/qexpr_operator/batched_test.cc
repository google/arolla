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
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/qexpr_operator/batched_operator.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::Test;

class DecisionForestBatchedTest : public Test {
 protected:
  void SetUp() override {
    const float inf = std::numeric_limits<float>::infinity();
    auto S = DecisionTreeNodeId::SplitNodeId;
    auto A = DecisionTreeNodeId::AdjustmentId;
    std::vector<DecisionTree> trees(2);
    trees[0].tag.submodel_id = 2;
    trees[0].adjustments = {0.5, 1.5, 2.5, 3.5};
    trees[0].split_nodes = {
        {S(1), S(2), IntervalSplit(0, 1.5, inf)},
        {A(0), A(2), SetOfValuesSplit<int64_t>(1, {1, 2}, false)},
        {A(1), A(3), IntervalSplit(0, -inf, 10)}};
    trees[1].tag.submodel_id = 1;
    trees[1].adjustments = {1.0};
    ASSERT_OK_AND_ASSIGN(forest_, DecisionForest::FromTrees(std::move(trees)));

    arg1_ = CreateDenseArray<float>({1.1, 0.5, 3.5, 4});
    arg2_ = CreateDenseArray<int64_t>({0, 1, 2, -3});
  }

  std::shared_ptr<const DecisionForest> forest_;
  DenseArray<float> arg1_;
  DenseArray<int64_t> arg2_;
};

TEST_F(DecisionForestBatchedTest, Run) {
  QTypePtr f32 = GetDenseArrayQType<float>();
  QTypePtr i64 = GetDenseArrayQType<int64_t>();
  auto output_type = MakeTupleQType({f32, f32});
  FrameLayout::Builder bldr;
  auto input1_slot = bldr.AddSlot<DenseArray<float>>();
  auto input2_slot = bldr.AddSlot<DenseArray<int64_t>>();
  auto result_tuple_slot = AddSlot(output_type, &bldr);
  ASSERT_OK_AND_ASSIGN(
      auto result1_slot,
      result_tuple_slot.SubSlot(0).ToSlot<DenseArray<float>>());
  ASSERT_OK_AND_ASSIGN(
      auto result2_slot,
      result_tuple_slot.SubSlot(1).ToSlot<DenseArray<float>>());

  auto forest_op_type = QExprOperatorSignature::Get({f32, i64}, output_type);
  ASSERT_OK_AND_ASSIGN(
      auto forest_op,
      CreateBatchedDecisionForestOperator(
          forest_, forest_op_type, {{.submodels = {1}}, {.submodels = {2}}}));

  ASSERT_OK_AND_ASSIGN(auto bound_forest_op,
                       forest_op->Bind({TypedSlot::FromSlot(input1_slot),
                                        TypedSlot::FromSlot(input2_slot)},
                                       {result_tuple_slot}));

  FrameLayout layout = std::move(bldr).Build();
  RootEvaluationContext root_ctx(&layout);
  EvaluationContext ctx(root_ctx);

  root_ctx.Set(input1_slot, arg1_);
  root_ctx.Set(input2_slot, arg2_);
  bound_forest_op->Run(&ctx, root_ctx.frame());
  EXPECT_OK(ctx.status());
  const auto& res1 = root_ctx.Get(result1_slot);
  const auto& res2 = root_ctx.Get(result2_slot);

  EXPECT_EQ(arg1_.size(), res1.size());
  EXPECT_THAT(res1, ElementsAre(1.0, 1.0, 1.0, 1.0));

  EXPECT_EQ(arg1_.size(), res2.size());
  EXPECT_THAT(res2, ElementsAre(0.5, 2.5, 3.5, 3.5));
}

}  // namespace
}  // namespace arolla
