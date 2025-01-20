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
#include "arolla/decision_forest/qexpr_operator/pointwise_operator.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

using ::testing::Test;

class DecisionForestPointwiseTest : public Test {
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
  }

  std::shared_ptr<const DecisionForest> forest_;
};

TEST_F(DecisionForestPointwiseTest, Run) {
  auto result_type = MakeTupleQType({GetQType<float>(), GetQType<float>()});
  FrameLayout::Builder bldr;
  auto input1_slot = bldr.AddSlot<OptionalValue<float>>();
  auto input2_slot = bldr.AddSlot<OptionalValue<int64_t>>();
  auto result_slot = AddSlot(result_type, &bldr);

  auto forest_op_type = QExprOperatorSignature::Get(
      {GetOptionalQType<float>(), GetOptionalQType<int64_t>()}, result_type);
  ASSERT_OK_AND_ASSIGN(
      auto forest_op,
      CreatePointwiseDecisionForestOperator(
          forest_, forest_op_type, {{.submodels = {1}}, {.submodels = {2}}}));
  ASSERT_OK_AND_ASSIGN(auto bound_forest_op,
                       forest_op->Bind({TypedSlot::FromSlot(input1_slot),
                                        TypedSlot::FromSlot(input2_slot)},
                                       {result_slot}));

  FrameLayout layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);
  EvaluationContext ctx;

  alloc.frame().Set(input1_slot, 1.0f);
  alloc.frame().Set(input2_slot, 2);
  bound_forest_op->Run(&ctx, alloc.frame());
  EXPECT_OK(ctx.status());
  EXPECT_EQ(alloc.frame().Get(result_slot.SubSlot(0).UnsafeToSlot<float>()),
            1.0);
  EXPECT_EQ(alloc.frame().Get(result_slot.SubSlot(1).UnsafeToSlot<float>()),
            2.5);

  alloc.frame().Set(input1_slot, 2.0f);
  alloc.frame().Set(input2_slot, 1);
  bound_forest_op->Run(&ctx, alloc.frame());
  EXPECT_OK(ctx.status());
  EXPECT_EQ(alloc.frame().Get(result_slot.SubSlot(0).UnsafeToSlot<float>()),
            1.0);
  EXPECT_EQ(alloc.frame().Get(result_slot.SubSlot(1).UnsafeToSlot<float>()),
            3.5);
}

}  // namespace
}  // namespace arolla
