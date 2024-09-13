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
#include "arolla/decision_forest/expr_operator/forest_model.h"

#include <cstdint>
#include <limits>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithNameAnnotation;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::NotNull;
using ::testing::WhenDynamicCastTo;

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

absl::StatusOr<ForestModelPtr> CreateForestModelOp() {
  ForestModel::SubmodelIds submodel_ids = {{"X", {0}}, {"Y", {1}}};
  ASSIGN_OR_RETURN(auto preprocessing,
                   expr::CallOp("math.add", {expr::Placeholder("arg"),
                                             expr::Literal<int64_t>(1)}));
  ASSIGN_OR_RETURN(auto expression,
                   expr::CallOp("math.add", {expr::Placeholder("X"),
                                             expr::Placeholder("Y")}));
  ASSIGN_OR_RETURN(auto forest, CreateForest());
  return ForestModel::Create({.forest = std::move(forest),
                              .submodel_ids = std::move(submodel_ids),
                              .inputs = {{"p1"}, {"p2", preprocessing}},
                              .expression = expression});
}

absl::Status InitAlias() {
  static absl::NoDestructor<absl::Status> init_status(
      expr::RegisterOperatorAlias("alias_math.add", "math.add").status());
  return *init_status;
}

class ForestModelTest : public ::testing::Test {
  void SetUp() override { CHECK_OK(InitAlias()); }
};

TEST_F(ForestModelTest, NotEnoughArgs) {
  ForestModel::ConstructorArgs model_data;
  model_data.submodel_ids = {{"X", {0}}, {"Y", {1}}};
  ASSERT_OK_AND_ASSIGN(model_data.expression,
                       expr::CallOp("math.add", {expr::Placeholder("X"),
                                                 expr::Placeholder("Y")}));
  ASSERT_OK_AND_ASSIGN(model_data.forest, CreateForest());
  model_data.inputs = {{"p1"}};
  EXPECT_THAT(ForestModel::Create(model_data),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("not enough args")));
}

TEST_F(ForestModelTest, ParameterNameCollision) {
  ForestModel::ConstructorArgs model_data;
  model_data.submodel_ids = {{"X", {0}}, {"Y", {1}}};
  ASSERT_OK_AND_ASSIGN(
      auto preprocessing,
      expr::CallOp("math.add", {expr::Placeholder("arg"),
                                expr::Literal<OptionalValue<int64_t>>(1)}));
  ASSERT_OK_AND_ASSIGN(model_data.expression,
                       expr::CallOp("math.add", {expr::Placeholder("X"),
                                                 expr::Placeholder("Y")}));

  ASSERT_OK_AND_ASSIGN(model_data.forest, CreateForest());

  model_data.inputs = {{"p1"}, {"p1", preprocessing}};
  EXPECT_THAT(ForestModel::Create(model_data),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("non-unique parameter name: 'p1'")));

  model_data.inputs = {{"X"}, {"p2", preprocessing}};
  EXPECT_THAT(
      ForestModel::Create(model_data),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("name collision of an input and a submodel: 'X'")));
}

TEST_F(ForestModelTest, IncorrectExpression) {
  ASSERT_OK_AND_ASSIGN(DecisionForestPtr forest, DecisionForest::FromTrees({}));
  {
    ASSERT_OK_AND_ASSIGN(expr::ExprNodePtr expression,
                         expr::CallOp("math.add", {expr::Placeholder("X"),
                                                   expr::Placeholder("Y")}));

    EXPECT_THAT(ForestModel::Create({.forest = forest,
                                     .submodel_ids = {{"X", {0}}},
                                     .inputs = {{"p1"}, {"p2"}},
                                     .expression = expression}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("P.Y doesn't correspond to any input and it "
                                   "is not found in submodel_ids")));
  }
  {
    expr::ExprNodePtr expression = expr::Placeholder("X");
    EXPECT_THAT(
        ForestModel::Create({.forest = forest,
                             .submodel_ids = {{"X", {0}}, {"Y", {1}}},
                             .expression = expression}),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("submodels [Y] are not used in the expression, but are "
                      "mentioned in submodel_ids")));
  }
  {
    expr::ExprNodePtr expression = expr::Leaf("X");
    EXPECT_THAT(
        ForestModel::Create({.forest = forest, .expression = expression}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("leaves are not allowed in an expression")));
  }
}

TEST_F(ForestModelTest, UsingInputInExpression) {
  ASSERT_OK_AND_ASSIGN(auto expression,
                       expr::CallOp("math.add", {expr::Placeholder("X"),
                                                 expr::Placeholder("p1")}));
  auto f1 = expr::Literal<float>(1.0);
  auto i5 = expr::Literal<int64_t>(5);

  ASSERT_OK_AND_ASSIGN(auto forest, CreateForest());
  ASSERT_OK_AND_ASSIGN(auto model_op,
                       ForestModel::Create({.forest = forest,
                                            .submodel_ids = {{"X", {0}}},
                                            .inputs = {{"p1"}, {"p2"}},
                                            .expression = expression}));
  ASSERT_OK_AND_ASSIGN(auto model, expr::CallOp(model_op, {f1, i5}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));
  ASSERT_TRUE(expanded_model->is_op());
  EXPECT_TRUE(IsRegisteredOperator(expanded_model->op()));
  EXPECT_TRUE(IsBackendOperator(*DecayRegisteredOperator(expanded_model->op()),
                                "math.add"));
  EXPECT_THAT(expanded_model->node_deps()[1], EqualsExpr(f1));
}

TEST_F(ForestModelTest, QTypePropagation) {
  ASSERT_OK_AND_ASSIGN(auto model_op, CreateForestModelOp());
  ASSERT_OK_AND_ASSIGN(
      auto model,
      expr::CallOp(model_op, {expr::Literal<OptionalValue<float>>(1.0),
                              expr::Literal<int64_t>(5)}));
  EXPECT_EQ(model->qtype(), GetQType<float>());
}

TEST_F(ForestModelTest, QTypePropagationUsesPreprocessing) {
  ForestModel::ConstructorArgs model_data;
  model_data.submodel_ids = {{"X", {0, 1}}};
  ASSERT_OK_AND_ASSIGN(auto preprocessing,
                       expr::CallOp("core.const_with_shape",
                                    {expr::Literal<DenseArrayShape>({5}),
                                     expr::Placeholder("arg")}));
  model_data.expression = expr::Placeholder("X");
  ASSERT_OK_AND_ASSIGN(model_data.forest, CreateForest());

  model_data.inputs = {{"p1", preprocessing}, {"p2", preprocessing}};
  ASSERT_OK_AND_ASSIGN(auto model_op, ForestModel::Create(model_data));
  ASSERT_OK_AND_ASSIGN(
      auto model,
      expr::CallOp(model_op, {expr::Literal<OptionalValue<float>>(1.0),
                              expr::Literal<int64_t>(5)}));
  EXPECT_EQ(model->qtype(), GetDenseArrayQType<float>());
}

TEST_F(ForestModelTest, QTypePropagationPlainSumWithBroadcasting) {
  ForestModel::ConstructorArgs model_data;
  model_data.submodel_ids = {{"X", {0, 1}}};
  ASSERT_OK_AND_ASSIGN(
      model_data.expression,
      expr::CallOp("math.add",
                   {expr::Literal(CreateDenseArray<float>({1., 2., 3.})),
                    expr::Placeholder("X")}));
  ASSERT_OK_AND_ASSIGN(model_data.forest, CreateForest());
  model_data.inputs = {{"p1"}, {"p2"}};
  ASSERT_OK_AND_ASSIGN(auto model_op, ForestModel::Create(model_data));
  ASSERT_OK_AND_ASSIGN(
      auto model,
      expr::CallOp(model_op, {expr::Literal<OptionalValue<float>>(1.0),
                              expr::Literal<int64_t>(5)}));
  EXPECT_EQ(model->qtype(), GetDenseArrayQType<float>());
  ASSERT_OK_AND_ASSIGN(auto lowered, expr::ToLowest(model));
  EXPECT_EQ(lowered->qtype(), GetDenseArrayQType<float>());
}

TEST_F(ForestModelTest, EmptyForest) {
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({}));
  expr::ExprNodePtr expression = expr::Literal<float>(0.5);
  ASSERT_OK_AND_ASSIGN(auto model_op,
                       ForestModel::Create({.forest = std::move(forest),
                                            .expression = expression}));
  ASSERT_OK_AND_ASSIGN(auto model, expr::CallOp(model_op, {}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));
  // We check that the forest was eliminated during optimization.
  EXPECT_THAT(expanded_model, EqualsExpr(expression));
}

TEST_F(ForestModelTest, ToLower) {
  ASSERT_OK_AND_ASSIGN(auto model_op, CreateForestModelOp());
  {  // literals
    ASSERT_OK_AND_ASSIGN(auto model,
                         expr::CallOp(model_op, {expr::Literal<float>(1.0),
                                                 expr::Literal<int64_t>(5)}));
    ASSERT_OK_AND_ASSIGN(model, WithNameAnnotation(model, "forest"));
    ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));
    EXPECT_NE(model->fingerprint(), expanded_model->fingerprint());
    EXPECT_EQ(ReadNameAnnotation(expanded_model), "forest");
  }
  {  // leaves without type information
    ASSERT_OK_AND_ASSIGN(
        auto model, expr::CallOp(model_op, {expr::Leaf("f"), expr::Leaf("i")}));
    ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));
    // Forest model can not be expanded without information about types,
    // so expanded_model is equal to model.
    EXPECT_EQ(model->fingerprint(), expanded_model->fingerprint());
  }
}

// returns P.A + (P.B op P.C)
absl::StatusOr<expr::ExprNodePtr> GetExpressionForTest(std::string A,
                                                       std::string B,
                                                       std::string C,
                                                       std::string op) {
  return expr::CallOp(
      "alias_math.add",
      {expr::Placeholder(A),
       expr::CallOp(op, {expr::Placeholder(B), expr::Placeholder(C)})});
}

TEST_F(ForestModelTest, ToLowerMergeSubmodels) {
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({}));
  ASSERT_OK_AND_ASSIGN(auto expression,  // P.input + (P.X + P.Y)
                       GetExpressionForTest("input", "X", "Y", "math.add"));

  ASSERT_OK_AND_ASSIGN(
      auto model_op,
      ForestModel::Create({.forest = std::move(forest),
                           .submodel_ids = {{"X", {0, 2}}, {"Y", {1, 3}}},
                           .inputs = {{"input"}},
                           .expression = expression}));
  ASSERT_OK_AND_ASSIGN(auto model,
                       expr::CallOp(model_op, {expr::Literal<float>(1.0)}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));
  EXPECT_TRUE(IsRegisteredOperator(expanded_model->op()));
  EXPECT_TRUE(IsBackendOperator(*DecayRegisteredOperator(expanded_model->op()),
                                "math.add"));
  EXPECT_THAT(expanded_model->node_deps()[0]->op().get(),
              WhenDynamicCastTo<const expr::GetNthOperator*>(NotNull()));
}

TEST_F(ForestModelTest, MergeDuplicatedSubmodels) {
  std::vector<DecisionTree> trees(2);
  trees[0].adjustments = {1.0};
  trees[0].tag.submodel_id = 0;
  trees[1].adjustments = {3.0};
  trees[1].tag.submodel_id = 1;
  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees({std::move(trees)}));
  ASSERT_OK_AND_ASSIGN(auto expression,  // P.X + (P.Y + P.X)
                       GetExpressionForTest("X", "Y", "X", "math.add"));

  ASSERT_OK_AND_ASSIGN(
      auto model_op,
      ForestModel::Create({.forest = std::move(forest),
                           .submodel_ids = {{"X", {0}}, {"Y", {1}}},
                           .expression = expression}));
  ASSERT_OK_AND_ASSIGN(auto model, expr::CallOp(model_op, {}));

  FrameLayout::Builder layout_builder;
  ASSERT_OK_AND_ASSIGN(
      auto executable_model,
      CompileAndBindForDynamicEvaluation(expr::DynamicEvaluationEngineOptions(),
                                         &layout_builder, model, {}));
  FrameLayout layout = std::move(layout_builder).Build();
  ASSERT_OK_AND_ASSIGN(const FrameLayout::Slot<float> output,
                       executable_model->output_slot().ToSlot<float>());

  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_model->InitializeLiterals(&ctx));
  EXPECT_THAT(executable_model->Execute(&ctx), IsOk());
  EXPECT_THAT(ctx.Get(output), Eq(5.0f));
}

TEST_F(ForestModelTest, DuplicatedNodes) {
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({}));
  ASSERT_OK_AND_ASSIGN(auto expression,  // P.input + (P.X + P.input)
                       GetExpressionForTest("input", "X", "input", "math.add"));

  ASSERT_OK_AND_ASSIGN(auto model_op,
                       ForestModel::Create({.forest = std::move(forest),
                                            .submodel_ids = {{"X", {0}}},
                                            .inputs = {{"input"}},
                                            .expression = expression}));
  ASSERT_OK_AND_ASSIGN(auto model,
                       expr::CallOp(model_op, {expr::Leaf("input")}));

  FrameLayout::Builder layout_builder;
  auto input_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      auto executable_model,
      CompileAndBindForDynamicEvaluation(
          expr::DynamicEvaluationEngineOptions(), &layout_builder, model,
          {{"input", TypedSlot::FromSlot(input_slot)}}));
  FrameLayout layout = std::move(layout_builder).Build();
  ASSERT_OK_AND_ASSIGN(const FrameLayout::Slot<float> output,
                       executable_model->output_slot().ToSlot<float>());

  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_model->InitializeLiterals(&ctx));
  ctx.Set(input_slot, 3.1f);
  EXPECT_THAT(executable_model->Execute(&ctx), IsOk());
  EXPECT_FLOAT_EQ(ctx.Get(output), 6.2f);
}

TEST_F(ForestModelTest, ToLowerSingleBag) {
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({}));
  ASSERT_OK_AND_ASSIGN(
      auto expression,  // P.input + (P.X * P.Y)
      GetExpressionForTest("input", "X", "Y", "math.multiply"));

  ASSERT_OK_AND_ASSIGN(
      auto model_op,
      ForestModel::Create({.forest = std::move(forest),
                           .submodel_ids = {{"X", {0}}, {"Y", {1}}},
                           .inputs = {{"input"}},
                           .expression = expression}));
  ASSERT_OK_AND_ASSIGN(auto model,
                       expr::CallOp(model_op, {expr::Literal<float>(1.0)}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));
  EXPECT_TRUE(IsRegisteredOperator(expanded_model->op()));
  EXPECT_TRUE(IsBackendOperator(*DecayRegisteredOperator(expanded_model->op()),
                                "math.add"));
  EXPECT_TRUE(expanded_model->node_deps()[0]->is_literal());
  EXPECT_TRUE(IsRegisteredOperator(expanded_model->node_deps()[1]->op()));
  EXPECT_TRUE(IsBackendOperator(
      *DecayRegisteredOperator(expanded_model->node_deps()[1]->op()),
      "math.multiply"));
}

TEST_F(ForestModelTest, ToLowerExpandBags) {
  std::vector<DecisionTree> trees(4);
  trees[0].adjustments = {1.0};
  trees[0].tag.submodel_id = 0;
  trees[1].adjustments = {2.0};
  trees[1].tag.submodel_id = 1;
  trees[2].adjustments = {4.0};
  trees[2].tag.submodel_id = 2;
  trees[3].adjustments = {8.0};
  trees[3].tag.submodel_id = 3;
  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees({std::move(trees)}));
  ASSERT_OK_AND_ASSIGN(
      auto expression,  // P.input + (P.X * P.Y)
      GetExpressionForTest("input", "X", "Y", "math.multiply"));

  ASSERT_OK_AND_ASSIGN(
      auto model_op,
      ForestModel::Create({.forest = std::move(forest),
                           .submodel_ids = {{"X", {0, 2}}, {"Y", {1, 3}}},
                           .inputs = {{"input"}},
                           .expression = expression}));
  ASSERT_OK_AND_ASSIGN(auto model,
                       expr::CallOp(model_op, {expr::Literal<float>(1.2)}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));
  EXPECT_TRUE(IsBackendOperator(*DecayRegisteredOperator(expanded_model->op()),
                                "math.divide"));

  ASSERT_OK_AND_ASSIGN(
      auto model_fn,
      (ExprCompiler<std::tuple<float>, float>()).CompileOperator(model_op));
  ASSERT_OK_AND_ASSIGN(float res, model_fn(1.2));
  EXPECT_FLOAT_EQ(res, 69.2f);
}

TEST_F(ForestModelTest, OutOfBagFilters) {
  std::vector<DecisionTree> trees(4);
  trees[0].adjustments = {1.0};
  trees[0].tag.submodel_id = 0;
  trees[1].adjustments = {2.0};
  trees[1].tag.submodel_id = 1;
  trees[2].adjustments = {4.0};
  trees[2].tag.submodel_id = 2;
  trees[3].adjustments = {8.0};
  trees[3].tag.submodel_id = 3;
  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees({std::move(trees)}));
  ASSERT_OK_AND_ASSIGN(
      auto expression,  // P.input + (P.X * P.Y)
      GetExpressionForTest("input", "X", "Y", "math.multiply"));
  ASSERT_OK_AND_ASSIGN(auto filter0,
                       expr::CallOp("core.less", {expr::Placeholder("input"),
                                                  expr::Literal(2.0f)}));
  ASSERT_OK_AND_ASSIGN(auto filter1,
                       expr::CallOp("core.less", {expr::Literal(2.0f),
                                                  expr::Placeholder("input")}));
  ASSERT_OK_AND_ASSIGN(
      auto model_op,
      ForestModel::Create({.forest = std::move(forest),
                           .submodel_ids = {{"X", {0, 2}}, {"Y", {1, 3}}},
                           .inputs = {{"input"}},
                           .expression = expression,
                           .oob_filters = std::vector{filter0, filter1}}));
  ASSERT_OK_AND_ASSIGN(auto model,
                       expr::CallOp(model_op, {expr::Literal<float>(1.2)}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));
  EXPECT_TRUE(IsBackendOperator(*DecayRegisteredOperator(expanded_model->op()),
                                "math.divide"));

  ASSERT_OK_AND_ASSIGN(auto model_fn,
                       (ExprCompiler<std::tuple<float>, OptionalValue<float>>())
                           .CompileOperator(model_op));
  {
    ASSERT_OK_AND_ASSIGN(OptionalValue<float> res, model_fn(1));
    EXPECT_EQ(res, 9.0f);
  }
  {
    ASSERT_OK_AND_ASSIGN(OptionalValue<float> res, model_fn(2));
    EXPECT_EQ(res, OptionalValue<float>{});
  }
  {
    ASSERT_OK_AND_ASSIGN(OptionalValue<float> res, model_fn(3));
    EXPECT_EQ(res, 131.0f);
  }
}

TEST_F(ForestModelTest, BagsAndTruncation) {
  std::vector<DecisionTree> trees(4);
  trees[0].adjustments = {1.0};
  trees[0].tag = {.step = 0, .submodel_id = 0};
  trees[1].adjustments = {2.0};
  trees[1].tag = {.step = 0, .submodel_id = 1};
  trees[2].adjustments = {4.0};
  trees[2].tag = {.step = 1, .submodel_id = 2};
  trees[3].adjustments = {8.0};
  trees[3].tag = {.step = 1, .submodel_id = 3};
  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees({std::move(trees)}));
  ASSERT_OK_AND_ASSIGN(
      auto expression,  // P.input + (P.X * P.Y)
      GetExpressionForTest("input", "X", "Y", "math.multiply"));

  ASSERT_OK_AND_ASSIGN(
      auto model_op,
      ForestModel::Create({.forest = std::move(forest),
                           .submodel_ids = {{"X", {0, 2}}, {"Y", {1, 3}}},
                           .inputs = {{"input"}},
                           .expression = expression,
                           .truncation_step = 1}));
  ASSERT_OK_AND_ASSIGN(
      auto model_fn,
      (ExprCompiler<std::tuple<float>, float>()).CompileOperator(model_op));
  ASSERT_OK_AND_ASSIGN(float res, model_fn(1.2));
  EXPECT_FLOAT_EQ(res, 5.2f);
}

TEST_F(ForestModelTest, ConversionToOptional) {
  ASSERT_OK_AND_ASSIGN(const auto model_op, CreateForestModelOp());
  const auto input = expr::Literal<float>(1.0);
  ASSERT_OK_AND_ASSIGN(const auto converted_input,
                       expr::CallOp("core.to_optional", {input}));

  ASSERT_OK_AND_ASSIGN(
      auto model, expr::CallOp(model_op, {input, expr::Literal<int64_t>(5)}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));

  ASSERT_OK_AND_ASSIGN(
      auto model_with_converted_input,
      expr::CallOp(model_op, {converted_input, expr::Literal<int64_t>(5)}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model_with_converted_input,
                       expr::ToLowest(model_with_converted_input));

  // Check that core.to_optional was automatically inserted into expanded_model.
  EXPECT_THAT(expanded_model_with_converted_input, EqualsExpr(expanded_model));
}

TEST_F(ForestModelTest, ConversionFromDouble) {
  ASSERT_OK_AND_ASSIGN(const auto model_op, CreateForestModelOp());
  const auto input = expr::Literal<double>(1.0);
  ASSERT_OK_AND_ASSIGN(const auto converted_input,
                       expr::CallOp("core.to_float32", {input}));

  ASSERT_OK_AND_ASSIGN(
      auto model, expr::CallOp(model_op, {input, expr::Literal<int64_t>(5)}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));

  ASSERT_OK_AND_ASSIGN(
      auto model_with_converted_input,
      expr::CallOp(model_op, {converted_input, expr::Literal<int64_t>(5)}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model_with_converted_input,
                       expr::ToLowest(model_with_converted_input));

  // Check that core.to_optional was automatically inserted into expanded_model.
  EXPECT_THAT(expanded_model_with_converted_input, EqualsExpr(expanded_model));
}

TEST_F(ForestModelTest, ConversionFromInteger) {
  ASSERT_OK_AND_ASSIGN(const auto model_op, CreateForestModelOp());
  const auto input = expr::Literal<int>(1);
  ASSERT_OK_AND_ASSIGN(const auto converted_input,
                       expr::CallOp("core.to_float32", {input}));

  ASSERT_OK_AND_ASSIGN(
      auto model, expr::CallOp(model_op, {input, expr::Literal<int64_t>(5)}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model, expr::ToLowest(model));

  ASSERT_OK_AND_ASSIGN(
      auto model_with_converted_input,
      expr::CallOp(model_op, {converted_input, expr::Literal<int64_t>(5)}));
  ASSERT_OK_AND_ASSIGN(auto expanded_model_with_converted_input,
                       expr::ToLowest(model_with_converted_input));

  // Check that core.to_optional was automatically inserted into expanded_model.
  EXPECT_THAT(expanded_model_with_converted_input, EqualsExpr(expanded_model));
}

TEST_F(ForestModelTest, EvaluateOnScalars) {
  ASSERT_OK_AND_ASSIGN(auto forest_model, CreateForestModelOp());
  ASSERT_OK_AND_ASSIGN(
      auto model,
      expr::CallOp(forest_model, {expr::Leaf("f"), expr::Leaf("i")}));

  FrameLayout::Builder layout_builder;
  auto f_slot = layout_builder.AddSlot<float>();
  auto i_slot = layout_builder.AddSlot<int64_t>();

  ASSERT_OK_AND_ASSIGN(
      auto executable_model,
      CompileAndBindForDynamicEvaluation(expr::DynamicEvaluationEngineOptions(),
                                         &layout_builder, model,
                                         {{"f", TypedSlot::FromSlot(f_slot)},
                                          {"i", TypedSlot::FromSlot(i_slot)}}));

  FrameLayout layout = std::move(layout_builder).Build();
  ASSERT_OK_AND_ASSIGN(const FrameLayout::Slot<float> output,
                       executable_model->output_slot().ToSlot<float>());

  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_model->InitializeLiterals(&ctx));

  ctx.Set(f_slot, 1.0f);
  ctx.Set(i_slot, 5);
  EXPECT_THAT(executable_model->Execute(&ctx), IsOk());
  EXPECT_FLOAT_EQ(ctx.Get(output), 5.5f);

  ctx.Set(f_slot, 3.0f);
  ctx.Set(i_slot, 0);
  EXPECT_THAT(executable_model->Execute(&ctx), IsOk());
  EXPECT_FLOAT_EQ(ctx.Get(output), 8.5f);
}

TEST_F(ForestModelTest, EvaluateOnScalarAndArray) {
  ASSERT_OK_AND_ASSIGN(auto forest_model, CreateForestModelOp());
  ASSERT_OK_AND_ASSIGN(
      auto model,
      expr::CallOp(forest_model, {expr::Leaf("f"), expr::Leaf("i")}));

  FrameLayout::Builder layout_builder;
  auto f_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto i_slot = layout_builder.AddSlot<int64_t>();

  EXPECT_THAT(
      CompileAndBindForDynamicEvaluation(expr::DynamicEvaluationEngineOptions(),
                                         &layout_builder, model,
                                         {{"f", TypedSlot::FromSlot(f_slot)},
                                          {"i", TypedSlot::FromSlot(i_slot)}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("either all forest inputs must be scalars or all "
                         "forest inputs must be arrays, but arg[0] is "
                         "DENSE_ARRAY_FLOAT32 and "
                         "arg[1] is OPTIONAL_INT64")));
}

TEST_F(ForestModelTest, EvaluateOnDenseArrays) {
  ASSERT_OK_AND_ASSIGN(const auto model_op, CreateForestModelOp());
  ASSERT_OK_AND_ASSIGN(
      auto model, expr::CallOp(model_op, {expr::Leaf("f"), expr::Leaf("i")}));

  FrameLayout::Builder layout_builder;
  auto f_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto i_slot = layout_builder.AddSlot<DenseArray<int64_t>>();

  ASSERT_OK_AND_ASSIGN(
      auto executable_model,
      CompileAndBindForDynamicEvaluation(expr::DynamicEvaluationEngineOptions(),
                                         &layout_builder, model,
                                         {{"f", TypedSlot::FromSlot(f_slot)},
                                          {"i", TypedSlot::FromSlot(i_slot)}}));

  FrameLayout layout = std::move(layout_builder).Build();
  ASSERT_OK_AND_ASSIGN(
      const FrameLayout::Slot<DenseArray<float>> output,
      executable_model->output_slot().ToSlot<DenseArray<float>>());

  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_model->InitializeLiterals(&ctx));

  ctx.Set(f_slot, CreateDenseArray<float>({1.0f, 3.0f}));
  ctx.Set(i_slot, CreateDenseArray<int64_t>({5, 0}));
  EXPECT_THAT(executable_model->Execute(&ctx), IsOk());

  EXPECT_THAT(ctx.Get(output), ElementsAre(5.5f, 8.5f));
}

}  // namespace
}  // namespace arolla
