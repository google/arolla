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
#include "arolla/expr/eval/eval.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/backend_wrapping_operator.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/extensions.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/eval/prepare_expression.h"
#include "arolla/expr/eval/side_output.h"
#include "arolla/expr/eval/test_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/optimization/default/default_optimizer.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/input_loader.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::InvokeExprOperator;
using ::arolla::testing::TypedValueWith;
using ::arolla::testing::WithExportAnnotation;
using ::arolla::testing::WithNameAnnotation;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::FloatEq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::Property;
using ::testing::UnorderedElementsAre;

struct TestParams {
  bool use_default_optimizer = false;
};

class EvalVisitorParameterizedTest
    : public ::testing::TestWithParam<TestParams> {
 protected:
  EvalVisitorParameterizedTest() {
    if (GetParam().use_default_optimizer) {
      auto optimizer_or = DefaultOptimizer();
      CHECK_OK(optimizer_or.status());
      options_.optimizer = optimizer_or.value();
    }
    options_.collect_op_descriptions = true;
  }

  DynamicEvaluationEngineOptions options_;
};

INSTANTIATE_TEST_SUITE_P(
    Optimizer, EvalVisitorParameterizedTest,
    ::testing::Values(TestParams{.use_default_optimizer = false},
                      TestParams{.use_default_optimizer = true}));

TEST_P(EvalVisitorParameterizedTest, SmokeTest) {
  // x + y + z
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("math.add", {CallOp("math.add", {Leaf("x"), Leaf("y")}),
                                     Leaf("z")}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();

  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, expr,
                                         {{"x", TypedSlot::FromSlot(x_slot)},
                                          {"y", TypedSlot::FromSlot(y_slot)},
                                          {"z", TypedSlot::FromSlot(z_slot)}}));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre(
                "FLOAT32 [0x10] = math.add(FLOAT32 [0x00], FLOAT32 [0x04])",
                "FLOAT32 [0x0C] = math.add(FLOAT32 [0x10], FLOAT32 [0x08])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 1.0f);
  ctx.Set(y_slot, 10.0f);
  ctx.Set(z_slot, 100.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  EXPECT_THAT(executable_expr->named_output_slots(), IsEmpty());
  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<float>());
  EXPECT_EQ(ctx.Get(output_slot), 111.0f);

  // make sure inputs are not garbage collected
  EXPECT_EQ(ctx.Get(x_slot), 1.0f);
  EXPECT_EQ(ctx.Get(y_slot), 10.0f);
  EXPECT_EQ(ctx.Get(z_slot), 100.0f);
}

TEST_P(EvalVisitorParameterizedTest, ReusingInputSlots) {
  // x1 + x2 + x3 + x4
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add",
             {CallOp("math.add", {CallOp("math.add", {Leaf("x1"), Leaf("x2")}),
                                  Leaf("x3")}),
              Leaf("x4")}));

  DynamicEvaluationEngineOptions options{.collect_op_descriptions = true};

  auto create_input_slots = [](FrameLayout::Builder& layout_builder) {
    return absl::flat_hash_map<std::string, TypedSlot>{
        {"x1", TypedSlot::FromSlot(layout_builder.AddSlot<float>())},
        {"x2", TypedSlot::FromSlot(layout_builder.AddSlot<float>())},
        {"x3", TypedSlot::FromSlot(layout_builder.AddSlot<float>())},
        {"x4", TypedSlot::FromSlot(layout_builder.AddSlot<float>())}};
  };

  {
    FrameLayout::Builder layout_builder;
    auto input_slots = create_input_slots(layout_builder);
    EXPECT_THAT(
        CompileAndBindForDynamicEvaluation(options, &layout_builder, expr,
                                           input_slots),
        IsOkAndHolds(AllOf(
            InitOperationsAre(),
            EvalOperationsAre(
                "FLOAT32 [0x14] = math.add(FLOAT32 [0x00], FLOAT32 [0x04])",
                // Not reusing input slot, despite it is not needed anymore.
                "FLOAT32 [0x18] = math.add(FLOAT32 [0x14], FLOAT32 [0x08])",
                "FLOAT32 [0x10] = math.add(FLOAT32 [0x18], FLOAT32 [0x0C])"))));
  }
  {
    options.allow_overriding_input_slots = true;
    FrameLayout::Builder layout_builder;
    auto input_slots = create_input_slots(layout_builder);
    EXPECT_THAT(
        CompileAndBindForDynamicEvaluation(options, &layout_builder, expr,
                                           input_slots),
        IsOkAndHolds(AllOf(
            InitOperationsAre(),
            EvalOperationsAre(
                "FLOAT32 [0x14] = math.add(FLOAT32 [0x00], FLOAT32 [0x04])",
                // Reusing input slot instead of allocating a new one.
                "FLOAT32 [0x04] = math.add(FLOAT32 [0x14], FLOAT32 [0x08])",
                "FLOAT32 [0x10] = math.add(FLOAT32 [0x04], FLOAT32 [0x0C])"))));
  }
}

// Tests that names are ignored for the evaluation.
TEST_P(EvalVisitorParameterizedTest, NamedNodesTest) {
  constexpr int kIters = 10;
  ASSERT_OK_AND_ASSIGN(auto xpy, CallOp("math.add", {Leaf("x"), Leaf("y")}));
  auto expr = xpy;
  for (int i = 0; i < kIters; ++i) {
    ASSERT_OK_AND_ASSIGN(
        expr, CallOp("math.maximum",
                     {expr, WithNameAnnotation(expr, std::to_string(i))}));
  }

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();

  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, expr,
                                         {{"x", TypedSlot::FromSlot(x_slot)},
                                          {"y", TypedSlot::FromSlot(y_slot)}}));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre(
                "FLOAT32 [0x0C] = math.add(FLOAT32 [0x00], FLOAT32 [0x04])",
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x0C], FLOAT32 [0x0C])",
                "FLOAT32 [0x0C] = math.maximum(FLOAT32 [0x10], FLOAT32 [0x10])",
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x0C], FLOAT32 [0x0C])",
                "FLOAT32 [0x0C] = math.maximum(FLOAT32 [0x10], FLOAT32 [0x10])",
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x0C], FLOAT32 [0x0C])",
                "FLOAT32 [0x0C] = math.maximum(FLOAT32 [0x10], FLOAT32 [0x10])",
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x0C], FLOAT32 [0x0C])",
                "FLOAT32 [0x0C] = math.maximum(FLOAT32 [0x10], FLOAT32 [0x10])",
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x0C], FLOAT32 [0x0C])",
                "FLOAT32 [0x08] = math.maximum(FLOAT32 [0x10], FLOAT32 "
                "[0x10])")));
  FrameLayout layout = std::move(layout_builder).Build();
  // Two input slots, one output, two used for computation. Names shouldn't
  // cause extra overhead.
  EXPECT_EQ(layout.AllocSize(), sizeof(float) * 5);

  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 1.0f);
  ctx.Set(y_slot, 10.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  EXPECT_THAT(executable_expr->named_output_slots(), IsEmpty());
  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<float>());
  EXPECT_EQ(ctx.Get(output_slot), 11);
}

TEST_P(EvalVisitorParameterizedTest, WithUsedSubSlotOfInput) {
  // has(x)
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core.has", {Leaf("x")}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OptionalValue<float>>();

  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, expr,
                                         {{"x", TypedSlot::FromSlot(x_slot)}}));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre(
                "OPTIONAL_UNIT [0x08] = core._copy(OPTIONAL_UNIT [0x00])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 1.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  EXPECT_THAT(executable_expr->named_output_slots(), IsEmpty());
  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<OptionalUnit>());
  EXPECT_EQ(ctx.Get(output_slot), kPresent);

  // make sure inputs are not garbage collected
  EXPECT_EQ(ctx.Get(x_slot), 1.0f);
}

TEST_P(EvalVisitorParameterizedTest, WithUsedSubSlotOfIntermediate) {
  // has(x + y)
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("core.has", {CallOp("math.add", {Leaf("x"), Leaf("y")})}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OptionalValue<float>>();
  auto y_slot = layout_builder.AddSlot<OptionalValue<float>>();

  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, expr,
                                         {{"x", TypedSlot::FromSlot(x_slot)},
                                          {"y", TypedSlot::FromSlot(y_slot)}}));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre(
                "OPTIONAL_FLOAT32 [0x14] = math.add(OPTIONAL_FLOAT32 [0x00], "
                "OPTIONAL_FLOAT32 [0x08])",
                "OPTIONAL_UNIT [0x10] = core._copy(OPTIONAL_UNIT [0x14])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 1.0f);
  ctx.Set(y_slot, 10.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  EXPECT_THAT(executable_expr->named_output_slots(), IsEmpty());
  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<OptionalUnit>());
  EXPECT_EQ(ctx.Get(output_slot), kPresent);

  // make sure inputs are not garbage collected
  EXPECT_EQ(ctx.Get(x_slot), 1.0f);
  EXPECT_EQ(ctx.Get(y_slot), 10.0f);
}

TEST_P(EvalVisitorParameterizedTest, EvalWithNamedOutput) {
  DynamicEvaluationEngineOptions options;
  options.collect_op_descriptions = true;
  // x + y + z
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("math.add",
                        {WithExportAnnotation(
                             CallOp("math.add", {Leaf("x"), Leaf("y")}), "x+y"),
                         Leaf("z")}));
  ASSERT_OK_AND_ASSIGN((auto [stripped_expr, side_outputs]),
                       ExtractSideOutputs(expr));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();

  const QTypePtr f32 = GetQType<float>();

  ASSERT_OK_AND_ASSIGN(auto compiled_expr,
                       CompileForDynamicEvaluation(
                           options, stripped_expr,
                           {{"x", f32}, {"y", f32}, {"z", f32}}, side_outputs));
  EXPECT_EQ(compiled_expr->output_type(), f32);
  EXPECT_THAT(compiled_expr->named_output_types(),
              UnorderedElementsAre(Pair("x+y", f32)));
  auto typed_output_slot =
      AddSlot(compiled_expr->output_type(), &layout_builder);
  ASSERT_OK_AND_ASSIGN(auto executable_expr,
                       compiled_expr->Bind(&layout_builder,
                                           {{"x", TypedSlot::FromSlot(x_slot)},
                                            {"y", TypedSlot::FromSlot(y_slot)},
                                            {"z", TypedSlot::FromSlot(z_slot)}},
                                           typed_output_slot));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre(
                "FLOAT32 [0x10] = math.add(FLOAT32 [0x00], FLOAT32 [0x04])",
                "FLOAT32 [0x0C] = math.add(FLOAT32 [0x10], FLOAT32 [0x08])")));

  FrameLayout layout = std::move(layout_builder).Build();
  EXPECT_EQ(layout.AllocSize(), sizeof(float) * 5)
      << "Side outputs shouldn't create any extra overhead";

  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 1.0f);
  ctx.Set(y_slot, 10.0f);
  ctx.Set(z_slot, 100.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  ASSERT_OK_AND_ASSIGN(auto output_slot, typed_output_slot.ToSlot<float>());
  ASSERT_THAT(executable_expr->named_output_slots(),
              UnorderedElementsAre(Pair("x+y", _)));
  ASSERT_OK_AND_ASSIGN(
      auto xpy_slot,
      executable_expr->named_output_slots().at("x+y").ToSlot<float>());
  EXPECT_EQ(ctx.Get(output_slot), 111.0f);
  EXPECT_EQ(ctx.Get(xpy_slot), 11.0f);
}

TEST_P(EvalVisitorParameterizedTest, EvalWithSideOutput) {
  DynamicEvaluationEngineOptions options;
  options.collect_op_descriptions = true;
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(auto side_output_expr,
                       CallOp("math.multiply", {Leaf("y"), Leaf("z")}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();

  ASSERT_OK_AND_ASSIGN(auto executable_expr,
                       CompileAndBindForDynamicEvaluation(
                           options, &layout_builder, expr,
                           {{"x", TypedSlot::FromSlot(x_slot)},
                            {"y", TypedSlot::FromSlot(y_slot)},
                            {"z", TypedSlot::FromSlot(z_slot)}},
                           /*output_slot=*/std::nullopt,
                           /*side_outputs=*/{{"y*z", side_output_expr}}));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre(
                "FLOAT32 [0x0C] = math.add(FLOAT32 [0x00], FLOAT32 [0x04])",
                "FLOAT32 [0x10] = math.multiply(FLOAT32 [0x04], FLOAT32 "
                "[0x08])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 1.0f);
  ctx.Set(y_slot, 10.0f);
  ctx.Set(z_slot, 100.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<float>());
  ASSERT_THAT(executable_expr->named_output_slots(),
              UnorderedElementsAre(Pair("y*z", _)));
  ASSERT_OK_AND_ASSIGN(
      auto side_output_slot,
      executable_expr->named_output_slots().at("y*z").ToSlot<float>());
  EXPECT_EQ(ctx.Get(output_slot), 11.0f);
  EXPECT_EQ(ctx.Get(side_output_slot), 1000.0f);
}

TEST_P(EvalVisitorParameterizedTest, EvalWithShortCircuit) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("core.where", {Leaf("do_divide"),
                            CallOp("math.multiply", {Leaf("x"), Leaf("y")}),
                            CallOp("math.floordiv", {Leaf("x"), Leaf("y")})}));

  FrameLayout::Builder layout_builder;
  // We keep different types for x and y to test implicit type casting.
  auto x_slot = layout_builder.AddSlot<OptionalValue<int>>();
  auto y_slot = layout_builder.AddSlot<int>();
  auto do_divide_slot = layout_builder.AddSlot<OptionalUnit>();

  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(
          options_, &layout_builder, expr,
          {{"x", TypedSlot::FromSlot(x_slot)},
           {"y", TypedSlot::FromSlot(y_slot)},
           {"do_divide", TypedSlot::FromSlot(do_divide_slot)}}));

  if (GetParam().use_default_optimizer) {
    EXPECT_THAT(
        executable_expr,
        AllOf(InitOperationsAre(),
              EvalOperationsAre(
                  "OPTIONAL_INT32 [0x18] = core.to_optional._scalar(INT32 "
                  "[0x08])",
                  "jump_if_not<+2>(OPTIONAL_UNIT [0x0C])",
                  "OPTIONAL_INT32 [0x10] = math.multiply(OPTIONAL_INT32 "
                  "[0x00], OPTIONAL_INT32 [0x18])",
                  "jump<+1>()",
                  "OPTIONAL_INT32 [0x10] = math.floordiv(OPTIONAL_INT32 "
                  "[0x00], OPTIONAL_INT32 [0x18])")));
  } else {
    EXPECT_THAT(
        executable_expr,
        AllOf(InitOperationsAre(),
              EvalOperationsAre(
                  "OPTIONAL_INT32 [0x18] = core.to_optional._scalar(INT32 "
                  "[0x08])",
                  "OPTIONAL_INT32 [0x20] = math.multiply(OPTIONAL_INT32 "
                  "[0x00], OPTIONAL_INT32 [0x18])",
                  "OPTIONAL_INT32 [0x28] = math.floordiv(OPTIONAL_INT32 "
                  "[0x00], OPTIONAL_INT32 [0x18])",
                  "OPTIONAL_INT32 [0x10] = core.where(OPTIONAL_UNIT [0x0C], "
                  "OPTIONAL_INT32 [0x20], OPTIONAL_INT32 [0x28])")));
  }

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 1);
  ctx.Set(y_slot, 0);
  ctx.Set(do_divide_slot, kPresent);

  if (GetParam().use_default_optimizer) {
    // With enabled optimizations we don't evaluate unused division-by-0 branch.
    EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());
    ASSERT_OK_AND_ASSIGN(
        auto output_slot,
        executable_expr->output_slot().ToSlot<OptionalValue<int>>());
    EXPECT_EQ(ctx.Get(output_slot), 0);
  } else {
    EXPECT_THAT(executable_expr->Execute(&ctx),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("division by zero; during evaluation of "
                                   "operator math.floordiv")));
  }
}

TEST_P(EvalVisitorParameterizedTest, EvalWithNamedOutputUnusedButExported) {
  DynamicEvaluationEngineOptions options;
  options.collect_op_descriptions = true;
  // operator dropping all but the first argument
  ASSERT_OK_AND_ASSIGN(
      auto first_op,
      MakeLambdaOperator(ExprOperatorSignature::Make("p0, _px, _py"),
                         Placeholder("p0")));
  // first(x + z, x + y, x * z)
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp(first_op,
             {CallOp("math.add", {Leaf("x"), Leaf("z")}),
              WithExportAnnotation(CallOp("math.add", {Leaf("x"), Leaf("y")}),
                                   "x+y"),
              WithExportAnnotation(
                  CallOp("math.multiply", {Leaf("y"), Leaf("z")}), "y*z")}));
  ASSERT_OK_AND_ASSIGN((auto [stripped_expr, side_outputs]),
                       ExtractSideOutputs(expr));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();

  ASSERT_OK_AND_ASSIGN(auto executable_expr,
                       CompileAndBindForDynamicEvaluation(
                           options, &layout_builder, stripped_expr,
                           {{"x", TypedSlot::FromSlot(x_slot)},
                            {"y", TypedSlot::FromSlot(y_slot)},
                            {"z", TypedSlot::FromSlot(z_slot)}},
                           /*output_slot=*/std::nullopt, side_outputs));

  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre(
                "FLOAT32 [0x0C] = math.add(FLOAT32 [0x00], FLOAT32 [0x08])",
                "FLOAT32 [0x10] = math.add(FLOAT32 [0x00], FLOAT32 [0x04])",
                "FLOAT32 [0x14] = math.multiply(FLOAT32 [0x04], FLOAT32 "
                "[0x08])")));

  FrameLayout layout = std::move(layout_builder).Build();
  EXPECT_EQ(layout.AllocSize(), sizeof(float) * 6)
      << "Side outputs used outside of main expression require "
         "extra slots";

  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 1.0f);
  ctx.Set(y_slot, 10.0f);
  ctx.Set(z_slot, 100.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<float>());
  EXPECT_EQ(ctx.Get(output_slot), 101.0f);

  ASSERT_THAT(executable_expr->named_output_slots(),
              UnorderedElementsAre(Pair("x+y", _), Pair("y*z", _)));
  ASSERT_OK_AND_ASSIGN(
      auto xpy_slot,
      executable_expr->named_output_slots().at("x+y").ToSlot<float>());
  EXPECT_EQ(ctx.Get(xpy_slot), 11.0f);
  ASSERT_OK_AND_ASSIGN(
      auto xtz_slot,
      executable_expr->named_output_slots().at("y*z").ToSlot<float>());
  EXPECT_EQ(ctx.Get(xtz_slot), 1000.0f);
}

TEST_P(EvalVisitorParameterizedTest, EvalWithExportAnnotation) {
  DynamicEvaluationEngineOptions options;
  options.collect_op_descriptions = true;
  // x + y + z
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("math.add",
                        {WithExportAnnotation(
                             CallOp("math.add", {Leaf("x"), Leaf("y")}), "x+y"),
                         Leaf("z")}));
  ASSERT_OK_AND_ASSIGN((auto [stripped_expr, side_outputs]),
                       ExtractSideOutputs(expr));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();

  ASSERT_OK_AND_ASSIGN(auto executable_expr,
                       CompileAndBindForDynamicEvaluation(
                           options, &layout_builder, stripped_expr,
                           {{"x", TypedSlot::FromSlot(x_slot)},
                            {"y", TypedSlot::FromSlot(y_slot)},
                            {"z", TypedSlot::FromSlot(z_slot)}},
                           /*output_slot=*/std::nullopt, side_outputs));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre(
                "FLOAT32 [0x10] = math.add(FLOAT32 [0x00], FLOAT32 [0x04])",
                "FLOAT32 [0x0C] = math.add(FLOAT32 [0x10], FLOAT32 [0x08])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 1.0f);
  ctx.Set(y_slot, 10.0f);
  ctx.Set(z_slot, 100.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<float>());
  ASSERT_THAT(executable_expr->named_output_slots(),
              UnorderedElementsAre(Pair("x+y", _)));
  ASSERT_OK_AND_ASSIGN(
      auto xpy_slot,
      executable_expr->named_output_slots().at("x+y").ToSlot<float>());
  EXPECT_EQ(ctx.Get(output_slot), 111.0f);
  EXPECT_EQ(ctx.Get(xpy_slot), 11.0f);
}

TEST_P(EvalVisitorParameterizedTest, EvalWithExportAnnotation_AllLiterals) {
  DynamicEvaluationEngineOptions options;
  options.collect_op_descriptions = true;
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add",
             {Literal(1.f), WithExportAnnotation(Literal(10.f), "out_y")}));
  ASSERT_OK_AND_ASSIGN((auto [stripped_expr, side_outputs]),
                       ExtractSideOutputs(expr));

  FrameLayout::Builder layout_builder;
  ASSERT_OK_AND_ASSIGN(auto executable_expr,
                       CompileAndBindForDynamicEvaluation(
                           options, &layout_builder, stripped_expr, {},
                           /*output_slot=*/std::nullopt, side_outputs));

  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre("FLOAT32 [0x04] = 11.\n"
                              "FLOAT32 [0x08] = 10."),
            EvalOperationsAre("FLOAT32 [0x00] = core._copy(FLOAT32 [0x04])")));
  FrameLayout layout = std::move(layout_builder).Build();

  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<float>());
  ASSERT_THAT(executable_expr->named_output_slots(),
              UnorderedElementsAre(Pair("out_y", _)));
  ASSERT_OK_AND_ASSIGN(
      auto out_y_slot,
      executable_expr->named_output_slots().at("out_y").ToSlot<float>());
  EXPECT_EQ(ctx.Get(output_slot), 11.0f);
  EXPECT_EQ(ctx.Get(out_y_slot), 10.0f);
}

TEST_P(EvalVisitorParameterizedTest, EvalWithLiteral) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("math.add", {Leaf("x"), Literal(1.f)}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, expr,
                                         {{"x", TypedSlot::FromSlot(x_slot)}}));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre("FLOAT32 [0x08] = 1."),
            EvalOperationsAre(
                "FLOAT32 [0x04] = math.add(FLOAT32 [0x00], FLOAT32 [0x08])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 2.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<float>());
  EXPECT_THAT(ctx.Get(output_slot), Eq(3.0f));
}

TEST_P(EvalVisitorParameterizedTest, EvalSingleLeaf) {
  auto expr = Leaf("x");

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto output_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, expr,
                                         {{"x", TypedSlot::FromSlot(x_slot)}},
                                         TypedSlot::FromSlot(output_slot)));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre("FLOAT32 [0x04] = core._copy(FLOAT32 [0x00])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 2.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());
  EXPECT_THAT(ctx.Get(output_slot), Eq(2.0f));
}

TEST_P(EvalVisitorParameterizedTest, EvalOnlyLiterals) {
  auto x = Literal(2.f);
  auto y = Literal(1.f);
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x, y}));

  FrameLayout::Builder layout_builder;
  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, expr, {}));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre("FLOAT32 [0x04] = 3."),
            EvalOperationsAre("FLOAT32 [0x00] = core._copy(FLOAT32 [0x04])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  // All computations should happen in initialization before even evaluation.
  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<float>());

  ctx.Set(output_slot, 57.0f);
  // InitializeLiterals does not affect output slot.
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  EXPECT_THAT(ctx.Get(output_slot), Eq(57.0f));

  // Evaluation copies the value into the output slot.
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());
  EXPECT_THAT(ctx.Get(output_slot), Eq(3.0f));
}

TEST_P(EvalVisitorParameterizedTest, EvalUnboundLeafError) {
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {Leaf("x"), Leaf("y")}));
  EXPECT_THAT(
      CompileForDynamicEvaluation(options_, expr, {{"y", GetQType<float>()}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("missing QType information for inputs {x}")));
  EXPECT_THAT(
      CompileForDynamicEvaluation(options_, expr, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("missing QType information for inputs {x, y}")));
  ASSERT_OK_AND_ASSIGN(auto compiled_model,
                       CompileForDynamicEvaluation(options_, expr,
                                                   {{"x", GetQType<float>()},
                                                    {"y", GetQType<float>()}}));
  FrameLayout::Builder layout_builder;
  EXPECT_THAT(compiled_model->Bind(
                  &layout_builder,
                  {{"y", TypedSlot::FromSlot(layout_builder.AddSlot<float>())}},
                  TypedSlot::FromSlot(layout_builder.AddSlot<float>())),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("missed slots: x")));
  EXPECT_THAT(compiled_model->Bind(
                  &layout_builder, {},
                  TypedSlot::FromSlot(layout_builder.AddSlot<float>())),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("missed slots: x,y")));
}

TEST_P(EvalVisitorParameterizedTest, EvalPlaceholderError) {
  auto x = Literal(2.f);
  ASSERT_OK_AND_ASSIGN(
      auto y, WithQTypeAnnotation(Placeholder("y"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x, y}));

  EXPECT_THAT(
      CompileForDynamicEvaluation(options_, expr, {{"y", GetQType<float>()}}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr(
                   "placeholders should be substituted before evaluation: y")));
}

TEST_P(EvalVisitorParameterizedTest, EvalOperatorTakingSameNodeTwice) {
  auto x = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x, x}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, expr,
                                         {{"x", TypedSlot::FromSlot(x_slot)}}));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre(
                "FLOAT32 [0x04] = math.add(FLOAT32 [0x00], FLOAT32 [0x00])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 2.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<float>());
  EXPECT_THAT(ctx.Get(output_slot), Eq(4.0f));
}

TEST_P(EvalVisitorParameterizedTest, EvalOperatorTakingTwoEqualNodes) {
  auto x = Leaf("x");
  auto y = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x, y}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, expr,
                                         {{"x", TypedSlot::FromSlot(x_slot)}}));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre(
                "FLOAT32 [0x04] = math.add(FLOAT32 [0x00], FLOAT32 [0x00])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));
  ctx.Set(x_slot, 2.0f);
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  ASSERT_OK_AND_ASSIGN(auto output_slot,
                       executable_expr->output_slot().ToSlot<float>());
  EXPECT_THAT(ctx.Get(output_slot), Eq(4.0f));
}

TEST_P(EvalVisitorParameterizedTest, EvalOperatorWithUnusedInputs) {
  ASSERT_OK_AND_ASSIGN(
      auto op_with_unused_input,
      MakeLambdaOperator(ExprOperatorSignature{{"unused_input"}},
                         Literal<int32_t>(1)));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op_with_unused_input, {Leaf("x")}));
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, expr,
                                         {{"x", TypedSlot::FromSlot(x_slot)}}));
  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre("INT32 [0x08] = 1"),
            EvalOperationsAre("INT32 [0x04] = core._copy(INT32 [0x08])")));
}

TEST_P(EvalVisitorParameterizedTest, GetNth) {
  const auto x = Literal<float>(2.f);
  const auto y = Literal<int64_t>(3);
  ASSERT_OK_AND_ASSIGN(const auto tuple, CallOp("core.make_tuple", {x, y}));
  ASSERT_OK_AND_ASSIGN(const auto first, CallOp("core.get_first", {tuple}));
  ASSERT_OK_AND_ASSIGN(const auto second, CallOp("core.get_second", {tuple}));
  ASSERT_OK_AND_ASSIGN(const auto second_by_index,
                       CallOp(std::make_shared<GetNthOperator>(1), {tuple}));

  ASSERT_OK_AND_ASSIGN(auto executable_first,
                       CompileForDynamicEvaluation(options_, first));
  ASSERT_OK_AND_ASSIGN(auto executable_second,
                       CompileForDynamicEvaluation(options_, second));
  ASSERT_OK_AND_ASSIGN(auto executable_second_by_index,
                       CompileForDynamicEvaluation(options_, second_by_index));

  FrameLayout::Builder layout_builder;
  ASSERT_OK_AND_ASSIGN(auto bound_executable_first,
                       executable_first->Bind(&layout_builder));
  EXPECT_THAT(
      bound_executable_first,
      AllOf(InitOperationsAre("FLOAT32 [0x04] = 2."),
            EvalOperationsAre("FLOAT32 [0x00] = core._copy(FLOAT32 [0x04])")));

  ASSERT_OK_AND_ASSIGN(auto bound_executable_second,
                       executable_second->Bind(&layout_builder));
  EXPECT_THAT(
      bound_executable_second,
      AllOf(InitOperationsAre("INT64 [0x10] = int64{3}"),
            EvalOperationsAre("INT64 [0x08] = core._copy(INT64 [0x10])")));

  ASSERT_OK_AND_ASSIGN(auto bound_executable_second_by_index,
                       executable_second_by_index->Bind(&layout_builder));
  EXPECT_THAT(
      bound_executable_second_by_index,
      AllOf(InitOperationsAre("INT64 [0x20] = int64{3}"),
            EvalOperationsAre("INT64 [0x18] = core._copy(INT64 [0x20])")));

  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);

  ASSERT_OK_AND_ASSIGN(auto output_first,
                       bound_executable_first->output_slot().ToSlot<float>());
  EXPECT_OK(bound_executable_first->InitializeLiterals(&ctx));
  EXPECT_OK(bound_executable_first->Execute(&ctx));
  EXPECT_THAT(ctx.Get(output_first), FloatEq(2.0f));

  ASSERT_OK_AND_ASSIGN(
      auto output_second,
      bound_executable_second->output_slot().ToSlot<int64_t>());
  EXPECT_OK(bound_executable_second->InitializeLiterals(&ctx));
  EXPECT_OK(bound_executable_second->Execute(&ctx));
  EXPECT_THAT(ctx.Get(output_second), Eq(3));

  ASSERT_OK_AND_ASSIGN(
      auto output_second_by_index,
      bound_executable_second->output_slot().ToSlot<int64_t>());
  EXPECT_OK(bound_executable_second_by_index->InitializeLiterals(&ctx));
  EXPECT_OK(bound_executable_second_by_index->Execute(&ctx));
  EXPECT_THAT(ctx.Get(output_second_by_index), Eq(3));
}

TEST_P(EvalVisitorParameterizedTest, OptimizedHas) {
  auto ten_times_has = Leaf("x");
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK_AND_ASSIGN(ten_times_has, CallOp("core.has", {ten_times_has}));
  }
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OptionalValue<float>>();
  EXPECT_THAT(
      CompileAndBindForDynamicEvaluation(options_, &layout_builder,
                                         ten_times_has,
                                         {{"x", TypedSlot::FromSlot(x_slot)}}),
      // Despite having core.has ten times in the original expression, none of
      // them is saved. The only existing operator copies the first slot of
      // the argument, which was "reinterpret_casted" to OptionalUnit.
      IsOkAndHolds(AllOf(
          InitOperationsAre(),
          EvalOperationsAre(
              "OPTIONAL_UNIT [0x08] = core._copy(OPTIONAL_UNIT [0x00])"))));
}

class IdentityAnnotation final : public AnnotationExprOperatorTag,
                                 public ExprOperatorWithFixedSignature {
 public:
  IdentityAnnotation()
      : ExprOperatorWithFixedSignature(
            "id", ExprOperatorSignature::MakeArgsN(1), "",
            FingerprintHasher("arolla::expr::IdentityAnnotation").Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    return inputs[0];
  }
};

TEST_P(EvalVisitorParameterizedTest, EvalAnnotation) {
  auto x = Leaf("x");
  const auto with_annotation = std::make_shared<IdentityAnnotation>();
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(with_annotation, {x}));
  EXPECT_THAT(Invoke(expr, {{"x", TypedValue::FromValue(2.0f)}}),
              IsOkAndHolds(TypedValueWith<float>(2.0f)));
}

TEST_P(EvalVisitorParameterizedTest, SlotRecycling) {
  ASSERT_OK_AND_ASSIGN(auto float_sum,
                       CallOp("math.maximum", {Leaf("x"), Literal<float>(57)}));
  ASSERT_OK_AND_ASSIGN(float_sum,
                       CallOp("math.maximum", {float_sum, Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(auto float_sum_4,
                       CallOp("math.maximum", {float_sum, Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(float_sum,
                       CallOp("math.maximum", {float_sum_4, Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(float_sum,
                       CallOp("math.maximum", {float_sum, Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(float_sum,
                       CallOp("math.maximum", {float_sum, Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(auto float_sum_8,
                       CallOp("math.maximum", {float_sum, Leaf("x")}));

  {
    FrameLayout::Builder layout_builder;
    auto x_slot = layout_builder.AddSlot<float>();
    EXPECT_THAT(
        CompileAndBindForDynamicEvaluation(
            options_, &layout_builder, float_sum_8,
            {{"x", TypedSlot::FromSlot(x_slot)}}),
        IsOkAndHolds(AllOf(
            // Slot 0x08 is used for a literal so is never recycled.
            InitOperationsAre("FLOAT32 [0x08] = 57."),
            EvalOperationsAre(
                // Slot 0x00 is provided as input and is never recycled.
                // Slots 0x0C and 0x10 are reused between the computations.
                "FLOAT32 [0x0C] = math.maximum(FLOAT32 [0x00], FLOAT32 [0x08])",
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x0C], FLOAT32 [0x00])",
                "FLOAT32 [0x0C] = math.maximum(FLOAT32 [0x10], FLOAT32 [0x00])",
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x0C], FLOAT32 [0x00])",
                "FLOAT32 [0x0C] = math.maximum(FLOAT32 [0x10], FLOAT32 [0x00])",
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x0C], FLOAT32 [0x00])",
                // Slot 0x04 is an output and is never recycled.
                "FLOAT32 [0x04] = math.maximum(FLOAT32 [0x10], FLOAT32 "
                "[0x00])"))));
  }
  {
    FrameLayout::Builder layout_builder;
    auto x_slot = layout_builder.AddSlot<float>();
    EXPECT_THAT(
        CompileAndBindForDynamicEvaluation(
            options_, &layout_builder, float_sum_8,
            {{"x", TypedSlot::FromSlot(x_slot)}},
            /*output_slot=*/{},
            /*side_outputs=*/{{"sum_of_four", float_sum_4}}),
        IsOkAndHolds(AllOf(
            // Slot 0x08 is used for a literal so is never recycled.
            InitOperationsAre("FLOAT32 [0x08] = 57."),
            EvalOperationsAre(
                // Slot 0x00 is provided as input and is never recycled.
                // Slots 0x0C and 0x10 are reused between the computations.
                "FLOAT32 [0x0C] = math.maximum(FLOAT32 [0x00], FLOAT32 [0x08])",
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x0C], FLOAT32 [0x00])",
                "FLOAT32 [0x0C] = math.maximum(FLOAT32 [0x10], FLOAT32 [0x00])",
                // 0x0C is used for the side output, so cannot be reused
                // anymore.
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x0C], FLOAT32 [0x00])",
                "FLOAT32 [0x14] = math.maximum(FLOAT32 [0x10], FLOAT32 [0x00])",
                "FLOAT32 [0x10] = math.maximum(FLOAT32 [0x14], FLOAT32 [0x00])",
                "FLOAT32 [0x04] = math.maximum(FLOAT32 [0x10], FLOAT32 "
                "[0x00])"),
            Pointee(Property(&BoundExpr::named_output_slots,
                             UnorderedElementsAre(Pair(
                                 "sum_of_four",
                                 Property(&TypedSlot::byte_offset, 0x0C))))))));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto int_sum,
        CallOp("math.maximum", {Leaf("y"), Literal<int32_t>(57)}));
    ASSERT_OK_AND_ASSIGN(int_sum, CallOp("math.maximum", {int_sum, Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(int_sum, CallOp("math.maximum", {int_sum, Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(int_sum, CallOp("math.maximum", {int_sum, Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(int_sum, CallOp("math.maximum", {int_sum, Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(int_sum, CallOp("math.maximum", {int_sum, Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(auto int_sum_8,
                         CallOp("math.maximum", {int_sum, Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(auto sums_pair,
                         CallOp("core.make_tuple", {int_sum_8, float_sum_8}));
    FrameLayout::Builder layout_builder;
    auto x_slot = layout_builder.AddSlot<float>();
    auto y_slot = layout_builder.AddSlot<int32_t>();
    EXPECT_THAT(
        CompileAndBindForDynamicEvaluation(
            options_, &layout_builder, sums_pair,
            {{"x", TypedSlot::FromSlot(x_slot)},
             {"y", TypedSlot::FromSlot(y_slot)}}),
        IsOkAndHolds(AllOf(
            // Slots 0x10 and 0x1C are used for literals and so never recycled.
            InitOperationsAre("INT32 [0x10] = 57\n"
                              "FLOAT32 [0x1C] = 57."),
            EvalOperationsAre(
                "INT32 [0x14] = math.maximum(INT32 [0x04], INT32 [0x10])",
                "INT32 [0x18] = math.maximum(INT32 [0x14], INT32 [0x04])",
                "INT32 [0x14] = math.maximum(INT32 [0x18], INT32 [0x04])",
                "INT32 [0x18] = math.maximum(INT32 [0x14], INT32 [0x04])",
                "INT32 [0x14] = math.maximum(INT32 [0x18], INT32 [0x04])",
                "INT32 [0x18] = math.maximum(INT32 [0x14], INT32 [0x04])",
                "INT32 [0x14] = math.maximum(INT32 [0x18], INT32 [0x04])",
                // Int slots are not recycled as floats.
                "FLOAT32 [0x20] = math.maximum(FLOAT32 [0x00], FLOAT32 [0x1C])",
                "FLOAT32 [0x24] = math.maximum(FLOAT32 [0x20], FLOAT32 [0x00])",
                "FLOAT32 [0x20] = math.maximum(FLOAT32 [0x24], FLOAT32 [0x00])",
                "FLOAT32 [0x24] = math.maximum(FLOAT32 [0x20], FLOAT32 [0x00])",
                "FLOAT32 [0x20] = math.maximum(FLOAT32 [0x24], FLOAT32 [0x00])",
                "FLOAT32 [0x24] = math.maximum(FLOAT32 [0x20], FLOAT32 [0x00])",
                "FLOAT32 [0x20] = math.maximum(FLOAT32 [0x24], FLOAT32 [0x00])",
                "tuple<INT32,FLOAT32> [0x08] = core.make_tuple(INT32 [0x14], "
                "FLOAT32 [0x20])"))));
  }
}

TEST_P(EvalVisitorParameterizedTest, TupleSubslotsNotRecycled) {
  ASSERT_OK_AND_ASSIGN(auto xy_tuple,
                       CallOp("core.make_tuple", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(auto xyz_tuple,
                       CallOp("core.make_tuple", {xy_tuple, Leaf("z")}));
  ASSERT_OK_AND_ASSIGN(
      auto x_plus_z,
      CallOp("math.maximum",
             {CallOp("core.get_first", {CallOp("core.get_first", {xyz_tuple})}),
              CallOp("core.get_second", {xyz_tuple})}));
  ASSERT_OK_AND_ASSIGN(auto x_plus_z_2,
                       CallOp("math.maximum", {x_plus_z, x_plus_z}));
  ASSERT_OK_AND_ASSIGN(
      auto x_plus_z_again,
      CallOp("core.get_first",
             {CallOp("core.make_tuple", {x_plus_z, x_plus_z_2})}));
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(auto bound_expr,
                       CompileAndBindForDynamicEvaluation(
                           options_, &layout_builder, x_plus_z_again,
                           {{"x", TypedSlot::FromSlot(x_slot)},
                            {"y", TypedSlot::FromSlot(y_slot)},
                            {"z", TypedSlot::FromSlot(z_slot)}}));
  if (GetParam().use_default_optimizer) {
    // This case is not interesting for the test, just keep it for completeness.
    EXPECT_THAT(bound_expr,
                AllOf(InitOperationsAre(),
                      EvalOperationsAre("FLOAT32 [0x0C] = math.maximum(FLOAT32 "
                                        "[0x00], FLOAT32 [0x08])")));
  } else {
    EXPECT_THAT(
        bound_expr,
        AllOf(
            InitOperationsAre(),
            EvalOperationsAre(
                "tuple<FLOAT32,FLOAT32> [0x10]"
                " = core.make_tuple(FLOAT32 [0x00], FLOAT32 [0x04])",
                "tuple<tuple<FLOAT32,FLOAT32>,FLOAT32> [0x18]"
                " = core.make_tuple(tuple<FLOAT32,FLOAT32> [0x10], FLOAT32 "
                "[0x08])",
                // The 0x10 tuple subslots are not reused.
                "FLOAT32 [0x24] = math.maximum(FLOAT32 [0x18], FLOAT32 [0x20])",
                "FLOAT32 [0x28] = math.maximum(FLOAT32 [0x24], FLOAT32 [0x24])",
                // But the whole 0x10 tuple slot reused.
                "tuple<FLOAT32,FLOAT32> [0x10]"
                " = core.make_tuple(FLOAT32 [0x24], FLOAT32 [0x28])",
                "FLOAT32 [0x0C] = core._copy(FLOAT32 [0x10])")));
  }
}

struct Point3D {
  float x;
  float y;
  float z;
};

TEST_P(EvalVisitorParameterizedTest, TestWithInputLoader) {
  // Build an expression.
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto z = Leaf("z");
  // x + y + z
  ASSERT_OK_AND_ASSIGN(auto xy, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {xy, z}));

  FrameLayout::Builder layout_builder;
  // Build input loader.
  ASSERT_OK_AND_ASSIGN(auto loader,
                       CreateAccessorsInputLoader<Point3D>(
                           "x", [](const Point3D& p) { return p.x; },  //
                           "y", [](const Point3D& p) { return p.y; },  //
                           "z", [](const Point3D& p) { return p.z; }));
  // All inputs will be populated.
  // TODO: support a way to CompileForDynamicEvaluation with not
  // all inputs provided.
  ASSERT_OK_AND_ASSIGN(auto output_types,
                       GetInputLoaderQTypes(*loader, GetLeafKeys(expr)));
  auto input_slots = AddSlotsMap(output_types, &layout_builder);
  ASSERT_OK_AND_ASSIGN(auto bound_loader, loader->Bind(input_slots));

  // Build executable model.
  ASSERT_OK_AND_ASSIGN(auto executable_expr,
                       CompileAndBindForDynamicEvaluation(
                           options_, &layout_builder, expr, input_slots));
  ASSERT_OK_AND_ASSIGN(auto output,
                       executable_expr->output_slot().ToSlot<float>());
  FrameLayout layout = std::move(layout_builder).Build();

  // Create context once per thread.
  RootEvaluationContext ctx(&layout);
  EXPECT_OK(executable_expr->InitializeLiterals(&ctx));

  // Run model against input.
  ASSERT_OK(bound_loader({1.0f, 10.0f, 100.0f}, ctx.frame()));
  EXPECT_THAT(executable_expr->Execute(&ctx), IsOk());

  // Validation of output.
  EXPECT_THAT(ctx.Get(output), Eq(111.0f));
}

TEST_P(EvalVisitorParameterizedTest, DetailedStackTrace) {
  ASSERT_OK_AND_ASSIGN(
      auto sum_of_4_lambda,
      MakeLambdaOperator(
          "sum_of_4", ExprOperatorSignature{{"x"}},
          CallOp("math.sum",
                 {Placeholder("x"),
                  CallOp("edge.from_sizes",
                         {CallOp("math.multiply",
                                 {Literal(CreateDenseArray<int64_t>({1, 1})),
                                  Literal(2)})})})));

  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(sum_of_4_lambda, {Leaf("x")}));
  auto options =
      DynamicEvaluationEngineOptions{.enable_expr_stack_trace = true};

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<DenseArray<int64_t>>();
  auto result_slot = layout_builder.AddSlot<DenseArray<int64_t>>();

  ASSERT_OK_AND_ASSIGN(
      auto executable_expr,
      CompileAndBindForDynamicEvaluation(options, &layout_builder, expr,
                                         {{"x", TypedSlot::FromSlot(x_slot)}},
                                         TypedSlot::FromSlot(result_slot)));

  auto layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);
  EvaluationContext ctx;
  executable_expr->InitializeLiterals(&ctx, alloc.frame());
  executable_expr->Execute(&ctx, alloc.frame());

  EXPECT_THAT(
      ctx.status(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("argument sizes mismatch: (4, 0); "
                    "during evaluation of operator math._sum\n"
                    "ORIGINAL NODE: sum_of_4(L.x)\n"
                    "COMPILED NODE: M.math._sum(L.x, dense_array_edge("
                    "split_points=dense_array([int64{0}, int64{2}, int64{4}]))"
                    ", optional_int64{0})")));
}

TEST_P(EvalVisitorParameterizedTest, OperatorWithoutProxy) {
  FrameLayout::Builder layout_builder;
  ASSERT_OK_AND_ASSIGN(
      auto node,
      CallOp(std::make_shared<::arolla::expr::testing::DummyOp>(
                 "test.Dummy", ExprOperatorSignature::MakeVariadicArgs()),
             {}));

  EXPECT_THAT(
      CompileAndBindForDynamicEvaluation(options_, &layout_builder, node, {}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("test.Dummy is not a builtin or backend ExprOperator; "
                    "while compiling node test.Dummy():INT32; the expression "
                    "is likely not fully compiled and is using derived "
                    "operators that are not supported in the backend")));
}

TEST_P(EvalVisitorParameterizedTest, DenseArrayStringReplace) {
  EXPECT_THAT(InvokeExprOperator<DenseArray<Text>>(
                  "strings.replace",
                  CreateDenseArray<Text>({Text("Fuzzy"), Text("Wuzzy")}),
                  Text("zz"), Text("zzz")),
              IsOkAndHolds(::testing::ElementsAre(
                  absl::string_view("Fuzzzy"), absl::string_view("Wuzzzy"))));
}

// strings.format() isn't defined for DenseArrays, so this will use core.map on
// the scalar version.
TEST_P(EvalVisitorParameterizedTest, VectorPrintf) {
  DenseArray<Text> format_spec =
      CreateConstDenseArray<Text>(3, "%s's atomic weight is %.4f");
  DenseArray<Text> elements = CreateDenseArray<Text>(
      {Text("Hydrogen"), Text("Helium"), Text("Lithium")});
  DenseArray<float> weights =
      CreateDenseArray<float>({1.0079f, 4.0026, 6.9410});
  EXPECT_THAT(InvokeExprOperator<DenseArray<Text>>(
                  "strings.printf", format_spec, elements, weights),
              IsOkAndHolds(ElementsAre("Hydrogen's atomic weight is 1.0079",
                                       "Helium's atomic weight is 4.0026",
                                       "Lithium's atomic weight is 6.9410")));
}

TEST_P(EvalVisitorParameterizedTest, CompileAndBindExprOperator) {
  ASSERT_OK_AND_ASSIGN(
      auto x_plus_y_plus_1_op,
      MakeLambdaOperator(
          ExprOperatorSignature::Make("x, y"),
          CallOp("math.add", {Placeholder("x"),
                              CallOp("math.add", {Placeholder("y"),
                                                  Literal<int64_t>(1)})})));
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<int64_t>();
  auto y_slot = layout_builder.AddSlot<int64_t>();
  auto result_slot = layout_builder.AddSlot<int64_t>();
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<BoundExpr> executable,
      CompileAndBindExprOperator(
          options_, &layout_builder, x_plus_y_plus_1_op,
          {TypedSlot::FromSlot(x_slot), TypedSlot::FromSlot(y_slot)},
          TypedSlot::FromSlot(result_slot)));
  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  ctx.Set(x_slot, 10);
  ctx.Set(y_slot, 100);
  ASSERT_OK(executable->InitializeLiterals(&ctx));
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_THAT(ctx.Get(result_slot), Eq(111));
}

// An operator that will be transformed into LowerLevelOtherOperator by a
// compiler extension.
class HigherLevelTestOperator final : public BasicExprOperator {
 public:
  HigherLevelTestOperator()
      : BasicExprOperator(
            "test.higher_level_test_op", ExprOperatorSignature::MakeArgsN(1),
            "",
            FingerprintHasher(
                "arolla::expr::eval_internal::HigherLevelTestOperator")
                .Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    return GetQType<float>();
  }
};

// An operator that will be compiled by a compiler extension.
class LowerLevelTestOperator final : public BasicExprOperator,
                                     public BuiltinExprOperatorTag {
 public:
  LowerLevelTestOperator()
      : BasicExprOperator(
            "test.lower_level_test_op", ExprOperatorSignature::MakeArgsN(1), "",
            FingerprintHasher(
                "arolla::expr::eval_internal::LowerLevelTestOperator")
                .Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    return GetQType<float>();
  }
};

TEST_P(EvalVisitorParameterizedTest, Extensions) {
  // Extension that transforms HigherLevelTestOperator into
  // LowerLevelTestOperator.
  eval_internal::NodeTransformationFn lower_transformation =
      [](const DynamicEvaluationEngineOptions&,
         ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
    if (node->is_op() &&
        fast_dynamic_downcast_final<const HigherLevelTestOperator*>(
            node->op().get()) != nullptr) {
      return BindOp(std::make_shared<LowerLevelTestOperator>(),
                    node->node_deps(), {});
    }
    return node;
  };
  eval_internal::CompilerExtensionRegistry::GetInstance()
      .RegisterNodeTransformationFn(lower_transformation);

  // Extension that compiles LowerLevelTestOperator.
  eval_internal::CompileOperatorFn compile_test_op =
      [](eval_internal::CompileOperatorFnArgs args)
      -> std::optional<absl::Status> {
    if (fast_dynamic_downcast_final<const LowerLevelTestOperator*>(
            args.op.get()) == nullptr) {
      return std::nullopt;
    }
    ASSIGN_OR_RETURN(auto output_slot, args.output_slot.ToSlot<float>());

    args.executable_builder->AddEvalOp(
        MakeBoundOperator(
            [output_slot](EvaluationContext* ctx, FramePtr frame) {
              frame.Set(output_slot, 57);
            }),
        eval_internal::FormatOperatorCall("lower level test operator", {},
                                          {args.output_slot}),
        "lower level test operator");
    return absl::OkStatus();
  };
  eval_internal::CompilerExtensionRegistry::GetInstance()
      .RegisterCompileOperatorFn(compile_test_op);

  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp(std::make_shared<HigherLevelTestOperator>(), {Leaf("x")}));

  FrameLayout::Builder layout_builder;
  auto x_slot = TypedSlot::FromSlot(layout_builder.AddSlot<float>());
  ASSERT_OK_AND_ASSIGN(auto bound_expr,
                       CompileAndBindForDynamicEvaluation(
                           options_, &layout_builder, expr, {{"x", x_slot}}));
  EXPECT_THAT(
      bound_expr,
      AllOf(InitOperationsAre(),
            EvalOperationsAre("FLOAT32 [0x04] = lower level test operator()")));
}

class OperatorThatFailsBind : public QExprOperator {
 public:
  OperatorThatFailsBind()
      : QExprOperator(QExprOperatorSignature::Get({GetQType<float>()},
                                                  GetQType<float>())) {}

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return absl::InternalError("test error");
  }
};

TEST_P(EvalVisitorParameterizedTest, OperatorThatFailsBind) {
  OperatorRegistry qexpr_registry;
  ASSERT_OK(qexpr_registry.RegisterOperator(
      "test.operator_that_fails_bind",
      std::make_unique<OperatorThatFailsBind>()));
  ExprOperatorPtr op = std::make_shared<BackendWrappingOperator>(
      "test.operator_that_fails_bind",
      ExprOperatorSignature::MakeVariadicArgs(),
      [](absl::Span<const QTypePtr> input_qtypes) -> absl::StatusOr<QTypePtr> {
        return GetQType<float>();
      },
      "");
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x")}));
  FrameLayout::Builder layout_builder;
  auto x_slot = TypedSlot::FromSlot(layout_builder.AddSlot<float>());
  DynamicEvaluationEngineOptions options(options_);
  options.operator_directory = &qexpr_registry;

  // Test that we annotate errors coming from QExprOperator::Bind sufficiently
  // to understand the context.
  EXPECT_THAT(
      CompileAndBindForDynamicEvaluation(options, &layout_builder, expr,
                                         {{"x", x_slot}}),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("test error; while binding operator "
                         "test.operator_that_fails_bind; while compiling node "
                         "test.operator_that_fails_bind(L.x)")));
}

}  // namespace
}  // namespace arolla::expr
