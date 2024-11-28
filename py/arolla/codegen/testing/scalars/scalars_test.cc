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
#include <memory>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//status/status.h"
#include "absl//status/status_matchers.h"
#include "py/arolla/codegen/testing/scalars/big_inline_chain_zero.h"
#include "py/arolla/codegen/testing/scalars/bytes_contains_me.h"
#include "py/arolla/codegen/testing/scalars/conditional_operators_test_zero_result.h"
#include "py/arolla/codegen/testing/scalars/const_ref_returning_operator_x_plus_y_result.h"
#include "py/arolla/codegen/testing/scalars/derived_qtype_casts.h"
#include "py/arolla/codegen/testing/scalars/identity_x.h"
#include "py/arolla/codegen/testing/scalars/identity_x_expensive_inputs.h"
#include "py/arolla/codegen/testing/scalars/literal_one.h"
#include "py/arolla/codegen/testing/scalars/many_nested_long_fibonacci_chains.h"
#include "py/arolla/codegen/testing/scalars/status_or_test_zero_result.h"
#include "py/arolla/codegen/testing/scalars/text_contains.h"
#include "py/arolla/codegen/testing/scalars/two_long_fibonacci_chains.h"
#include "py/arolla/codegen/testing/scalars/two_long_fibonacci_chains_expensive_inputs.h"
#include "py/arolla/codegen/testing/scalars/variadic_equation_str_printf.h"
#include "py/arolla/codegen/testing/scalars/variadic_equation_str_printf_optional.h"
#include "py/arolla/codegen/testing/scalars/variadic_hello_str_join.h"
#include "py/arolla/codegen/testing/scalars/variadic_hello_str_join_optional.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_optional.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_times_32_with_named_nodes.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_times_5.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_times_5_duplicated_export.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_times_5_nested_export.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_times_5_with_export.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_times_5_with_export_nodes_but_disabled_export.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_times_5_with_unused_export_x_minus_5.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_times_5_with_unused_two_nested_exports_xm5_andxm10.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_times_x.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::testing::_;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

TEST(CodegenScalarTest, LiteralOne) {
  FrameLayout::Builder layout_builder;
  auto z_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledLiteralOne().Bind(
                           &layout_builder, {}, TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(z_slot, -1.0f);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 1.0f);
}

TEST(CodegenScalarTest, IdentityX) {
  for (const auto* model :
       {&::test_namespace::GetCompiledIdentityX(),
        &::test_namespace::GetCompiledIdentityXExpensiveInputs()}) {
    FrameLayout::Builder layout_builder;
    auto x_slot = layout_builder.AddSlot<float>();
    auto z_slot = layout_builder.AddSlot<float>();
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<BoundExpr> executable,
        model->Bind(&layout_builder, {{"x", TypedSlot::FromSlot(x_slot)}},
                    TypedSlot::FromSlot(z_slot)));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    RootEvaluationContext ctx(&memory_layout);
    ASSERT_OK(executable->InitializeLiterals(&ctx));

    // Actual evaluation
    ctx.Set(x_slot, 3.0f);
    ctx.Set(z_slot, -1.0f);  // garbage value
    ASSERT_OK(executable->Execute(&ctx));
    EXPECT_EQ(ctx.Get(z_slot), 3.0f);
  }
}

TEST(CodegenScalarTest, TestCompiledXPlusYTimesX) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledXPlusYTimesX().Bind(
                           &layout_builder,
                           {{"x", TypedSlot::FromSlot(x_slot)},
                            {"y", TypedSlot::FromSlot(y_slot)}},
                           TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0f);
  ctx.Set(y_slot, 7.0f);
  ctx.Set(z_slot, -1.0f);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 30.0f);
}

TEST(CodegenScalarTest, TestCompiledXPlusYTimes5) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<double>();
  auto y_slot = layout_builder.AddSlot<double>();
  auto z_slot = layout_builder.AddSlot<double>();
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledXPlusYTimes5().Bind(
                           &layout_builder,
                           {{"x", TypedSlot::FromSlot(x_slot)},
                            {"y", TypedSlot::FromSlot(y_slot)}},
                           TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0);
  ctx.Set(y_slot, 7.0);
  ctx.Set(z_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 50.0);
}

TEST(CodegenScalarTest, TestCompiledXPlusYTimes32WithNamedNodes) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<double>();
  auto y_slot = layout_builder.AddSlot<double>();
  auto z_slot = layout_builder.AddSlot<double>();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledXPlusYTimes32WithNamedNodes().Bind(
          &layout_builder,
          {{"x", TypedSlot::FromSlot(x_slot)},
           {"y", TypedSlot::FromSlot(y_slot)}},
          TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0);
  ctx.Set(y_slot, 7.0);
  ctx.Set(z_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 320.0);
}

TEST(CodegenScalarTest, TestCompiledXPlusYTimes5WithExport) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<double>();
  auto y_slot = layout_builder.AddSlot<double>();
  auto z_slot = layout_builder.AddSlot<double>();
  // Collection of side outputs need to be explicitly enabled.
  EXPECT_THAT(::test_namespace::GetCompiledXPlusYTimes5WithExportButDisabled()
                  .named_output_types(),
              IsEmpty());

  EXPECT_THAT(::test_namespace::GetCompiledXPlusYTimes5WithExport()
                  .named_output_types(),
              UnorderedElementsAre(Pair("xpy", GetQType<double>()),
                                   Pair("xty", GetQType<double>())));
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledXPlusYTimes5WithExport().Bind(
          &layout_builder,
          {{"x", TypedSlot::FromSlot(x_slot)},
           {"y", TypedSlot::FromSlot(y_slot)}},
          TypedSlot::FromSlot(z_slot)));

  EXPECT_THAT(executable->named_output_slots(),
              UnorderedElementsAre(Pair("xpy", _), Pair("xty", _)));

  ASSERT_OK_AND_ASSIGN(
      auto xpy_slot,
      executable->named_output_slots().at("xpy").ToSlot<double>());
  ASSERT_OK_AND_ASSIGN(
      auto xty_slot,
      executable->named_output_slots().at("xty").ToSlot<double>());

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0);
  ctx.Set(y_slot, 7.0);
  ctx.Set(z_slot, -1.0);    // garbage value
  ctx.Set(xpy_slot, -2.0);  // garbage value
  ctx.Set(xty_slot, -3.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 50.0);
  EXPECT_EQ(ctx.Get(xpy_slot), 10.0);
  EXPECT_EQ(ctx.Get(xty_slot), 21.0);
}

TEST(CodegenScalarTest, TestCompiledXPlusYTimes5NestedExport) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<double>();
  auto y_slot = layout_builder.AddSlot<double>();
  auto z_slot = layout_builder.AddSlot<double>();
  const auto& compiled_expr =
      ::test_namespace::GetCompiledXPlusYTimes5NestedExport();
  EXPECT_THAT(compiled_expr.named_output_types(),
              UnorderedElementsAre(Pair("xpy", GetQType<double>()),
                                   Pair("x", GetQType<double>()),
                                   Pair("y", GetQType<double>())));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       compiled_expr.Bind(&layout_builder,
                                          {{"x", TypedSlot::FromSlot(x_slot)},
                                           {"y", TypedSlot::FromSlot(y_slot)}},
                                          TypedSlot::FromSlot(z_slot)));

  ASSERT_THAT(executable->named_output_slots(),
              UnorderedElementsAre(Pair("xpy", _), Pair("x", _), Pair("y", _)));

  ASSERT_OK_AND_ASSIGN(
      auto xpy_slot,
      executable->named_output_slots().at("xpy").ToSlot<double>());
  ASSERT_OK_AND_ASSIGN(
      auto x_out_slot,
      executable->named_output_slots().at("x").ToSlot<double>());
  ASSERT_OK_AND_ASSIGN(
      auto y_out_slot,
      executable->named_output_slots().at("y").ToSlot<double>());

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0);
  ctx.Set(y_slot, 7.0);
  ctx.Set(z_slot, -1.0);    // garbage value
  ctx.Set(xpy_slot, -2.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 50.0);
  EXPECT_EQ(ctx.Get(xpy_slot), 10.0);
  EXPECT_EQ(ctx.Get(x_out_slot), 3.0);
  EXPECT_EQ(ctx.Get(y_out_slot), 7.0);
}

TEST(CodegenScalarTest, TestCompiledXPlusYTimes5DuplicatedExport) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<double>();
  auto y_slot = layout_builder.AddSlot<double>();
  auto z_slot = layout_builder.AddSlot<double>();
  const auto& compiled_expr =
      ::test_namespace::GetCompiledXPlusYTimes5DuplicatedExport();
  EXPECT_THAT(compiled_expr.named_output_types(),
              UnorderedElementsAre(Pair("x", GetQType<double>()),
                                   Pair("x2", GetQType<double>()),
                                   Pair("x3", GetQType<double>())));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       compiled_expr.Bind(&layout_builder,
                                          {{"x", TypedSlot::FromSlot(x_slot)},
                                           {"y", TypedSlot::FromSlot(y_slot)}},
                                          TypedSlot::FromSlot(z_slot)));

  ASSERT_THAT(executable->named_output_slots(),
              UnorderedElementsAre(Pair("x", _), Pair("x2", _), Pair("x3", _)));

  ASSERT_OK_AND_ASSIGN(
      auto x_out_slot,
      executable->named_output_slots().at("x").ToSlot<double>());
  ASSERT_OK_AND_ASSIGN(
      auto x2_out_slot,
      executable->named_output_slots().at("x2").ToSlot<double>());
  ASSERT_OK_AND_ASSIGN(
      auto x3_out_slot,
      executable->named_output_slots().at("x3").ToSlot<double>());

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0);
  ctx.Set(y_slot, 7.0);
  ctx.Set(z_slot, -1.0);       // garbage value
  ctx.Set(x_out_slot, -2.0);   // garbage value
  ctx.Set(x2_out_slot, -2.0);  // garbage value
  ctx.Set(x3_out_slot, -2.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 50.0);
  EXPECT_EQ(ctx.Get(x_out_slot), 3.0);
  EXPECT_EQ(ctx.Get(x2_out_slot), 3.0);
  EXPECT_EQ(ctx.Get(x3_out_slot), 3.0);
}

TEST(CodegenScalarTest, TestCompiledXPlusYTimes5DuplicatedExportUnused) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<double>();
  auto y_slot = layout_builder.AddSlot<double>();
  auto z_slot = layout_builder.AddSlot<double>();
  const auto& compiled_expr =
      ::test_namespace::GetCompiledXPlusYTimes5DuplicatedExportUnused();
  EXPECT_THAT(compiled_expr.named_output_types(),
              UnorderedElementsAre(Pair("xy", GetQType<double>()),
                                   Pair("xy2", GetQType<double>())));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       compiled_expr.Bind(&layout_builder,
                                          {{"x", TypedSlot::FromSlot(x_slot)},
                                           {"y", TypedSlot::FromSlot(y_slot)}},
                                          TypedSlot::FromSlot(z_slot)));

  ASSERT_THAT(executable->named_output_slots(),
              UnorderedElementsAre(Pair("xy", _), Pair("xy2", _)));

  ASSERT_OK_AND_ASSIGN(
      auto xy_out_slot,
      executable->named_output_slots().at("xy").ToSlot<double>());
  ASSERT_OK_AND_ASSIGN(
      auto xy2_out_slot,
      executable->named_output_slots().at("xy2").ToSlot<double>());

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0);
  ctx.Set(y_slot, 7.0);
  ctx.Set(z_slot, -1.0);        // garbage value
  ctx.Set(xy_out_slot, -2.0);   // garbage value
  ctx.Set(xy2_out_slot, -2.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 50.0);
  EXPECT_EQ(ctx.Get(xy_out_slot), 21.0);
  EXPECT_EQ(ctx.Get(xy2_out_slot), 21.0);
}

// XMinus5 is exported, but not used for the computation of the root.
TEST(CodegenScalarTest, TestCompiledXPlusYTWithUnusedXMinus5) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<double>();
  auto y_slot = layout_builder.AddSlot<double>();
  auto z_slot = layout_builder.AddSlot<double>();
  const auto& compiled_model =
      ::test_namespace::GetCompiledXPlusYTimes5WithUnusedExportXMinus5();
  EXPECT_THAT(compiled_model.named_output_types(),
              UnorderedElementsAre(Pair("xpy", GetQType<double>()),
                                   Pair("xm5", GetQType<double>())));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       compiled_model.Bind(&layout_builder,
                                           {{"x", TypedSlot::FromSlot(x_slot)},
                                            {"y", TypedSlot::FromSlot(y_slot)}},
                                           TypedSlot::FromSlot(z_slot)));

  EXPECT_THAT(executable->named_output_slots(),
              UnorderedElementsAre(Pair("xpy", _), Pair("xm5", _)));

  ASSERT_OK_AND_ASSIGN(
      auto xpy_slot,
      executable->named_output_slots().at("xpy").ToSlot<double>());
  ASSERT_OK_AND_ASSIGN(
      auto xm5_slot,
      executable->named_output_slots().at("xm5").ToSlot<double>());

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0);
  ctx.Set(y_slot, 7.0);
  ctx.Set(z_slot, -100.0);    // garbage value
  ctx.Set(xpy_slot, -200.0);  // garbage value
  ctx.Set(xm5_slot, -200.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 50.0);
  EXPECT_EQ(ctx.Get(xpy_slot), 10.0);
  EXPECT_EQ(ctx.Get(xm5_slot), -2.0);
}

// XMinus5 and XMinus10 are exported and both
// not used for the computation of the root. XMinus5 is also used for
// computation of XMinus10.
TEST(CodegenScalarTest, TestCompiledXPlusYTWithUnusedXM5AndXM10) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<double>();
  auto y_slot = layout_builder.AddSlot<double>();
  auto z_slot = layout_builder.AddSlot<double>();
  const auto& compiled_model = ::test_namespace::
      GetCompiledXPlusYTimes5WithUnusedTwoNestedExportsXM5AndXm10();
  EXPECT_THAT(compiled_model.named_output_types(),
              UnorderedElementsAre(Pair("xpy", GetQType<double>()),
                                   Pair("xm5", GetQType<double>()),
                                   Pair("xm10", GetQType<double>())));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       compiled_model.Bind(&layout_builder,
                                           {{"x", TypedSlot::FromSlot(x_slot)},
                                            {"y", TypedSlot::FromSlot(y_slot)}},
                                           TypedSlot::FromSlot(z_slot)));

  EXPECT_THAT(
      executable->named_output_slots(),
      UnorderedElementsAre(Pair("xpy", _), Pair("xm5", _), Pair("xm10", _)));

  ASSERT_OK_AND_ASSIGN(
      auto xpy_slot,
      executable->named_output_slots().at("xpy").ToSlot<double>());
  ASSERT_OK_AND_ASSIGN(
      auto xm5_slot,
      executable->named_output_slots().at("xm5").ToSlot<double>());
  ASSERT_OK_AND_ASSIGN(
      auto xm10_slot,
      executable->named_output_slots().at("xm10").ToSlot<double>());

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0);
  ctx.Set(y_slot, 7.0);
  ctx.Set(z_slot, -100.0);     // garbage value
  ctx.Set(xpy_slot, -200.0);   // garbage value
  ctx.Set(xm5_slot, -200.0);   // garbage value
  ctx.Set(xm10_slot, -200.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 50.0);
  EXPECT_EQ(ctx.Get(xpy_slot), 10.0);
  EXPECT_EQ(ctx.Get(xm5_slot), -2.0);
  EXPECT_EQ(ctx.Get(xm10_slot), -7.0);
}

TEST(CodegenScalarTest, TestCompiledTwoFibonacciChains) {
  for (const auto* model :
       {&::test_namespace::GetCompiledTwoFibonacciChains(),
        &::test_namespace::GetCompiledTwoFibonacciChainsExpensiveInputs()}) {
    FrameLayout::Builder layout_builder;
    auto x_slot = layout_builder.AddSlot<float>();
    auto y_slot = layout_builder.AddSlot<float>();
    auto z_slot = layout_builder.AddSlot<float>();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                         model->Bind(&layout_builder,
                                     {{"x", TypedSlot::FromSlot(x_slot)},
                                      {"y", TypedSlot::FromSlot(y_slot)}},
                                     TypedSlot::FromSlot(z_slot)));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    RootEvaluationContext ctx(&memory_layout);
    ASSERT_OK(executable->InitializeLiterals(&ctx));

    // Actual evaluation
    ctx.Set(x_slot, 3.0f);
    ctx.Set(y_slot, 7.0f);
    ctx.Set(z_slot, -1.0);  // garbage value
    ASSERT_OK(executable->Execute(&ctx));
    EXPECT_EQ(ctx.Get(z_slot), 0.0f);
  }
}

TEST(CodegenScalarTest, TestGetCompiledInlineChainZero) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledInlineChainZero().Bind(
                           &layout_builder,
                           {{"x", TypedSlot::FromSlot(x_slot)},
                            {"y", TypedSlot::FromSlot(y_slot)}},
                           TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0f);
  ctx.Set(y_slot, 7.0f);
  ctx.Set(z_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 0.0f);
}

TEST(CodegenScalarTest, TestCompiledManyNestedFibonacciChains) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledManyNestedFibonacciChains().Bind(
          &layout_builder,
          {{"x", TypedSlot::FromSlot(x_slot)},
           {"y", TypedSlot::FromSlot(y_slot)}},
          TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 3.0f);
  ctx.Set(y_slot, 7.0f);
  ctx.Set(z_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 0.0f);
}

TEST(CodegenScalarTest, TestGetCompiledStatusOrTestZeroResult) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();
  EXPECT_THAT(::test_namespace::GetCompiledStatusOrTestZeroResult()
                  .named_output_types(),
              UnorderedElementsAre(Pair("x_floordiv_y", GetQType<float>()),
                                   Pair("y_floordiv_x", GetQType<float>())));
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledStatusOrTestZeroResult().Bind(
          &layout_builder,
          {{"x", TypedSlot::FromSlot(x_slot)},
           {"y", TypedSlot::FromSlot(y_slot)}},
          TypedSlot::FromSlot(z_slot)));
  ASSERT_OK_AND_ASSIGN(
      auto x_floordiv_y_slot,
      executable->named_output_slots().at("x_floordiv_y").ToSlot<float>());
  ASSERT_OK_AND_ASSIGN(
      auto y_floordiv_x_slot,
      executable->named_output_slots().at("y_floordiv_x").ToSlot<float>());

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Successful evaluation #1
  ctx.Set(x_slot, 7.0f);
  ctx.Set(y_slot, 3.0f);
  ctx.Set(z_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 0.0f);
  EXPECT_EQ(ctx.Get(x_floordiv_y_slot), 2.0f);
  EXPECT_EQ(ctx.Get(y_floordiv_x_slot), 0.0f);

  // Successful evaluation #2
  ctx.Set(x_slot, 3.0f);
  ctx.Set(y_slot, 7.0f);
  ctx.Set(z_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 0.0f);
  EXPECT_EQ(ctx.Get(x_floordiv_y_slot), 0.0f);
  EXPECT_EQ(ctx.Get(y_floordiv_x_slot), 2.0f);

  // Error evaluation
  ctx.Set(x_slot, 7.0f);
  ctx.Set(y_slot, 0.0f);
  ctx.Set(z_slot, -1.0);  // garbage value
  EXPECT_THAT(executable->Execute(&ctx),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("division by zero")));

  // Error on evaluation of unused for final result side output
  ctx.Set(x_slot, 0.0f);
  ctx.Set(y_slot, 7.0f);
  ctx.Set(z_slot, -1.0);  // garbage value
  EXPECT_THAT(executable->Execute(&ctx),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("division by zero")));
}

TEST(CodegenScalarTest, GetCompiledConditionalOperatorsTestZeroResult) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OptionalValue<float>>();
  auto y_slot = layout_builder.AddSlot<OptionalValue<float>>();
  auto z_slot = layout_builder.AddSlot<float>();
  const auto& compiled_expr =
      ::test_namespace::GetCompiledConditionalOperatorsTestZeroResult();
  EXPECT_THAT(compiled_expr.named_output_types(),
              UnorderedElementsAre(Pair("null", GetOptionalQType<float>())));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       compiled_expr.Bind(&layout_builder,
                                          {{"x", TypedSlot::FromSlot(x_slot)},
                                           {"y", TypedSlot::FromSlot(y_slot)}},
                                          TypedSlot::FromSlot(z_slot)));
  ASSERT_OK_AND_ASSIGN(auto null_slot, executable->named_output_slots()
                                           .at("null")
                                           .ToSlot<OptionalValue<float>>());

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Successful evaluation #1
  ctx.Set(x_slot, 7.0f);
  ctx.Set(y_slot, 3.0f);
  ctx.Set(z_slot, -1.0);     // garbage value
  ctx.Set(null_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 0.0f);
  EXPECT_EQ(ctx.Get(null_slot), 0.0f);

  // Successful evaluation #2. "null" is not needed for final computations
  ctx.Set(x_slot, -3.0f);
  ctx.Set(y_slot, 7.0f);
  ctx.Set(z_slot, -1.0);     // garbage value
  ctx.Set(null_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 0.0f);
  EXPECT_EQ(ctx.Get(null_slot), 0.0f);

  // Error evaluation
  ctx.Set(x_slot, 7.0f);
  ctx.Set(y_slot, 0.0f);
  ctx.Set(z_slot, -1.0);     // garbage value
  ctx.Set(null_slot, -1.0);  // garbage value
  EXPECT_THAT(executable->Execute(&ctx),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("division by zero")));
}

TEST(CodegenScalarTest, GetCompiledConstRefReturnXPlusYResultResult) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OptionalValue<float>>();
  auto y_slot = layout_builder.AddSlot<OptionalValue<float>>();
  auto z_slot = layout_builder.AddSlot<OptionalValue<float>>();
  auto res_slot = layout_builder.AddSlot<float>();
  const auto& compiled_expr =
      ::test_namespace::GetCompiledConstRefReturnXPlusYResult();
  EXPECT_THAT(compiled_expr.named_output_types(), IsEmpty());
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       compiled_expr.Bind(&layout_builder,
                                          {{"x", TypedSlot::FromSlot(x_slot)},
                                           {"y", TypedSlot::FromSlot(y_slot)},
                                           {"z", TypedSlot::FromSlot(z_slot)}},
                                          TypedSlot::FromSlot(res_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Successful evaluation true case
  ctx.Set(x_slot, 3.0f);
  ctx.Set(y_slot, 5.0f);
  ctx.Set(z_slot, 99.0);
  ctx.Set(res_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(res_slot), 89.0f);

  // Successful evaluation false case
  ctx.Set(x_slot, 7.0f);
  ctx.Set(y_slot, 3.0f);
  ctx.Set(z_slot, 99.0);
  ctx.Set(res_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(res_slot), 87.0f);

  // Successful evaluation missing case # 1
  ctx.Set(x_slot, std::nullopt);
  ctx.Set(y_slot, 5.0f);
  ctx.Set(z_slot, 99.0);
  ctx.Set(res_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(res_slot), 93.0f);

  // Successful evaluation missing case # 2
  ctx.Set(x_slot, 3.0f);
  ctx.Set(y_slot, std::nullopt);
  ctx.Set(z_slot, 99.0);
  ctx.Set(res_slot, -1.0);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(res_slot), 95.0f);
}

TEST(CodegenScalarTest, TestCompiledXPlusYOptional) {
  FrameLayout::Builder layout_builder;
  using Of32 = OptionalValue<float>;
  auto x_slot = layout_builder.AddSlot<Of32>();
  auto y_slot = layout_builder.AddSlot<Of32>();
  auto z_slot = layout_builder.AddSlot<Of32>();
  const auto& compiled_expr = ::test_namespace::GetCompiledXPlusYOptional();
  ASSERT_EQ(compiled_expr.output_type(), GetOptionalQType<float>());
  ASSERT_THAT(compiled_expr.input_types(),
              UnorderedElementsAre(Pair("x", GetOptionalQType<float>()),
                                   Pair("y", GetOptionalQType<float>())));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       compiled_expr.Bind(&layout_builder,
                                          {{"x", TypedSlot::FromSlot(x_slot)},
                                           {"y", TypedSlot::FromSlot(y_slot)}},
                                          TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Present values
  ctx.Set(x_slot, 3.0f);
  ctx.Set(y_slot, 7.0f);
  ctx.Set(z_slot, -1.0f);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), 10.0f);

  // Missed values
  ctx.Set(x_slot, std::nullopt);
  ctx.Set(y_slot, 7.0f);
  ctx.Set(z_slot, -1.0f);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(z_slot), std::nullopt);
}

TEST(CodegenScalarTest, TestCompiledTextContains) {
  FrameLayout::Builder layout_builder;
  auto text_slot = layout_builder.AddSlot<Text>();
  auto substr_slot = layout_builder.AddSlot<Text>();
  auto out_slot = layout_builder.AddSlot<OptionalUnit>();
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledTextContains().Bind(
                           &layout_builder,
                           {{"text", TypedSlot::FromSlot(text_slot)},
                            {"substr", TypedSlot::FromSlot(substr_slot)}},
                           TypedSlot::FromSlot(out_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(text_slot, Text("Find me here!"));
  ctx.Set(substr_slot, Text("me"));
  ctx.Set(out_slot, kMissing);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), kPresent);

  ctx.Set(text_slot, Text("Find m_e here!"));
  ctx.Set(substr_slot, Text("me"));
  ctx.Set(out_slot, kPresent);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), kMissing);
}

TEST(CodegenScalarTest, TestCompiledBytesContainsMe) {
  FrameLayout::Builder layout_builder;
  auto text_slot = layout_builder.AddSlot<Bytes>();
  auto out_slot = layout_builder.AddSlot<OptionalUnit>();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledBytesContainsMe().Bind(
          &layout_builder, {{"text", TypedSlot::FromSlot(text_slot)}},
          TypedSlot::FromSlot(out_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(text_slot, Bytes("Find me→ here!"));
  ctx.Set(out_slot, kMissing);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), kPresent);

  ctx.Set(text_slot, Bytes("Find m_e here!"));
  ctx.Set(out_slot, kPresent);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), kMissing);
}

TEST(CodegenScalarTest, TestGetCompiledHelloVariadicJoin) {
  FrameLayout::Builder layout_builder;
  auto title_slot = layout_builder.AddSlot<Text>();
  auto name_slot = layout_builder.AddSlot<Text>();
  auto out_slot = layout_builder.AddSlot<Text>();
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledHelloVariadicJoin().Bind(
                           &layout_builder,
                           {{"title", TypedSlot::FromSlot(title_slot)},
                            {"name", TypedSlot::FromSlot(name_slot)}},
                           TypedSlot::FromSlot(out_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(title_slot, Text("Dr."));
  ctx.Set(name_slot, Text("Haus"));
  ctx.Set(out_slot, Text("----"));  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), "Hello, Dr. Haus!");
}

TEST(CodegenScalarTest, TestGetCompiledHelloVariadicJoinOptional) {
  FrameLayout::Builder layout_builder;
  auto title_slot = layout_builder.AddSlot<OptionalValue<Bytes>>();
  auto name_slot = layout_builder.AddSlot<OptionalValue<Bytes>>();
  auto out_slot = layout_builder.AddSlot<OptionalValue<Bytes>>();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledHelloVariadicJoinOptional().Bind(
          &layout_builder,
          {{"title", TypedSlot::FromSlot(title_slot)},
           {"name", TypedSlot::FromSlot(name_slot)}},
          TypedSlot::FromSlot(out_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(title_slot, Bytes("Dr."));
  ctx.Set(name_slot, Bytes("Haus"));
  ctx.Set(out_slot, Bytes("----"));  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), "Hello, Dr. Haus!");

  ctx.Set(title_slot, Bytes("Dr."));
  ctx.Set(name_slot, std::nullopt);
  ctx.Set(out_slot, Bytes("----"));  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), std::nullopt);

  ctx.Set(title_slot, std::nullopt);
  ctx.Set(name_slot, Bytes("Haus"));
  ctx.Set(out_slot, Bytes("----"));  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), std::nullopt);

  ctx.Set(title_slot, std::nullopt);
  ctx.Set(name_slot, std::nullopt);
  ctx.Set(out_slot, Bytes("----"));  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), std::nullopt);
}

TEST(CodegenScalarTest, TestGetCompiledEquationVariadicStrPrintf) {
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<int>();
  auto out_slot = layout_builder.AddSlot<Bytes>();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledEquationVariadicStrPrintf().Bind(
          &layout_builder,
          {{"a", TypedSlot::FromSlot(a_slot)},
           {"b", TypedSlot::FromSlot(b_slot)}},
          TypedSlot::FromSlot(out_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(a_slot, 3);
  ctx.Set(b_slot, 4);
  ctx.Set(out_slot, Bytes("----"));  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), "3 + 4 = 7");
}

TEST(CodegenScalarTest, TestGetCompiledEquationVariadicStrPrintfOptional) {
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<OptionalValue<int>>();
  auto out_slot = layout_builder.AddSlot<OptionalValue<Bytes>>();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledEquationVariadicStrPrintfOptional().Bind(
          &layout_builder,
          {{"a", TypedSlot::FromSlot(a_slot)},
           {"b", TypedSlot::FromSlot(b_slot)}},
          TypedSlot::FromSlot(out_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(a_slot, 3);
  ctx.Set(b_slot, 4);
  ctx.Set(out_slot, Bytes("----"));  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), "3 + 4 = 7");

  ctx.Set(b_slot, OptionalValue<int>());
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), OptionalValue<Bytes>());
}

TEST(CodegenScalarTest, TestGetCompiledDerivedQTypeCasts) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<double>();
  auto out_slot = layout_builder.AddSlot<double>();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledDerivedQTypeCasts().Bind(
          &layout_builder, {{"x", TypedSlot::FromSlot(x_slot)}},
          TypedSlot::FromSlot(out_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(&ctx));

  // Actual evaluation
  ctx.Set(x_slot, 123.);
  ctx.Set(out_slot, 456.);  // garbage value
  ASSERT_OK(executable->Execute(&ctx));
  EXPECT_EQ(ctx.Get(out_slot), 123.);
}

}  // namespace
}  // namespace arolla
