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
#include "arolla/expr/eval/dynamic_compiled_operator.h"

#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/test_utils.h"
#include "arolla/expr/eval/expr_stack_trace.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/util/text.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::WithSourceLocationAnnotation;
using ::testing::HasSubstr;

TEST(DynamicCompiledOperatorTest, DynamicCompiledOperator) {
  ASSERT_OK_AND_ASSIGN(
      auto lambda,
      MakeLambdaOperator(
          ExprOperatorSignature::Make("x, y"),
          CallOp("math.add",
                 {CallOp("math.add", {Placeholder("x"), Placeholder("y")}),
                  Literal(1.)})));
  ASSERT_OK_AND_ASSIGN(
      DynamicCompiledOperator op,
      DynamicCompiledOperator::Build(DynamicEvaluationEngineOptions{}, lambda,
                                     {GetQType<float>(), GetQType<double>()}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<double>();
  auto output_slot = layout_builder.AddSlot<double>();
  ExecutableBuilder executable_builder(&layout_builder,
                                       /*collect_op_descriptions=*/true);

  EXPECT_THAT(op.BindTo(executable_builder, {TypedSlot::FromSlot(x_slot)},
                        TypedSlot::FromSlot(output_slot)),
              StatusIs(absl::StatusCode::kInternal,
                       "input count mismatch in DynamicCompiledOperator: "
                       "expected 2, got 1"));

  EXPECT_THAT(
      op.BindTo(executable_builder,
                {TypedSlot::FromSlot(x_slot), TypedSlot::FromSlot(x_slot)},
                TypedSlot::FromSlot(output_slot)),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("slot types mismatch")));

  ASSERT_OK(
      op.BindTo(executable_builder,
                {TypedSlot::FromSlot(x_slot), TypedSlot::FromSlot(y_slot)},
                TypedSlot::FromSlot(output_slot)));
  std::unique_ptr<BoundExpr> executable_expr =
      std::move(executable_builder)
          .Build({{"x", TypedSlot::FromSlot(x_slot)},
                  {"y", TypedSlot::FromSlot(y_slot)}},
                 TypedSlot::FromSlot(output_slot));

  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre("FLOAT64 [0x28] = float64{1}"),
            EvalOperationsAre(
                "FLOAT64 [0x18] = core.to_float64(FLOAT32 [0x00])",
                "FLOAT64 [0x20] = math.add(FLOAT64 [0x18], FLOAT64 [0x08])",
                "FLOAT64 [0x10] = math.add(FLOAT64 [0x20], FLOAT64 [0x28])")));
}

TEST(DynamicCompiledOperatorTest, DynamicCompiledOperator_Literal) {
  ASSERT_OK_AND_ASSIGN(
      auto lambda, MakeLambdaOperator(ExprOperatorSignature{}, Literal(1.)));
  ASSERT_OK_AND_ASSIGN(DynamicCompiledOperator op,
                       DynamicCompiledOperator::Build(
                           DynamicEvaluationEngineOptions{}, lambda, {}));

  FrameLayout::Builder layout_builder;
  auto output_slot = layout_builder.AddSlot<double>();
  ExecutableBuilder executable_builder(&layout_builder,
                                       /*collect_op_descriptions=*/true);

  ASSERT_OK(
      op.BindTo(executable_builder, {}, TypedSlot::FromSlot(output_slot)));
  std::unique_ptr<BoundExpr> executable_expr =
      std::move(executable_builder).Build({}, TypedSlot::FromSlot(output_slot));

  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre("FLOAT64 [0x08] = float64{1}"),
            EvalOperationsAre("FLOAT64 [0x00] = core._copy(FLOAT64 [0x08])")));
}

TEST(DynamicCompiledOperatorTest, StackTraceMapping) {
  ASSERT_OK_AND_ASSIGN(
      auto op,
      MakeLambdaOperator(ExprOperatorSignature::Make("x"),
                         CallOp("core.with_assertion",
                                {Placeholder("x"), Literal(OptionalUnit{}),
                                 Literal(Text("error_msg"))})));

  ASSERT_OK_AND_ASSIGN(auto original_node, CallOp(op, {Leaf("a")}));
  ASSERT_OK_AND_ASSIGN(auto original_node_with_loc,
                       WithSourceLocationAnnotation(original_node, "my_func",
                                                    "file.py", 10, 0, ""));

  DetailedExprStackTrace stack_trace;
  stack_trace.InitNode(original_node_with_loc);

  ASSERT_OK_AND_ASSIGN(DynamicCompiledOperator compiled_op,
                       DynamicCompiledOperator::Build(
                           {.enable_expr_stack_trace = true}, op,
                           {GetQType<float>()}, original_node, &stack_trace));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<float>();
  auto output_slot = layout_builder.AddSlot<float>();

  ExecutableBuilder executable_builder(&layout_builder,
                                       /*collect_op_descriptions=*/true,
                                       stack_trace.StartBinding()());
  ASSERT_OK(compiled_op.BindTo(executable_builder,
                               {TypedSlot::FromSlot(a_slot)},
                               TypedSlot::FromSlot(output_slot)));
  std::unique_ptr<BoundExpr> executable_expr =
      std::move(executable_builder)
          .Build({{"x", TypedSlot::FromSlot(a_slot)}},
                 TypedSlot::FromSlot(output_slot));

  EvaluationContext ctx;
  FrameLayout layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);
  alloc.frame().Set(a_slot, 1.0f);

  ASSERT_OK(executable_expr->InitializeLiterals(alloc.frame()));
  executable_expr->Execute(&ctx, alloc.frame());

  absl::Status status = ctx.status();
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kFailedPrecondition,
                               HasSubstr("error_msg")));
  EXPECT_THAT(status.ToString(), HasSubstr("file.py:10"));
}


}  // namespace
}  // namespace arolla::expr::eval_internal
