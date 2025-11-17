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
#include "arolla/expr/eval/dynamic_compiled_expr.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/prepare_expression.h"
#include "arolla/expr/eval/test_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr::eval_internal {
namespace {

TEST(DynamicCompiledExprTest, BindToExecutableBuilder) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("math.add", {Leaf("x"), Literal<int32_t>(1)}));
  absl::flat_hash_map<std::string, QTypePtr> input_types = {
      {"x", GetQType<int32_t>()}};
  ASSERT_OK_AND_ASSIGN(
      expr,
      PrepareExpression(expr, input_types, DynamicEvaluationEngineOptions{}));
  absl::flat_hash_map<Fingerprint, QTypePtr> node_types;
  ASSERT_OK_AND_ASSIGN(expr, ExtractQTypesForCompilation(expr, &node_types));
  DynamicCompiledExpr compiled_expr(
      DynamicEvaluationEngineOptions{}, input_types,
      /*output_type=*/GetQType<int32_t>(), /*named_output_types=*/{}, expr,
      /*side_output_names=*/{}, node_types);

  FrameLayout::Builder layout_builder;
  ExecutableBuilder executable_builder(&layout_builder,
                                       /*collect_op_descriptions=*/true);
  auto x1_slot = layout_builder.AddSlot<int32_t>();
  auto x2_slot = layout_builder.AddSlot<int32_t>();
  auto result_slot = layout_builder.AddSlot<int32_t>();
  auto other_result_slot = layout_builder.AddSlot<int32_t>();
  // result = x1 + 1
  ASSERT_OK(compiled_expr.BindToExecutableBuilder(
      executable_builder, {{"x", TypedSlot::FromSlot(x1_slot)}},
      TypedSlot::FromSlot(result_slot)));
  // other_result = x1 + 1
  ASSERT_OK(compiled_expr.BindToExecutableBuilder(
      executable_builder, {{"x", TypedSlot::FromSlot(x1_slot)}},
      TypedSlot::FromSlot(other_result_slot)));
  // other_result = x2 + 1
  ASSERT_OK(compiled_expr.BindToExecutableBuilder(
      executable_builder, {{"x", TypedSlot::FromSlot(x2_slot)}},
      TypedSlot::FromSlot(other_result_slot)));
  std::unique_ptr<BoundExpr> executable_expr =
      std::move(executable_builder)
          .Build({{"x1", TypedSlot::FromSlot(x1_slot)},
                  {"x2", TypedSlot::FromSlot(x2_slot)}},
                 TypedSlot::FromSlot(result_slot));

  EXPECT_THAT(
      executable_expr,
      AllOf(InitOperationsAre(
                // Each subexpr initializes its own literal.
                "INT32 [0x10] = 1\n"
                "INT32 [0x14] = 1\n"
                "INT32 [0x18] = 1"),
            EvalOperationsAre(
                "INT32 [0x08] = math.add(INT32 [0x00], INT32 [0x10])",
                "INT32 [0x0C] = math.add(INT32 [0x00], INT32 [0x14])",
                "INT32 [0x0C] = math.add(INT32 [0x04], INT32 [0x18])")));

  auto layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);
  alloc.frame().Set(x1_slot, 56);
  alloc.frame().Set(x2_slot, 1);
  EvaluationContext ctx;
  executable_expr->InitializeLiterals(&ctx, alloc.frame());
  executable_expr->Execute(&ctx, alloc.frame());
  EXPECT_OK(ctx.status());
  EXPECT_EQ(alloc.frame().Get(result_slot), 57);
  // While other_result_slot is not executable_expr->output_slot(), it is
  // updated anyway.
  EXPECT_EQ(alloc.frame().Get(other_result_slot), 2);
}

TEST(DynamicCompiledExprTest, BindToExecutableBuilder_DerivedQTypes) {
  ASSERT_OK_AND_ASSIGN(auto weak_tv,
                       TypedValue::FromValueWithQType(1., GetWeakFloatQType()));
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      // Make this a weak float literal.
      CallOp("math.add",
             {Leaf("x"), ExprNode::MakeLiteralNode(std::move(weak_tv))}));
  absl::flat_hash_map<std::string, QTypePtr> input_types = {
      {"x", GetWeakFloatQType()}};
  ASSERT_OK_AND_ASSIGN(
      expr,
      PrepareExpression(expr, input_types, DynamicEvaluationEngineOptions{}));
  absl::flat_hash_map<Fingerprint, QTypePtr> node_types;
  ASSERT_OK_AND_ASSIGN(expr, ExtractQTypesForCompilation(expr, &node_types));
  DynamicCompiledExpr compiled_expr(
      DynamicEvaluationEngineOptions{}, input_types,
      /*output_type=*/GetWeakFloatQType(), /*named_output_types=*/{}, expr,
      /*side_output_names=*/{}, node_types);

  FrameLayout::Builder layout_builder;
  ExecutableBuilder executable_builder(&layout_builder,
                                       /*collect_op_descriptions=*/true);
  auto x_slot = layout_builder.AddSlot<double>();
  auto result_slot = layout_builder.AddSlot<double>();
  ASSERT_OK(compiled_expr.BindToExecutableBuilder(
      executable_builder,
      {{"x", TypedSlot::FromSlot(x_slot, GetWeakFloatQType())}},
      TypedSlot::FromSlot(result_slot, GetWeakFloatQType())));
  std::unique_ptr<BoundExpr> executable_expr =
      std::move(executable_builder)
          .Build({{"x", TypedSlot::FromSlot(x_slot, GetWeakFloatQType())}},
                 TypedSlot::FromSlot(result_slot, GetWeakFloatQType()));

  auto layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&layout);
  alloc.frame().Set(x_slot, 3.f);
  EvaluationContext ctx;
  executable_expr->InitializeLiterals(&ctx, alloc.frame());
  executable_expr->Execute(&ctx, alloc.frame());
  EXPECT_OK(ctx.status());
  EXPECT_EQ(alloc.frame().Get(result_slot), 4.f);
}

}  // namespace
}  // namespace arolla::expr::eval_internal
