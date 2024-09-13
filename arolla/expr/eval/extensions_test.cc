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
#include "arolla/expr/eval/extensions.h"

#include <memory>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/prepare_expression.h"
#include "arolla/expr/eval/test_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

using ::arolla::testing::EqualsExpr;
using ::testing::Eq;

TEST(ExtensionsTest, RegisterNodeTransformationFn) {
  CompilerExtensionRegistry registry;

  NodeTransformationFn replace_add_with_sub =
      [](const DynamicEvaluationEngineOptions&,
         ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
    if (node->is_op() && node->op()->display_name() == "math.add") {
      return BindOp("math.subtract", node->node_deps(), {});
    }
    return node;
  };
  registry.RegisterNodeTransformationFn(replace_add_with_sub);

  NodeTransformationFn replace_sub_with_mul =
      [](const DynamicEvaluationEngineOptions&,
         ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
    if (node->is_op() && node->op()->display_name() == "math.subtract") {
      return BindOp("math.multiply", node->node_deps(), {});
    }
    return node;
  };
  registry.RegisterNodeTransformationFn(replace_sub_with_mul);

  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                       CallOp("math.add", {Leaf("x"), Literal(57)}));

  CompilerExtensionSet extensions = registry.GetCompilerExtensionSet();

  DynamicEvaluationEngineOptions options;
  ASSERT_OK_AND_ASSIGN(auto transformed_expr,
                       extensions.node_transformation_fn(options, expr));
  EXPECT_THAT(transformed_expr,
              EqualsExpr(CallOp("math.subtract", {Leaf("x"), Literal(57)})));

  ASSERT_OK_AND_ASSIGN(
      auto transforemed_transformed_expr,
      extensions.node_transformation_fn(options, transformed_expr));
  EXPECT_THAT(transforemed_transformed_expr,
              EqualsExpr(CallOp("math.multiply", {Leaf("x"), Literal(57)})));
}

class TestOperator final : public UnnamedExprOperator,
                           public BuiltinExprOperatorTag {
 public:
  TestOperator()
      : UnnamedExprOperator(
            ExprOperatorSignature::MakeArgsN(1),
            FingerprintHasher("arolla::expr::eval_internal::TestOperator")
                .Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    return GetQType<float>();
  }
};

class OtherOperator final : public UnnamedExprOperator,
                            public BuiltinExprOperatorTag {
 public:
  OtherOperator()
      : UnnamedExprOperator(
            ExprOperatorSignature::MakeArgsN(1),
            FingerprintHasher("arolla::expr::eval_internal::OtherOperator")
                .Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    return GetQType<float>();
  }
};

TEST(ExtensionsTest, RegisterCompileOperatorFn) {
  CompilerExtensionRegistry registry;

  CompileOperatorFn dummy_compile_op =
      [](CompileOperatorFnArgs args) -> std::optional<absl::Status> {
    return std::nullopt;
  };
  registry.RegisterCompileOperatorFn(dummy_compile_op);

  CompileOperatorFn compile_test_op =
      [](CompileOperatorFnArgs args) -> std::optional<absl::Status> {
    if (fast_dynamic_downcast_final<const TestOperator*>(args.op.get()) ==
        nullptr) {
      return std::nullopt;
    }
    ASSIGN_OR_RETURN(auto output_slot, args.output_slot.ToSlot<float>());

    args.executable_builder->AddEvalOp(
        MakeBoundOperator(
            [output_slot](EvaluationContext* ctx, FramePtr frame) {
              frame.Set(output_slot, 57);
            }),
        "eval test operator", "eval test operator");
    return absl::OkStatus();
  };
  registry.RegisterCompileOperatorFn(compile_test_op);

  CompilerExtensionSet extensions = registry.GetCompilerExtensionSet();

  FrameLayout::Builder layout_builder;
  ExecutableBuilder executable_builder(&layout_builder,
                                       /*collect_op_descriptions=*/true);
  auto out_slot = layout_builder.AddSlot<float>();

  // Unsupported operator.
  ExprOperatorPtr other_op = std::make_shared<OtherOperator>();
  EXPECT_THAT(extensions.compile_operator_fn(CompileOperatorFnArgs{
                  .options = DynamicEvaluationEngineOptions{},
                  .op = other_op,
                  .input_slots = {},
                  .output_slot = TypedSlot::FromSlot(out_slot),
                  .executable_builder = &executable_builder}),
              Eq(std::nullopt));

  // Supported operator.
  ExprOperatorPtr test_op = std::make_shared<TestOperator>();
  EXPECT_THAT(extensions.compile_operator_fn(CompileOperatorFnArgs{
                  .options = DynamicEvaluationEngineOptions{},
                  .op = test_op,
                  .input_slots = {},
                  .output_slot = TypedSlot::FromSlot(out_slot),
                  .executable_builder = &executable_builder}),
              Eq(absl::OkStatus()));

  std::unique_ptr<BoundExpr> bound_expr =
      std::move(executable_builder)
          .Build(/*input_slots=*/{},
                 /*output_slot=*/TypedSlot::FromSlot(out_slot));
  EXPECT_THAT(bound_expr, EvalOperationsAre("eval test operator"));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
