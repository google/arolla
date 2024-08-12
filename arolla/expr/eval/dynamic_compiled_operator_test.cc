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
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

class DynamicCompiledOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(DynamicCompiledOperatorTest, DynamicCompiledOperator) {
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

TEST_F(DynamicCompiledOperatorTest, DynamicCompiledOperator_Literal) {
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

}  // namespace
}  // namespace arolla::expr::eval_internal
