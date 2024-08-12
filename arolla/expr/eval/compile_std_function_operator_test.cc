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
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/eval/test_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operators/std_function_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::TypedValueWith;
using ::testing::AllOf;
using ::testing::Eq;

class StdFunctionOperatorTest
    : public ::testing::TestWithParam<DynamicEvaluationEngineOptions> {
 protected:
  void SetUp() override { InitArolla(); }

  DynamicEvaluationEngineOptions GetOptions() const { return GetParam(); }
};

absl::StatusOr<TypedValue> Add(absl::Span<const TypedRef> inputs) {
  ASSIGN_OR_RETURN(int32_t x, inputs[0].As<int32_t>());
  ASSIGN_OR_RETURN(int64_t y, inputs[1].As<int64_t>());
  double z = 3.0;
  return TypedValue::FromValue(x + y + z);
}

INSTANTIATE_TEST_SUITE_P(
    GarbageCollection, StdFunctionOperatorTest,
    ::testing::Values(
        DynamicEvaluationEngineOptions{.collect_op_descriptions = true,
                                       .allow_overriding_input_slots = false},
        DynamicEvaluationEngineOptions{.collect_op_descriptions = true,
                                       .allow_overriding_input_slots = true}));

// Evaluation is more thoroughly tested in std_function_operator_test.cc.
TEST_P(StdFunctionOperatorTest, SimpleFn) {
  auto op = std::make_shared<expr_operators::StdFunctionOperator>(
      "add", ExprOperatorSignature{{"x"}, {"y"}}, "dummy op docstring",
      [](absl::Span<const QTypePtr> input_qtypes) {
        return GetQType<double>();
      },
      Add);
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Leaf("y")}));

  FrameLayout::Builder layout_builder;
  expr::DynamicEvaluationEngineOptions options;
  options.collect_op_descriptions = true;

  auto x_slot = layout_builder.AddSlot<int32_t>();
  auto y_slot = layout_builder.AddSlot<int64_t>();

  EXPECT_THAT(expr::CompileAndBindForDynamicEvaluation(
                  options, &layout_builder, expr,
                  {{"x", TypedSlot::FromSlot(x_slot)},
                   {"y", TypedSlot::FromSlot(y_slot)}}),
              IsOkAndHolds(AllOf(
                  InitOperationsAre(),
                  EvalOperationsAre(
                      "FLOAT64 [0x10] = add(INT32 [0x00], INT64 [0x08])"))));
  EXPECT_THAT(Invoke(expr,
                     {{"x", TypedValue::FromValue(1)},
                      {"y", TypedValue::FromValue(int64_t{2})}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<double>(Eq(6.0))));
}

TEST_F(StdFunctionOperatorTest, StackTraceTest) {
  auto error_op = std::make_shared<expr_operators::StdFunctionOperator>(
      "error_op", ExprOperatorSignature{}, "dummy op docstring",
      [](absl::Span<const QTypePtr> input_qtypes) {
        return GetQType<double>();
      },
      [](absl::Span<const TypedRef> refs) -> absl::StatusOr<TypedValue> {
        return absl::InternalError("Error from StdFunctionOperator");
      });
  ASSERT_OK_AND_ASSIGN(
      auto error_lambda,
      MakeLambdaOperator("error_lambda", ExprOperatorSignature{},
                         CallOp(error_op, {})));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(error_lambda, {}));

  FrameLayout::Builder layout_builder;
  expr::DynamicEvaluationEngineOptions options{.enable_expr_stack_trace = true};

  EXPECT_THAT(expr::CompileAndBindForDynamicEvaluation(options, &layout_builder,
                                                       expr, {}),
              StatusIs(absl::StatusCode::kInternal,
                       "Error from StdFunctionOperator; "
                       "during evaluation of operator error_op\n"
                       "ORIGINAL NODE: error_lambda():FLOAT64\n"
                       "COMPILED NODE: error_op():FLOAT64; while doing literal"
                       " folding; while transforming error_lambda():FLOAT64"));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
