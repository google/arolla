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
// NOTE: The main test is in
// py/arolla/operator_tests/seq_reduce_test.py

#include "arolla/qexpr/eval_extensions/seq_reduce_operator.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/prepare_expression.h"
#include "arolla/expr/eval/test_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace arolla::expr::eval_internal {
namespace {

using ::arolla::testing::EqualsExpr;
using ::arolla::testing::IsOkAndHolds;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::NotNull;

class SeqReduceOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_OK(InitArolla()); }
};

TEST_F(SeqReduceOperatorTest, SeqMapOperatorTransformation) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr add_operator,
                       LookupOperator("math.add"));
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("seq.reduce", {Literal(add_operator), Leaf("xs"),
                                             Literal(int32_t{0})}));
  EXPECT_THAT(expr->qtype(), Eq(GetQType<int32_t>()));
  QTypePtr seq_i32 = GetSequenceQType(GetQType<int32_t>());
  ASSERT_OK_AND_ASSIGN(auto prepared_expr,
                       PrepareExpression(expr, {{"xs", seq_i32}},
                                         DynamicEvaluationEngineOptions{}));
  EXPECT_THAT(prepared_expr->qtype(), Eq(GetQType<int32_t>()));
  auto packed_op =
      dynamic_cast<const PackedSeqReduceOperator*>(prepared_expr->op().get());
  ASSERT_THAT(packed_op, NotNull());
  EXPECT_THAT(packed_op->op()->display_name(), Eq("math.add"));
  EXPECT_THAT(packed_op->display_name(), Eq("packed_seq_reduce[math.add]"));
  EXPECT_THAT(prepared_expr->node_deps(),
              ElementsAre(
                  // The first argument (reducer) got moved into packed_op.
                  EqualsExpr(CallOp(QTypeAnnotation::Make(),
                                    {Leaf("xs"), Literal(seq_i32)})),
                  EqualsExpr(Literal(int32_t{0}))));
}

TEST_F(SeqReduceOperatorTest, CompilePackedSeqReduceOperator) {
  ASSERT_OK_AND_ASSIGN(
      ExprOperatorPtr x_plus_y_mul_2,
      MakeLambdaOperator(
          "x_plus_y_mul_2", ExprOperatorSignature::Make("x, y"),
          CallOp("math.multiply",
                 {CallOp("math.add", {Placeholder("x"), Placeholder("y")}),
                  Literal(int32_t{2})})));

  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("seq.reduce", {Literal(x_plus_y_mul_2), Leaf("xs"),
                                       Literal(int32_t{0})}));
  QTypePtr seq_i32 = GetSequenceQType(GetQType<int32_t>());

  FrameLayout::Builder layout_builder;
  auto xs_slot = AddSlot(seq_i32, &layout_builder);
  DynamicEvaluationEngineOptions options{.collect_op_descriptions = true};
  EXPECT_THAT(
      CompileAndBindForDynamicEvaluation(options, &layout_builder, expr,
                                         {{"xs", xs_slot}}),
      IsOkAndHolds(AllOf(
          InitOperationsAre("packed_seq_reduce[x_plus_y_mul_2]:init{"
                            /**/ "INT32 [0x34] = 2"
                            "}()",
                            "INT32 [0x24] = 0"),
          EvalOperationsAre(
              "INT32 [0x20] = packed_seq_reduce[x_plus_y_mul_2]:eval{"
              /**/ "INT32 [0x30] = math.add(INT32 [0x28], INT32 [0x2C]); "
              /**/ "INT32 [0x20] = math.multiply(INT32 [0x30], INT32 [0x34])"
              "}(SEQUENCE[INT32] [0x00], INT32 [0x24])"))));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
