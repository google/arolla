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
// python/arolla/operator_tests/seq_map_test.py

#include "arolla/qexpr/eval_extensions/seq_map_operator.h"

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

class SeqMapOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_OK(InitArolla()); }
};

TEST_F(SeqMapOperatorTest, SeqMapOperatorTransformation) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr add_operator,
                       LookupOperator("math.add"));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("seq.map", {Literal(add_operator),
                                                     Leaf("xs"), Leaf("ys")}));
  EXPECT_THAT(expr->qtype(), Eq(nullptr));
  QTypePtr seq_i32 = GetSequenceQType(GetQType<int32_t>());
  ASSERT_OK_AND_ASSIGN(
      auto prepared_expr,
      PrepareExpression(expr, {{"xs", seq_i32}, {"ys", seq_i32}},
                        DynamicEvaluationEngineOptions{}));
  EXPECT_THAT(prepared_expr->qtype(), Eq(seq_i32));
  auto packed_op =
      dynamic_cast<const PackedSeqMapOperator*>(prepared_expr->op().get());
  ASSERT_THAT(packed_op, NotNull());
  EXPECT_THAT(packed_op->op()->display_name(), Eq("math.add"));
  EXPECT_THAT(packed_op->display_name(), Eq("packed_seq_map[math.add]"));
  EXPECT_THAT(prepared_expr->node_deps(),
              ElementsAre(
                  // The first argument (mapper) got moved into packed_op.
                  EqualsExpr(CallOp(QTypeAnnotation::Make(),
                                    {Leaf("xs"), Literal(seq_i32)})),
                  EqualsExpr(CallOp(QTypeAnnotation::Make(),
                                    {Leaf("ys"), Literal(seq_i32)}))));
}

TEST_F(SeqMapOperatorTest, CompilePackedSeqMapOperator) {
  ASSERT_OK_AND_ASSIGN(
      ExprOperatorPtr x_plus_y_mul_2,
      MakeLambdaOperator(
          "x_plus_y_mul_2", ExprOperatorSignature::Make("x, y"),
          CallOp("math.multiply",
                 {CallOp("math.add", {Placeholder("x"), Placeholder("y")}),
                  Literal(int32_t{2})})));

  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("seq.map", {Literal(x_plus_y_mul_2),
                                                     Leaf("xs"), Leaf("ys")}));
  QTypePtr seq_i32 = GetSequenceQType(GetQType<int32_t>());

  FrameLayout::Builder layout_builder;
  auto xs_slot = AddSlot(seq_i32, &layout_builder);
  auto ys_slot = AddSlot(seq_i32, &layout_builder);
  DynamicEvaluationEngineOptions options{.collect_op_descriptions = true};
  EXPECT_THAT(
      CompileAndBindForDynamicEvaluation(options, &layout_builder, expr,
                                         {{"xs", xs_slot}, {"ys", ys_slot}}),
      IsOkAndHolds(AllOf(
          InitOperationsAre("packed_seq_map[x_plus_y_mul_2]:init{"
                            /**/ "INT32 [0x70] = 2"
                            "}()"),
          EvalOperationsAre(
              "SEQUENCE[INT32] [0x40] = packed_seq_map[x_plus_y_mul_2]:eval{"
              /**/ "INT32 [0x6C] = math.add(INT32 [0x60], INT32 [0x64]); "
              /**/ "INT32 [0x68] = math.multiply(INT32 [0x6C], INT32 [0x70])"
              "}(SEQUENCE[INT32] [0x00], SEQUENCE[INT32] [0x20])"))));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
