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
// py/arolla/operator_tests/core_map_test.py

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/qtype/types.h"
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
#include "arolla/qexpr/eval_extensions/prepare_core_map_operator.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::EqualsExpr;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::NotNull;

class MapOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(MapOperatorTest, MapOperatorTransformation) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr add_operator,
                       LookupOperator("math.add"));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core.map", {Literal(add_operator),
                                                      Leaf("x"), Literal(1)}));
  EXPECT_THAT(expr->qtype(), Eq(nullptr));
  ASSERT_OK_AND_ASSIGN(
      auto prepared_expr,
      PrepareExpression(expr, {{"x", GetArrayQType<int64_t>()}},
                        DynamicEvaluationEngineOptions{}));
  EXPECT_THAT(prepared_expr->qtype(), Eq(GetArrayQType<int64_t>()));
  auto packed_op =
      dynamic_cast<const PackedCoreMapOperator*>(prepared_expr->op().get());
  ASSERT_THAT(packed_op, NotNull());
  EXPECT_THAT(packed_op->mapper().display_name(), Eq("wrapped[math.add]"));
  EXPECT_THAT(packed_op->display_name(),
              Eq("packed_core_map[wrapped[math.add]]"));
  EXPECT_THAT(packed_op->mapper().input_qtypes(),
              ElementsAre(GetOptionalQType<int64_t>()));
  EXPECT_THAT(packed_op->mapper().output_qtype(),
              Eq(GetOptionalQType<int64_t>()));
  EXPECT_THAT(
      prepared_expr->node_deps(),
      ElementsAre(
          // The first argument (op) got moved into packed_op.
          EqualsExpr(CallOp(QTypeAnnotation::Make(),
                            {Leaf("x"), Literal(GetArrayQType<int64_t>())}))
          // The third argument, Literal(1) got moved into op.
          ));
}

TEST_F(MapOperatorTest, CompilePackedCoreMapOperator) {
  ASSERT_OK_AND_ASSIGN(
      ExprOperatorPtr x_plus_y_mul_2,
      MakeLambdaOperator(
          "x_plus_y_mul_2", ExprOperatorSignature::Make("x, y"),
          CallOp("math.multiply",
                 {CallOp("math.add", {Placeholder("x"), Placeholder("y")}),
                  Literal(int32_t{2})})));

  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core.map", {Literal(x_plus_y_mul_2),
                                                      Leaf("xs"), Leaf("y")}));
  QTypePtr ai32 = GetDenseArrayQType<int32_t>();

  FrameLayout::Builder layout_builder;
  auto xs_slot = AddSlot(ai32, &layout_builder);
  auto y_slot = AddSlot(GetQType<int32_t>(), &layout_builder);
  DynamicEvaluationEngineOptions options{.collect_op_descriptions = true};
  EXPECT_THAT(
      CompileAndBindForDynamicEvaluation(options, &layout_builder, expr,
                                         {{"xs", xs_slot}, {"y", y_slot}}),
      IsOkAndHolds(AllOf(
          InitOperationsAre(),
          EvalOperationsAre(
              "DENSE_ARRAY_INT32 [0x50] = packed_core_map[x_plus_y_mul_2]"
              ":init{"
              /**/ "OPTIONAL_INT32 [0x24] = optional_int32{2}"
              "}:eval{"
              /**/ "OPTIONAL_INT32 [0x14] = core.to_optional._scalar(INT32 "
              /*    */ "[0x08]); "
              /**/ "OPTIONAL_INT32 [0x1C] = math.add(OPTIONAL_INT32 [0x00], "
              /*    */ "OPTIONAL_INT32 [0x14]); "
              "OPTIONAL_INT32 [0x0C] = math.multiply(OPTIONAL_INT32 [0x1C], "
              /*    */ "OPTIONAL_INT32 [0x24])"
              "}(DENSE_ARRAY_INT32 [0x00], INT32 [0x48])"))));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
