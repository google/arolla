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
#include "arolla/expr/eval/compile_where_operator.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/eval/dynamic_compiled_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/eval/prepare_expression.h"
#include "arolla/expr/eval/test_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/optimization/optimizer.h"
#include "arolla/expr/optimization/peephole_optimizations/short_circuit_where.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/unit.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::TypedValueWith;
using ::arolla::testing::WithNameAnnotation;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::NotNull;

// Creates a FrameLayout with slots of the given types, and compiles & binds
// expression to these slots. Useful for tests with InitOperationsAre() and
// EvalOperationsAre().
absl::StatusOr<std::unique_ptr<BoundExpr>> CompileExprWithTypes(
    DynamicEvaluationEngineOptions options, ExprNodePtr expr,
    absl::flat_hash_map<std::string, QTypePtr> leaf_qtypes) {
  // We sort leaves to guarantee persistent order of input slots.
  std::vector<std::string> leaves_in_order;
  for (const auto& [leaf, _] : leaf_qtypes) {
    leaves_in_order.push_back(leaf);
  }
  std::sort(leaves_in_order.begin(), leaves_in_order.end());

  absl::flat_hash_map<std::string, TypedSlot> input_slots;
  FrameLayout::Builder layout_builder;
  for (const auto& leaf : leaves_in_order) {
    input_slots.emplace(leaf, AddSlot(leaf_qtypes.at(leaf), &layout_builder));
  }

  return CompileAndBindForDynamicEvaluation(options, &layout_builder, expr,
                                            input_slots);
}

class WhereOperatorTest
    : public ::testing::TestWithParam<DynamicEvaluationEngineOptions> {
 protected:
  DynamicEvaluationEngineOptions GetOptions() const { return GetParam(); }
};

INSTANTIATE_TEST_SUITE_P(
    GarbageCollection, WhereOperatorTest,
    ::testing::Values(
        DynamicEvaluationEngineOptions{.collect_op_descriptions = true,
                                       .allow_overriding_input_slots = false},
        DynamicEvaluationEngineOptions{.collect_op_descriptions = true,
                                       .allow_overriding_input_slots = true}));

TEST_P(WhereOperatorTest,
       WhereOperatorGlobalTransformation_AnnotationHandling) {
  ASSERT_OK_AND_ASSIGN(
      auto cond, WithQTypeAnnotation(Leaf("cond"), GetOptionalQType<Unit>()));
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto named_x_plus_y,
                       WithNameAnnotation(x_plus_y, "name_for_x_plus_y"));
  // cond ? (x + y) * y : (x + y) * (y + 1)
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp(
          "core._short_circuit_where",
          {
              cond,
              // All the annotations, except the ones on leaves are stripped
              // during PrepareExpression — and so are not allowed in
              // WhereOperatorGlobalTransformation.
              WithQTypeAnnotation(CallOp("math.multiply", {named_x_plus_y, y}),
                                  GetQType<float>()),
              CallOp("math.multiply",
                     {x_plus_y, CallOp("math.add", {y, Literal<float>(1.)})}),
          }));
  EXPECT_THAT(WhereOperatorGlobalTransformation(GetOptions(), expr),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("WhereOperatorGlobalTransformation does not "
                                 "support annotations except for leaves")));

  ASSERT_OK_AND_ASSIGN(auto prepared_expr,
                       PrepareExpression(expr, {}, GetOptions()));

  const auto* packed_where =
      dynamic_cast<const PackedWhereOp*>(prepared_expr->op().get());
  ASSERT_THAT(packed_where, NotNull());
  ASSERT_THAT(prepared_expr->node_deps(),
              ElementsAre(EqualsExpr(cond),
                          // First branch arguments.
                          EqualsExpr(x_plus_y), EqualsExpr(y),
                          // Second branch arguments.
                          EqualsExpr(x_plus_y), EqualsExpr(y),
                          EqualsExpr(Literal<float>(1))));
}

TEST_P(WhereOperatorTest, SimpleWhere) {
  // cond ? x + y : x - y
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("core._short_circuit_where",
             {Leaf("cond"), CallOp("math.add", {Leaf("x"), Leaf("y")}),
              CallOp("math.subtract", {Leaf("x"), Leaf("y")})}));

  EXPECT_THAT(
      CompileExprWithTypes(GetOptions(), expr,
                           {{"cond", GetQType<OptionalUnit>()},
                            {"x", GetQType<int32_t>()},
                            {"y", GetQType<int32_t>()}}),
      IsOkAndHolds(AllOf(
          InitOperationsAre(),
          EvalOperationsAre(
              "jump_if_not<+2>(OPTIONAL_UNIT [0x00])",
              "INT32 [0x0C] = math.add(INT32 [0x04], INT32 [0x08])",
              "jump<+1>()",
              "INT32 [0x0C] = math.subtract(INT32 [0x04], INT32 [0x08])"))));

  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kPresent)},
                      {"x", TypedValue::FromValue(1)},
                      {"y", TypedValue::FromValue(2)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(3))));
  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kMissing)},
                      {"x", TypedValue::FromValue(1)},
                      {"y", TypedValue::FromValue(2)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(-1))));
}

TEST_P(WhereOperatorTest, PackedWhereOpComputeOutputQType) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr math_add, LookupOperator("math.add"));
  ASSERT_OK_AND_ASSIGN(
      auto add_float_double,
      DynamicCompiledOperator::Build(GetOptions(), math_add,
                                     {GetQType<float>(), GetQType<double>()}));
  ASSERT_OK_AND_ASSIGN(
      auto add_doubles,
      DynamicCompiledOperator::Build(GetOptions(), math_add,
                                     {GetQType<double>(), GetQType<double>()}));
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr packed_where,
                       PackedWhereOp::Create(std::move(add_float_double),
                                             std::move(add_doubles)));
  EXPECT_THAT(packed_where->InferAttributes({}),
              StatusIs(absl::StatusCode::kInternal,
                       "number of args for internal.packed_where operator "
                       "changed during compilation"));
  auto b = ExprAttributes(GetQType<bool>());
  auto f = ExprAttributes(GetQType<float>());
  auto d = ExprAttributes(GetQType<double>());
  EXPECT_THAT(packed_where->InferAttributes({b, f, f, d, d}),
              StatusIs(absl::StatusCode::kInternal,
                       "input types for internal.packed_where operator changed "
                       "during compilation"));
  {
    ASSERT_OK_AND_ASSIGN(auto attr,
                         packed_where->InferAttributes({b, f, d, d, d}));
    EXPECT_THAT(attr.qtype(), Eq(GetQType<double>()));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto attr, packed_where->InferAttributes(
                       {ExprAttributes{}, ExprAttributes{}, ExprAttributes{},
                        ExprAttributes{}, ExprAttributes{}}));
    EXPECT_THAT(attr.qtype(), Eq(GetQType<double>()));
  }
}

TEST_P(WhereOperatorTest, WhereWithTypeCasting) {
  // cond ? x : y
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core._short_circuit_where",
                                         {Leaf("cond"), Leaf("x"), Leaf("y")}));

  EXPECT_THAT(
      CompileExprWithTypes(GetOptions(), expr,
                           {{"cond", GetQType<OptionalUnit>()},
                            {"x", GetQType<int32_t>()},
                            {"y", GetOptionalQType<int32_t>()}}),
      IsOkAndHolds(AllOf(
          InitOperationsAre(),
          EvalOperationsAre(
              "jump_if_not<+2>(OPTIONAL_UNIT [0x00])",
              "OPTIONAL_INT32 [0x10] = core.to_optional._scalar(INT32 [0x04])",
              "jump<+1>()",
              "OPTIONAL_INT32 [0x10] = core._copy(OPTIONAL_INT32 [0x08])"))));

  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kPresent)},
                      {"x", TypedValue::FromValue(1)},
                      {"y", TypedValue::FromValue(OptionalValue(0))}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<OptionalValue<int32_t>>(Eq(1))));
}

TEST_P(WhereOperatorTest, WhereWithEqualBranches) {
  ASSERT_OK_AND_ASSIGN(auto x_plus_y,
                       CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core._short_circuit_where",
                                         {Leaf("cond"), x_plus_y, x_plus_y}));

  EXPECT_THAT(CompileExprWithTypes(GetOptions(), expr,
                                   {{"cond", GetQType<OptionalUnit>()},
                                    {"x", GetQType<int32_t>()},
                                    {"y", GetQType<int32_t>()}}),
              IsOkAndHolds(AllOf(
                  InitOperationsAre(),
                  EvalOperationsAre(
                      "INT32 [0x10] = math.add(INT32 [0x04], INT32 [0x08])",
                      "INT32 [0x0C] = core.where(OPTIONAL_UNIT [0x00], INT32 "
                      "[0x10], INT32 [0x10])"))));

  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kPresent)},
                      {"x", TypedValue::FromValue(1)},
                      {"y", TypedValue::FromValue(2)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(3))));
}

TEST_P(WhereOperatorTest, NothingToShortCircuit) {
  // `x_plus_y` is also computed unconditionally, so there is nothing left to
  // short circuit.
  auto x_plus_y = CallOp("math.add", {Leaf("x"), Leaf("y")});
  auto cond = Leaf("cond");
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add", {CallOp("core._short_circuit_where",
                                 {Leaf("cond"), x_plus_y, Leaf("y")}),
                          x_plus_y}));

  DynamicEvaluationEngineOptions options = GetOptions();
  // Depending on the flag we get different offsets in the generated program, so
  // we te only the more interesting setting.
  options.allow_overriding_input_slots = true;

  EXPECT_THAT(CompileExprWithTypes(options, expr,
                                   {{"cond", GetQType<OptionalUnit>()},
                                    {"x", GetQType<int32_t>()},
                                    {"y", GetQType<int32_t>()}}),
              IsOkAndHolds(AllOf(
                  InitOperationsAre(),
                  EvalOperationsAre(
                      "INT32 [0x10] = math.add(INT32 [0x04], INT32 [0x08])",
                      "INT32 [0x04] = core.where(OPTIONAL_UNIT [0x00], INT32 "
                      "[0x10], INT32 [0x08])",
                      "INT32 [0x0C] = math.add(INT32 [0x04], INT32 [0x10])"))));

  // In case of (scalar, array, array) arguments we rely on the fact that the
  // corresponding version of `core.where` is available in backend, so we test
  // it explicitly.
  EXPECT_THAT(
      CompileExprWithTypes(options, expr,
                           {{"cond", GetQType<OptionalUnit>()},
                            {"x", GetDenseArrayQType<int32_t>()},
                            {"y", GetDenseArrayQType<int32_t>()}}),
      IsOkAndHolds(AllOf(
          InitOperationsAre(),
          EvalOperationsAre(
              "DENSE_ARRAY_INT32 [0xE0] = math.add(DENSE_ARRAY_INT32 [0x08], "
              "DENSE_ARRAY_INT32 [0x50])",
              "DENSE_ARRAY_INT32 [0x08] = core.where(OPTIONAL_UNIT [0x00], "
              "DENSE_ARRAY_INT32 [0xE0], DENSE_ARRAY_INT32 [0x50])",
              "DENSE_ARRAY_INT32 [0x98] = math.add(DENSE_ARRAY_INT32 [0x08], "
              "DENSE_ARRAY_INT32 [0xE0])"))));
  EXPECT_THAT(
      CompileExprWithTypes(options, expr,
                           {{"cond", GetQType<OptionalUnit>()},
                            {"x", GetArrayQType<int32_t>()},
                            {"y", GetArrayQType<int32_t>()}}),
      IsOkAndHolds(AllOf(
          InitOperationsAre(),
          EvalOperationsAre("ARRAY_INT32 [0x1A0] = math.add(ARRAY_INT32 "
                            "[0x08], ARRAY_INT32 [0x90])",
                            "ARRAY_INT32 [0x08] = core.where(OPTIONAL_UNIT "
                            "[0x00], ARRAY_INT32 [0x1A0], ARRAY_INT32 [0x90])",
                            "ARRAY_INT32 [0x118] = math.add(ARRAY_INT32 "
                            "[0x08], ARRAY_INT32 [0x1A0])"))));
}

TEST_P(WhereOperatorTest, WhereWithIndependentBranches) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("core._short_circuit_where",
             {Leaf("cond"), CallOp("math.add", {Literal(1), Literal(2)}),
              CallOp("math.add", {Literal(2), Literal(3)})}));

  auto options = GetOptions();
  // We want some non-trivial expressions to be short-circuited.
  options.enabled_preparation_stages &=
      ~DynamicEvaluationEngineOptions::PreparationStage::kLiteralFolding;

  EXPECT_THAT(
      CompileExprWithTypes(options, expr, {{"cond", GetQType<OptionalUnit>()}}),
      IsOkAndHolds(
          AllOf(InitOperationsAre("INT32 [0x08] = 1\n"
                                  "INT32 [0x0C] = 2\n"
                                  "INT32 [0x10] = 3"),
                EvalOperationsAre(
                    "jump_if_not<+2>(OPTIONAL_UNIT [0x00])",
                    "INT32 [0x04] = math.add(INT32 [0x08], INT32 [0x0C])",
                    "jump<+1>()",
                    "INT32 [0x04] = math.add(INT32 [0x0C], INT32 [0x10])"))));

  EXPECT_THAT(
      Invoke(expr, {{"cond", TypedValue::FromValue(kPresent)}}, options),
      IsOkAndHolds(TypedValueWith<int32_t>(Eq(3))));
  EXPECT_THAT(
      Invoke(expr, {{"cond", TypedValue::FromValue(kMissing)}}, options),
      IsOkAndHolds(TypedValueWith<int32_t>(Eq(5))));
}

TEST_P(WhereOperatorTest, WhereWithIncompatibleTypes) {
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core._short_circuit_where",
                                         {Leaf("cond"), Leaf("x"), Leaf("y")}));

  EXPECT_THAT(CompileExprWithTypes(GetOptions(), expr,
                                   {{"cond", GetQType<OptionalUnit>()},
                                    {"x", GetQType<int32_t>()},
                                    {"y", GetQType<Bytes>()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("incompatible types true_branch: INT32 and "
                                 "false_branch: BYTES")));
}

TEST_P(WhereOperatorTest, WhereWithExpressions) {
  auto cond = Leaf("cond");
  auto x = Leaf("x");
  auto y = Leaf("y");

  // cond ? x * y : x + y
  ASSERT_OK_AND_ASSIGN(auto x_mul_y, CallOp("math.multiply", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr, CallOp("core._short_circuit_where",
                                                {cond, x_mul_y, x_plus_y}));

  EXPECT_THAT(
      CompileExprWithTypes(GetOptions(), expr,
                           {{"cond", GetQType<OptionalUnit>()},
                            {"x", GetQType<int32_t>()},
                            {"y", GetQType<int32_t>()}}),
      IsOkAndHolds(
          AllOf(InitOperationsAre(),
                EvalOperationsAre(
                    "jump_if_not<+2>(OPTIONAL_UNIT [0x00])",
                    "INT32 [0x0C] = math.multiply(INT32 [0x04], INT32 [0x08])",
                    "jump<+1>()",
                    "INT32 [0x0C] = math.add(INT32 [0x04], INT32 [0x08])"))));

  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kPresent)},
                      {"x", TypedValue::FromValue(3)},
                      {"y", TypedValue::FromValue(19)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(57))));
  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kMissing)},
                      {"x", TypedValue::FromValue(50)},
                      {"y", TypedValue::FromValue(7)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(57))));
}

TEST_P(WhereOperatorTest, WhereWithInputSlotsOverwriting) {
  auto cond = Leaf("cond");
  auto x = Leaf("x");

  ASSERT_OK_AND_ASSIGN(auto mult, CallOp("math.multiply", {x, x}));
  ASSERT_OK_AND_ASSIGN(mult, CallOp("math.multiply", {mult, mult}));
  ASSERT_OK_AND_ASSIGN(mult, CallOp("math.multiply", {mult, mult}));

  ASSERT_OK_AND_ASSIGN(auto sum, CallOp("math.add", {mult, mult}));
  ASSERT_OK_AND_ASSIGN(sum, CallOp("math.add", {sum, sum}));
  ASSERT_OK_AND_ASSIGN(sum, CallOp("math.add", {sum, sum}));

  ASSERT_OK_AND_ASSIGN(auto sub, CallOp("math.subtract", {mult, mult}));
  ASSERT_OK_AND_ASSIGN(sub, CallOp("math.subtract", {sub, sub}));
  ASSERT_OK_AND_ASSIGN(sub, CallOp("math.subtract", {sub, sub}));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                       CallOp("core._short_circuit_where", {cond, sum, sub}));

  if (GetOptions().allow_overriding_input_slots) {
    EXPECT_THAT(
        CompileExprWithTypes(
            GetOptions(), expr,
            {{"cond", GetQType<OptionalUnit>()}, {"x", GetQType<int32_t>()}}),
        IsOkAndHolds(AllOf(
            InitOperationsAre(),
            EvalOperationsAre(
                "INT32 [0x0C] = math.multiply(INT32 [0x04], INT32 [0x04])",
                // Global input slot "INT32 [0x04]" is reused.
                "INT32 [0x04] = math.multiply(INT32 [0x0C], INT32 [0x0C])",
                "INT32 [0x0C] = math.multiply(INT32 [0x04], INT32 [0x04])",
                "jump_if_not<+4>(OPTIONAL_UNIT [0x00])",
                // A branch input slot "INT32 [0x0C]" is not reused.
                "INT32 [0x10] = math.add(INT32 [0x0C], INT32 [0x0C])",
                "INT32 [0x14] = math.add(INT32 [0x10], INT32 [0x10])",
                "INT32 [0x08] = math.add(INT32 [0x14], INT32 [0x14])",
                "jump<+3>()",
                // A branch input slot "INT32 [0x0C]" is not reused.
                "INT32 [0x18] = math.subtract(INT32 [0x0C], INT32 [0x0C])",
                "INT32 [0x1C] = math.subtract(INT32 [0x18], INT32 [0x18])",
                "INT32 [0x08] = math.subtract(INT32 [0x1C], INT32 [0x1C])"))));
  } else {
    EXPECT_THAT(
        CompileExprWithTypes(
            GetOptions(), expr,
            {{"cond", GetQType<OptionalUnit>()}, {"x", GetQType<int32_t>()}}),
        IsOkAndHolds(AllOf(
            InitOperationsAre(),
            EvalOperationsAre(
                // Global input slot "INT32 [0x04]" is not reused.
                "INT32 [0x0C] = math.multiply(INT32 [0x04], INT32 [0x04])",
                "INT32 [0x10] = math.multiply(INT32 [0x0C], INT32 [0x0C])",
                "INT32 [0x0C] = math.multiply(INT32 [0x10], INT32 [0x10])",
                // A branch input slot "INT32 [0x0C]" is not reused.
                "jump_if_not<+4>(OPTIONAL_UNIT [0x00])",
                "INT32 [0x14] = math.add(INT32 [0x0C], INT32 [0x0C])",
                "INT32 [0x18] = math.add(INT32 [0x14], INT32 [0x14])",
                "INT32 [0x08] = math.add(INT32 [0x18], INT32 [0x18])",
                "jump<+3>()",
                // A branch input slot "INT32 [0x0C]" is not reused.
                "INT32 [0x1C] = math.subtract(INT32 [0x0C], INT32 [0x0C])",
                "INT32 [0x20] = math.subtract(INT32 [0x1C], INT32 [0x1C])",
                "INT32 [0x08] = math.subtract(INT32 [0x20], INT32 [0x20])"))));
  }

  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kPresent)},
                      {"x", TypedValue::FromValue(2)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(2048))));
  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kMissing)},
                      {"x", TypedValue::FromValue(2)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(0))));
}

TEST_P(WhereOperatorTest, ShortCircuit) {
  // cond ? x + 1 : x / 0
  ASSERT_OK_AND_ASSIGN(ExprNodePtr x_plus_1,
                       CallOp("math.add", {Leaf("x"), Literal(1)}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr x_div_0,
                       CallOp("math.floordiv", {Leaf("x"), Literal(0)}));
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expr,
      CallOp("core._short_circuit_where", {Leaf("cond"), x_plus_1, x_div_0}));
  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kPresent)},
                      {"x", TypedValue::FromValue(56)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(57))));
  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kMissing)},
                      {"x", TypedValue::FromValue(56)}},
                     GetOptions()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("division by zero")));
}

TEST_P(WhereOperatorTest, WhereWithLiteral) {
  // cond ? x + 1 : x + 2
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expr,
      CallOp("core._short_circuit_where",
             {Leaf("cond"), CallOp("math.add", {Leaf("x"), Literal(27)}),
              CallOp("math.subtract", {Leaf("x"), Literal(27)})}));

  EXPECT_THAT(
      CompileExprWithTypes(GetOptions(), expr,
                           {{"cond", GetQType<OptionalUnit>()},
                            {"x", GetQType<int32_t>()},
                            {"y", GetQType<int32_t>()}}),
      IsOkAndHolds(AllOf(
          InitOperationsAre("INT32 [0x10] = 27"),
          EvalOperationsAre(
              "jump_if_not<+2>(OPTIONAL_UNIT [0x00])",
              "INT32 [0x0C] = math.add(INT32 [0x04], INT32 [0x10])",
              "jump<+1>()",
              "INT32 [0x0C] = math.subtract(INT32 [0x04], INT32 [0x10])"))));

  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kPresent)},
                      {"x", TypedValue::FromValue(30)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(57))));
  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kMissing)},
                      {"x", TypedValue::FromValue(30)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(3))));
}

TEST_P(WhereOperatorTest, WhereWithCommonBranches) {
  // cond ? x + y : x + y + 1
  ASSERT_OK_AND_ASSIGN(ExprNodePtr x_plus_y,
                       CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr x_plus_y_plus_1,
                       CallOp("math.add", {x_plus_y, Literal(1)}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                       CallOp("core._short_circuit_where",
                              {Leaf("cond"), x_plus_y, x_plus_y_plus_1}));

  EXPECT_THAT(CompileExprWithTypes(GetOptions(), expr,
                                   {{"cond", GetQType<OptionalUnit>()},
                                    {"x", GetQType<int32_t>()},
                                    {"y", GetQType<int32_t>()}}),
              IsOkAndHolds(AllOf(
                  InitOperationsAre("INT32 [0x14] = 1"),
                  EvalOperationsAre(
                      "INT32 [0x10] = math.add(INT32 [0x04], INT32 [0x08])",
                      "jump_if_not<+2>(OPTIONAL_UNIT [0x00])",
                      "INT32 [0x0C] = core._copy(INT32 [0x10])",  //
                      "jump<+1>()",
                      "INT32 [0x0C] = math.add(INT32 [0x10], INT32 [0x14])"))));

  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kPresent)},
                      {"x", TypedValue::FromValue(50)},
                      {"y", TypedValue::FromValue(7)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(57))));
  EXPECT_THAT(Invoke(expr,
                     {{"cond", TypedValue::FromValue(kMissing)},
                      {"x", TypedValue::FromValue(50)},
                      {"y", TypedValue::FromValue(7)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(58))));
}

TEST_P(WhereOperatorTest, NestedWhere) {
  auto cond1 = Leaf("cond1");
  auto cond2 = Leaf("cond2");
  auto x = Leaf("x");
  auto y = Leaf("y");
  // cond1 ? (cond2 ? x + y : y) : (cond2 ? x - y : x)
  ASSERT_OK_AND_ASSIGN(auto true_case_where,
                       CallOp("core._short_circuit_where",
                              {cond2, CallOp("math.add", {x, y}), y}));
  ASSERT_OK_AND_ASSIGN(auto false_case_where,
                       CallOp("core._short_circuit_where",
                              {cond2, CallOp("math.subtract", {x, y}), x}));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                       CallOp("core._short_circuit_where",
                              {cond1, true_case_where, false_case_where}));

  EXPECT_THAT(
      CompileExprWithTypes(GetOptions(), expr,
                           {{"cond1", GetQType<OptionalUnit>()},
                            {"cond2", GetQType<OptionalUnit>()},
                            {"x", GetQType<int32_t>()},
                            {"y", GetQType<int32_t>()}}),
      IsOkAndHolds(
          AllOf(InitOperationsAre(),
                EvalOperationsAre(
                    "jump_if_not<+5>(OPTIONAL_UNIT [0x00])",
                    "jump_if_not<+2>(OPTIONAL_UNIT [0x01])",
                    "INT32 [0x0C] = math.add(INT32 [0x04], INT32 [0x08])",
                    "jump<+1>()",  //
                    "INT32 [0x0C] = core._copy(INT32 [0x08])",
                    "jump<+4>()",  //
                    "jump_if_not<+2>(OPTIONAL_UNIT [0x01])",
                    "INT32 [0x0C] = math.subtract(INT32 [0x04], INT32 [0x08])",
                    "jump<+1>()",  //
                    "INT32 [0x0C] = core._copy(INT32 [0x04])"))));

  EXPECT_THAT(Invoke(expr,
                     {{"cond1", TypedValue::FromValue(kPresent)},
                      {"cond2", TypedValue::FromValue(kPresent)},
                      {"x", TypedValue::FromValue(50)},
                      {"y", TypedValue::FromValue(7)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(57))));
  EXPECT_THAT(Invoke(expr,
                     {{"cond1", TypedValue::FromValue(kPresent)},
                      {"cond2", TypedValue::FromValue(kMissing)},
                      {"x", TypedValue::FromValue(50)},
                      {"y", TypedValue::FromValue(7)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(7))));
  EXPECT_THAT(Invoke(expr,
                     {{"cond1", TypedValue::FromValue(kMissing)},
                      {"cond2", TypedValue::FromValue(kPresent)},
                      {"x", TypedValue::FromValue(50)},
                      {"y", TypedValue::FromValue(7)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(43))));
  EXPECT_THAT(Invoke(expr,
                     {{"cond1", TypedValue::FromValue(kMissing)},
                      {"cond2", TypedValue::FromValue(kMissing)},
                      {"x", TypedValue::FromValue(50)},
                      {"y", TypedValue::FromValue(7)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(50))));
}

TEST_P(WhereOperatorTest, Optimizations) {
  auto cond = Placeholder("cond");
  auto x = Leaf("x");
  auto y = Leaf("y");
  // x + y
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {x, y}));
  // x * y
  ASSERT_OK_AND_ASSIGN(auto x_mul_y, CallOp("math.multiply", {x, y}));
  // cond ? x + y : x * y
  ASSERT_OK_AND_ASSIGN(ExprNodePtr where,
                       CallOp("core.where", {cond, x_plus_y, x_mul_y}));

  // 5 == 5
  ASSERT_OK_AND_ASSIGN(auto true_condition,
                       CallOp("core.equal", {Literal(5), Literal(5)}));
  // 5 == 7
  ASSERT_OK_AND_ASSIGN(auto false_condition,
                       CallOp("core.equal", {Literal(5), Literal(7)}));
  // 5 == 7 ? 5 == 7 : 5 == 5
  ASSERT_OK_AND_ASSIGN(
      auto true_nested_condition,
      CallOp("core.where", {false_condition, false_condition, true_condition}));

  absl::flat_hash_map<std::string, QTypePtr> input_types = {
      {"x", GetQType<int64_t>()}, {"y", GetQType<int64_t>()}};

  ASSERT_OK_AND_ASSIGN(auto lower_x_plus_y,
                       PopulateQTypes(x_plus_y, input_types));
  ASSERT_OK_AND_ASSIGN(lower_x_plus_y, ToLowest(lower_x_plus_y));
  ASSERT_OK_AND_ASSIGN(auto lower_x_mul_y,
                       PopulateQTypes(x_mul_y, input_types));
  ASSERT_OK_AND_ASSIGN(lower_x_mul_y, ToLowest(lower_x_mul_y));

  // With enabled optimizations all where operators should be elimitated.
  auto options = GetOptions();
  ASSERT_OK_AND_ASSIGN(
      auto peephole_optimizer,
      CreatePeepholeOptimizer({ShortCircuitWhereOptimizations}));
  options.optimizer = MakeOptimizer(std::move(peephole_optimizer));
  {
    // `5 == 5 ? x + y : x * y` transformed into `x + y`
    ASSERT_OK_AND_ASSIGN(
        auto expr, SubstitutePlaceholders(where, {{"cond", true_condition}}));
    EXPECT_THAT(PrepareExpression(expr, input_types, options),
                IsOkAndHolds(EqualsExpr(lower_x_plus_y)));
  }
  {
    // `5 == 7 ? x + y : x * y` transformed into `x * y`
    ASSERT_OK_AND_ASSIGN(
        auto expr, SubstitutePlaceholders(where, {{"cond", false_condition}}));
    EXPECT_THAT(PrepareExpression(expr, input_types, options),
                IsOkAndHolds(EqualsExpr(lower_x_mul_y)));
  }
  {
    // `(5 == 7 ? 5 == 7 : 5 == 5) ? x + y : x * y` transformed into `x + y`
    ASSERT_OK_AND_ASSIGN(
        auto expr,
        SubstitutePlaceholders(where, {{"cond", true_nested_condition}}));
    EXPECT_THAT(PrepareExpression(expr, input_types, options),
                IsOkAndHolds(EqualsExpr(lower_x_plus_y)));
  }
}

}  // namespace
}  // namespace arolla::expr::eval_internal
