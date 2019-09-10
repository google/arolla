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
#include "arolla/qexpr/operators.h"

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "arolla/codegen/qexpr/testing/test_operators.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace arolla {
namespace {

using ::arolla::testing::IsOkAndHolds;
using ::arolla::testing::StatusIs;
using ::testing::ContainsRegex;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::Property;

class OperatorsTest : public ::testing::Test {
  void SetUp() final { ASSERT_OK(InitArolla()); }
};

TEST_F(OperatorsTest, LookupTestOperator) {
  // Lookup operator test.add(float, float).
  QTypePtr f32_type = GetQType<float>();
  auto op = OperatorRegistry::GetInstance()
                ->LookupOperator("test.add", {f32_type, f32_type}, f32_type)
                .value();

  // Examine operator output types.
  EXPECT_EQ(op->GetQType(),
            QExprOperatorSignature::Get({f32_type, f32_type}, f32_type));

  // Create a FrameLayout for testing the operator.
  FrameLayout::Builder layout_builder;
  auto arg1_slot = layout_builder.AddSlot<float>();
  auto arg2_slot = layout_builder.AddSlot<float>();
  auto result_slot = layout_builder.AddSlot<float>();
  auto bound_op = op->Bind(ToTypedSlots(arg1_slot, arg2_slot),
                           TypedSlot::FromSlot(result_slot))
                      .value();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  // Create an evaluation context and run the operator
  RootEvaluationContext root_ctx(&memory_layout);
  EvaluationContext ctx(root_ctx);
  root_ctx.Set(arg1_slot, 2.0f);
  root_ctx.Set(arg2_slot, 3.0f);
  bound_op->Run(&ctx, root_ctx.frame());
  EXPECT_OK(ctx.status());
  float result = root_ctx.Get(result_slot);
  EXPECT_THAT(result, Eq(5.0f));
}

TEST_F(OperatorsTest, LookupOperator_WithOutputType) {
  // Lookup operator test.add(float, float)->float.
  QTypePtr f32_type = GetQType<float>();
  auto op_float = OperatorRegistry::GetInstance()
                ->LookupOperator("test.add", {f32_type, f32_type}, f32_type)
                .value();
  EXPECT_EQ(op_float->GetQType(),
            QExprOperatorSignature::Get({f32_type, f32_type}, f32_type));

  // Lookup operator test.add(float, float)->double.
  QTypePtr f64_type = GetQType<float>();
  auto op_double = OperatorRegistry::GetInstance()
                ->LookupOperator("test.add", {f32_type, f32_type}, f64_type)
                .value();
  EXPECT_EQ(op_double->GetQType(),
            QExprOperatorSignature::Get({f64_type, f64_type}, f64_type));

  // Lookup operator test.add(float, float)->int.
  EXPECT_THAT(
      OperatorRegistry::GetInstance()->LookupOperator(
          "test.add", {f32_type, f32_type}, GetQType<int32_t>()),
      StatusIs(
          absl::StatusCode::kNotFound,
          HasSubstr(
              "QExpr operator test.add(FLOAT32,FLOAT32)->INT32 not found")));
}

TEST_F(OperatorsTest, Bind) {
  // Lookup operator test.add(float, float).
  QTypePtr float_type = GetQType<float>();
  auto op =
      OperatorRegistry::GetInstance()
          ->LookupOperator("test.add", {float_type, float_type}, float_type)
          .value();

  // Examine operator output types.
  EXPECT_EQ(op->GetQType(),
            QExprOperatorSignature::Get({float_type, float_type}, float_type));

  // Create a FrameLayout for testing the operator.
  FrameLayout::Builder layout_builder;
  auto arg1_slot = layout_builder.AddSlot<float>();
  auto arg2_slot = layout_builder.AddSlot<float>();
  auto double_slot = layout_builder.AddSlot<double>();
  auto result_slot = layout_builder.AddSlot<float>();
  auto bound_op = op->Bind(ToTypedSlots(arg1_slot, arg2_slot),
                           TypedSlot::FromSlot(result_slot))
                      .value();

  // Attempts to bind operator to incorrect slots.
  EXPECT_THAT(
      op->Bind(ToTypedSlots(arg1_slot), TypedSlot::FromSlot(result_slot)),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               "incorrect input types for operator test.add: expected "
               "(FLOAT32,FLOAT32), got (FLOAT32)"));
  EXPECT_THAT(op->Bind(ToTypedSlots(arg1_slot, double_slot),
                       TypedSlot::FromSlot(result_slot)),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "incorrect input types for operator test.add: expected "
                       "(FLOAT32,FLOAT32), got (FLOAT32,FLOAT64)"));
  EXPECT_THAT(op->Bind(ToTypedSlots(arg1_slot, arg2_slot),
                       TypedSlot::FromSlot(double_slot)),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "incorrect output types for operator test.add: "
                       "expected (FLOAT32), got (FLOAT64)"));

  FrameLayout memory_layout = std::move(layout_builder).Build();

  // Create an evaluation context and run the operator
  RootEvaluationContext root_ctx(&memory_layout);
  EvaluationContext ctx(root_ctx);
  root_ctx.Set(arg1_slot, 2.0f);
  root_ctx.Set(arg2_slot, 3.0f);
  bound_op->Run(&ctx, root_ctx.frame());
  EXPECT_OK(ctx.status());
  float result = root_ctx.Get(result_slot);
  EXPECT_THAT(result, Eq(5.0f));
}

// Test a user defined data type (FloatVector3) and associated operators,
// all defined in test_operators.
TEST_F(OperatorsTest, TestUserDefinedDataType) {
  QTypePtr f64_type = GetQType<double>();
  QTypePtr v3_type = GetQType<testing::Vector3<double>>();

  auto op1 = OperatorRegistry::GetInstance()
                 ->LookupOperator("test.vector3",
                                  {f64_type, f64_type, f64_type}, v3_type)
                 .value();
  EXPECT_EQ(op1->GetQType(), QExprOperatorSignature::Get(
                                 {f64_type, f64_type, f64_type}, v3_type));

  auto op2 = OperatorRegistry::GetInstance()
                 ->LookupOperator("test.dot_prod", {v3_type, v3_type}, f64_type)
                 .value();
  EXPECT_EQ(op2->GetQType(),
            QExprOperatorSignature::Get({v3_type, v3_type}, f64_type));

  // Create an memory layout for computing the squared magnitude of
  // a vector.
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<double>();
  auto y_slot = layout_builder.AddSlot<double>();
  auto z_slot = layout_builder.AddSlot<double>();
  auto v_slot = layout_builder.AddSlot<testing::Vector3<double>>();
  auto v_typed_slot = TypedSlot::FromSlot(v_slot, v3_type);
  auto result_slot = layout_builder.AddSlot<double>();
  auto bound_op1 =
      op1->Bind(ToTypedSlots(x_slot, y_slot, z_slot), v_typed_slot).value();
  auto bound_op2 =
      op2->Bind({v_typed_slot, v_typed_slot}, TypedSlot::FromSlot(result_slot))
          .value();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  // Create an evaluation context and run the computation.
  RootEvaluationContext root_ctx(&memory_layout);
  EvaluationContext ctx(root_ctx);
  root_ctx.Set(x_slot, 3.0);
  root_ctx.Set(y_slot, 4.0);
  root_ctx.Set(z_slot, 5.0);
  bound_op1->Run(&ctx, root_ctx.frame());
  EXPECT_OK(ctx.status());
  bound_op2->Run(&ctx, root_ctx.frame());
  EXPECT_OK(ctx.status());
  double result = root_ctx.Get(result_slot);
  EXPECT_THAT(result, Eq(50.0));
}

TEST_F(OperatorsTest, OperatorNotFound) {
  auto error = OperatorRegistry::GetInstance()->LookupOperator(
      "test.halts", {}, GetQType<int64_t>());
  EXPECT_THAT(error, StatusIs(absl::StatusCode::kNotFound,
                              ContainsRegex(
                                  "QExpr operator test.halts not found; adding "
                                  "\".*\" build dependency may help")));
}

TEST_F(OperatorsTest, OperatorOverloadNotFound) {
  QTypePtr bool_type = GetQType<bool>();
  QTypePtr float_type = GetQType<float>();
  EXPECT_THAT(
      OperatorRegistry::GetInstance()->LookupOperator(
          "test.add", {bool_type, float_type}, float_type),
      StatusIs(
          absl::StatusCode::kNotFound,
          ContainsRegex("QExpr operator test.add\\(BOOLEAN,FLOAT32\\)->FLOAT32 "
                        "not found; adding \".*\" build dependency may help")));
}

TEST_F(OperatorsTest, InvokeOperator) {
  ASSERT_OK_AND_ASSIGN(
      auto mul_op, OperatorRegistry::GetInstance()->LookupOperator(
                       "test.mul", {GetQType<int64_t>(), GetQType<int64_t>()},
                       GetQType<int64_t>()));

  EXPECT_THAT(
      InvokeOperator(*mul_op, {TypedValue::FromValue(int64_t{3}),
                               TypedValue::FromValue(int64_t{19})}),
      IsOkAndHolds(Property(&TypedValue::As<int64_t>, IsOkAndHolds(Eq(57)))));
  EXPECT_THAT(InvokeOperator(*mul_op, {TypedValue::FromValue(3.0),
                                       TypedValue::FromValue(int64_t{19})}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "incorrect input types for operator test.mul: expected "
                       "(INT64,INT64), got (FLOAT64,INT64)"));
}

TEST_F(OperatorsTest, QExprOperatorSignatureTypeAndName) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  auto fn = QExprOperatorSignature::Get({i32}, f64);
  EXPECT_EQ(fn->name(), "(INT32)->FLOAT64");
}

TEST_F(OperatorsTest, GetQExprOperatorSignature) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  const QExprOperatorSignature* fn = QExprOperatorSignature::Get({i32}, f64);
  EXPECT_THAT(fn->GetInputTypes(), ElementsAre(i32));
  EXPECT_THAT(fn->GetOutputType(), Eq(f64));
}

TEST_F(OperatorsTest, QExprOperatorSignatureInputsAreStored) {
  auto i32 = GetQType<int32_t>();
  std::vector<QTypePtr> types(100, i32);
  auto fn_type = QExprOperatorSignature::Get(types, i32);
  auto f64 = GetQType<double>();
  // Fill with a new types to check that no pointers to this are stored.
  std::fill(types.begin(), types.end(), f64);
  std::vector<QTypePtr> types2(100, i32);
  auto fn2_type = QExprOperatorSignature::Get(types2, i32);
  ASSERT_EQ(fn_type, fn2_type);
}

TEST_F(OperatorsTest, QExprOperatorSignatureSingleton) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  EXPECT_TRUE(QExprOperatorSignature::Get({f64}, i32) ==
              QExprOperatorSignature::Get({f64}, i32));
  auto get_complex_fn = [&]() {
    return QExprOperatorSignature::Get({f64, i32, MakeTupleQType({f64, i32})},
                                       MakeTupleQType({f64, i32, f64}));
  };
  EXPECT_TRUE(get_complex_fn() == get_complex_fn());
}

}  // namespace
}  // namespace arolla
