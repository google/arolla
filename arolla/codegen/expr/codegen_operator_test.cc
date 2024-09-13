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
#include "arolla/codegen/expr/codegen_operator.h"

#include <cstdint>
#include <initializer_list>
#include <set>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla::codegen {
namespace {

using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::testing::WithExportAnnotation;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

// Finds minimal natural number not listed in used.
int64_t MinUnused(std::set<int64_t> used) {
  for (int64_t i = 0; i != used.size(); ++i) {
    if (used.count(i) == 0) {
      return i;
    }
  }
  return used.size();
}

TEST(CodegenTest, IsInlinableLiteralTypeTest) {
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetQType<int>()));
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetQType<float>()));
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetQType<double>()));
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetQType<int64_t>()));
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetQType<uint64_t>()));
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetQType<bool>()));
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetQType<Unit>()));
  EXPECT_FALSE(codegen_impl::IsInlinableLiteralType(GetQType<Bytes>()));
  EXPECT_FALSE(codegen_impl::IsInlinableLiteralType(GetQType<Text>()));

  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetOptionalQType<int>()));
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetOptionalQType<float>()));
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetOptionalQType<double>()));
  EXPECT_TRUE(
      codegen_impl::IsInlinableLiteralType(GetOptionalQType<int64_t>()));
  EXPECT_TRUE(
      codegen_impl::IsInlinableLiteralType(GetOptionalQType<uint64_t>()));
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetOptionalQType<bool>()));
  EXPECT_TRUE(codegen_impl::IsInlinableLiteralType(GetOptionalQType<Unit>()));
  EXPECT_FALSE(codegen_impl::IsInlinableLiteralType(GetOptionalQType<Bytes>()));
  EXPECT_FALSE(codegen_impl::IsInlinableLiteralType(GetOptionalQType<Text>()));

  EXPECT_FALSE(
      codegen_impl::IsInlinableLiteralType(GetDenseArrayQType<bool>()));
  EXPECT_FALSE(codegen_impl::IsInlinableLiteralType(GetDenseArrayQType<int>()));
  EXPECT_FALSE(
      codegen_impl::IsInlinableLiteralType(GetDenseArrayQType<float>()));
  EXPECT_FALSE(
      codegen_impl::IsInlinableLiteralType(GetDenseArrayQType<double>()));
}

TEST(CodegenTest, SmokeTest) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      expr::CallOp("math.add",
                   {expr::CallOp("math.add", {WithQTypeAnnotation(
                                                  Leaf("x"), GetQType<float>()),
                                              Literal(1.f)}),
                    WithQTypeAnnotation(Leaf("y"), GetQType<float>())}));

  ASSERT_OK_AND_ASSIGN(
      OperatorCodegenData op,
      GenerateOperatorCode(expr, /*inputs_are_cheap_to_read=*/true));
  EXPECT_THAT(op.headers,
              ElementsAre(                //
                  "arolla/"
                  "qexpr/operators/math/arithmetic.h"));
  EXPECT_THAT(op.deps,
              ElementsAre("//"
                          "arolla/"
                          "qexpr/operators/math:lib"));
  EXPECT_THAT(op.inputs, ElementsAre(Pair("x", _), Pair("y", _)));
  int64_t input_x_id = op.inputs["x"];
  EXPECT_THAT(op.assignments[input_x_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_x_id].rvalue(), Eq(RValue::CreateInput()));
  EXPECT_TRUE(op.assignments[input_x_id].is_inlinable());
  int64_t input_y_id = op.inputs["y"];
  EXPECT_THAT(op.assignments[input_y_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_y_id].rvalue(), Eq(RValue::CreateInput()));
  EXPECT_TRUE(op.assignments[input_x_id].is_inlinable());

  // 0. static float literal = 1.0f;
  // 1. float tmp_0 = AddOp{}(input_x, literal);
  // 2. float tmp_1 = AddOp{}(tmp_0, input_y);
  EXPECT_EQ(op.assignments.size(), 3 + 2 /* inputs */);
  // Order is not specified between literal and inputs.
  int64_t literal_id = MinUnused({input_x_id, input_y_id});
  ASSERT_LT(literal_id, op.assignments.size());
  EXPECT_THAT(op.assignments[literal_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLiteral}));
  EXPECT_THAT(op.assignments[literal_id].rvalue(),
              Eq(RValue::CreateLiteral("float{1.}")));

  // Order is not specified between literal and inputs.
  int64_t tmp0_id = MinUnused({input_x_id, input_y_id, literal_id});
  ASSERT_LT(tmp0_id, op.assignments.size());
  EXPECT_TRUE(op.assignments[tmp0_id].is_inlinable());
  EXPECT_THAT(op.assignments[tmp0_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp0_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFunctionCall,
                        .operator_returns_status_or = false,
                        .code = "::arolla::AddOp{}",
                        .argument_ids = {input_x_id, literal_id}}));
  int64_t tmp1_id = 4;
  EXPECT_THAT(op.assignments[tmp1_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp1_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFunctionCall,
                        .operator_returns_status_or = false,
                        .code = "::arolla::AddOp{}",
                        .argument_ids = {tmp0_id, input_y_id}}));
  EXPECT_EQ(op.output_id, tmp1_id);

  EXPECT_THAT(op.function_entry_points(),
              UnorderedElementsAre(Pair(tmp0_id, 0), Pair(tmp1_id, 1)));
}

TEST(CodegenTest, SmokeWithNonGlobalInputsTest) {
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(
      auto expr, expr::CallOp("math.add", {expr::CallOp("math.add", {x, x}),
                                           WithQTypeAnnotation(
                                               Leaf("y"), GetQType<float>())}));

  ASSERT_OK_AND_ASSIGN(
      OperatorCodegenData op,
      GenerateOperatorCode(expr, /*inputs_are_cheap_to_read=*/false));
  EXPECT_THAT(op.headers,
              ElementsAre(                //
                  "arolla/"
                  "qexpr/operators/math/arithmetic.h"));
  EXPECT_THAT(op.deps,
              ElementsAre("//"
                          "arolla/"
                          "qexpr/operators/math:lib"));
  EXPECT_THAT(op.inputs, ElementsAre(Pair("x", _), Pair("y", _)));
  int64_t input_x_id = op.inputs["x"];
  EXPECT_THAT(op.assignments[input_x_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_x_id].rvalue(), Eq(RValue::CreateInput()));
  EXPECT_FALSE(op.assignments[input_x_id].is_inlinable());
  int64_t input_y_id = op.inputs["y"];
  EXPECT_THAT(op.assignments[input_y_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_y_id].rvalue(), Eq(RValue::CreateInput()));
  EXPECT_TRUE(op.assignments[input_y_id].is_inlinable());

  // 0. input_x;
  // 1. float tmp_0 = AddOp{}(input_x, input_x);
  // 2. input_y;
  // 3. float tmp_1 = AddOp{}(tmp_0, input_y);
  ASSERT_EQ(op.assignments.size(), 2 + 2 /* inputs */);
  int64_t tmp0_id = 1;
  ASSERT_LT(tmp0_id, op.assignments.size());
  EXPECT_TRUE(op.assignments[tmp0_id].is_inlinable());
  EXPECT_THAT(op.assignments[tmp0_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp0_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFunctionCall,
                        .operator_returns_status_or = false,
                        .code = "::arolla::AddOp{}",
                        .argument_ids = {input_x_id, input_x_id}}));
  int64_t tmp1_id = 3;
  EXPECT_THAT(op.assignments[tmp1_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp1_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFunctionCall,
                        .operator_returns_status_or = false,
                        .code = "::arolla::AddOp{}",
                        .argument_ids = {tmp0_id, input_y_id}}));
  EXPECT_EQ(op.output_id, tmp1_id);

  EXPECT_THAT(op.function_entry_points(),
              UnorderedElementsAre(Pair(tmp0_id, 0), Pair(tmp1_id, 1)));
}

TEST(CodegenTest, SmokeWithStatusOrTest) {
  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto y,
                       WithQTypeAnnotation(Leaf("y"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto floor_div, expr::CallOp("math.floordiv", {x, y}));
  ASSERT_OK_AND_ASSIGN(auto expr, expr::CallOp("math.add", {floor_div, y}));

  ASSERT_OK_AND_ASSIGN(
      OperatorCodegenData op,
      GenerateOperatorCode(expr, /*inputs_are_cheap_to_read=*/true));
  EXPECT_THAT(op.headers,
              ElementsAre(                //
                  "arolla/"
                  "qexpr/operators/math/arithmetic.h"));
  EXPECT_THAT(op.deps,
              ElementsAre("//"
                          "arolla/"
                          "qexpr/operators/math:lib"));
  EXPECT_THAT(op.inputs, ElementsAre(Pair("x", _), Pair("y", _)));
  int64_t input_x_id = op.inputs["x"];
  EXPECT_THAT(op.assignments[input_x_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .is_local_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_x_id].rvalue(), Eq(RValue::CreateInput()));
  int64_t input_y_id = op.inputs["y"];
  EXPECT_THAT(op.assignments[input_y_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .is_local_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_y_id].rvalue(), Eq(RValue::CreateInput()));

  // 0. absl::StatusOr<float> tmp_0 = FloorDivOp{}(input_x, input_y);
  // 1. absl::StatusOr<float> tmp_1 = AddOp{}(tmp_0, input_y);
  EXPECT_EQ(op.assignments.size(), 2 + 2 /* inputs */);
  int64_t tmp0_id = 2;
  ASSERT_LT(tmp0_id, op.assignments.size());
  EXPECT_TRUE(op.assignments[tmp0_id].is_inlinable());
  EXPECT_THAT(op.assignments[tmp0_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = true,
                        .is_local_expr_status_or = true,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp0_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFunctionCall,
                        .operator_returns_status_or = true,
                        .code = "::arolla::FloorDivOp{}",
                        .argument_ids = {input_x_id, input_y_id}}));

  int64_t tmp1_id = 3;
  ASSERT_LT(tmp1_id, op.assignments.size());
  EXPECT_TRUE(op.assignments[tmp1_id].is_inlinable());
  EXPECT_THAT(op.assignments[tmp1_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = true,
                        .is_local_expr_status_or = true,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp1_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFunctionCall,
                        .operator_returns_status_or = false,
                        .code = "::arolla::AddOp{}",
                        .argument_ids = {tmp0_id, input_y_id}}));
  EXPECT_EQ(op.output_id, tmp1_id);
}

TEST(CodegenTest, SmokeWithContextTest) {
  ASSERT_OK_AND_ASSIGN(
      auto x, WithQTypeAnnotation(Leaf("x"), GetDenseArrayQType<float>()));
  ASSERT_OK_AND_ASSIGN(
      auto y, WithQTypeAnnotation(Leaf("y"), GetDenseArrayQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto expr, expr::CallOp("math.add", {x, y}));

  ASSERT_OK_AND_ASSIGN(
      OperatorCodegenData op,
      GenerateOperatorCode(expr, /*inputs_are_cheap_to_read=*/true));
  EXPECT_THAT(op.headers,
              ElementsAre(                //
                  "arolla/"
                  "dense_array/qtype/types.h",
                  "arolla/"
                  "qexpr/operators/dense_array/lifter.h",
                  "arolla/"
                  "qexpr/operators/math/arithmetic.h"));
  EXPECT_THAT(op.deps,
              ElementsAre(                //
                  "//"                    //
                  "arolla/"
                  "dense_array/qtype",
                  "//"                    //
                  "arolla/"
                  "qexpr/operators/dense_array:lib",
                  "//"                    //
                  "arolla/"
                  "qexpr/operators/math:lib"));
  EXPECT_THAT(op.inputs, ElementsAre(Pair("x", _), Pair("y", _)));
  int64_t input_x_id = op.inputs["x"];
  EXPECT_THAT(op.assignments[input_x_id].lvalue(),
              Eq(LValue{.type_name = "::arolla::DenseArray<float>",
                        .is_entire_expr_status_or = false,
                        .is_local_expr_status_or = false,
                        .qtype = GetDenseArrayQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_x_id].rvalue(), Eq(RValue::CreateInput()));
  int64_t input_y_id = op.inputs["y"];
  EXPECT_THAT(op.assignments[input_y_id].lvalue(),
              Eq(LValue{.type_name = "::arolla::DenseArray<float>",
                        .is_entire_expr_status_or = false,
                        .is_local_expr_status_or = false,
                        .qtype = GetDenseArrayQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_y_id].rvalue(), Eq(RValue::CreateInput()));

  // 0. absl::StatusOr<float> tmp_0 = AddOp{}(input_x, input_y);
  EXPECT_EQ(op.assignments.size(), 1 + 2 /* inputs */);
  int64_t tmp0_id = 2;
  ASSERT_LT(tmp0_id, op.assignments.size());
  EXPECT_TRUE(op.assignments[tmp0_id].is_inlinable());
  EXPECT_THAT(op.assignments[tmp0_id].lvalue(),
              Eq(LValue{.type_name = "::arolla::DenseArray<float>",
                        .is_entire_expr_status_or = true,
                        .is_local_expr_status_or = true,
                        .qtype = GetDenseArrayQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp0_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFunctionWithContextCall,
                        .operator_returns_status_or = true,
                        .code = "::arolla::DenseArrayLifter<::arolla::AddOp, "
                                "::arolla::meta::type_list<float, float>, "
                                "/*NoBitmapOffset=*/true>{}",
                        .argument_ids = {input_x_id, input_y_id}}));

  EXPECT_EQ(op.output_id, tmp0_id);
}

TEST(CodegenTest, SmokeTestWithExport) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      expr::CallOp(
          "math.add",
          {WithExportAnnotation(
               expr::CallOp("math.add",
                            {WithQTypeAnnotation(Leaf("x"), GetQType<float>()),
                             Literal(1.f)}),
               "output"),
           WithQTypeAnnotation(Leaf("y"), GetQType<float>())}));

  ASSERT_OK_AND_ASSIGN(
      OperatorCodegenData op,
      GenerateOperatorCode(expr, /*inputs_are_cheap_to_read=*/true));
  EXPECT_THAT(op.headers,
              ElementsAre(                //
                  "arolla/"
                  "qexpr/operators/math/arithmetic.h"));
  EXPECT_THAT(op.deps,
              ElementsAre("//"
                          "arolla/"
                          "qexpr/operators/math:lib"));
  EXPECT_THAT(op.inputs, ElementsAre(Pair("x", _), Pair("y", _)));
  int64_t input_x_id = op.inputs["x"];
  EXPECT_THAT(op.assignments[input_x_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_x_id].rvalue(), Eq(RValue::CreateInput()));
  int64_t input_y_id = op.inputs["y"];
  EXPECT_THAT(op.assignments[input_y_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_y_id].rvalue(), Eq(RValue::CreateInput()));

  // 0. static float literal = 1.0f;
  // 1. float tmp_0 = AddOp{}(input_x, literal);
  // 2. float tmp_1 = Export[0](tmp_0);
  // 3. float tmp_2 = AddOp{}(tmp_1, input_y);
  EXPECT_EQ(op.assignments.size(), 4 + 2 /* inputs */);
  // Order is not specified between literal and inputs.
  int64_t literal_id = MinUnused({input_x_id, input_y_id});
  ASSERT_LT(literal_id, op.assignments.size());
  EXPECT_THAT(op.assignments[literal_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLiteral}));
  EXPECT_THAT(op.assignments[literal_id].rvalue(),
              Eq(RValue::CreateLiteral("float{1.}")));

  // Order is not specified between literal and inputs.
  int64_t tmp0_id = MinUnused({input_x_id, input_y_id, literal_id});
  ASSERT_LT(tmp0_id, op.assignments.size());
  EXPECT_TRUE(op.assignments[tmp0_id].is_inlinable())
      << "used for output, but export is inside of the expression";
  EXPECT_THAT(op.assignments[tmp0_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp0_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFunctionCall,
                        .operator_returns_status_or = false,
                        .code = "::arolla::AddOp{}",
                        .argument_ids = {input_x_id, literal_id}}));
  // Order is not specified between tmp1_id and input_y.
  int64_t tmp1_id =
      MinUnused({input_x_id, input_y_id, literal_id, tmp0_id});  // export
  ASSERT_LT(tmp1_id, op.assignments.size());
  EXPECT_TRUE(op.assignments[tmp1_id].is_inlinable());
  EXPECT_THAT(op.assignments[tmp1_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp1_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kOutput,
                        .operator_returns_status_or = false,
                        .code = "0",
                        .argument_ids = {tmp0_id}}));
  int64_t tmp2_id = 5;
  EXPECT_THAT(op.assignments[tmp2_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp2_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFunctionCall,
                        .operator_returns_status_or = false,
                        .code = "::arolla::AddOp{}",
                        .argument_ids = {tmp1_id, input_y_id}}));
  EXPECT_EQ(op.output_id, tmp2_id);
  EXPECT_THAT(op.side_outputs, ElementsAre(Pair("output", tmp1_id)));
}

TEST(CodegenTest, SmokeTestWithDerivedQTypeDowncast) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      expr::CallOp("derived_qtype.downcast",
                   {Literal(GetWeakFloatQType()),
                    WithQTypeAnnotation(Leaf("x"), GetQType<double>())}));

  ASSERT_OK_AND_ASSIGN(
      OperatorCodegenData op,
      GenerateOperatorCode(expr, /*inputs_are_cheap_to_read=*/true));

  EXPECT_THAT(op.inputs, ElementsAre(Pair("x", _)));
  int64_t input_x_id = op.inputs["x"];
  EXPECT_THAT(op.assignments[input_x_id].lvalue(),
              Eq(LValue{.type_name = "double",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<double>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_x_id].rvalue(), Eq(RValue::CreateInput()));

  // 0. double tmp_1 = input_x;
  EXPECT_EQ(op.assignments.size(), 1 + 1 /* inputs */);
  int64_t tmp0_id = MinUnused({input_x_id});
  ASSERT_LT(tmp0_id, op.assignments.size());
  EXPECT_TRUE(op.assignments[tmp0_id].is_inlinable())
      << "used for output, but export is inside of the expression";
  EXPECT_THAT(op.assignments[tmp0_id].lvalue(),
              Eq(LValue{.type_name = "double",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<double>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp0_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFirst,
                        .operator_returns_status_or = false,
                        .code = "",
                        .argument_ids = {input_x_id}}));
  EXPECT_EQ(op.output_id, tmp0_id);
}

TEST(CodegenTest, SmokeTestWithExportUnusedForMainOutput) {
  ASSERT_OK_AND_ASSIGN(
      auto get_first_op,
      expr::MakeLambdaOperator(expr::ExprOperatorSignature({{"x"}, {"y"}}),
                               expr::Placeholder("x")));
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      expr::CallOp(
          get_first_op,
          {WithExportAnnotation(
               WithQTypeAnnotation(Leaf("y"), GetQType<float>()),
               "named_main_output"),
           WithExportAnnotation(
               expr::CallOp("math.add",
                            {WithQTypeAnnotation(Leaf("x"), GetQType<float>()),
                             Literal(1.f)}),
               "output")}));

  ASSERT_OK_AND_ASSIGN(
      OperatorCodegenData op,
      GenerateOperatorCode(expr, /*inputs_are_cheap_to_read=*/true));
  EXPECT_THAT(op.headers,
              ElementsAre(                //
                  "arolla/"
                  "qexpr/operators/math/arithmetic.h"));
  EXPECT_THAT(op.deps,
              ElementsAre("//"
                          "arolla/"
                          "qexpr/operators/math:lib"));
  EXPECT_THAT(op.inputs, ElementsAre(Pair("x", _), Pair("y", _)));
  int64_t input_x_id = op.inputs["x"];
  EXPECT_THAT(op.assignments[input_x_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_x_id].rvalue(), Eq(RValue::CreateInput()));
  int64_t input_y_id = op.inputs["y"];
  EXPECT_THAT(op.assignments[input_y_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kInput}));
  EXPECT_THAT(op.assignments[input_y_id].rvalue(), Eq(RValue::CreateInput()));

  // 0. float tmp_0 = Export[0](input_y);
  // 1. static float literal = 1.0f;
  // 2. float tmp_1 = AddOp{}(input_x, literal);
  // 3. float tmp_2 = Export[1](tmp_1);
  // 4. float tmp_3 = NoOp(tmp_0, tmp_2);
  EXPECT_EQ(op.assignments.size(), 5 + 2 /* inputs */);
  // Order is not specified between literal and inputs.
  int64_t tmp0_id = MinUnused({input_x_id, input_y_id});  // export
  ASSERT_LT(tmp0_id, op.assignments.size());
  EXPECT_TRUE(op.assignments[tmp0_id].is_inlinable())
      << "used for output, but export is inside of the expression";
  EXPECT_THAT(op.assignments[tmp0_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp0_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kOutput,
                        .operator_returns_status_or = false,
                        .code = "0",
                        .argument_ids = {input_y_id}}));

  // Order is not specified between literal and inputs.
  int64_t literal_id = MinUnused({input_x_id, input_y_id, tmp0_id});
  ASSERT_LT(literal_id, op.assignments.size());
  EXPECT_THAT(op.assignments[literal_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLiteral}));
  EXPECT_THAT(op.assignments[literal_id].rvalue(),
              Eq(RValue::CreateLiteral("float{1.}")));

  // Order is not specified between tmp1_id and input_y.
  int64_t tmp1_id = MinUnused({input_x_id, input_y_id, literal_id, tmp0_id});
  ASSERT_LT(tmp1_id, op.assignments.size());
  EXPECT_TRUE(op.assignments[tmp1_id].is_inlinable());
  EXPECT_THAT(op.assignments[tmp1_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp1_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFunctionCall,
                        .operator_returns_status_or = false,
                        .code = "::arolla::AddOp{}",
                        .argument_ids = {input_x_id, literal_id}}));
  int64_t tmp2_id = 5;
  EXPECT_THAT(op.assignments[tmp2_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp2_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kOutput,
                        .operator_returns_status_or = false,
                        .code = "1",
                        .argument_ids = {tmp1_id}}));
  int64_t tmp3_id = 6;
  EXPECT_THAT(op.assignments[tmp3_id].lvalue(),
              Eq(LValue{.type_name = "float",
                        .is_entire_expr_status_or = false,
                        .qtype = GetQType<float>(),
                        .kind = LValueKind::kLocal}));
  EXPECT_THAT(op.assignments[tmp3_id].rvalue(),
              Eq(RValue{.kind = RValueKind::kFirst,
                        .operator_returns_status_or = false,
                        .code = "",
                        .argument_ids = {tmp0_id, tmp2_id}}));
  EXPECT_EQ(op.output_id, tmp3_id);
  EXPECT_THAT(op.side_outputs, ElementsAre(Pair("named_main_output", tmp0_id),
                                           Pair("output", tmp2_id)));
}

TEST(CodegenTest, LambdaAndFunctionSinityTest) {
  auto lx = WithQTypeAnnotation(Leaf("x"), GetQType<float>());  // f1
  auto ly = WithQTypeAnnotation(Leaf("y"), GetQType<float>());  // f2
  // leaves are global, so next two statements are in the separate functions
  auto x = expr::CallOp("math.add", {lx, ly});       // f3
  auto y = expr::CallOp("math.subtract", {lx, ly});  // f4

  // the rest must be in the single function

  // both x and y can be reached by any chain, so we couldn't separate chains
  // but we can first compute `x, y` and use two lambdas capturing them.
  // each chain is going to be fully evaluated in a lambda.
  auto a = expr::CallOp("math.add", {x, y});
  auto b = expr::CallOp("math.subtract", {x, y});
  constexpr int64_t kChainLength = 500;
  // Creating two chains: a[i + 1] = a[i] % a[i - 1]
  for (int i = 0; i != kChainLength; ++i) {
    auto na = expr::CallOp("math.mod", {a, x});
    x = a;
    a = na;
    auto nb = expr::CallOp("math.mod", {b, y});
    y = b;
    b = nb;
  }
  // sum two chains
  ASSERT_OK_AND_ASSIGN(auto expr, expr::CallOp("math.add", {a, b}));
  ASSERT_OK_AND_ASSIGN(
      OperatorCodegenData op,
      GenerateOperatorCode(expr, /*inputs_are_cheap_to_read=*/true));
  EXPECT_THAT(op.functions.size(), Eq(3));
  for (int i = 0; i != 2; ++i) {  // first 2 functions are single statement
    EXPECT_THAT(op.functions[i].assignment_ids, IsEmpty()) << i;
  }
  EXPECT_THAT(op.functions[2].assignment_ids.size(), Eq(4));

  // Two lambdas, one per chain.
  EXPECT_THAT(op.lambdas.size(), Eq(2));
  // All except last element are used twice. Last one get inlined.
  EXPECT_THAT(op.lambdas[0].assignment_ids.size(), Eq(kChainLength - 1));
  EXPECT_THAT(op.lambdas[1].assignment_ids.size(), Eq(kChainLength - 1));
}

}  // namespace
}  // namespace arolla::codegen
