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
#include "arolla/codegen/io/testing/test_input_loader_operators_set.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//status/status_matchers.h"
#include "absl//strings/string_view.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qexpr/operator_metadata.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::TypedValueWith;

// NOTE: operator names are harcoded in this low level test.
// Feel free to update them if you need to change the way operator names are
// generated.

constexpr absl::string_view kBit0OperatorName =
    "_G_Gmy_Rnamespace_G_GLoadFromInt_G_Gbit_N_20_2_P";
constexpr absl::string_view kDoubleOperatorName =
    "_G_Gmy_Rnamespace_G_GLoadFromInt_G_Gdouble";
constexpr absl::string_view kSelfOperatorName =
    "_G_Gmy_Rnamespace_G_GLoadFromInt_G_Gself";

TEST(InputLoaderTest, TestOperators) {
  ASSERT_OK_AND_ASSIGN(auto bit0_op, expr::LookupOperator(kBit0OperatorName));
  ASSERT_OK_AND_ASSIGN(auto double_op,
                       expr::LookupOperator(kDoubleOperatorName));
  ASSERT_OK_AND_ASSIGN(auto self_op, expr::LookupOperator(kSelfOperatorName));
  auto input = expr::Leaf("x");
  ASSERT_OK_AND_ASSIGN(
      auto e, expr::CallOp("math.add", {expr::CallOp(bit0_op, {input}),
                                        expr::CallOp(double_op, {input})}));
  ASSERT_OK_AND_ASSIGN(
      e, expr::CallOp("math.add", {e, expr::CallOp(self_op, {input})}));
  EXPECT_THAT(expr::Invoke(e, {{"x", TypedValue::FromValue(int32_t{7})}}),
              IsOkAndHolds(TypedValueWith<double>(15)));
  EXPECT_THAT(expr::Invoke(e, {{"x", TypedValue::FromValue(int32_t{8})}}),
              IsOkAndHolds(TypedValueWith<double>(16)));
}

TEST(InputLoaderTest, TestOperatorMetadatas) {
  const QTypePtr input_qtype = GetQType<int32_t>();
  ASSERT_OK_AND_ASSIGN(
      auto bit0_op_metadata,
      QExprOperatorMetadataRegistry::GetInstance().LookupOperatorMetadata(
          kBit0OperatorName, {input_qtype}));
  EXPECT_EQ(bit0_op_metadata.build_details.op_class,
            "my_namespace::LoadFromIntFunctor0/*bit[\"0\"]*/");
  EXPECT_EQ(my_namespace::LoadFromIntFunctor0{}(7), 1);
  EXPECT_EQ(my_namespace::LoadFromIntFunctor0{}(8), 0);

  ASSERT_OK_AND_ASSIGN(
      auto self_op_metadata,
      QExprOperatorMetadataRegistry::GetInstance().LookupOperatorMetadata(
          kSelfOperatorName, {input_qtype}));
  EXPECT_EQ(self_op_metadata.build_details.op_class,
            "my_namespace::LoadFromIntFunctor2/*self*/");
  EXPECT_EQ(my_namespace::LoadFromIntFunctor2{}(7), 7);
  EXPECT_EQ(my_namespace::LoadFromIntFunctor2{}(8), 8);
}

}  // namespace
}  // namespace arolla
