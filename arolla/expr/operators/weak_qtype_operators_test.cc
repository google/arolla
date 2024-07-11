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
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/operators/bootstrap_operators.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::ExprOperatorPtr;
using ::arolla::testing::InvokeExprOperator;

class WeakQTypeOperatorsTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(WeakQTypeOperatorsTest, ToWeakFloat) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr to_weak_float, GetCoreToWeakFloat());
  ASSERT_OK_AND_ASSIGN(auto res,
                       InvokeExprOperator<TypedValue>(to_weak_float, 1.0));
  EXPECT_EQ(res.GetType(), GetWeakFloatQType());
}

TEST_F(WeakQTypeOperatorsTest, ToWeakFloat_Float32) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr to_weak_float, GetCoreToWeakFloat());
  ASSERT_OK_AND_ASSIGN(auto res,
                       InvokeExprOperator<TypedValue>(to_weak_float, 1.0f));
  EXPECT_EQ(res.GetType(), GetWeakFloatQType());
}

TEST_F(WeakQTypeOperatorsTest, ToWeakFloat_Optional) {
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr to_weak_float, GetCoreToWeakFloat());
  ASSERT_OK_AND_ASSIGN(auto res, InvokeExprOperator<TypedValue>(
                                     to_weak_float, OptionalValue<float>(1.0)));
  EXPECT_EQ(res.GetType(), GetOptionalWeakFloatQType());
}

TEST_F(WeakQTypeOperatorsTest, ToWeakFloat_Array) {
  // Initialize Array<weak_float>.
  GetArrayWeakFloatQType();

  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr to_weak_float, GetCoreToWeakFloat());
  auto values = CreateArray<float>({1, std::nullopt, std::nullopt, 2});
  ASSERT_OK_AND_ASSIGN(auto res,
                       InvokeExprOperator<TypedValue>(to_weak_float, values));
  EXPECT_EQ(res.GetType(), GetArrayWeakFloatQType());
}

}  // namespace
}  // namespace arolla::expr_operators
