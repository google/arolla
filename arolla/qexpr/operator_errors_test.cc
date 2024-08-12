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
#include "arolla/qexpr/operator_errors.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::Eq;

TEST(OperatorErrorsTest, OperatorNotDefinedError) {
  absl::string_view op_name = "test.Not";
  EXPECT_THAT(
      OperatorNotDefinedError(op_name, {GetQType<int>(), GetQType<float>()}),
      StatusIs(absl::StatusCode::kNotFound,
               "operator test.Not is not defined for argument types "
               "(INT32,FLOAT32)"));
  EXPECT_THAT(OperatorNotDefinedError(op_name, {GetQType<int>()}, "Oops"),
              StatusIs(absl::StatusCode::kNotFound,
                       "operator test.Not is not defined for argument types "
                       "(INT32): Oops"));
}

TEST(OperatorErrorsTest, VerifySlotTypes) {
  absl::string_view op_name = "test.Not";
  FrameLayout::Builder builder;
  auto int_slot = builder.AddSlot<int>();
  auto double_slot = builder.AddSlot<double>();

  EXPECT_THAT(
      VerifyInputSlotTypes(ToTypedSlots(int_slot, double_slot),
                           {GetQType<int>(), GetQType<double>()}, op_name),
      IsOk());
  EXPECT_THAT(
      VerifyInputSlotTypes(ToTypedSlots(int_slot, double_slot),
                           {GetQType<int>(), GetQType<float>()}, op_name),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               "incorrect input types for operator test.Not: expected "
               "(INT32,FLOAT32), got (INT32,FLOAT64)"));
}

TEST(OperatorErrorsTest, VerifyValueTypes) {
  absl::string_view op_name = "test.Not";
  auto int_value = TypedValue::FromValue(57);
  auto double_value = TypedValue::FromValue(5.7);

  EXPECT_THAT(
      VerifyInputValueTypes({int_value, double_value},
                            {GetQType<int>(), GetQType<double>()}, op_name),
      IsOk());
  EXPECT_THAT(
      VerifyInputValueTypes({int_value, double_value},
                            {GetQType<int>(), GetQType<float>()}, op_name),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               "incorrect input types for operator test.Not: expected "
               "(INT32,FLOAT32), got (INT32,FLOAT64)"));
}

TEST(OperatorErrorsTest, GuessLibraryName) {
  EXPECT_THAT(GuessLibraryName("math.add"),
              Eq("//arolla/qexpr/operators/math"));
  EXPECT_THAT(GuessLibraryName("math.complex.add"),
              Eq("//arolla/qexpr/operators/math/complex"));
}

TEST(OperatorErrorsTest, GuessOperatorLibraryName) {
  EXPECT_THAT(GuessOperatorLibraryName("math.add"),
              Eq("//arolla/qexpr/operators/math:operator_add"));
  EXPECT_THAT(
      GuessOperatorLibraryName("math.complex.add"),
      Eq("//arolla/qexpr/operators/math/complex:operator_add"));
}

}  // namespace
}  // namespace arolla
