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
#include "arolla/expr/operator_loader/dummy_operator.h"

#include <cstdint>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/array/qtype/types.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/unit.h"

namespace arolla::operator_loader {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::testing::AllOf;
using ::testing::HasSubstr;

TEST(DummyOperatorTest, GetName) {
  DummyOperator op("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                   "dummy op docstring", GetArrayQType<int32_t>());
  ASSERT_THAT(op.display_name(), "my_dummy_op");
}

TEST(DummyOperatorTest, GetDoc) {
  DummyOperator op("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                   "dummy op docstring", GetArrayQType<int32_t>());
  ASSERT_THAT(op.doc(), "dummy op docstring");
  ASSERT_THAT(op.GetDoc(), IsOkAndHolds("dummy op docstring"));
}

TEST(DummyOperatorTest, GetOutputQType) {
  {
    DummyOperator op("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                     "dummy op docstring", GetArrayQType<int32_t>());
    EXPECT_EQ(op.GetOutputQType(), GetArrayQType<int32_t>());
  }
  {
    DummyOperator op("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                     "dummy op docstring", GetQType<OptionalValue<float>>());
    EXPECT_EQ(op.GetOutputQType(), GetQType<OptionalValue<float>>());
  }
}

TEST(DummyOperatorTest, QTypeInference) {
  {
    auto op = std::make_shared<DummyOperator>(
        "my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring", GetArrayQType<int32_t>());
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Literal(1.5f), Literal(kUnit)}));
    EXPECT_EQ(expr->qtype(), GetArrayQType<int32_t>());
  }
  {
    // Missing input qtype.
    auto op = std::make_shared<DummyOperator>(
        "my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring", GetArrayQType<int32_t>());
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Leaf("y")}));
    EXPECT_EQ(expr->qtype(), GetArrayQType<int32_t>());
  }
}

TEST(DummyOperatorTest, InferAttributesIncorrectArity) {
  DummyOperator op("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                   "dummy op docstring", GetArrayQType<int32_t>());
  EXPECT_THAT(op.InferAttributes({}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       AllOf(HasSubstr("incorrect number of dependencies"),
                             HasSubstr("expected 2 but got 0"))));
}

TEST(DummyOperatorTest, Eval) {
  auto op = std::make_shared<DummyOperator>(
      "my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}}, "dummy op docstring",
      GetArrayQType<int32_t>());
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp(op, {Literal(1.5f), Literal(OptionalValue<Unit>())}));
  EXPECT_THAT(
      Invoke(expr, {}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("my_dummy_op is not a builtin or backend ExprOperator")));
}

TEST(DummyOperatorTest, Fingerprint) {
  DummyOperator op1("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                    "dummy op docstring", GetQType<float>());
  {
    // Deterministic (same inputs).
    DummyOperator op2("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                      "dummy op docstring", GetQType<float>());
    EXPECT_EQ(op1.fingerprint(), op2.fingerprint());
  }
  {
    // Different name.
    DummyOperator op2("another_name", ExprOperatorSignature{{"x"}, {"y"}},
                      "dummy op docstring", GetQType<float>());
    EXPECT_NE(op1.fingerprint(), op2.fingerprint());
  }
  {
    // Different signature.
    DummyOperator op2("my_dummy_op", ExprOperatorSignature{{"x"}},
                      "dummy op docstring", GetQType<float>());
    EXPECT_NE(op1.fingerprint(), op2.fingerprint());
  }
  {
    // Different docstring.
    DummyOperator op2("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                      "another docstring", GetQType<float>());
    EXPECT_NE(op1.fingerprint(), op2.fingerprint());
  }
  {
    // Different result_qtype.
    DummyOperator op2("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                      "dummy op docstring", GetQType<int32_t>());
    EXPECT_NE(op1.fingerprint(), op2.fingerprint());
  }
}

}  // namespace
}  // namespace arolla::operator_loader
