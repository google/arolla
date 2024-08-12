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
#include "arolla/expr/operators/std_function_operator.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/array/qtype/types.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::testing::HasSubstr;

class StdFunctionOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

absl::StatusOr<TypedValue> GetFirst(absl::Span<const TypedRef> inputs) {
  return TypedValue(inputs[0]);
}

absl::StatusOr<QTypePtr> FirstQType(absl::Span<const QTypePtr> input_qtypes) {
  return input_qtypes[0];
}

absl::StatusOr<TypedValue> Add(absl::Span<const TypedRef> inputs) {
  ASSIGN_OR_RETURN(int32_t x, inputs[0].As<int32_t>());
  ASSIGN_OR_RETURN(int32_t y, inputs[1].As<int32_t>());
  return TypedValue::FromValue(x + y);
}

TEST_F(StdFunctionOperatorTest, GetName) {
  StdFunctionOperator op("get_first_fn", ExprOperatorSignature{{"x"}, {"y"}},
                         "dummy op docstring", FirstQType, GetFirst);
  ASSERT_THAT(op.display_name(), "get_first_fn");
}

TEST_F(StdFunctionOperatorTest, GetDoc) {
  StdFunctionOperator op("get_first_fn", ExprOperatorSignature{{"x"}, {"y"}},
                         "dummy op docstring", FirstQType, GetFirst);
  ASSERT_THAT(op.GetDoc(), IsOkAndHolds("dummy op docstring"));
}

TEST_F(StdFunctionOperatorTest, GetEvalFn) {
  StdFunctionOperator op("add_fn", ExprOperatorSignature{{"x"}, {"y"}},
                         "dummy op docstring", FirstQType, Add);
  int32_t x = 1;
  int32_t y = 2;
  auto res = op.GetEvalFn()({TypedRef::FromValue(x), TypedRef::FromValue(y)});
  EXPECT_OK(res);
  EXPECT_THAT(res.value().As<int32_t>(), IsOkAndHolds(x + y));
}

TEST_F(StdFunctionOperatorTest, GetOutputQTypeFn) {
  StdFunctionOperator op("add_fn", ExprOperatorSignature{{"x"}, {"y"}},
                         "dummy op docstring", FirstQType, Add);
  auto output_qtype_fn = op.GetOutputQTypeFn();
  auto res = output_qtype_fn({GetArrayQType<int32_t>(), GetQType<int32_t>()});
  EXPECT_THAT(res, IsOkAndHolds(GetArrayQType<int32_t>()));
}

TEST_F(StdFunctionOperatorTest, GetOutputQType) {
  {
    StdFunctionOperator op("get_first_fn", ExprOperatorSignature{{"x"}, {"y"}},
                           "dummy op docstring", FirstQType, GetFirst);
    EXPECT_THAT(
        op.GetOutputQType({GetArrayQType<int32_t>(), GetQType<int32_t>()}),
        IsOkAndHolds(GetArrayQType<int32_t>()));
  }
  {
    auto get_snd =
        [](absl::Span<const QTypePtr> inputs) -> absl::StatusOr<QTypePtr> {
      return inputs[1];
    };
    StdFunctionOperator op("add_fn", ExprOperatorSignature{{"x"}, {"y"}},
                           "dummy op docstring", std::move(get_snd), Add);
    EXPECT_THAT(
        op.GetOutputQType({GetArrayQType<int32_t>(), GetQType<float>()}),
        IsOkAndHolds(GetQType<float>()));
  }
  {
    auto status_fn =
        [](absl::Span<const QTypePtr> inputs) -> absl::StatusOr<QTypePtr> {
      return absl::InvalidArgumentError("foo bar");
    };
    StdFunctionOperator op("add_fn", ExprOperatorSignature{{"x"}, {"y"}},
                           "dummy op docstring", std::move(status_fn), Add);
    EXPECT_THAT(
        op.GetOutputQType({GetArrayQType<int32_t>(), GetQType<float>()}),
        StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("foo bar")));
  }
}

TEST_F(StdFunctionOperatorTest, QTypeInference) {
  {
    auto op = std::make_shared<StdFunctionOperator>(
        "my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring", FirstQType, GetFirst);
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Literal(1.5f), Literal(kUnit)}));
    EXPECT_EQ(expr->qtype(), GetQType<float>());
  }
  {
    auto get_snd =
        [](absl::Span<const QTypePtr> inputs) -> absl::StatusOr<QTypePtr> {
      return inputs[1];
    };
    auto op = std::make_shared<StdFunctionOperator>(
        "my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring", std::move(get_snd), GetFirst);
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Literal(1.5f), Literal(kUnit)}));
    EXPECT_EQ(expr->qtype(), GetQType<Unit>());
  }
  {
    // Missing input qtype.
    auto op = std::make_shared<StdFunctionOperator>(
        "my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring", FirstQType, GetFirst);
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Leaf("y")}));
    EXPECT_EQ(expr->qtype(), nullptr);
  }
}

TEST_F(StdFunctionOperatorTest, Eval) {
  {
    auto op = std::make_shared<StdFunctionOperator>(
        "get_first", ExprOperatorSignature{{"x"}, {"y"}}, "dummy op docstring",
        FirstQType, GetFirst);
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Literal(1), Literal(2)}));
    auto res = Invoke(expr, {});
    EXPECT_OK(res.status());
    EXPECT_THAT(res.value().As<int32_t>(), IsOkAndHolds(1));
  }
  {
    auto op = std::make_shared<StdFunctionOperator>(
        "add", ExprOperatorSignature{{"x"}, {"y"}}, "dummy op docstring",
        FirstQType, Add);
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Literal(1), Literal(2)}));
    auto res = Invoke(expr, {});
    EXPECT_OK(res.status());
    EXPECT_THAT(res.value().As<int32_t>(), IsOkAndHolds(3));
  }
  {
    auto op = std::make_shared<StdFunctionOperator>(
        "add", ExprOperatorSignature{{"x"}, {"y"}}, "dummy op docstring",
        FirstQType, Add);
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Leaf("y")}));
    auto res = Invoke(expr, {{"x", TypedValue::FromValue(1)},
                             {"y", TypedValue::FromValue(2)}});
    EXPECT_OK(res.status());
    EXPECT_THAT(res.value().As<int32_t>(), IsOkAndHolds(3));
  }
}

TEST_F(StdFunctionOperatorTest, VariadicInput) {
  ASSERT_OK_AND_ASSIGN(auto signature, ExprOperatorSignature::Make("*args"));
  auto op = std::make_shared<StdFunctionOperator>(
      "add", signature, "dummy op docstring", FirstQType, Add);
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Literal(1), Literal(2)}));
  auto res = Invoke(expr, {});
  EXPECT_OK(res.status());
  EXPECT_THAT(res.value().As<int32_t>(), IsOkAndHolds(3));
}

TEST_F(StdFunctionOperatorTest, IncorrectFnOutput) {
  auto op = std::make_shared<StdFunctionOperator>(
      "get_first", ExprOperatorSignature{{"x"}}, "dummy op docstring",
      [](absl::Span<const QTypePtr> input_qtypes) {
        return GetQType<int32_t>();
      },
      GetFirst);
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Literal(1.0)}));
  EXPECT_THAT(
      Invoke(expr, {}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected the result to have qtype INT32, got FLOAT64")));
}

TEST_F(StdFunctionOperatorTest, FnRaises) {
  auto op = std::make_shared<StdFunctionOperator>(
      "get_first", ExprOperatorSignature{{"x"}}, "dummy op docstring",
      FirstQType, [](absl::Span<const TypedRef> inputs) {
        return absl::InvalidArgumentError("foo bar");
      });
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Literal(1)}));
  EXPECT_THAT(Invoke(expr, {}), StatusIs(absl::StatusCode::kInvalidArgument,
                                         HasSubstr("foo bar")));
}

TEST_F(StdFunctionOperatorTest, Fingerprint) {
  StdFunctionOperator op1("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                          "dummy op docstring", FirstQType, GetFirst);
  {
    // Non-deterministic (same inputs).
    StdFunctionOperator op2("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                            "dummy op docstring", FirstQType, GetFirst);
    EXPECT_NE(op1.fingerprint(), op2.fingerprint());
  }
  // Regression tests to ensure that the fingerprints differ for different
  // inputs, even if the fingerprint generation is made deterministic.
  {
    // Different name.
    StdFunctionOperator op2("another_name", ExprOperatorSignature{{"x"}, {"y"}},
                            "dummy op docstring", FirstQType, GetFirst);
    EXPECT_NE(op1.fingerprint(), op2.fingerprint());
  }
  {
    // Different signature.
    StdFunctionOperator op2("my_dummy_op", ExprOperatorSignature{{"x"}},
                            "dummy op docstring", FirstQType, GetFirst);
    EXPECT_NE(op1.fingerprint(), op2.fingerprint());
  }
  {
    // Different docstring.
    StdFunctionOperator op2("my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
                            "another docstring", FirstQType, GetFirst);
    EXPECT_NE(op1.fingerprint(), op2.fingerprint());
  }
  {
    // Different output_qtype_fn.
    StdFunctionOperator op2(
        "my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring",
        [](absl::Span<const QTypePtr> input_qtypes) {
          return GetQType<float>();
        },
        GetFirst);
    EXPECT_NE(op1.fingerprint(), op2.fingerprint());
  }
  {
    // Different function.
    StdFunctionOperator op2(
        "my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring", FirstQType,
        [](absl::Span<const TypedRef> inputs) -> absl::StatusOr<TypedValue> {
          return TypedValue(inputs[1]);
        });
    EXPECT_NE(op1.fingerprint(), op2.fingerprint());
  }
}

}  // namespace
}  // namespace arolla::expr_operators
