// Copyright 2025 Google LLC
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
#include <type_traits>
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
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::testing::EqualsAttr;
using ::testing::HasSubstr;

absl::StatusOr<TypedValue> GetFirst(absl::Span<const TypedRef> inputs) {
  return TypedValue(inputs[0]);
}

absl::StatusOr<const QType*> FirstQType(
    absl::Span<const QType* const> input_qtypes) {
  return input_qtypes[0];
}

absl::StatusOr<const QType*> FirstQTypeIsInt32(
    absl::Span<const QType* const> input_qtypes) {
  if (input_qtypes[0] != GetQType<int>()) {
    return absl::InvalidArgumentError("not viable");
  }
  return input_qtypes[0];
}

absl::StatusOr<TypedValue> Add(absl::Span<const TypedRef> inputs) {
  ASSIGN_OR_RETURN(int32_t x, inputs[0].As<int32_t>());
  ASSIGN_OR_RETURN(int32_t y, inputs[1].As<int32_t>());
  return TypedValue::FromValue(x + y);
}

TEST(StdFunctionOperatorTest, GetName) {
  StdFunctionOperator op("get_first_fn", ExprOperatorSignature{{"x"}, {"y"}},
                         "dummy op docstring", FirstQType, GetFirst);
  ASSERT_THAT(op.display_name(), "get_first_fn");
}

TEST(StdFunctionOperatorTest, GetDoc) {
  StdFunctionOperator op("get_first_fn", ExprOperatorSignature{{"x"}, {"y"}},
                         "dummy op docstring", FirstQType, GetFirst);
  ASSERT_THAT(op.GetDoc(), IsOkAndHolds("dummy op docstring"));
}

TEST(StdFunctionOperatorTest, GetEvalFn) {
  StdFunctionOperator op("add_fn", ExprOperatorSignature{{"x"}, {"y"}},
                         "dummy op docstring", FirstQType, Add);
  int32_t x = 1;
  int32_t y = 2;
  auto res = op.GetEvalFn()({TypedRef::FromValue(x), TypedRef::FromValue(y)});
  EXPECT_OK(res);
  EXPECT_THAT(res.value().As<int32_t>(), IsOkAndHolds(x + y));
}

TEST(StdFunctionOperatorTest, GetOutputQTypeFn) {
  StdFunctionOperator op("add_fn", ExprOperatorSignature{{"x"}, {"y"}},
                         "dummy op docstring", FirstQType, Add);
  auto output_qtype_fn = op.GetOutputQTypeFn();
  auto res = output_qtype_fn({GetArrayQType<int32_t>(), GetQType<int32_t>()});
  EXPECT_THAT(res, IsOkAndHolds(GetArrayQType<int32_t>()));
}

TEST(StdFunctionOperatorTest, InferAttributes) {
  {
    StdFunctionOperator op("get_first_fn", ExprOperatorSignature{{"x"}, {"y"}},
                           "dummy op docstring", FirstQType, GetFirst);
    EXPECT_THAT(op.InferAttributes({ExprAttributes(GetArrayQType<int32_t>()),
                                    ExprAttributes(GetQType<int32_t>())}),
                IsOkAndHolds(EqualsAttr(GetArrayQType<int32_t>())));
  }
  {
    auto get_snd = [](absl::Span<const QType* const> inputs)
        -> absl::StatusOr<const QType*> { return inputs[1]; };
    StdFunctionOperator op("add_fn", ExprOperatorSignature{{"x"}, {"y"}},
                           "dummy op docstring", std::move(get_snd), Add);
    EXPECT_THAT(op.InferAttributes({ExprAttributes(GetArrayQType<int32_t>()),
                                    ExprAttributes(GetQType<float>())}),
                IsOkAndHolds(EqualsAttr(GetQType<float>())));
  }
  {
    auto status_fn = [](absl::Span<const QType* const> inputs)
        -> absl::StatusOr<const QType*> {
      return absl::InvalidArgumentError("foo bar");
    };
    StdFunctionOperator op("add_fn", ExprOperatorSignature{{"x"}, {"y"}},
                           "dummy op docstring", std::move(status_fn), Add);
    EXPECT_THAT(
        op.InferAttributes({ExprAttributes(GetArrayQType<int32_t>()),
                            ExprAttributes(GetQType<float>())}),
        StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("foo bar")));
  }
  {
    // Partial input QTypes - success.
    StdFunctionOperator op("get_first_fn", ExprOperatorSignature{{"x"}, {"y"}},
                           "dummy op docstring", FirstQType, GetFirst);
    EXPECT_THAT(op.InferAttributes({ExprAttributes(GetArrayQType<int32_t>()),
                                    ExprAttributes()}),
                IsOkAndHolds(EqualsAttr(GetArrayQType<int32_t>())));
  }
  {
    // Partial input QTypes - early failure.
    StdFunctionOperator op("get_first_fn", ExprOperatorSignature{{"x"}, {"y"}},
                           "dummy op docstring", FirstQTypeIsInt32, GetFirst);
    EXPECT_THAT(
        op.InferAttributes(
            {ExprAttributes(GetArrayQType<float>()), ExprAttributes()}),
        StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("not viable")));
  }
  {
    // Unexpected missing output qtype.
    StdFunctionOperator op(
        "get_first_fn", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring",
        [](absl::Span<const QType* const> input_qtypes) { return nullptr; },
        GetFirst);
    EXPECT_THAT(op.InferAttributes({ExprAttributes(GetQType<int32_t>()),
                                    ExprAttributes(GetQType<int32_t>())}),
                StatusIs(absl::StatusCode::kInternal,
                         HasSubstr("unexpected missing output qtype when all "
                                   "inputs are present")));
  }
}

TEST(StdFunctionOperatorTest, QTypeInference) {
  {
    auto op = std::make_shared<StdFunctionOperator>(
        "my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring", FirstQType, GetFirst);
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Literal(1.5f), Literal(kUnit)}));
    EXPECT_EQ(expr->qtype(), GetQType<float>());
  }
  {
    auto get_snd = [](absl::Span<const QType* const> inputs)
        -> absl::StatusOr<const QType*> { return inputs[1]; };
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
  {
    // Missing input qtype - early failure.
    auto op = std::make_shared<StdFunctionOperator>(
        "my_dummy_op", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring", FirstQTypeIsInt32, GetFirst);
    EXPECT_THAT(
        CallOp(op, {Literal(1.5f), Leaf("y")}),
        StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("not viable")));
  }
  {
    // Unexpected missing output qtype.
    auto op = std::make_shared<StdFunctionOperator>(
        "get_first_fn", ExprOperatorSignature{{"x"}, {"y"}},
        "dummy op docstring",
        [](absl::Span<const QType* const> input_qtypes) { return nullptr; },
        GetFirst);
    EXPECT_THAT(CallOp(op, {Literal(1), Literal(2)}),
                StatusIs(absl::StatusCode::kInternal,
                         HasSubstr("unexpected missing output qtype when all "
                                   "inputs are present")));
  }
}

TEST(StdFunctionOperatorTest, Eval) {
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

TEST(StdFunctionOperatorTest, VariadicInput) {
  ASSERT_OK_AND_ASSIGN(auto signature, ExprOperatorSignature::Make("*args"));
  auto op = std::make_shared<StdFunctionOperator>(
      "add", signature, "dummy op docstring", FirstQType, Add);
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Literal(1), Literal(2)}));
  auto res = Invoke(expr, {});
  EXPECT_OK(res.status());
  EXPECT_THAT(res.value().As<int32_t>(), IsOkAndHolds(3));
}

TEST(StdFunctionOperatorTest, IncorrectFnOutput) {
  auto op = std::make_shared<StdFunctionOperator>(
      "get_first", ExprOperatorSignature{{"x"}}, "dummy op docstring",
      [](absl::Span<const QType* const> input_qtypes) {
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

TEST(StdFunctionOperatorTest, FnRaises) {
  auto op = std::make_shared<StdFunctionOperator>(
      "get_first", ExprOperatorSignature{{"x"}}, "dummy op docstring",
      FirstQType, [](absl::Span<const TypedRef> inputs) {
        return absl::InvalidArgumentError("foo bar");
      });
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Literal(1)}));
  EXPECT_THAT(Invoke(expr, {}), StatusIs(absl::StatusCode::kInvalidArgument,
                                         HasSubstr("foo bar")));
}

TEST(StdFunctionOperatorTest, Fingerprint) {
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
        [](absl::Span<const QType* const> input_qtypes) {
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

TEST(StdFunctionOperatorTest, WrapAsEvalFn) {
  {  // without StatusOr
    auto eval_fn = WrapAsEvalFn([](int x) { return x + 1; });
    static_assert(
        std::is_same_v<decltype(eval_fn), StdFunctionOperator::EvalFn>);
    ASSERT_OK_AND_ASSIGN(TypedValue res_tv, eval_fn({TypedRef::FromValue(42)}));
    EXPECT_THAT(res_tv.As<int>(), IsOkAndHolds(43));
  }
  {  // with StatusOr
    auto eval_fn = WrapAsEvalFn([](int x, int y) -> absl::StatusOr<int> {
      if (y == 0) {
        return absl::InvalidArgumentError("division by zero");
      }
      return x / y;
    });
    static_assert(
        std::is_same_v<decltype(eval_fn), StdFunctionOperator::EvalFn>);
    ASSERT_OK_AND_ASSIGN(TypedValue res_tv, eval_fn({TypedRef::FromValue(42),
                                                     TypedRef::FromValue(2)}));
    EXPECT_THAT(res_tv.As<int>(), IsOkAndHolds(21));
    EXPECT_THAT(eval_fn({TypedRef::FromValue(42), TypedRef::FromValue(0)}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("division by zero")));
  }
}

}  // namespace
}  // namespace arolla::expr_operators
