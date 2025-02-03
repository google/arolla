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
#include "arolla/qexpr/operators/math/arithmetic.h"

#include <cmath>
#include <cstdint>
#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::DoubleEq;
using ::testing::Eq;
using ::testing::Truly;

auto IsNan_() {
  return Truly([](double value) { return std::isnan(value); });
}

template <typename Functor, typename Result, typename... Args>
void AssertResultType() {
  static_assert(
      std::is_same_v<decltype(Functor()(std::declval<Args>()...)), Result>);
}

TEST(ArithmeticOperatorsTest, IsNanFunctions) {
  EXPECT_THAT(std::numeric_limits<double>::quiet_NaN(), IsNan_());
  EXPECT_THAT(std::numeric_limits<float>::quiet_NaN(), IsNan_());
  EXPECT_THAT(double{1.0}, Not(IsNan_()));
  EXPECT_THAT(float{1.0}, Not(IsNan_()));
}

TEST(ArithmeticOperatorsTest, Add) {
  AssertResultType<AddOp, /*Result=*/int32_t, /*Args=*/int32_t, int32_t>();
  AssertResultType<AddOp, /*Result=*/int64_t, /*Args=*/int64_t, int64_t>();
  AssertResultType<AddOp, /*Result=*/float, /*Args=*/float, float>();
  AssertResultType<AddOp, /*Result=*/double, /*Args=*/double, double>();

  EXPECT_THAT(AddOp()(1, 1), Eq(2));
  EXPECT_THAT(AddOp()(1., 1.), Eq(2.));

  AddOp()(std::numeric_limits<int>::max(), 1);  // no UB
}

TEST(ArithmeticOperatorsTest, Sign) {
  EXPECT_THAT(InvokeOperator<float>("math.sign", 1.f), IsOkAndHolds(1.f));
  EXPECT_THAT(InvokeOperator<float>("math.sign", 10.f), IsOkAndHolds(1.f));
  EXPECT_THAT(InvokeOperator<float>("math.sign", -1.f), IsOkAndHolds(-1.f));
  EXPECT_THAT(InvokeOperator<float>("math.sign", -10.f), IsOkAndHolds(-1.f));
  EXPECT_THAT(InvokeOperator<float>("math.sign", 0.f), IsOkAndHolds(0.f));
  EXPECT_THAT(InvokeOperator<float>("math.sign", -0.f), IsOkAndHolds(0.f));

  EXPECT_THAT(InvokeOperator<double>("math.sign", 1.), IsOkAndHolds(1.));
  EXPECT_THAT(InvokeOperator<double>("math.sign", 10.), IsOkAndHolds(1.));
  EXPECT_THAT(InvokeOperator<double>("math.sign", -1.), IsOkAndHolds(-1.));
  EXPECT_THAT(InvokeOperator<double>("math.sign", -10.), IsOkAndHolds(-1.));
  EXPECT_THAT(InvokeOperator<double>("math.sign", 0.), IsOkAndHolds(0.));

  EXPECT_THAT(InvokeOperator<double>("math.sign",
                                     std::numeric_limits<double>::quiet_NaN()),
              IsOkAndHolds(IsNan_()));

  EXPECT_THAT(InvokeOperator<int>("math.sign", 1), IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<int64_t>("math.sign", int64_t{10}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<int>("math.sign", 0), IsOkAndHolds(0));
}

TEST(ArithmeticOperatorsTest, Subtract) {
  AssertResultType<SubtractOp, /*Result=*/int32_t, /*Args=*/int32_t, int32_t>();
  AssertResultType<SubtractOp, /*Result=*/int64_t, /*Args=*/int64_t, int64_t>();
  AssertResultType<SubtractOp, /*Result=*/float, /*Args=*/float, float>();
  AssertResultType<SubtractOp, /*Result=*/double, /*Args=*/double, double>();

  EXPECT_THAT(SubtractOp()(1, 1), Eq(0));
  EXPECT_THAT(SubtractOp()(1., 1.), Eq(0.));

  SubtractOp()(std::numeric_limits<int>::min(), 1);  // no UB
}

TEST(ArithmeticOperatorsTest, Multiply) {
  AssertResultType<MultiplyOp, /*Result=*/int32_t, /*Args=*/int32_t, int32_t>();
  AssertResultType<MultiplyOp, /*Result=*/int64_t, /*Args=*/int64_t, int64_t>();
  AssertResultType<MultiplyOp, /*Result=*/float, /*Args=*/float, float>();
  AssertResultType<MultiplyOp, /*Result=*/double, /*Args=*/double, double>();

  EXPECT_THAT(MultiplyOp()(1, 1), Eq(1));
  EXPECT_THAT(MultiplyOp()(1., 1.), Eq(1.));

  MultiplyOp()(std::numeric_limits<int>::max(),
               std::numeric_limits<int>::max());  // no UB
}

TEST(ArithmeticOperatorsTest, FloorDiv) {
  EXPECT_THAT(InvokeOperator<int32_t>("math.floordiv", int32_t{5}, int32_t{2}),
              IsOkAndHolds(2));
  EXPECT_THAT(InvokeOperator<int64_t>("math.floordiv", int64_t{5}, int64_t{2}),
              IsOkAndHolds(2));
  EXPECT_THAT(InvokeOperator<int64_t>("math.floordiv", int64_t{5}, int64_t{2}),
              IsOkAndHolds(2));
  EXPECT_THAT(InvokeOperator<float>("math.floordiv", float{5}, float{2}),
              IsOkAndHolds(2));
  EXPECT_THAT(InvokeOperator<double>("math.floordiv", double{5}, double{2}),
              IsOkAndHolds(2));

  EXPECT_THAT(InvokeOperator<int32_t>("math.floordiv", int32_t{5}, int32_t{0}),
              StatusIs(absl::StatusCode::kInvalidArgument, "division by zero"));
  EXPECT_THAT(InvokeOperator<float>("math.floordiv", float{5}, float{0}),
              StatusIs(absl::StatusCode::kInvalidArgument, "division by zero"));

  EXPECT_THAT(InvokeOperator<int32_t>("math.floordiv", int32_t{5}, int32_t{-1}),
              IsOkAndHolds(-5));
  EXPECT_THAT(
      InvokeOperator<int32_t>("math.floordiv", int32_t{-5}, int32_t{-1}),
      IsOkAndHolds(5));

  EXPECT_THAT(InvokeOperator<int32_t>("math.floordiv", int32_t{5}, int32_t{2}),
              IsOkAndHolds(2));
  EXPECT_THAT(InvokeOperator<int32_t>("math.floordiv", int32_t{5}, int32_t{-2}),
              IsOkAndHolds(-3));
  EXPECT_THAT(InvokeOperator<int32_t>("math.floordiv", int32_t{-5}, int32_t{2}),
              IsOkAndHolds(-3));
  EXPECT_THAT(
      InvokeOperator<int32_t>("math.floordiv", int32_t{-5}, int32_t{-2}),
      IsOkAndHolds(2));

  EXPECT_THAT(InvokeOperator<double>("math.floordiv", double{5}, double{2}),
              IsOkAndHolds(2.));
  EXPECT_THAT(InvokeOperator<double>("math.floordiv", double{5}, double{-2}),
              IsOkAndHolds(-3.));
  EXPECT_THAT(InvokeOperator<double>("math.floordiv", double{-5}, double{2}),
              IsOkAndHolds(-3.));
  EXPECT_THAT(InvokeOperator<double>("math.floordiv", double{-5}, double{-2}),
              IsOkAndHolds(2.));

  EXPECT_THAT(
      InvokeOperator<int32_t>("math.floordiv",
                              std::numeric_limits<int32_t>::max(), int32_t{3}),
      IsOkAndHolds(715827882));
  EXPECT_THAT(
      InvokeOperator<int32_t>("math.floordiv",
                              std::numeric_limits<int32_t>::max(), int32_t{-3}),
      IsOkAndHolds(-715827883));
  EXPECT_THAT(
      InvokeOperator<int32_t>("math.floordiv",
                              std::numeric_limits<int32_t>::min(), int32_t{3}),
      IsOkAndHolds(-715827883));
  EXPECT_THAT(
      InvokeOperator<int32_t>("math.floordiv",
                              std::numeric_limits<int32_t>::min(), int32_t{-3}),
      IsOkAndHolds(715827882));

  EXPECT_THAT(InvokeOperator<int32_t>("math.floordiv",
                                      std::numeric_limits<int32_t>::max(), -1),
              IsOkAndHolds(-std::numeric_limits<int32_t>::max()));
  EXPECT_THAT(
      InvokeOperator<int32_t>(
          "math.floordiv", -std::numeric_limits<int32_t>::max(), int32_t{-1}),
      IsOkAndHolds(std::numeric_limits<int32_t>::max()));
  EXPECT_THAT(
      InvokeOperator<int32_t>("math.floordiv",
                              std::numeric_limits<int32_t>::min(), int32_t{1}),
      IsOkAndHolds(std::numeric_limits<int32_t>::min()));

  EXPECT_THAT(
      InvokeOperator<int32_t>("math.floordiv",
                              std::numeric_limits<int32_t>::min(), int32_t{-1}),
      IsOkAndHolds(
          std::numeric_limits<int32_t>::min()));  // This result is not
                                                  // mathmatically
                                                  // correct, and subject
                                                  // to change. (no UB)
}

TEST(ArithmeticOperatorsTest, Mod) {
  EXPECT_THAT(InvokeOperator<int32_t>("math.mod", int32_t{5}, int32_t{2}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<int64_t>("math.mod", int64_t{5}, int64_t{2}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<int64_t>("math.mod", int64_t{5}, int64_t{2}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<float>("math.mod", float{5}, float{2}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<double>("math.mod", double{5}, double{2}),
              IsOkAndHolds(1));

  EXPECT_THAT(InvokeOperator<int32_t>("math.mod", int32_t{5}, int32_t{0}),
              StatusIs(absl::StatusCode::kInvalidArgument, "division by zero"));
  EXPECT_THAT(InvokeOperator<float>("math.mod", float{5}, float{0}),
              StatusIs(absl::StatusCode::kInvalidArgument, "division by zero"));

  EXPECT_THAT(InvokeOperator<int32_t>("math.mod", int32_t{5}, int32_t{-1}),
              IsOkAndHolds(0));
  EXPECT_THAT(InvokeOperator<int32_t>("math.mod", int32_t{-5}, int32_t{-1}),
              IsOkAndHolds(0));

  EXPECT_THAT(InvokeOperator<int32_t>("math.mod", int32_t{5}, int32_t{2}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<int32_t>("math.mod", int32_t{5}, int32_t{-2}),
              IsOkAndHolds(-1));
  EXPECT_THAT(InvokeOperator<int32_t>("math.mod", int32_t{-5}, int32_t{2}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<int32_t>("math.mod", int32_t{-5}, int32_t{-2}),
              IsOkAndHolds(-1));
  EXPECT_THAT(InvokeOperator<int32_t>(
                  "math.mod", std::numeric_limits<int32_t>::min(), int32_t{-1}),
              IsOkAndHolds(0));

  EXPECT_THAT(InvokeOperator<int32_t>(
                  "math.mod", std::numeric_limits<int32_t>::max(), int32_t{3}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<int32_t>(
                  "math.mod", std::numeric_limits<int32_t>::max(), int32_t{-3}),
              IsOkAndHolds(-2));
  EXPECT_THAT(InvokeOperator<int32_t>(
                  "math.mod", std::numeric_limits<int32_t>::min(), int32_t{3}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<int32_t>(
                  "math.mod", std::numeric_limits<int32_t>::min(), int32_t{-3}),
              IsOkAndHolds(-2));

  EXPECT_THAT(InvokeOperator<double>("math.mod", 5., 2.), IsOkAndHolds(1.));
  EXPECT_THAT(InvokeOperator<double>("math.mod", 5., -2.), IsOkAndHolds(-1.));
  EXPECT_THAT(InvokeOperator<double>("math.mod", -5., 2.), IsOkAndHolds(1.));
  EXPECT_THAT(InvokeOperator<double>("math.mod", -5., -2.), IsOkAndHolds(-1.));
}

TEST(ArithmeticOperatorsTest, DivideOp) {
  AssertResultType<DivideOp, /*Result=*/float, /*Args=*/float, float>();
  AssertResultType<DivideOp, /*Result=*/double, /*Args=*/double, double>();

  EXPECT_THAT(DivideOp()(1., 1.), Eq(1.));
  EXPECT_THAT(DivideOp()(1., 2.), Eq(.5));

  EXPECT_THAT(DivideOp()(1., 0.), Eq(std::numeric_limits<double>::infinity()));
  EXPECT_THAT(DivideOp()(-1., 0.),
              Eq(-std::numeric_limits<double>::infinity()));
  EXPECT_THAT(DivideOp()(0., 0.), IsNan_());
}

TEST(ArithmeticOperatorsTest, Fmod) {
  AssertResultType<FmodOp, /*Result=*/float, /*Args=*/float, float>();
  AssertResultType<FmodOp, /*Result=*/double, /*Args=*/double, double>();

  EXPECT_THAT(FmodOp()(5., 3.), Eq(2.));
  EXPECT_THAT(FmodOp()(7.5, 3.5), DoubleEq(.5));
  EXPECT_THAT(FmodOp()(-7.5, 3.5), DoubleEq(-.5));
  EXPECT_THAT(FmodOp()(0., 0.), IsNan_());
  EXPECT_THAT(FmodOp()(5., 0.), IsNan_());
  EXPECT_THAT(FmodOp()(-3.1, 0.0), IsNan_());
}

TEST(ArithmeticOperatorsTest, Pos) {
  AssertResultType<PosOp, /*Result=*/int32_t, /*Args=*/int32_t>();
  AssertResultType<PosOp, /*Result=*/int64_t, /*Args=*/int64_t>();
  AssertResultType<PosOp, /*Result=*/float, /*Args=*/float>();
  AssertResultType<PosOp, /*Result=*/double, /*Args=*/double>();

  EXPECT_THAT(PosOp()(1), Eq(1));
  EXPECT_THAT(PosOp()(-1), Eq(-1));
  EXPECT_THAT(PosOp()(1.), Eq(1.));
  EXPECT_THAT(PosOp()(-1.), Eq(-1.));
}

TEST(ArithmeticOperatorsTest, Neg) {
  AssertResultType<NegOp, /*Result=*/int32_t, /*Args=*/int32_t>();
  AssertResultType<NegOp, /*Result=*/int64_t, /*Args=*/int64_t>();
  AssertResultType<NegOp, /*Result=*/float, /*Args=*/float>();
  AssertResultType<NegOp, /*Result=*/double, /*Args=*/double>();

  EXPECT_THAT(NegOp()(1), Eq(-1));
  EXPECT_THAT(NegOp()(-1), Eq(1));
  EXPECT_THAT(NegOp()(1.), Eq(-1.));
  EXPECT_THAT(NegOp()(-1.), Eq(1.));

  NegOp()(std::numeric_limits<int>::min());  // no UB
}

TEST(ArithmeticOperatorsTest, Abs) {
  EXPECT_THAT(InvokeOperator<int32_t>("math.abs", int32_t{1}), IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<int32_t>("math.abs", int32_t{-1}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<int64_t>("math.abs", int64_t{-1}),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<float>("math.abs", float{-1.5}),
              IsOkAndHolds(1.5));
  EXPECT_THAT(InvokeOperator<double>("math.abs", double{-1.5}),
              IsOkAndHolds(1.5));
  EXPECT_THAT(InvokeOperator<double>("math.abs", double{1.5}),
              IsOkAndHolds(1.5));

  (void)InvokeOperator<int32_t>("math.abs",
                                std::numeric_limits<int32_t>::min());  // no UB
}

TEST(ArithmeticOperatorsTest, Max) {
  EXPECT_THAT(InvokeOperator<int32_t>("math.maximum", int32_t{5}, int32_t{2}),
              IsOkAndHolds(5));
  EXPECT_THAT(InvokeOperator<int64_t>("math.maximum", int64_t{5}, int64_t{2}),
              IsOkAndHolds(5));
  EXPECT_THAT(InvokeOperator<int64_t>("math.maximum", int64_t{5}, int64_t{2}),
              IsOkAndHolds(5));
  EXPECT_THAT(InvokeOperator<float>("math.maximum", float{5}, float{2}),
              IsOkAndHolds(5));
  EXPECT_THAT(InvokeOperator<double>("math.maximum", double{5}, double{2}),
              IsOkAndHolds(5));
  {
    // Negative 0 tests.
    EXPECT_THAT(InvokeOperator<float>("math.maximum", float{-0.0}, float{0.0}),
                IsOkAndHolds(-0.0));
    EXPECT_THAT(InvokeOperator<float>("math.maximum", float{0.0}, float{-0.0}),
                IsOkAndHolds(0.0));
    EXPECT_THAT(
        InvokeOperator<double>("math.maximum", double{-0.0}, double{0.0}),
        IsOkAndHolds(-0.0));
    EXPECT_THAT(
        InvokeOperator<double>("math.maximum", double{0.0}, double{-0.0}),
        IsOkAndHolds(0.0));
  }
  {
    // NaN tests.
    EXPECT_THAT(InvokeOperator<float>("math.maximum", float{NAN}, float{2}),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<float>("math.maximum", float{2}, float{NAN}),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(
        InvokeOperator<float>(
            "math.maximum", std::numeric_limits<float>::infinity(), float{NAN}),
        IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<float>("math.maximum", float{NAN},
                                      std::numeric_limits<float>::infinity()),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<double>("math.maximum", double{NAN}, double{2}),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<double>("math.maximum", double{2}, double{NAN}),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<double>("math.maximum",
                                       std::numeric_limits<double>::infinity(),
                                       double{NAN}),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<double>("math.maximum", double{NAN},
                                       std::numeric_limits<double>::infinity()),
                IsOkAndHolds(IsNan_()));
  }
}

TEST(ArithmeticOperatorsTest, Min) {
  EXPECT_THAT(InvokeOperator<int32_t>("math.minimum", int32_t{5}, int32_t{2}),
              IsOkAndHolds(2));
  EXPECT_THAT(InvokeOperator<int64_t>("math.minimum", int64_t{5}, int64_t{2}),
              IsOkAndHolds(2));
  EXPECT_THAT(InvokeOperator<int64_t>("math.minimum", int64_t{5}, int64_t{2}),
              IsOkAndHolds(2));
  EXPECT_THAT(InvokeOperator<float>("math.minimum", float{5}, float{2}),
              IsOkAndHolds(2));
  EXPECT_THAT(InvokeOperator<double>("math.minimum", double{5}, double{2}),
              IsOkAndHolds(2));
  {
    // Negative 0 tests.
    EXPECT_THAT(InvokeOperator<float>("math.minimum", float{-0.0}, float{0.0}),
                IsOkAndHolds(-0.0));
    EXPECT_THAT(InvokeOperator<float>("math.minimum", float{0.0}, float{-0.0}),
                IsOkAndHolds(0.0));
    EXPECT_THAT(
        InvokeOperator<double>("math.minimum", double{-0.0}, double{0.0}),
        IsOkAndHolds(-0.0));
    EXPECT_THAT(
        InvokeOperator<double>("math.minimum", double{0.0}, double{-0.0}),
        IsOkAndHolds(0.0));
  }
  {
    // NaN tests.
    EXPECT_THAT(InvokeOperator<float>("math.minimum", float{NAN}, float{2}),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<float>("math.minimum", float{2}, float{NAN}),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(
        InvokeOperator<float>(
            "math.minimum", std::numeric_limits<float>::infinity(), float{NAN}),
        IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<float>("math.minimum", float{NAN},
                                      std::numeric_limits<float>::infinity()),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<double>("math.minimum", double{NAN}, double{2}),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<double>("math.minimum", double{2}, double{NAN}),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<double>("math.minimum",
                                       std::numeric_limits<double>::infinity(),
                                       double{NAN}),
                IsOkAndHolds(IsNan_()));
    EXPECT_THAT(InvokeOperator<double>("math.minimum", double{NAN},
                                       std::numeric_limits<double>::infinity()),
                IsOkAndHolds(IsNan_()));
  }
}

}  // namespace
}  // namespace arolla
