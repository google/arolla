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

#include <cmath>
#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/init_arolla.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::AllOf;
using ::testing::DoubleEq;
using ::testing::DoubleNear;
using ::testing::Eq;
using ::testing::FloatEq;
using ::testing::FloatNear;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::IsNan;
using ::testing::Le;
using ::testing::Lt;

const float kPi = 3.1415927f;

class ArithmeticOperatorsTest : public ::testing::Test {
  void SetUp() final { InitArolla(); }
};

class MathOperatorsTest : public ::testing::Test {
  void SetUp() final { InitArolla(); }
};

TEST_F(ArithmeticOperatorsTest, Log) {
  EXPECT_THAT(InvokeOperator<float>("math.log", 1.f), IsOkAndHolds(0.f));
  EXPECT_THAT(InvokeOperator<float>("math.log", 2.f),
              IsOkAndHolds(FloatEq(std::log(2.f))));
  EXPECT_THAT(InvokeOperator<float>("math.log", 0.f),
              IsOkAndHolds(-std::numeric_limits<float>::infinity()));
  EXPECT_THAT(InvokeOperator<float>("math.log", -5.f), IsOkAndHolds(IsNan()));

  EXPECT_THAT(InvokeOperator<double>("math.log", 2.),
              IsOkAndHolds(DoubleEq(std::log(2.))));
  EXPECT_THAT(InvokeOperator<double>("math.log", 1.), IsOkAndHolds(0.));
  EXPECT_THAT(InvokeOperator<double>("math.log", 0.),
              IsOkAndHolds(-std::numeric_limits<double>::infinity()));
  EXPECT_THAT(InvokeOperator<double>("math.log", -4.), IsOkAndHolds(IsNan()));
}

TEST_F(ArithmeticOperatorsTest, Log2) {
  EXPECT_THAT(InvokeOperator<float>("math.log2", 1.f), IsOkAndHolds(0.f));
  EXPECT_THAT(InvokeOperator<float>("math.log2", 2.f),
              IsOkAndHolds(FloatEq(std::log2(2.f))));
  EXPECT_THAT(InvokeOperator<float>("math.log2", 0.f),
              IsOkAndHolds(-std::numeric_limits<float>::infinity()));
  EXPECT_THAT(InvokeOperator<float>("math.log2", -5.f), IsOkAndHolds(IsNan()));

  EXPECT_THAT(InvokeOperator<double>("math.log2", 1.), IsOkAndHolds(0.));
  EXPECT_THAT(InvokeOperator<double>("math.log2", 2.),
              IsOkAndHolds(DoubleEq(std::log2(2.))));
  EXPECT_THAT(InvokeOperator<double>("math.log2", 0.),
              IsOkAndHolds(-std::numeric_limits<double>::infinity()));
  EXPECT_THAT(InvokeOperator<double>("math.log2", -4.), IsOkAndHolds(IsNan()));
}

TEST_F(ArithmeticOperatorsTest, Log10) {
  EXPECT_THAT(InvokeOperator<float>("math.log10", 1.f), IsOkAndHolds(0.f));
  EXPECT_THAT(InvokeOperator<float>("math.log10", 2.f),
              IsOkAndHolds(FloatEq(std::log10(2.f))));
  EXPECT_THAT(InvokeOperator<float>("math.log10", 0.f),
              IsOkAndHolds(-std::numeric_limits<float>::infinity()));
  EXPECT_THAT(InvokeOperator<float>("math.log10", -5.f), IsOkAndHolds(IsNan()));

  EXPECT_THAT(InvokeOperator<double>("math.log10", 1.), IsOkAndHolds(0.));
  EXPECT_THAT(InvokeOperator<double>("math.log10", 2.),
              IsOkAndHolds(DoubleEq(std::log10(2.))));
  EXPECT_THAT(InvokeOperator<double>("math.log10", 0.),
              IsOkAndHolds(-std::numeric_limits<double>::infinity()));
  EXPECT_THAT(InvokeOperator<double>("math.log10", -4.), IsOkAndHolds(IsNan()));
}

TEST_F(ArithmeticOperatorsTest, Log1p) {
  EXPECT_THAT(InvokeOperator<float>("math.log1p", 0.f), IsOkAndHolds(0.f));
  EXPECT_THAT(InvokeOperator<float>("math.log1p", 2.f),
              IsOkAndHolds(FloatEq(std::log1p(2.f))));
  EXPECT_THAT(InvokeOperator<float>("math.log1p", -1.f),
              IsOkAndHolds(-std::numeric_limits<float>::infinity()));
  EXPECT_THAT(InvokeOperator<float>("math.log1p", -5.f), IsOkAndHolds(IsNan()));

  EXPECT_THAT(InvokeOperator<double>("math.log1p", 0.), IsOkAndHolds(0.));
  EXPECT_THAT(InvokeOperator<double>("math.log1p", 2.),
              IsOkAndHolds(DoubleEq(std::log1p(2.))));
  EXPECT_THAT(InvokeOperator<double>("math.log1p", -1.),
              IsOkAndHolds(-std::numeric_limits<double>::infinity()));
  EXPECT_THAT(InvokeOperator<double>("math.log1p", -4.), IsOkAndHolds(IsNan()));
}

TEST_F(ArithmeticOperatorsTest, Symlog1p) {
  EXPECT_THAT(InvokeOperator<float>("math.symlog1p", 0.f), IsOkAndHolds(0.));
  EXPECT_THAT(InvokeOperator<float>("math.symlog1p", 2.f),
              IsOkAndHolds(FloatEq(std::log1p(2.))));
  EXPECT_THAT(InvokeOperator<float>("math.symlog1p", -2.f),
              IsOkAndHolds(FloatEq(-std::log1p(2.))));

  EXPECT_THAT(InvokeOperator<double>("math.symlog1p", 0.), IsOkAndHolds(0.));
  EXPECT_THAT(InvokeOperator<double>("math.symlog1p", 2.),
              IsOkAndHolds(DoubleEq(std::log1p(2.))));
  EXPECT_THAT(InvokeOperator<double>("math.symlog1p", -2.),
              IsOkAndHolds(DoubleEq(-std::log1p(2.))));
}

TEST_F(MathOperatorsTest, Exp) {
  EXPECT_THAT(InvokeOperator<float>("math.exp", 0.f), IsOkAndHolds(1.f));
  EXPECT_THAT(InvokeOperator<float>("math.exp", 2.f),
              IsOkAndHolds(FloatEq(std::exp(2.f))));

  EXPECT_THAT(InvokeOperator<double>("math.exp", 0.), IsOkAndHolds(Eq(1.)));
  EXPECT_THAT(InvokeOperator<double>("math.exp", 2.),
              IsOkAndHolds(DoubleEq(std::exp(2.))));
}

TEST_F(MathOperatorsTest, Expm1) {
  EXPECT_THAT(InvokeOperator<float>("math.expm1", 0.f), IsOkAndHolds(0.f));
  EXPECT_THAT(InvokeOperator<float>("math.expm1", 2.f),
              IsOkAndHolds(FloatEq(std::expm1(2.f))));

  EXPECT_THAT(InvokeOperator<double>("math.expm1", 0.), IsOkAndHolds(Eq(0.)));
  EXPECT_THAT(InvokeOperator<double>("math.expm1", 2.),
              IsOkAndHolds(DoubleEq(std::expm1(2.))));
}

TEST_F(MathOperatorsTest, Sigmoid) {
  for (float slope = 1; slope < 5; slope++) {
    // Verify that the sigmoid of the half point is 0.5 for various slopes.
    EXPECT_THAT(InvokeOperator<float>("math.sigmoid", 10.f, 10.f, slope),
                IsOkAndHolds(0.5f))
        << slope;
    EXPECT_THAT(InvokeOperator<double>("math.sigmoid", 10., 10., double{slope}),
                IsOkAndHolds(0.5))
        << slope;

    // Verify that sigmoids of low values are small but not negative.
    // while high ones map to numbers close to but not more than 1.
    float epsilon = 0.001;
    EXPECT_THAT(InvokeOperator<float>("math.sigmoid", -10.f, 10.f, slope),
                IsOkAndHolds(AllOf(Lt(epsilon), Ge(0))))
        << slope;
    EXPECT_THAT(InvokeOperator<float>("math.sigmoid", 20.f, 10.f, slope),
                IsOkAndHolds(AllOf(Gt(1. - epsilon), Le(1.))))
        << slope;
  }
  EXPECT_THAT(InvokeOperator<float>("math.sigmoid", 2.f, 4.f, 5.f),
              IsOkAndHolds(FloatEq(1.f / (1.f + std::exp(5.f * (2.f))))));
  EXPECT_THAT(InvokeOperator<double>("math.sigmoid", 2., 4., 5.),
              IsOkAndHolds(DoubleEq(1. / (1. + std::exp(5. * (2.))))));
}

TEST_F(MathOperatorsTest, LogSigmoid) {
  // LogSigmoid matches the naive log(sigmoid(x)) for modest values of |x|.
  EXPECT_THAT(
      InvokeOperator<float>("math.log_sigmoid", 5.f),
      IsOkAndHolds(FloatNear(std::log(1 / (1 + std::exp(-5.f))), 1e-5)));
  EXPECT_THAT(
      InvokeOperator<float>("math.log_sigmoid", 0.f),
      IsOkAndHolds(FloatNear(std::log(1 / (1 + std::exp(-0.f))), 1e-5)));
  EXPECT_THAT(InvokeOperator<float>("math.log_sigmoid", -5.f),
              IsOkAndHolds(FloatNear(std::log(1 / (1 + std::exp(5.f))), 1e-5)));

  EXPECT_THAT(
      InvokeOperator<double>("math.log_sigmoid", 5.),
      IsOkAndHolds(DoubleNear(std::log(1 / (1 + std::exp(-5.))), 1e-5)));
  EXPECT_THAT(
      InvokeOperator<double>("math.log_sigmoid", 0.),
      IsOkAndHolds(DoubleNear(std::log(1 / (1 + std::exp(-0.))), 1e-5)));
  EXPECT_THAT(InvokeOperator<double>("math.log_sigmoid", -5.),
              IsOkAndHolds(DoubleNear(std::log(1 / (1 + std::exp(5.))), 1e-5)));

  // For small x, log(1 / (1 + e^-x)) = -log(1 + e^-x) ~= -log(e^-x) = x.
  // A naive log(sigmoid(x)) would prematurely round 1 + e^-x to inf.
  EXPECT_THAT(InvokeOperator<float>("math.log_sigmoid", -1000.f),
              IsOkAndHolds(FloatNear(-1000.f, 1e-05)));
  EXPECT_THAT(InvokeOperator<double>("math.log_sigmoid", -1000.),
              IsOkAndHolds(DoubleNear(-1000., 1e-05)));

  // For large x, log(1 / (1 + e^-x)) = -log(1 + e^-x) ~= -e^-x.
  // A naive log(sigmoid(x)) would prematurely round 1 + e^-x to 1.
  EXPECT_THAT(InvokeOperator<float>("math.log_sigmoid", 100.f),
              IsOkAndHolds(FloatNear(-std::exp(-100.f), 1e-50)));
  EXPECT_THAT(InvokeOperator<double>("math.log_sigmoid", 100.),
              IsOkAndHolds(DoubleNear(-std::exp(-100.), 1e-50)));
}

TEST_F(MathOperatorsTest, Pow) {
  EXPECT_THAT(InvokeOperator<float>("math._pow", 2.f, 2.f), IsOkAndHolds(4.f));
  EXPECT_THAT(InvokeOperator<float>("math._pow", -1.f, 0.5f),
              IsOkAndHolds(IsNan()));

  EXPECT_THAT(InvokeOperator<double>("math._pow", 2., 2.), IsOkAndHolds(4.));
  EXPECT_THAT(InvokeOperator<double>("math._pow", -1., 0.5),
              IsOkAndHolds(IsNan()));
}

TEST_F(MathOperatorsTest, Logit) {
  EXPECT_THAT(InvokeOperator<float>("math.logit", 0.f),
              IsOkAndHolds(-std::numeric_limits<float>::infinity()));
  EXPECT_THAT(InvokeOperator<float>("math.logit", 1.f),
              IsOkAndHolds(std::numeric_limits<float>::infinity()));
  EXPECT_THAT(InvokeOperator<float>("math.logit", 0.5f),
              IsOkAndHolds(FloatNear(0.f, 1e-05)));
  EXPECT_THAT(InvokeOperator<float>("math.logit", -1.f), IsOkAndHolds(IsNan()));
  EXPECT_THAT(InvokeOperator<float>("math.logit", 2.f), IsOkAndHolds(IsNan()));

  EXPECT_THAT(InvokeOperator<double>("math.logit", 0.),
              IsOkAndHolds(-std::numeric_limits<float>::infinity()));
  EXPECT_THAT(InvokeOperator<double>("math.logit", 1.),
              IsOkAndHolds(std::numeric_limits<float>::infinity()));
  EXPECT_THAT(InvokeOperator<double>("math.logit", 0.5),
              IsOkAndHolds(DoubleNear(0., 1e-05)));
  EXPECT_THAT(InvokeOperator<double>("math.logit", -1.), IsOkAndHolds(IsNan()));
  EXPECT_THAT(InvokeOperator<double>("math.logit", 2.), IsOkAndHolds(IsNan()));
}

TEST_F(MathOperatorsTest, Sin) {
  EXPECT_THAT(InvokeOperator<float>("math.trig.sin", kPi),
              IsOkAndHolds(FloatNear(0.f, 1e-05)));
  EXPECT_THAT(InvokeOperator<float>("math.trig.sin", 1.f),
              IsOkAndHolds(FloatEq(std::sin(1.f))));

  EXPECT_THAT(InvokeOperator<double>("math.trig.sin", double{kPi}),
              IsOkAndHolds(DoubleNear(0.f, 1e-05)));
  EXPECT_THAT(InvokeOperator<double>("math.trig.sin", 1.),
              IsOkAndHolds(DoubleEq(std::sin(1.))));
}

TEST_F(MathOperatorsTest, Cos) {
  EXPECT_THAT(InvokeOperator<float>("math.trig.cos", kPi),
              IsOkAndHolds(FloatNear(-1.f, 1e-05)));
  EXPECT_THAT(InvokeOperator<float>("math.trig.cos", 1.f),
              IsOkAndHolds(FloatEq(std::cos(1.f))));

  EXPECT_THAT(InvokeOperator<double>("math.trig.cos", double{kPi}),
              IsOkAndHolds(DoubleNear(-1.f, 1e-05)));
  EXPECT_THAT(InvokeOperator<double>("math.trig.cos", 1.),
              IsOkAndHolds(DoubleEq(std::cos(1.))));
}

TEST_F(MathOperatorsTest, Sinh) {
  EXPECT_THAT(InvokeOperator<float>("math.trig.sinh", 0.f),
              IsOkAndHolds(FloatNear(0.f, 1e-05)));
  EXPECT_THAT(InvokeOperator<float>("math.trig.sinh", 1.f),
              IsOkAndHolds(FloatEq(std::sinh(1.f))));

  EXPECT_THAT(InvokeOperator<double>("math.trig.sinh", 0.),
              IsOkAndHolds(DoubleNear(0.f, 1e-05)));
  EXPECT_THAT(InvokeOperator<double>("math.trig.sinh", 1.),
              IsOkAndHolds(DoubleEq(std::sinh(1.))));
}

TEST_F(MathOperatorsTest, atan) {
  EXPECT_THAT(InvokeOperator<float>("math.trig.atan", 0.f),
              IsOkAndHolds(FloatNear(0.f, 1e-05)));
  EXPECT_THAT(InvokeOperator<float>("math.trig.atan", 1.f),
              IsOkAndHolds(FloatEq(std::atan(1.f))));

  EXPECT_THAT(InvokeOperator<double>("math.trig.atan", 0.),
              IsOkAndHolds(DoubleNear(0.f, 1e-05)));
  EXPECT_THAT(InvokeOperator<double>("math.trig.atan", 1.),
              IsOkAndHolds(DoubleEq(std::atan(1.))));
}

}  // namespace
}  // namespace arolla
