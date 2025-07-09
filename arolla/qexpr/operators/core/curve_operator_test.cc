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
#include "arolla/qexpr/operators/core/curve_operator.h"

#include <cstdint>
#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/pwlcurve/curves.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/dense_array/testing/util.h"
#include "arolla/memory/buffer.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/bytes.h"

namespace arolla::testing {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::pwlcurve::CurvePtr;
using ::testing::ElementsAre;
using ::testing::Eq;

constexpr auto kInf = std::numeric_limits<float>::infinity();

TEST(CurveOperatorTest, CreateCurveFromSpec) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      InvokeOperator<CurvePtr>("core._create_curve",
                               Bytes("PWLCurve({{1;0};{5;1};{inf;inf}})")));
  EXPECT_THAT(curve->Eval(5.), Eq(1));
  EXPECT_THAT(curve->Eval(5.f), Eq(1));
}

TEST(CurveOperatorTest, CreateCurveFromDenseArrays) {
  ASSERT_OK_AND_ASSIGN(auto curve,
                       InvokeOperator<CurvePtr>(
                           "core._create_curve", int32_t{pwlcurve::LogPWLCurve},
                           CreateDenseArray<float>({1, 5, kInf}),
                           CreateDenseArray<float>({0, 1, kInf})));
  EXPECT_THAT(curve->Eval(5.), Eq(1));
  EXPECT_THAT(curve->Eval(5.f), Eq(1));
}

TEST(CurveOperatorTest, EvalCurveOnScalar) {
  CurvePtr curve{
      pwlcurve::NewCurve("PWLCurve({{1;0};{5;1};{inf;inf}})").value()};
  EXPECT_THAT(InvokeOperator<float>("core._eval_curve", curve, 5.f),
              IsOkAndHolds(1));
  EXPECT_THAT(InvokeOperator<double>("core._eval_curve", curve, 5.),
              IsOkAndHolds(1));
}

TEST(CurveOperatorTest, EvalCurveOnDenseArray) {
  CurvePtr curve{
      pwlcurve::NewCurve("PWLCurve({{1;0};{5;1};{inf;inf}})").value()};
  ASSERT_OK_AND_ASSIGN(DenseArray<float> float_res,
                       InvokeOperator<DenseArray<float>>(
                           "core._eval_curve", curve,
                           DenseArray<float>{CreateBuffer<float>({0., 5.})}));
  EXPECT_THAT(ToVectorOptional(float_res), ElementsAre(0, 1.));

  ASSERT_OK_AND_ASSIGN(DenseArray<double> double_res,
                       InvokeOperator<DenseArray<double>>(
                           "core._eval_curve", curve,
                           DenseArray<double>{CreateBuffer<double>({0., 5.})}));
  EXPECT_THAT(ToVectorOptional(double_res), ElementsAre(0, 1.));
}

}  // namespace
}  // namespace arolla::testing
