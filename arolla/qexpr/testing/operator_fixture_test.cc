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
#include "arolla/qexpr/testing/operator_fixture.h"

#include <cstdint>
#include <tuple>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/codegen/qexpr/testing/test_operators.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/operator_factory.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::Vector3;
using ::testing::Eq;

template <typename T>
using Slot = FrameLayout::Slot<T>;

TEST(OperatorFixtureTest, TestSingleResultOperator) {
  // Get the Operator
  auto float_type = GetQType<float>();
  auto op =
      OperatorRegistry::GetInstance()
          ->LookupOperator("test.add", {float_type, float_type}, float_type)
          .value();

  // Create the fixture. ARG_Ts and RES_Ts are the argument and result types
  // of the operator.
  using ARG_Ts = std::tuple<float, float>;
  using RES_Ts = float;
  auto fixture = OperatorFixture<ARG_Ts, RES_Ts>::Create(*op).value();

  // Invoke the operator.
  float result = fixture.Call(10.0f, 20.0f).value();

  // Verify result.
  EXPECT_THAT(result, Eq(30.0f));
}

TEST(OperatorFixtureTest, TestMultipleResultOperator) {
  // Get the Operator
  auto vector_type = GetQType<Vector3<float>>();
  auto op =
      OperatorRegistry::GetInstance()
          ->LookupOperator("test.vector_components", {vector_type},
                           MakeTupleQType({GetQType<float>(), GetQType<float>(),
                                           GetQType<float>()}))
          .value();

  // Create the fixture. ARG_Ts and RES_Ts are the argument and result types
  // of the operator.
  using ARG_Ts = std::tuple<Vector3<float>>;
  using RES_Ts = std::tuple<float, float, float>;
  auto fixture = OperatorFixture<ARG_Ts, RES_Ts>::Create(*op).value();

  // Invoke the operator.
  float a, b, c;
  std::tie(a, b, c) = fixture.Call(Vector3<float>(10.0f, 20.0f, 30.0f)).value();

  // Verify result.
  EXPECT_THAT(a, Eq(10.0f));
  EXPECT_THAT(b, Eq(20.0f));
  EXPECT_THAT(c, Eq(30.0f));
}

TEST(OperatorFixtureTest, TestReturningTupleOperator) {
  ASSERT_OK_AND_ASSIGN(auto op, QExprOperatorFromFunction([]() {
                         return std::tuple<int32_t, float>(57, 57.75f);
                       }));
  using ARG_Ts = std::tuple<>;
  using RES_Ts = std::tuple<int32_t, float>;
  auto fixture = OperatorFixture<ARG_Ts, RES_Ts>::Create(*op).value();
  EXPECT_THAT(fixture.Call(), IsOkAndHolds(Eq(std::tuple(57, 57.75f))));
}

}  // namespace
}  // namespace arolla
