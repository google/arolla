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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/lazy/lazy.h"
#include "arolla/lazy/lazy_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::expr::CallOp;
using ::arolla::expr::Invoke;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::testing::TypedValueWith;

class LazyIntegrationTest : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

TEST_F(LazyIntegrationTest, Test) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add", {Literal(1), CallOp("lazy.get", {Leaf("x")})}));
  EXPECT_THAT(Invoke(expr, {{"x", MakeLazyQValue(MakeLazyFromQValue(
                                      TypedValue::FromValue(1)))}}),
              IsOkAndHolds(TypedValueWith<int>(2)));
}

}  // namespace
}  // namespace arolla
