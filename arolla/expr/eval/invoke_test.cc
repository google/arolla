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
#include "arolla/expr/eval/invoke.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::TypedValueWith;
using ::testing::Eq;

TEST(InvokeTest, SimpleAST) {
  // x * y + z
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expr,
      CallOp("math.add",
             {CallOp("math.multiply", {Leaf("x"), Leaf("y")}), Leaf("z")}));

  EXPECT_THAT(Invoke(expr, {{"x", TypedValue::FromValue(5)},
                            {"y", TypedValue::FromValue(10)},
                            {"z", TypedValue::FromValue(7)}}),
              IsOkAndHolds(TypedValueWith<int32_t>(Eq(57))));
  EXPECT_THAT(Invoke(expr, {{"x", TypedValue::FromValue(5)},
                            {"y", TypedValue::FromValue(10)}}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace arolla::expr
