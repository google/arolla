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
#include "arolla/codegen/expr/optimizations.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"

namespace arolla::codegen {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::_;
using ::testing::HasSubstr;

TEST(GetOptimizer, DefaultIsOk) {
  EXPECT_THAT(GetOptimizer(""), IsOkAndHolds(_));
}

TEST(GetOptimizer, Unknown) {
  EXPECT_THAT(GetOptimizer("unknown_name").status(),
              StatusIs(absl::StatusCode::kNotFound, HasSubstr("unknown_name")));
}

TEST(GetOptimizer, Register) {
  EXPECT_THAT(RegisterOptimization(
                  "new_opt",
                  [](expr::ExprNodePtr) -> absl::StatusOr<expr::ExprNodePtr> {
                    return absl::InternalError("fake optimization");
                  }),
              IsOk());
  ASSERT_OK_AND_ASSIGN(auto optimizer, GetOptimizer("new_opt"));
  EXPECT_THAT(
      optimizer(expr::Leaf("x")).status(),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("fake optimization")));
}

}  // namespace
}  // namespace arolla::codegen
