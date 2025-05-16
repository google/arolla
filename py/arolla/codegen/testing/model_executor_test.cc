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
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/input_loader.h"
#include "arolla/serving/expr_compiler.h"
#include "py/arolla/codegen/testing/scalars/x_plus_y_times_x.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;

struct TestInputs {
  float x;
  float y;
};

absl::StatusOr<std::unique_ptr<InputLoader<TestInputs>>>
CreateTestInputsLoader() {
  return CreateAccessorsInputLoader<TestInputs>(
      "x", [](const TestInputs& in) { return in.x; },  //
      "y", [](const TestInputs& in) { return in.y; });
}

TEST(ModelExecutorTest, Basic) {
  // This can be the same input loader that is used for Dynamic Eval.
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());

  ASSERT_OK_AND_ASSIGN(
      auto executor,
      (ExprCompiler<TestInputs, float>()
           .SetInputLoader(std::move(input_loader))
           .Compile(::test_namespace::GetCompiledXPlusYTimesX())));

  EXPECT_THAT(executor(TestInputs{5., 7.}), IsOkAndHolds(60.));
}

}  // namespace
}  // namespace arolla::expr
