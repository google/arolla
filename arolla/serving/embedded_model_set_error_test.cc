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
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/input_loader.h"
#include "arolla/serving/embedded_model.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/init_arolla.h"

namespace {

// Do not use using from `::arolla` in order to test that
// macro doesn't use not fully specified (Arolla local) names.

struct TestInput {
  float x;
  float y;
};

absl::StatusOr<std::unique_ptr<arolla::InputLoader<TestInput>>>
CreateInputLoader() {
  return arolla::CreateAccessorsInputLoader<TestInput>(
      "y", [](const auto& x) { return x.y; });
}

absl::StatusOr<::arolla::expr::ExprNodePtr> CreateExpr() {
  using arolla::expr::Leaf;
  return arolla::expr::CallOp("math.add", {Leaf("x"), Leaf("y")});
}

// Define outside of arolla namespace to verify that macro is friendly to that.
namespace test_namespace {

// Make sure that function can be defined with the same type in header file.
// NOLINTNEXTLINE(clang-diagnostic-unused-function)
absl::StatusOr<std::reference_wrapper<
    const arolla::ExprCompiler<TestInput, std::optional<float>>::Function>>
MyDynamicErrorEmbeddedModelSet(absl::string_view model_name);

AROLLA_DEFINE_EMBEDDED_MODEL_SET_FN(
    MyDynamicErrorEmbeddedModelSet,
    arolla::CompileExprSet(
        arolla::ExprCompiler<TestInput, std::optional<float>>().SetInputLoader(
            CreateInputLoader()),
        absl::flat_hash_map<std::string,
                            absl::StatusOr<arolla::expr::ExprNodePtr>>{
            {"first_expr", CreateExpr()}, {"second_expr", CreateExpr()}}));

}  // namespace test_namespace

TEST(ExprCompilerDeathTest, UseEmbeddedExprWithIncorrectInputLoader) {
  EXPECT_DEATH(arolla::InitArolla(),
               ".*unknown inputs: x \\(available: y\\)"
               ".*while initializing model \".*\""
               ".*MyDynamicErrorEmbeddedModelSet");
}

}  // namespace
