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
#include "arolla/serving/embedded_model.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/accessors_slot_listener.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace {

// Do not use using from `::arolla` in order to test that
// macro doesn't use not fully specified (Arolla local) names.
using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::Eq;

struct TestInput {
  float x;
  float y;
};

struct TestSideOutput {
  std::optional<float> subtract;
};

absl::StatusOr<std::unique_ptr<arolla::InputLoader<TestInput>>>
CreateInputLoader() {
  return ::arolla::CreateAccessorsInputLoader<TestInput>(
      "x", [](const auto& x) { return x.x; },  //
      "y", [](const auto& x) { return x.y; });
}

absl::StatusOr<std::unique_ptr<::arolla::SlotListener<TestSideOutput>>>
CreateSlotListener() {
  return ::arolla::CreateAccessorsSlotListener<TestSideOutput>(
      "subtract", [](float x, TestSideOutput* out) { out->subtract = x; });
}

absl::StatusOr<::arolla::expr::ExprNodePtr> CreateExpr() {
  using ::arolla::expr::CallOp;
  using ::arolla::expr::Leaf;
  using ::arolla::testing::WithExportValueAnnotation;
  ASSIGN_OR_RETURN(auto add_expr, CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSIGN_OR_RETURN(auto subtract_expr,
                   CallOp("math.subtract", {Leaf("x"), Leaf("y")}));
  return WithExportValueAnnotation(add_expr, "subtract", subtract_expr);
}

absl::StatusOr<std::unique_ptr<::arolla::CompiledExpr>> CreateCompiledExpr() {
  using ::arolla::GetQType;
  using ::arolla::expr::CallOp;
  using ::arolla::expr::Leaf;
  ASSIGN_OR_RETURN(auto add_expr, CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSIGN_OR_RETURN(auto subtract_expr,
                   CallOp("math.subtract", {Leaf("x"), Leaf("y")}));
  return ::arolla::expr::CompileForDynamicEvaluation(
      ::arolla::expr::DynamicEvaluationEngineOptions(), add_expr,
      {{"x", GetQType<float>()}, {"y", GetQType<float>()}},
      /*side_outputs=*/{{"subtract", subtract_expr}});
}

// Define outside of arolla namespace to verify that macro is friendly to that.
namespace test_namespace {

// Make sure that function can be defined with the same type in header file.
const ::arolla::ExprCompiler<TestInput, std::optional<float>>::Function&
MyDynamicEmbeddedModel();

AROLLA_DEFINE_EMBEDDED_MODEL_FN(
    MyDynamicEmbeddedModel,
    (::arolla::ExprCompiler<TestInput, std::optional<float>>()
         .SetInputLoader(CreateInputLoader())
         .AllowOutputCasting()
         .Compile(CreateExpr().value())));

}  // namespace test_namespace

TEST(ExprCompilerTest, UseDynamicEmbeddedExpr) {
  auto model = ::test_namespace::MyDynamicEmbeddedModel();
  static_assert(
      std::is_same_v<std::decay_t<decltype(model)>,
                     std::function<absl::StatusOr<std::optional<float>>(
                         const TestInput&)>>);

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input), IsOkAndHolds(57));
}

// Define outside of arolla namespace to verify that macro is friendly to that.
namespace test_namespace {

// Make sure that function can be defined with the same type in header file.
const ::arolla::ExprCompiler<TestInput, std::optional<float>,
                             TestSideOutput>::Function&
MyCompiledEmbeddedModel();

AROLLA_DEFINE_EMBEDDED_MODEL_FN(
    MyCompiledEmbeddedModel,
    (::arolla::ExprCompiler<TestInput, std::optional<float>, TestSideOutput>()
         .SetInputLoader(CreateInputLoader())
         .SetSlotListener(CreateSlotListener())
         .AllowOutputCasting()
         .Compile(*CreateCompiledExpr().value())));

}  // namespace test_namespace

TEST(ExprCompilerTest, UseCompiledEmbeddedExprWithSideOutput) {
  auto model = ::test_namespace::MyCompiledEmbeddedModel();
  static_assert(
      std::is_same_v<std::decay_t<decltype(model)>,
                     std::function<absl::StatusOr<std::optional<float>>(
                         const TestInput&, TestSideOutput*)>>);

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input, nullptr), IsOkAndHolds(57));
  TestSideOutput side_output;
  EXPECT_THAT(model(input, &side_output), IsOkAndHolds(57));
  EXPECT_THAT(side_output.subtract, Eq(-1));
}

namespace test_namespace {

absl::flat_hash_map<std::string, absl::StatusOr<::arolla::expr::ExprNodePtr>>
CreateExprSet() {
  return {{"first_expr", CreateExpr()}, {"second_expr", CreateExpr()}};
}

// Make sure that function can be defined with the same type in header file.
absl::StatusOr<std::reference_wrapper<
    const ::arolla::ExprCompiler<TestInput, std::optional<float>>::Function>>
    MyDynamicEmbeddedExprSet(absl::string_view);

AROLLA_DEFINE_EMBEDDED_MODEL_SET_FN(
    MyDynamicEmbeddedExprSet,
    ::arolla::CompileExprSet(
        ::arolla::ExprCompiler<TestInput, std::optional<float>>()
            .SetInputLoader(CreateInputLoader())
            .AllowOutputCasting(),
        CreateExprSet()));

}  // namespace test_namespace

TEST(ExprCompilerTest, UseDynamicEmbeddedExprSet) {
  ASSERT_OK_AND_ASSIGN(auto model,
                       test_namespace::MyDynamicEmbeddedExprSet("first_expr"));
  static_assert(
      std::is_same_v<decltype(model),
                     std::reference_wrapper<const std::function<absl::StatusOr<
                         std::optional<float>>(const TestInput&)>>>);

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input), IsOkAndHolds(57));

  EXPECT_THAT(test_namespace::MyDynamicEmbeddedExprSet("second_expr"), IsOk());
  EXPECT_THAT(
      test_namespace::MyDynamicEmbeddedExprSet("missing_expr"),
      StatusIs(absl::StatusCode::kNotFound,
               "model \"missing_expr\" not found in MyDynamicEmbeddedExprSet"));
}

namespace test_namespace {

absl::flat_hash_map<std::string,
                    std::reference_wrapper<const ::arolla::CompiledExpr>>
CreateCompiledExprSet() {
  static const absl::NoDestructor<std::unique_ptr<::arolla::CompiledExpr>>
      compiled_expr(CreateCompiledExpr().value());
  return absl::flat_hash_map<
      std::string, std::reference_wrapper<const ::arolla::CompiledExpr>>{
      {"first_expr", **compiled_expr}, {"second_expr", **compiled_expr}};
}

// Make sure that function can be defined with the same type in header file.
absl::StatusOr<std::reference_wrapper<
    const ::arolla::ExprCompiler<TestInput, std::optional<float>>::Function>>
    MyCompiledEmbeddedExprSet(absl::string_view);

AROLLA_DEFINE_EMBEDDED_MODEL_SET_FN(
    MyCompiledEmbeddedExprSet,
    ::arolla::CompileExprSet(
        ::arolla::ExprCompiler<TestInput, std::optional<float>>()
            .SetInputLoader(CreateInputLoader())
            .AllowOutputCasting(),
        CreateCompiledExprSet()));

}  // namespace test_namespace

TEST(ExprCompilerTest, UseCompiledEmbeddedExprSet) {
  ASSERT_OK_AND_ASSIGN(auto model,
                       test_namespace::MyCompiledEmbeddedExprSet("first_expr"));
  static_assert(
      std::is_same_v<decltype(model),
                     std::reference_wrapper<const std::function<absl::StatusOr<
                         std::optional<float>>(const TestInput&)>>>);

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input), IsOkAndHolds(57));

  EXPECT_THAT(test_namespace::MyCompiledEmbeddedExprSet("second_expr"), IsOk());
  EXPECT_THAT(
      test_namespace::MyCompiledEmbeddedExprSet("missing_expr"),
      StatusIs(
          absl::StatusCode::kNotFound,
          "model \"missing_expr\" not found in MyCompiledEmbeddedExprSet"));
}

void BM_MyDynamicEmbeddedModel_Request(benchmark::State& state) {
  arolla::InitArolla();
  TestInput input{.x = 28, .y = 29};
  for (auto _ : state) {
    benchmark::DoNotOptimize(input);
    CHECK_EQ(**test_namespace::MyDynamicEmbeddedModel()(input), 57);
  }
}

void BM_MyDynamicEmbeddedModel_ConstructOutOfTheLoop(benchmark::State& state) {
  arolla::InitArolla();
  TestInput input{.x = 28, .y = 29};
  auto model = test_namespace::MyDynamicEmbeddedModel();
  for (auto _ : state) {
    benchmark::DoNotOptimize(input);
    CHECK_EQ(*model(input).value(), 57);
  }
}

BENCHMARK(BM_MyDynamicEmbeddedModel_Request);
BENCHMARK(BM_MyDynamicEmbeddedModel_ConstructOutOfTheLoop);

}  // namespace
