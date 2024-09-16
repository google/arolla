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
#include "arolla/serving/expr_compiler.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/thread_safe_model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/accessors_slot_listener.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/io/tuple_input_loader.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/simple_executable.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::CompiledExpr;
using ::arolla::GetQType;
using ::arolla::InputLoaderPtr;
using ::arolla::SlotListener;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::Leaf;
using ::arolla::testing::WithExportValueAnnotation;
using ::testing::_;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

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

absl::StatusOr<std::unique_ptr<SlotListener<TestSideOutput>>>
CreateSlotListener() {
  return ::arolla::CreateAccessorsSlotListener<TestSideOutput>(
      "subtract", [](float x, TestSideOutput* out) { out->subtract = x; });
}

absl::StatusOr<ExprNodePtr> CreateExpr() {
  ASSIGN_OR_RETURN(auto add_expr, CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSIGN_OR_RETURN(auto subtract_expr,
                   CallOp("math.subtract", {Leaf("x"), Leaf("y")}));
  return WithExportValueAnnotation(add_expr, "subtract", subtract_expr);
}

absl::StatusOr<std::unique_ptr<CompiledExpr>> CreateCompiledExpr() {
  ASSIGN_OR_RETURN(auto add_expr, CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSIGN_OR_RETURN(auto subtract_expr,
                   CallOp("math.subtract", {Leaf("x"), Leaf("y")}));
  return ::arolla::expr::CompileForDynamicEvaluation(
      ::arolla::expr::DynamicEvaluationEngineOptions(), add_expr,
      {{"x", GetQType<float>()}, {"y", GetQType<float>()}},
      /*side_outputs=*/{{"subtract", subtract_expr}});
}

}  // namespace

namespace arolla {
namespace {

class TestInplaceCompiledExpr : public InplaceCompiledExpr {
 public:
  TestInplaceCompiledExpr()
      : InplaceCompiledExpr(
            /*input_types=*/{}, /*output_type=*/GetQType<float>(),
            /*named_output_types=*/{}) {}

  absl::StatusOr<std::unique_ptr<BoundExpr>> InplaceBind(
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
      TypedSlot output_slot,
      const absl::flat_hash_map<std::string, TypedSlot>& named_output_slots)
      const final {
    return std::make_unique<SimpleBoundExpr>(
        input_slots, output_slot,
        /*init_ops=*/std::vector<std::unique_ptr<BoundOperator>>{},
        /*eval_ops=*/std::vector<std::unique_ptr<BoundOperator>>{},
        named_output_slots);
  }
};

class ExprCompilerTest : public ::testing::Test {
 public:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(auto add_expr,
                         expr::CallOp("math.add", {Leaf("x"), Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(auto subtract_expr,
                         expr::CallOp("math.subtract", {Leaf("x"), Leaf("y")}));
    ASSERT_OK_AND_ASSIGN(expr_, CreateExpr());
    ASSERT_OK_AND_ASSIGN(compiled_expr_, CreateCompiledExpr());
  }

  expr::ExprNodePtr expr_;
  std::unique_ptr<const CompiledExpr> compiled_expr_;
};

TEST_F(ExprCompilerTest, CompileExprNodePtr) {
  ASSERT_OK_AND_ASSIGN(auto model,
                       (ExprCompiler<TestInput, std::optional<float>>())
                           .SetInputLoader(CreateInputLoader())
                           .AllowOutputCasting()
                           .Compile(expr_));
  ASSERT_OK_AND_ASSIGN(
      auto model_with_options,
      (ExprCompiler<TestInput, std::optional<float>>())
          .SetInputLoader(CreateInputLoader())
          .AllowOutputCasting()
          .Compile<ExprCompilerFlags::kEvalWithOptions>(expr_));
  static_assert(
      std::is_same_v<decltype(model),
                     std::function<absl::StatusOr<std::optional<float>>(
                         const TestInput&)>>);
  static_assert(
      std::is_same_v<decltype(model_with_options),
                     std::function<absl::StatusOr<std::optional<float>>(
                         const ModelFunctionOptions&, const TestInput&)>>);

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input), IsOkAndHolds(57));
  EXPECT_THAT(model_with_options({}, input), IsOkAndHolds(57));
}

TEST_F(ExprCompilerTest, CompileExprNodePtrWithSideOutput) {
  ASSERT_OK_AND_ASSIGN(
      auto model,
      (ExprCompiler<TestInput, std::optional<float>, TestSideOutput>())
          .SetInputLoader(CreateInputLoader())
          .SetSlotListener(CreateSlotListener())
          .AllowOutputCasting()
          .Compile(expr_));
  static_assert(
      std::is_same_v<decltype(model),
                     std::function<absl::StatusOr<std::optional<float>>(
                         const TestInput&, TestSideOutput*)>>);

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input, nullptr), IsOkAndHolds(57));
  TestSideOutput side_output;
  EXPECT_THAT(model(input, &side_output), IsOkAndHolds(57));
  EXPECT_THAT(side_output.subtract, Eq(-1));
}

TEST_F(ExprCompilerTest, CompileCompiledExpr) {
  ASSERT_OK_AND_ASSIGN(auto model,
                       (ExprCompiler<TestInput, std::optional<float>>())
                           .SetInputLoader(CreateInputLoader())
                           .AllowOutputCasting()
                           .Compile(*compiled_expr_));
  static_assert(
      std::is_same_v<decltype(model),
                     std::function<absl::StatusOr<std::optional<float>>(
                         const TestInput&)>>);

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input), IsOkAndHolds(57));
}

TEST_F(ExprCompilerTest, CompileCompiledExprForceNonOptionalOutput) {
  ASSERT_OK_AND_ASSIGN(auto model, (ExprCompiler<TestInput, float>())
                                       .SetInputLoader(CreateInputLoader())
                                       .ForceNonOptionalOutput()
                                       .Compile(*compiled_expr_));
  static_assert(
      std::is_same_v<decltype(model),
                     std::function<absl::StatusOr<float>(const TestInput&)>>);

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input), IsOkAndHolds(57));
}

TEST_F(ExprCompilerTest, CompileCompiledExprWithSideOutput) {
  ASSERT_OK_AND_ASSIGN(
      auto model,
      (ExprCompiler<TestInput, std::optional<float>, TestSideOutput>())
          .SetInputLoader(CreateInputLoader())
          .SetSlotListener(CreateSlotListener())
          .AllowOutputCasting()
          .Compile(*compiled_expr_));
  static_assert(
      std::is_same_v<decltype(model),
                     std::function<absl::StatusOr<std::optional<float>>(
                         const TestInput&, TestSideOutput*)>>);

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input, nullptr), IsOkAndHolds(57));
  TestSideOutput side_output;
  EXPECT_THAT(model(input, &side_output), IsOkAndHolds(57));
  EXPECT_THAT(side_output.subtract, Eq(-1));
}

TEST_F(ExprCompilerTest, CompileExprOperatorWithTuple) {
  ASSERT_OK_AND_ASSIGN(auto model,
                       (ExprCompiler<std::tuple<float, float>, float>())
                           .CompileOperator(expr::LookupOperator("math.add")));
  static_assert(
      std::is_same_v<decltype(model), std::function<absl::StatusOr<float>(
                                          const std::tuple<float, float>&)>>);

  EXPECT_THAT(model({28, 29}), IsOkAndHolds(57));
}

TEST_F(ExprCompilerTest, CompileExprOperatorWithTypedRefs) {
  ASSERT_OK_AND_ASSIGN(
      auto model, (ExprCompiler<absl::Span<const TypedRef>, TypedValue>())
                      .CompileOperator(expr::LookupOperator("math.add"),
                                       {GetQType<float>(), GetQType<float>()}));
  static_assert(
      std::is_same_v<decltype(model), std::function<absl::StatusOr<TypedValue>(
                                          const absl::Span<const TypedRef>&)>>);
  auto a = TypedValue::FromValue<float>(28);
  auto b = TypedValue::FromValue<float>(29);
  std::vector<TypedRef> args{a.AsRef(), b.AsRef()};
  ASSERT_OK_AND_ASSIGN(TypedValue res, model(args));
  EXPECT_THAT(res.As<float>(), IsOkAndHolds(57));
}

TEST_F(ExprCompilerTest, Ownership) {
  ExprCompiler<TestInput, std::optional<float>, TestSideOutput> mc;
  mc.SetInputLoader(CreateInputLoader());
  ExprCompiler<TestInput, std::optional<float>, TestSideOutput> other_mc =
      std::move(mc);
  other_mc.SetSlotListener(CreateSlotListener());
  mc = std::move(other_mc);
  mc.AllowOutputCasting();
  // Let ASAN check that mc works well after all these moves;
  ASSERT_OK(mc.Compile(expr_));
}

TEST_F(ExprCompilerTest, Move) {
  auto set_input_loader = [](auto mc) {
    return std::move(mc).SetInputLoader(CreateInputLoader());
  };

  ASSERT_OK_AND_ASSIGN(
      auto model,
      // We can set options on rvalue inside a function.
      set_input_loader(
          ExprCompiler<TestInput, std::optional<float>, TestSideOutput>()
              // We can set options on rvalue before the move into a function.
              .SetSlotListener(CreateSlotListener()))
          // We can set options on rvalue after the move into a function.
          .SetExperimentalArenaAllocator()
          .AllowOutputCasting()
          .Compile(expr_));

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input, nullptr), IsOkAndHolds(57));
}

TEST_F(ExprCompilerTest, Optimizer) {
  auto replace_add_with_subtract =
      [](expr::ExprNodePtr x) -> absl::StatusOr<expr::ExprNodePtr> {
    if (expr::IsBackendOperator(*expr::DecayRegisteredOperator(x->op()),
                                "math.add")) {
      return expr::WithNewOperator(x, *expr::LookupOperator("math.subtract"));
    }
    return x;
  };
  ASSERT_OK_AND_ASSIGN(auto model,
                       (ExprCompiler<TestInput, std::optional<float>>())
                           .SetInputLoader(CreateInputLoader())
                           .SetExprOptimizer(replace_add_with_subtract)
                           .AllowOutputCasting()
                           .Compile(expr_));

  TestInput input{.x = 28, .y = 29};
  // Subtracting instead of adding.
  EXPECT_THAT(model(input), IsOkAndHolds(-1));
}

TEST_F(ExprCompilerTest, OtherOptionsSmokeTest) {
  ASSERT_OK_AND_ASSIGN(
      auto model,
      (ExprCompiler<TestInput, std::optional<float>, TestSideOutput>())
          .SetInputLoader(CreateInputLoader())
          .SetSlotListener(CreateSlotListener())
          .SetExperimentalArenaAllocator()
          .SetAlwaysCloneThreadSafetyPolicy()
          .AllowOutputCasting()
          .Compile(expr_));
  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(model(input, nullptr), IsOkAndHolds(57));
  TestSideOutput side_output;
  EXPECT_THAT(model(input, &side_output), IsOkAndHolds(57));
  EXPECT_THAT(side_output.subtract, Eq(-1));
}

TEST_F(ExprCompilerTest, DefaultThreadSafetyPolicy) {
  ASSERT_OK_AND_ASSIGN(
      auto model,
      (ExprCompiler<TestInput, std::optional<float>, TestSideOutput>())
          .SetInputLoader(CreateInputLoader())
          .SetSlotListener(CreateSlotListener())
          .AllowOutputCasting()
          .Compile(expr_));
  TestInput input{.x = 28, .y = 29};
  TestSideOutput side_output;
  EXPECT_THAT(model(input, &side_output), IsOkAndHolds(57));
  EXPECT_THAT(side_output.subtract, Eq(-1));
  EXPECT_THAT((model.target<expr::ThreadSafePoolModelExecutor<
                   TestInput, std::optional<float>, TestSideOutput>>()),
              NotNull());
}

TEST_F(ExprCompilerTest, DefaultThreadSafetyPolicy_Codegen) {
  ASSERT_OK_AND_ASSIGN(auto eval_model, (ExprCompiler<TestInput, float>())
                                            .SetInputLoader(CreateInputLoader())
                                            .Compile(*compiled_expr_));
  ASSERT_OK_AND_ASSIGN(auto codegen_model,
                       (ExprCompiler<TestInput, float>())
                           .SetInputLoader(CreateInputLoader())
                           .Compile(TestInplaceCompiledExpr()));
  EXPECT_THAT(
      (eval_model
           .target<expr::ThreadSafePoolModelExecutor<TestInput, float>>()),
      NotNull());
  // There is no way to test exactly the returned function, but we can check
  // that it is not the same as for eval_model.
  EXPECT_THAT(
      (codegen_model
           .target<expr::ThreadSafePoolModelExecutor<TestInput, float>>()),
      IsNull());
}

TEST_F(ExprCompilerTest, PoolThreadSafetyPolicy) {
  ASSERT_OK_AND_ASSIGN(
      auto model,
      (ExprCompiler<TestInput, std::optional<float>, TestSideOutput>())
          .SetInputLoader(CreateInputLoader())
          .SetSlotListener(CreateSlotListener())
          .SetPoolThreadSafetyPolicy()
          .AllowOutputCasting()
          .Compile(expr_));
  TestInput input{.x = 28, .y = 29};
  TestSideOutput side_output;
  // NOTE: Thread safety is tested in
  // expr/eval/thread_safe_model_executor_test.cc
  EXPECT_THAT(model(input, &side_output), IsOkAndHolds(57));
  EXPECT_THAT(side_output.subtract, Eq(-1));
  EXPECT_THAT((model.target<expr::ThreadSafePoolModelExecutor<
                   TestInput, std::optional<float>, TestSideOutput>>()),
              NotNull());
}

TEST_F(ExprCompilerTest, AlwaysCloneThreadSafetyPolicy) {
  ASSERT_OK_AND_ASSIGN(
      auto model,
      (ExprCompiler<TestInput, std::optional<float>, TestSideOutput>())
          .SetInputLoader(CreateInputLoader())
          .SetSlotListener(CreateSlotListener())
          .SetAlwaysCloneThreadSafetyPolicy()
          .AllowOutputCasting()
          .Compile(expr_));
  TestInput input{.x = 28, .y = 29};
  TestSideOutput side_output;
  EXPECT_THAT(model(input, &side_output), IsOkAndHolds(57));
  EXPECT_THAT(side_output.subtract, Eq(-1));
}

TEST_F(ExprCompilerTest, ThreadUnsafe) {
  ASSERT_OK_AND_ASSIGN(
      auto model,
      (ExprCompiler<TestInput, std::optional<float>, TestSideOutput>())
          .SetInputLoader(CreateInputLoader())
          .SetSlotListener(CreateSlotListener())
          .SetThreadUnsafe_I_SWEAR_TO_COPY_MODEL_FUNCTION_BEFORE_CALL()
          .AllowOutputCasting()
          .Compile(expr_));
  TestInput input{.x = 28, .y = 29};
  TestSideOutput side_output;
  // NOTE: Thread safety is tested in
  // expr/eval/thread_safe_model_executor_test.cc
  EXPECT_THAT(model(input, &side_output), IsOkAndHolds(57));
  EXPECT_THAT(side_output.subtract, Eq(-1));
}

TEST_F(ExprCompilerTest, ForceNonOptionalOutput) {
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.neg", {Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      ::arolla::CreateAccessorsInputLoader<std::optional<float>>(
          "x", [](const auto& x) { return OptionalValue<float>(x); }));

  ASSERT_OK_AND_ASSIGN(
      auto model,
      (ExprCompiler<std::optional<float>, std::optional<float>>())
          .SetInputLoader(MakeNotOwningInputLoader(input_loader.get()))
          .Compile(expr));
  EXPECT_THAT(model(std::nullopt), IsOkAndHolds(std::nullopt));

  // Does not compile with non-optional output by default.
  EXPECT_THAT((ExprCompiler<std::optional<float>, float>())
                  .SetInputLoader(MakeNotOwningInputLoader(input_loader.get()))
                  .AllowOutputCasting()
                  .Compile(expr),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("model output is deduced to optional, while "
                                 "non-optional is requested")));

  // Compiles with non-optional output when ForceNonOptionalOutput is set.
  ASSERT_OK_AND_ASSIGN(
      auto full_model,
      (ExprCompiler<std::optional<float>, float>())
          .SetInputLoader(MakeNotOwningInputLoader(input_loader.get()))
          .ForceNonOptionalOutput()
          .Compile(expr));
  EXPECT_THAT(full_model(-57), IsOkAndHolds(57));
  EXPECT_THAT(full_model(std::nullopt),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("expects a present value, got missing")));
}

// Dummy SlotListener<void> subclass, just to test that it is prohibited.
class VoidSlotListener : public StaticSlotListener<void> {
 public:
  VoidSlotListener() : StaticSlotListener<void>({}) {}

  absl::StatusOr<BoundSlotListener<Output>> BindImpl(
      // The slots corresponding to this SlotListener's inputs.
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots)
      const final {
    return absl::UnimplementedError("unimplemented");
  }

 private:
};

TEST_F(ExprCompilerTest, Errors) {
  EXPECT_THAT((ExprCompiler<TestInput, std::optional<float>, TestSideOutput>())
                  .SetSlotListener(CreateSlotListener())
                  .Compile(expr_),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("InputLoader is not specified, use "
                                 "ExprCompiler::SetInputLoader()")));
  EXPECT_THAT(
      (ExprCompiler<TestInput, std::optional<float>, TestSideOutput>())
          .SetInputLoader(CreateInputLoader())
          .Compile(expr_),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("SlotListener is not specified, use "
                         "ExprCompiler::SetSlotListener() or ExprCompiler<...> "
                         "without SideOutput template parameter")));
  EXPECT_THAT((ExprCompiler<TestInput, std::optional<float>, void>())
                  .SetInputLoader(CreateInputLoader())
                  .SetSlotListener(std::unique_ptr<SlotListener<void>>(
                      new VoidSlotListener()))
                  .Compile(expr_),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("SlotListener with SideOutput==void is not "
                                 "supported by ExprCompiler")));
  EXPECT_THAT(
      (ExprCompiler<std::tuple<float, float>, std::optional<float>>())
          .SetInputLoader(
              TupleInputLoader<std::tuple<float, float>>::Create({"x", "y"}))
          .CompileOperator(nullptr),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("InputLoader is specified, but not needed for "
                         "ExprCompiler::CompilerOperator")));
}

TEST_F(ExprCompilerTest, CompileExprSet) {
  ASSERT_OK_AND_ASSIGN(
      auto models,
      CompileExprSet(
          ExprCompiler<TestInput, std::optional<float>>()
              .SetInputLoader(CreateInputLoader())
              .AllowOutputCasting(),
          absl::flat_hash_map<std::string, absl::StatusOr<expr::ExprNodePtr>>{
              {"first", expr_}, {"second", expr_}}));
  ASSERT_THAT(models,
              UnorderedElementsAre(Pair("first", _), Pair("second", _)));

  static_assert(
      std::is_same_v<std::decay_t<decltype(models[""])>,
                     std::function<absl::StatusOr<std::optional<float>>(
                         const TestInput&)>>);

  TestInput input{.x = 28, .y = 29};
  EXPECT_THAT(models["first"](input), IsOkAndHolds(57));
}

TEST_F(ExprCompilerTest, CompileExprSet_Errors) {
  EXPECT_THAT(
      CompileExprSet(
          ExprCompiler<TestInput, std::optional<float>>()
              .SetInputLoader(CreateInputLoader())
              .AllowOutputCasting(),
          absl::flat_hash_map<std::string, absl::StatusOr<expr::ExprNodePtr>>{
              {"first", expr_},
              {"bad_model", absl::FailedPreconditionError("very bad model")}}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               "very bad model; while initializing model \"bad_model\""));
}

}  // namespace
}  // namespace arolla
