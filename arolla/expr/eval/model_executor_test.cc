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
#include "arolla/expr/eval/model_executor.h"

#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/side_output.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operator_factory.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/status_matchers_backport.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::arolla::testing::IsOk;
using ::arolla::testing::IsOkAndHolds;
using ::arolla::testing::StatusIs;
using ::arolla::testing::TypedValueWith;
using ::arolla::testing::WithExportAnnotation;
using ::testing::_;
using ::testing::AllOf;
using ::testing::AnyNumber;
using ::testing::AtLeast;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsFalse;
using ::testing::IsTrue;

struct TestInputs {
  int64_t x;
  int64_t y;
  std::optional<int64_t> optional_z;
};

absl::StatusOr<InputLoaderPtr<TestInputs>> CreateTestInputLoader() {
  return CreateAccessorsInputLoader<TestInputs>(
      "x", [](const TestInputs& in) { return in.x; },  //
      "y", [](const TestInputs& in) { return in.y; });
}

absl::StatusOr<InputLoaderPtr<TestInputs>> CreateTestInt32InputLoader() {
  return CreateAccessorsInputLoader<TestInputs>(
      "x", [](const TestInputs& in) -> int32_t { return in.x; },  //
      "y", [](const TestInputs& in) -> int32_t { return in.y; });
}

class ModelExecutorTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_OK(InitArolla()); }
};

TEST_F(ModelExecutorTest, Move) {
  ASSERT_OK_AND_ASSIGN(auto x_plus_y,
                       CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputLoader());

  ASSERT_OK_AND_ASSIGN(
      auto executor,
      (ModelExecutor<TestInputs, int64_t>::Compile(x_plus_y, *input_loader)));
  ASSERT_THAT(executor.IsValid(), IsTrue());
  EXPECT_THAT(executor.Execute(TestInputs{50, 7}), IsOkAndHolds(57));

  ModelExecutor<TestInputs, int64_t> other_executor{std::move(executor)};
  ASSERT_THAT(other_executor.IsValid(), IsTrue());
  EXPECT_THAT(other_executor.Execute(TestInputs{50, 7}), IsOkAndHolds(57));
  // NOLINTNEXTLINE(bugprone-use-after-move)
  ASSERT_THAT(executor.IsValid(), IsFalse());

  executor = std::move(other_executor);
  ASSERT_THAT(executor.IsValid(), IsTrue());
  EXPECT_THAT(executor.Execute(TestInputs{50, 7}), IsOkAndHolds(57));
  // NOLINTNEXTLINE(bugprone-use-after-move)
  ASSERT_THAT(other_executor.IsValid(), IsFalse());
}

TEST_F(ModelExecutorTest, MissingInputs) {
  ASSERT_OK_AND_ASSIGN(auto x_plus_y, CallOp("math.add", {Leaf("unknown_x"),
                                                          Leaf("unknown_y")}));

  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputLoader());
  EXPECT_THAT(
      (ModelExecutor<TestInputs, int64_t>::Compile(x_plus_y, *input_loader)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "unknown inputs: unknown_x, unknown_y (available: x, y)"));
}

TEST_F(ModelExecutorTest, SimpleExpr) {
  ASSERT_OK_AND_ASSIGN(auto x_plus_y,
                       CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputLoader());

  // Incorrect output type.
  ModelExecutorOptions options;
  options.allow_output_casting = true;
  EXPECT_THAT(
      (ModelExecutor<TestInputs, Bytes>::Compile(x_plus_y, *input_loader,
                                                 nullptr, options)),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          AllOf(HasSubstr("casting from INT64 to BYTES is not allowed"),
                HasSubstr(
                    "while casting model outputs due to `AllowOutputCasting()` "
                    "or `AllowSideOutputsCasting()` options"))));

  // Correct case on stored frame.
  {
    ASSERT_OK_AND_ASSIGN(
        auto executor,
        (ModelExecutor<TestInputs, int64_t>::Compile(x_plus_y, *input_loader)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}), IsOkAndHolds(12));
  }
  // Correct case on heap.
  {
    ASSERT_OK_AND_ASSIGN(
        auto executor,
        (ModelExecutor<TestInputs, int64_t>::Compile(x_plus_y, *input_loader)));
    EXPECT_THAT(executor.ExecuteOnHeap({}, TestInputs{5, 7}), IsOkAndHolds(12));
  }
  // Correct case on stack.
  {
    ASSERT_OK_AND_ASSIGN(
        auto executor,
        (ModelExecutor<TestInputs, int64_t>::Compile(x_plus_y, *input_loader)));
    EXPECT_FALSE(executor.CanExecuteOnStack(8));
    EXPECT_TRUE(executor.CanExecuteOnStack(24));
    EXPECT_THAT(executor.ExecuteOnStack<24>({}, TestInputs{5, 7}),
                IsOkAndHolds(12));
  }
  // Correct case with CompileModelExecutor function.
  {
    ASSERT_OK_AND_ASSIGN(auto executor, (CompileModelExecutor<int64_t>(
                                            x_plus_y, *input_loader)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}), IsOkAndHolds(12));
  }
  // Correct case with TypedValue output.
  {
    ASSERT_OK_AND_ASSIGN(auto executor,
                         (ModelExecutor<TestInputs, TypedValue>::Compile(
                             x_plus_y, *input_loader)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}),
                IsOkAndHolds(TypedValueWith<int64_t>(12)));
  }
  // Correct case with result type casting.
  {
    ModelExecutorOptions options;
    options.allow_output_casting = true;
    ASSERT_OK_AND_ASSIGN(
        auto executor,
        (ModelExecutor<TestInputs, OptionalValue<int64_t>>::Compile(
            x_plus_y, *input_loader, nullptr, options)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}),
                IsOkAndHolds(OptionalValue<int64_t>(12)));
  }
  // Result type casting not allowed.
  {
    ModelExecutorOptions options;
    EXPECT_THAT(
        (ModelExecutor<TestInputs, OptionalValue<int64_t>>::Compile(
            x_plus_y, *input_loader, nullptr, options)),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("output casting is not allowed: INT64 -> OPTIONAL_INT64; "
                      "to fix add explicit `AllowOutputCasting()` in model "
                      "compiler")));
  }
}

TEST_F(ModelExecutorTest, ReturnsStdOptional) {
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputLoader());
  {
    ASSERT_OK_AND_ASSIGN(auto optional_x,
                         CallOp("core.to_optional", {Leaf("x")}));
    ASSERT_OK_AND_ASSIGN(
        (ModelExecutor<TestInputs, std::optional<int64_t>> executor),
        CompileModelExecutor<std::optional<int64_t>>(optional_x,
                                                     *input_loader));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}), IsOkAndHolds(Eq(5)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto empty_like_x,
                         CallOp("core.empty_like", {Leaf("x")}));
    ASSERT_OK_AND_ASSIGN(
        (ModelExecutor<TestInputs, std::optional<int64_t>> executor),
        CompileModelExecutor<std::optional<int64_t>>(empty_like_x,
                                                     *input_loader));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}),
                IsOkAndHolds(Eq(std::nullopt)));
  }
}

TEST_F(ModelExecutorTest, ReturnsStdVectorOfOptional) {
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      CreateAccessorsInputLoader<TestInputs>(
          "x",
          [](const TestInputs& in) {
            return CreateDenseArray<int64_t>({0, in.x, std::nullopt});
          },  //
          "y",
          [](const TestInputs& in) {
            return CreateDenseArray<int64_t>({0, in.y, std::nullopt});
          }));
  ASSERT_OK_AND_ASSIGN(auto x_mul_y,
                       CallOp("math.multiply", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(
      (ModelExecutor<TestInputs, std::vector<std::optional<int64_t>>> executor),
      CompileModelExecutor<std::vector<std::optional<int64_t>>>(x_mul_y,
                                                                *input_loader));
  EXPECT_THAT(executor.Execute(TestInputs{3, 19}),
              IsOkAndHolds(ElementsAre(0, 57, std::nullopt)));
}

TEST_F(ModelExecutorTest, ReturnsStdVector) {
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      CreateAccessorsInputLoader<TestInputs>("x", [](const TestInputs& in) {
        return CreateDenseArray<int64_t>({in.x, in.y, in.optional_z});
      }));
  ASSERT_OK_AND_ASSIGN(auto x_mul_x,
                       CallOp("math.multiply", {Leaf("x"), Leaf("x")}));

  EXPECT_THAT(
      (CompileModelExecutor<std::vector<int64_t>>(x_mul_x, *input_loader)),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("non-optional std::vector model output is supported "
                         "only with ForceNonOptionalOutput() setting")));

  ModelExecutorOptions options;
  options.force_non_optional_output = true;
  ASSERT_OK_AND_ASSIGN(
      (ModelExecutor<TestInputs, std::vector<int64_t>> executor),
      CompileModelExecutor<std::vector<int64_t>>(x_mul_x, *input_loader,
                                                 options));
  EXPECT_THAT(executor.Execute(TestInputs{1, 0, std::nullopt}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "non-full model output (element 2 is missing) while "
                       "full std::vector output is requested"));
  EXPECT_THAT(executor.Execute(TestInputs{1, 0, 16}),
              IsOkAndHolds(ElementsAre(1, 0, 256)));
}

class MockOperatorDirectory : public OperatorDirectory {
 public:
  MockOperatorDirectory() {
    ON_CALL(*this, DoLookupOperator)
        .WillByDefault([](absl::string_view name,
                          absl::Span<const QTypePtr> input_types,
                          QTypePtr output_type) {
          return OperatorRegistry::GetInstance()->LookupOperator(
              name, input_types, output_type);
        });
  }
  MOCK_METHOD(absl::StatusOr<OperatorPtr>, DoLookupOperator,
              (absl::string_view name, absl::Span<const QTypePtr> input_types,
               QTypePtr output_type),
              (const, override));
};

TEST_F(ModelExecutorTest, OptionsPropagatedToCasting) {
  ASSERT_OK_AND_ASSIGN(auto x_plus_y,
                       CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputLoader());

  MockOperatorDirectory operator_directory;

  ModelExecutorOptions options;
  options.allow_output_casting = true;
  options.eval_options.operator_directory = &operator_directory;

  EXPECT_CALL(operator_directory, DoLookupOperator(_, _, _)).Times(AnyNumber());
  // core.to_optional operator is inserted because we require the output be
  // of type OptionalValue<int64_t>. The fact that we register the call means
  // that the operator_directory got propagated to casting.
  EXPECT_CALL(operator_directory,
              DoLookupOperator("core.to_optional._scalar", _, _))
      .Times(AtLeast(1));

  ASSERT_OK_AND_ASSIGN(
      auto executor,
      (ModelExecutor<TestInputs, OptionalValue<int64_t>>::Compile(
          x_plus_y, *input_loader, nullptr, options)));
  EXPECT_THAT(executor.Execute(TestInputs{5, 7}),
              IsOkAndHolds(OptionalValue<int64_t>(12)));
}

TEST_F(ModelExecutorTest, ExternalBufferFactory) {
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("array.as_dense_array",
                        {CallOp("core.make_tuple", {Leaf("x"), Leaf("y")})}));
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (ModelExecutor<TestInputs, DenseArray<int64_t>>::Compile(
                           expr, *input_loader)));

  UnsafeArenaBufferFactory arena(64 << 10);
  auto [buf1, data1] = arena.CreateRawBuffer(8);
  auto [buf2, data2] = arena.CreateRawBuffer(8);
  ASSERT_OK_AND_ASSIGN(
      DenseArray<int64_t> res,
      executor.Execute({.buffer_factory = &arena}, TestInputs{5, 7}));
  auto [buf3, data3] = arena.CreateRawBuffer(8);

  // Check that `arena` was used in `executor.Execute`.
  EXPECT_NE(reinterpret_cast<char*>(data2) - reinterpret_cast<char*>(data1),
            reinterpret_cast<char*>(data3) - reinterpret_cast<char*>(data2));

  // Check that the result array was copied out of the arena and owns its memory
  EXPECT_TRUE(res.is_owned());
}

TEST_F(ModelExecutorTest, ReturnsNonOptional) {
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      CreateAccessorsInputLoader<TestInputs>(
          "y",
          [](const TestInputs& in) { return OptionalValue<int64_t>(in.y); },  //
          "z",
          [](const TestInputs& in) {
            return OptionalValue<int64_t>(in.optional_z);
          }));
  ASSERT_OK_AND_ASSIGN(auto y_mul_z,
                       CallOp("math.multiply", {Leaf("y"), Leaf("z")}));

  EXPECT_THAT((CompileModelExecutor<int64_t>(y_mul_z, *input_loader)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("model output is deduced to optional, while "
                                 "non-optional is requested")));

  ModelExecutorOptions options;
  options.force_non_optional_output = true;
  ASSERT_OK_AND_ASSIGN(
      (ModelExecutor<TestInputs, int64_t> executor),
      CompileModelExecutor<int64_t>(y_mul_z, *input_loader, options));
  EXPECT_THAT(executor.Execute(TestInputs{1, 0, std::nullopt}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "expects a present value, got missing"));
  EXPECT_THAT(executor.Execute(TestInputs{1, 2, 3}), IsOkAndHolds(6));

  EXPECT_THAT(
      (CompileModelExecutor<TypedValue>(y_mul_z, *input_loader, options)),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("ForceNonOptionalOutput() is not supported for "
                         "TypedValue outputs")));
}

TEST_F(ModelExecutorTest, ReturnsStdVectorBytes) {
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      CreateAccessorsInputLoader<TestInputs>(
          "x",
          [](const TestInputs& in) {
            return CreateDenseArray<Bytes>(
                {Bytes{"foo"}, Bytes{absl::StrCat(in.x)}, std::nullopt});
          },  //
          "y",
          [](const TestInputs& in) {
            return CreateDenseArray<Bytes>(
                {Bytes{"bar"}, Bytes{absl::StrCat(in.y)}, std::nullopt});
          }));
  ASSERT_OK_AND_ASSIGN(auto x_plus_y,
                       CallOp("strings.join", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(
      (ModelExecutor<TestInputs, std::vector<std::optional<Bytes>>> executor),
      CompileModelExecutor<std::vector<std::optional<Bytes>>>(x_plus_y,
                                                              *input_loader));
  EXPECT_THAT(
      executor.Execute(TestInputs{5, 7}),
      IsOkAndHolds(ElementsAre(Bytes{"foobar"}, Bytes{"57"}, std::nullopt)));

  // Test that initialization and destruction is correct for heap and stack.
  EXPECT_THAT(
      executor.ExecuteOnHeap({}, TestInputs{5, 7}),
      IsOkAndHolds(ElementsAre(Bytes{"foobar"}, Bytes{"57"}, std::nullopt)));
  EXPECT_TRUE(executor.CanExecuteOnStack(1024));
  EXPECT_THAT(
      executor.ExecuteOnStack<1024>({}, TestInputs{5, 7}),
      IsOkAndHolds(ElementsAre(Bytes{"foobar"}, Bytes{"57"}, std::nullopt)));
}

TEST_F(ModelExecutorTest, SimpleExprBind) {
  ASSERT_OK_AND_ASSIGN(auto x_plus_y,
                       CallOp("math.add", {Leaf("x"), Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputLoader());

  ASSERT_OK_AND_ASSIGN(
      auto output_types,
      GetInputLoaderQTypes(*input_loader, GetLeafKeys(x_plus_y)));
  ASSERT_OK_AND_ASSIGN(auto compiled_expr, CompileForDynamicEvaluation(
                                               DynamicEvaluationEngineOptions(),
                                               x_plus_y, output_types));
  {
    ASSERT_OK_AND_ASSIGN(auto executor,
                         (ModelExecutor<TestInputs, int64_t>::Bind(
                             *compiled_expr, *input_loader)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}), IsOkAndHolds(12));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto executor, BindModelExecutor<int64_t>(
                                            *compiled_expr, *input_loader));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}), IsOkAndHolds(12));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto executor, BindModelExecutor<int64_t>(
                                            *compiled_expr, *input_loader));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}), IsOkAndHolds(12));
  }
  {  // casting output to OptionalValue
    ModelExecutorOptions options;
    options.allow_output_casting = true;
    ASSERT_OK_AND_ASSIGN(auto executor,
                         BindModelExecutor<OptionalValue<int64_t>>(
                             *compiled_expr, *input_loader, options));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}), IsOkAndHolds(12));
  }
}

struct SideOutput {
  OptionalValue<int64_t> out_x;
  OptionalValue<int64_t> out_xpy;
};

template <class OutXT, class OutYT>
struct TestSlotListener : public SlotListener<SideOutput> {
  TestSlotListener() = default;
  explicit TestSlotListener(
      absl::flat_hash_map<std::string, QTypePtr> input_types)
      : input_types(std::move(input_types)) {}

  absl::Nullable<const QType*> GetQTypeOf(
      absl::string_view name, const QType* desired_qtype) const final {
    auto it = input_types.find(name);
    if (it == input_types.end()) {
      return nullptr;
    }
    return it->second == GetUnspecifiedQType() ? desired_qtype : it->second;
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    std::vector<std::string> names;
    names.reserve(input_types.size());
    for (const auto& [name, _] : input_types) {
      names.emplace_back(name);
    }
    std::sort(names.begin(), names.end());
    return names;
  }

  absl::StatusOr<BoundSlotListener<SideOutput>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots)
      const final {
    return [input_slots](::arolla::ConstFramePtr frame,
                         SideOutput* output) -> absl::Status {
      if (input_slots.contains("out_x")) {
        ASSIGN_OR_RETURN(auto slot, input_slots.at("out_x").ToSlot<OutXT>());
        output->out_x = frame.Get(slot);
      }
      if (input_slots.contains("out_xpy")) {
        ASSIGN_OR_RETURN(auto slot, input_slots.at("out_xpy").ToSlot<OutYT>());
        output->out_xpy = frame.Get(slot);
      }
      return absl::OkStatus();
    };
  }

  absl::flat_hash_map<std::string, QTypePtr> input_types = {
      {"out_x", GetQType<OutXT>()}, {"out_xpy", GetQType<OutYT>()}};
};

TEST_F(ModelExecutorTest, SimpleExprWithSlotListener) {
  ASSERT_OK_AND_ASSIGN(auto x, WithExportAnnotation(Leaf("x"), "out_x"));
  auto y = Leaf("y");
  ASSERT_OK_AND_ASSIGN(
      auto x_plus_y,
      WithExportAnnotation(CallOp("math.add", {x, y}), "out_xpy"));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x_plus_y, y}));

  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputLoader());

  TestSlotListener<int64_t, int64_t> slot_listener;

  // Listener is not provided at construction.
  {
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(
        auto executor, (ModelExecutor<TestInputs, int64_t, SideOutput>::Compile(
                           expr, *input_loader)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("slot listener was not provided")));
  }

  // Incorrect type.
  {
    TestSlotListener<int64_t, int64_t> wrong_slot_listener{
        {{"out_x", GetQType<int64_t>()},
         {"out_xpy", GetQType<::arolla::Bytes>()}}};
    EXPECT_THAT(
        CompileModelExecutor<int64_t>(expr, *input_loader, wrong_slot_listener),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("casting from INT64 to BYTES is not allowed")));
  }

  // Correct case on stored frame.
  {
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(
        auto executor, (ModelExecutor<TestInputs, int64_t, SideOutput>::Compile(
                           expr, *input_loader, &slot_listener)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x.value, 5);
    EXPECT_EQ(side_output.out_xpy.value, 12);
    // No crashes with nullptr
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, nullptr), IsOkAndHolds(19));
  }
  // Correct case on heap.
  {
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(
        auto executor, (ModelExecutor<TestInputs, int64_t, SideOutput>::Compile(
                           expr, *input_loader, &slot_listener)));
    EXPECT_THAT(executor.ExecuteOnHeap({}, TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x.value, 5);
    EXPECT_EQ(side_output.out_xpy.value, 12);
    // No crashes with nullptr
    EXPECT_THAT(executor.ExecuteOnHeap({}, TestInputs{5, 7}, nullptr),
                IsOkAndHolds(19));
  }
  // Correct case on stack.
  {
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(
        auto executor, (ModelExecutor<TestInputs, int64_t, SideOutput>::Compile(
                           expr, *input_loader, &slot_listener)));
    EXPECT_FALSE(executor.CanExecuteOnStack(8));
    EXPECT_TRUE(executor.CanExecuteOnStack(64));
    EXPECT_THAT(executor.ExecuteOnStack<64>({}, TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x.value, 5);
    EXPECT_EQ(side_output.out_xpy.value, 12);
    // No crashes with nullptr
    EXPECT_THAT(executor.ExecuteOnStack<64>({}, TestInputs{5, 7}, nullptr),
                IsOkAndHolds(19));
  }
  // Correct case with CompileModelExecutor function.
  {
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(
        auto executor,
        (CompileModelExecutor<int64_t>(expr, *input_loader, slot_listener)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x.value, 5);
    EXPECT_EQ(side_output.out_xpy.value, 12);
  }
  // Correct case with casting to optional.
  {
    TestSlotListener<OptionalValue<int64_t>, OptionalValue<int64_t>>
        optional_slot_listener;
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(
        auto executor, (CompileModelExecutor<int64_t>(expr, *input_loader,
                                                      optional_slot_listener)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x.value, 5);
    EXPECT_EQ(side_output.out_xpy.value, 12);
  }
  // Correct case with unspecified type.
  {
    TestSlotListener<int64_t, int64_t> slot_listener{
        {{"out_x", GetQType<int64_t>()}, {"out_xpy", GetUnspecifiedQType()}}};
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(
        auto executor,
        (CompileModelExecutor<int64_t>(expr, *input_loader, slot_listener)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x.value, 5);
    EXPECT_EQ(side_output.out_xpy.value, 12);
  }
  // Correct case with casting from int32_t to optional<int64_t>.
  {
    TestSlotListener<OptionalValue<int64_t>, OptionalValue<int64_t>>
        optional_slot_listener;
    ASSERT_OK_AND_ASSIGN(auto int32_loader, CreateTestInt32InputLoader());
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(auto executor,
                         (CompileModelExecutor<int>(expr, *int32_loader,
                                                    optional_slot_listener)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x.value, 5);
    EXPECT_EQ(side_output.out_xpy.value, 12);
  }

  // Limiting outputs.
  {
    TestSlotListener<int64_t, int64_t> limited_slot_listener{
        {{"out_xpy", GetQType<int64_t>()}}};
    SideOutput side_output;
    ModelExecutorOptions options;
    options.ignore_not_listened_named_outputs = true;
    ASSERT_OK_AND_ASSIGN(auto executor, (CompileModelExecutor<int64_t>(
                                            expr, *input_loader,
                                            limited_slot_listener, options)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x, std::nullopt);  // not populated
    EXPECT_EQ(side_output.out_xpy.value, 12);
  }
}

TEST_F(ModelExecutorTest, SimpleExprBindWithSlotListener) {
  ASSERT_OK_AND_ASSIGN(auto x, WithExportAnnotation(Leaf("x"), "out_x"));
  auto y = Leaf("y");
  ASSERT_OK_AND_ASSIGN(
      auto x_plus_y,
      WithExportAnnotation(CallOp("math.add", {x, y}), "out_xpy"));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("math.add", {x_plus_y, y}));

  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputLoader());
  TestSlotListener<int64_t, int64_t> slot_listener;

  // Correct case with Bind operation.
  ASSERT_OK_AND_ASSIGN((auto [stripped_expr, side_outputs]),
                       ExtractSideOutputs(expr));
  ASSERT_OK_AND_ASSIGN(
      auto output_types,
      GetInputLoaderQTypes(*input_loader, GetLeafKeys(x_plus_y)));
  ASSERT_OK_AND_ASSIGN(auto compiled_expr, CompileForDynamicEvaluation(
                                               DynamicEvaluationEngineOptions(),
                                               stripped_expr, output_types,
                                               /*side_outputs=*/{}));
  ASSERT_OK_AND_ASSIGN(
      auto compiled_expr_with_side_output,
      CompileForDynamicEvaluation(DynamicEvaluationEngineOptions(),
                                  stripped_expr, output_types, side_outputs));
  {
    // Only expr without side output.
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(
        auto executor,
        (ModelExecutor<TestInputs, int64_t, SideOutput>::Bind(
            *compiled_expr, *input_loader,
            /*compiled_expr_with_side_output=*/nullptr, &slot_listener)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_FALSE(side_output.out_x.present);
    EXPECT_FALSE(side_output.out_xpy.present);
  }
  {
    // Only expr with side output.
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(
        auto executor,
        (ModelExecutor<TestInputs, int64_t, SideOutput>::Bind(
            /*compiled_expr=*/*compiled_expr_with_side_output, *input_loader,
            /*compiled_expr_with_side_output=*/nullptr, &slot_listener)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x.value, 5);
    EXPECT_EQ(side_output.out_xpy.value, 12);
  }
  {
    // Boths exprs with and without side output.
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(
        auto executor,
        (ModelExecutor<TestInputs, int64_t, SideOutput>::Bind(
            *compiled_expr, *input_loader, compiled_expr_with_side_output.get(),
            &slot_listener)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x.value, 5);
    EXPECT_EQ(side_output.out_xpy.value, 12);
  }
  {
    SideOutput side_output;
    ASSERT_OK_AND_ASSIGN(auto executor, BindModelExecutor<int64_t>(
                                            *compiled_expr_with_side_output,
                                            *input_loader, slot_listener));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x, int64_t{5});
    EXPECT_EQ(side_output.out_xpy, int64_t{12});
  }

  // Casting slot listener from int64_t to optional<int64_t>.
  {
    TestSlotListener<OptionalValue<int64_t>, OptionalValue<int64_t>>
        optional_slot_listener;
    SideOutput side_output;
    ModelExecutorOptions options;
    options.allow_side_outputs_casting = true;
    ASSERT_OK_AND_ASSIGN(auto executor,
                         (BindModelExecutor<int64_t>(
                             *compiled_expr_with_side_output, *input_loader,
                             optional_slot_listener, options)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x, int64_t{5});
    EXPECT_EQ(side_output.out_xpy, int64_t{12});
  }
  // Not listened side outputs validation.
  {
    TestSlotListener<OptionalValue<int64_t>, OptionalValue<int64_t>>
        irrelevant_slot_listener(
            /*input_types=*/{{"foo", GetOptionalQType<int64_t>()},
                             {"bar", GetOptionalQType<int64_t>()}});
    ModelExecutorOptions options;
    options.ignore_not_listened_named_outputs = false;
    EXPECT_THAT(
        (ModelExecutor<TestInputs, int64_t, SideOutput>::Bind(
            /*compiled_expr=*/*compiled_expr_with_side_output, *input_loader,
            /*compiled_expr_with_side_output=*/nullptr,
            &irrelevant_slot_listener, options)),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr(
                     "slot listener does not listen for named outputs {out_x, "
                     "out_xpy} (it listens to {bar, foo});")));
    EXPECT_THAT(
        (ModelExecutor<TestInputs, int64_t, SideOutput>::Bind(
            /*compiled_expr=*/*compiled_expr, *input_loader,
            /*compiled_expr_with_side_output=*/
            compiled_expr_with_side_output.get(), &irrelevant_slot_listener,
            options)),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr(
                     "slot listener does not listen for named outputs {out_x, "
                     "out_xpy} (it listens to {bar, foo});")));
    options.ignore_not_listened_named_outputs = true;
    EXPECT_THAT(
        (ModelExecutor<TestInputs, int64_t, SideOutput>::Bind(
            /*compiled_expr=*/*compiled_expr_with_side_output, *input_loader,
            /*compiled_expr_with_side_output=*/
            nullptr, &irrelevant_slot_listener, options)),
        IsOk());
    EXPECT_THAT((ModelExecutor<TestInputs, int64_t, SideOutput>::Bind(
                    /*compiled_expr=*/*compiled_expr, *input_loader,
                    /*compiled_expr_with_side_output=*/
                    compiled_expr_with_side_output.get(),
                    &irrelevant_slot_listener, options)),
                IsOk());
  }
  // Side output casting is not allowed.
  {
    TestSlotListener<OptionalValue<int64_t>, OptionalValue<int64_t>>
        optional_slot_listener;
    EXPECT_THAT(
        (BindModelExecutor<int64_t>(*compiled_expr_with_side_output,
                                    *input_loader, optional_slot_listener)),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr(
                "side outputs casting is not allowed: out_x, out_xpy; to fix "
                "add explicit `AllowSideOutputsCasting()` in model compiler")));
  }
  // Casting slot listener and output from int64_t to optional<int64_t>.
  {
    TestSlotListener<OptionalValue<int64_t>, OptionalValue<int64_t>>
        optional_slot_listener;
    SideOutput side_output;
    ModelExecutorOptions options;
    options.allow_output_casting = true;
    options.allow_side_outputs_casting = true;
    ASSERT_OK_AND_ASSIGN(auto executor,
                         (BindModelExecutor<OptionalValue<int64_t>>(
                             *compiled_expr_with_side_output, *input_loader,
                             optional_slot_listener, options)));
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}, &side_output),
                IsOkAndHolds(19));
    EXPECT_EQ(side_output.out_x, int64_t{5});
    EXPECT_EQ(side_output.out_xpy, int64_t{12});
  }
}

// Test operator to validate passed RawBufferFactory.
struct CreateTestDenseArrayOp {
  DenseArray<int> operator()(EvaluationContext* ctx, int64_t) const {
    // Argument int64_t is needed to prevent evaluating on
    // compilation stage (in this case UnsafeArenaBufferFactory is not used).
    CHECK(dynamic_cast<UnsafeArenaBufferFactory*>(&ctx->buffer_factory()));
    auto res = CreateConstDenseArray<int>(3, 1, &ctx->buffer_factory());
    CHECK(!res.is_owned());
    return res;
  }
};

TEST_F(ModelExecutorTest, ArenaMakeOwned) {
  constexpr absl::string_view op_name = "test.create_test_dense_array";
  ASSERT_OK_AND_ASSIGN(
      auto op_factory,
      (OperatorFactory()
           .WithName(std::string(op_name))
           .BuildFromFunctor<CreateTestDenseArrayOp, int64_t>()));
  ASSERT_OK(
      ::arolla::OperatorRegistry::GetInstance()->RegisterOperator(op_factory));
  auto ReturnsDenseArray = [](absl::Span<const QTypePtr>)
      -> absl::StatusOr<expr_operators::type_meta::QTypes> {
    return expr_operators::type_meta::QTypes{GetDenseArrayQType<int>()};
  };
  ASSERT_OK(expr_operators::RegisterBackendOperator(
      op_name, ExprOperatorSignature::MakeArgsN(1), ReturnsDenseArray));

  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op_name, {Leaf("x")}));

  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputLoader());
  ModelExecutorOptions options;
  options.arena_page_size = 64 << 10;
  ASSERT_OK_AND_ASSIGN(auto executor, (CompileModelExecutor<DenseArray<int>>(
                                          expr, *input_loader, options)));

  ASSERT_OK_AND_ASSIGN(DenseArray<int> res, executor.Execute(TestInputs{5, 7}));
  EXPECT_EQ(res.size(), 3);
  EXPECT_TRUE(res.is_owned());
}

static RawBufferFactory* kLastOpUsedFactory = nullptr;
static void* kLastOpAllocatedBuffer = nullptr;

// Test operator to validate passed RawBufferFactory.
struct FactorySideEffectOp {
  template <class T>
  T operator()(EvaluationContext* ctx, T a) const {
    kLastOpUsedFactory = &ctx->buffer_factory();
    kLastOpAllocatedBuffer =
        std::get<1>(ctx->buffer_factory().CreateRawBuffer(1024));
    return a;
  }
};

static RawBufferFactory* kLastLoaderUsedFactory = nullptr;
static void* kLastLoaderAllocatedBuffer = nullptr;

absl::StatusOr<InputLoaderPtr<TestInputs>>
CreateArenaSideEffectTestInputLoader() {
  return CreateAccessorsInputLoader<TestInputs>(
      "x", [](const TestInputs& in, RawBufferFactory* factory) {
        kLastLoaderUsedFactory = factory;
        kLastLoaderAllocatedBuffer = std::get<1>(factory->CreateRawBuffer(128));
        return in.x;
      });
}

TEST_F(ModelExecutorTest, ArenaPropagated) {
  using ::arolla::expr_operators::type_meta::Nth;
  constexpr absl::string_view op_name = "test.factory_side_effect";
  ASSERT_OK_AND_ASSIGN(auto factory_side_effect,
                       (OperatorFactory()
                            .WithName(std::string(op_name))
                            .BuildFromFunctor<FactorySideEffectOp, int64_t>()));
  // register qexpr operator
  ASSERT_OK(::arolla::OperatorRegistry::GetInstance()->RegisterOperator(
      factory_side_effect));
  // register expr operator
  ASSERT_OK_AND_ASSIGN(auto x_sig, ExprOperatorSignature::Make("x"));
  ASSERT_OK(expr_operators::RegisterBackendOperator(op_name, x_sig, Nth(0)));

  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op_name, {Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       CreateArenaSideEffectTestInputLoader());
  ModelExecutorOptions options;
  options.arena_page_size = 64 << 10;
  ASSERT_OK_AND_ASSIGN(auto executor, CompileModelExecutor<int64_t>(
                                          expr, *input_loader, options));
  EXPECT_EQ(kLastOpUsedFactory, nullptr);
  EXPECT_EQ(kLastOpAllocatedBuffer, nullptr);
  EXPECT_EQ(kLastLoaderUsedFactory, nullptr);
  EXPECT_EQ(kLastLoaderAllocatedBuffer, nullptr);
  EXPECT_THAT(executor.Execute(TestInputs{5, 7}), IsOkAndHolds(5));
  EXPECT_NE(kLastOpUsedFactory, nullptr);
  EXPECT_NE(kLastOpAllocatedBuffer, nullptr);
  EXPECT_NE(kLastLoaderUsedFactory, nullptr);
  EXPECT_NE(kLastLoaderAllocatedBuffer, nullptr);
  EXPECT_NE(kLastOpUsedFactory, GetHeapBufferFactory());
  EXPECT_NE(kLastLoaderUsedFactory, GetHeapBufferFactory());
  EXPECT_EQ(std::string(typeid(*kLastLoaderUsedFactory).name()),
            std::string(typeid(UnsafeArenaBufferFactory).name()));
  EXPECT_EQ(std::string(typeid(*kLastOpUsedFactory).name()),
            std::string(typeid(UnsafeArenaBufferFactory).name()));

  // test that memory is cleaned and reused
  RawBufferFactory* prev_used_op_factory = kLastOpUsedFactory;
  void* prev_allocated_op_buffer = kLastOpAllocatedBuffer;
  RawBufferFactory* prev_used_loader_factory = kLastLoaderUsedFactory;
  void* prev_allocated_loader_buffer = kLastLoaderAllocatedBuffer;
  {
    EXPECT_THAT(executor.Execute(TestInputs{5, 7}), IsOkAndHolds(5));
    EXPECT_EQ(kLastOpUsedFactory, prev_used_op_factory);
    EXPECT_EQ(kLastOpAllocatedBuffer, prev_allocated_op_buffer);
    EXPECT_EQ(kLastLoaderUsedFactory, prev_used_loader_factory);
    EXPECT_EQ(kLastLoaderAllocatedBuffer, prev_allocated_loader_buffer);
  }
}

}  // namespace
}  // namespace arolla::expr
