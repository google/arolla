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
#include "arolla/serving/inplace_expr_compiler.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/struct_field.h"
#include "arolla/util/testing/status_matchers_backport.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace {

using ::arolla::testing::IsOkAndHolds;
using ::arolla::testing::StatusIs;
using ::testing::HasSubstr;
using ::testing::MatchesRegex;

struct TestOutputStruct {
  double x_plus_y;
  double x_times_y;
  double unused;

  static auto ArollaStructFields() {
    using CppType = TestOutputStruct;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(x_plus_y),
        AROLLA_DECLARE_STRUCT_FIELD(x_times_y),
        AROLLA_DECLARE_STRUCT_FIELD(unused),
    };
  }
  void ArollaFingerprint(FingerprintHasher* hasher) const {
    CombineStructFields(hasher, *this);
  }
};

struct TestStruct {
  float x;
  double y;
  TestOutputStruct side_outputs;

  static auto ArollaStructFields() {
    using CppType = TestStruct;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(x),
        AROLLA_DECLARE_STRUCT_FIELD(y),
        AROLLA_DECLARE_STRUCT_FIELD(side_outputs),
    };
  }
  void ArollaFingerprint(FingerprintHasher* hasher) const {
    CombineStructFields(hasher, *this);
  }
};

struct TestStructWithOptional {
  OptionalValue<float> x;
  OptionalValue<double> y;
  OptionalValue<double> x_plus_y;

  constexpr static auto ArollaStructFields() {
    using CppType = TestStructWithOptional;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(x),
        AROLLA_DECLARE_STRUCT_FIELD(y),
        AROLLA_DECLARE_STRUCT_FIELD(x_plus_y),
    };
  }
  void ArollaFingerprint(FingerprintHasher* hasher) const {
    CombineStructFields(hasher, *this);
  }
};

struct TestStructWithString {
  OptionalValue<::arolla::Bytes> name;

  static auto ArollaStructFields() {
    using CppType = TestStructWithString;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(name),
    };
  }
  void ArollaFingerprint(FingerprintHasher* hasher) const {
    CombineStructFields(hasher, *this);
  }
};

}  // namespace

AROLLA_DECLARE_SIMPLE_QTYPE(TEST_OUTPUT_STRUCT, TestOutputStruct);
AROLLA_DEFINE_SIMPLE_QTYPE(TEST_OUTPUT_STRUCT, TestOutputStruct);

AROLLA_DECLARE_SIMPLE_QTYPE(TEST_STRUCT, TestStruct);
AROLLA_DEFINE_SIMPLE_QTYPE(TEST_STRUCT, TestStruct);

AROLLA_DECLARE_SIMPLE_QTYPE(TEST_STRUCT_WITH_OPTIONAL, TestStructWithOptional);
AROLLA_DEFINE_SIMPLE_QTYPE(TEST_STRUCT_WITH_OPTIONAL, TestStructWithOptional);

AROLLA_DECLARE_SIMPLE_QTYPE(TEST_STRUCT_WITH_STRING, TestStructWithString);
AROLLA_DEFINE_SIMPLE_QTYPE(TEST_STRUCT_WITH_STRING, TestStructWithString);

namespace {

class FailingCompiledExpr : public InplaceCompiledExpr {
 public:
  using InplaceCompiledExpr::InplaceCompiledExpr;
  absl::StatusOr<std::unique_ptr<BoundExpr>> InplaceBind(
      const absl::flat_hash_map<std::string, TypedSlot>& slots,
      TypedSlot output_slot,
      const absl::flat_hash_map<std::string, TypedSlot>& /*named_output_slots*/)
      const final {
    return absl::InternalError("Fake:(");
  }
};

TEST(CompileInplaceExprOnStructInput, NoFieldNames) {
  FailingCompiledExpr compiled_expr({}, GetQType<double>(), {});
  EXPECT_THAT(
      CompileInplaceExprOnStructInput<int32_t>(compiled_expr, "/final_output"),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex(".*registered field.*INT32.*")));
}

TEST(CompileInplaceExprOnStructInput, NoFinalOutputName) {
  FailingCompiledExpr compiled_expr({}, GetQType<double>(), {});
  EXPECT_THAT(CompileInplaceExprOnStructInput<TestStruct>(compiled_expr,
                                                          "/final_output"),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*input.*/final_output.*TEST_STRUCT.*")));
}

TEST(CompileInplaceExprOnStructInput, InputTypeMismatch) {
  FailingCompiledExpr compiled_expr({}, GetQType<double>(), {});
  EXPECT_THAT(
      CompileInplaceExprOnStructInput<TestStruct>(compiled_expr, "/x"),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex(
                   ".*/x.*TEST_STRUCT.*expected.*FLOAT32.*found.*FLOAT64")));
}

TEST(CompileInplaceExprOnStructInput, InputTypeUnknown) {
  FailingCompiledExpr compiled_expr({}, GetQType<double>(), {});
  EXPECT_THAT(CompileInplaceExprOnStructInput<TestStruct>(compiled_expr, "/qq"),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*input.*/qq.*TEST_STRUCT.*")));
}

TEST(CompileInplaceExprOnStructInput, FinalOutputTypeMismatch) {
  FailingCompiledExpr compiled_expr({{"/x", GetQType<double>()}},
                                    GetQType<double>(), {});
  EXPECT_THAT(
      CompileInplaceExprOnStructInput<TestStruct>(compiled_expr,
                                                  "/side_outputs/x_plus_y"),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex(
                   ".*/x.*TEST_STRUCT.*expected.*FLOAT32.*found.*FLOAT64")));
}

TEST(CompileInplaceExprOnStructInput, SideOutputTypeMismatch) {
  FailingCompiledExpr compiled_expr(
      {{"/x", GetQType<float>()}}, GetQType<double>(),
      {{"/side_outputs/x_times_y", GetQType<float>()}});
  EXPECT_THAT(
      CompileInplaceExprOnStructInput<TestStruct>(compiled_expr,
                                                  "/side_outputs/x_plus_y"),
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          MatchesRegex(
              ".*/side_outputs/"
              "x_times_y.*TEST_STRUCT.*expected.*FLOAT64.*found.*FLOAT32")));
}

TEST(CompileInplaceExprOnStructInput, SideOutputUnknown) {
  FailingCompiledExpr compiled_expr(
      {{"/x", GetQType<float>()}}, GetQType<double>(),
      {{"/side_outputs/x_power_y", GetQType<double>()}});
  EXPECT_THAT(
      CompileInplaceExprOnStructInput<TestStruct>(compiled_expr,
                                                  "/side_outputs/x_plus_y"),
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          MatchesRegex(".*/side_outputs/x_power_y.*not found.*TEST_STRUCT.*")));
}

TEST(CompileInplaceExprOnStructInput, CompiledExprBindingFailure) {
  FailingCompiledExpr compiled_expr({{"/x", GetQType<float>()}},
                                    GetQType<double>(), {});
  EXPECT_THAT(CompileInplaceExprOnStructInput<TestStruct>(
                  compiled_expr, "/side_outputs/x_plus_y"),
              StatusIs(absl::StatusCode::kInternal, "Fake:("));
}

TEST(CompileInplaceExprOnStructInput, InputSideOutputCollision) {
  FailingCompiledExpr compiled_expr({{"/y", GetQType<double>()}},
                                    GetQType<double>(),
                                    {{"/y", GetQType<double>()}});
  EXPECT_THAT(CompileInplaceExprOnStructInput<TestStruct>(
                  compiled_expr, "/side_outputs/x_plus_y"),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*/y.*input.*named output.*")));
}

TEST(CompileInplaceExprOnStructInput, InputFinalOutputCollision) {
  FailingCompiledExpr compiled_expr(
      {{"/y", GetQType<double>()}}, GetQType<double>(),
      {{"/side_outputs/x_plus_y", GetQType<double>()}});
  EXPECT_THAT(CompileInplaceExprOnStructInput<TestStruct>(compiled_expr, "/y"),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*/y.*input.*final output.*")));
}

TEST(CompileInplaceExprOnStructInput, SideOutputFinalOutputCollision) {
  FailingCompiledExpr compiled_expr(
      {{"/y", GetQType<double>()}}, GetQType<double>(),
      {{"/side_outputs/x_plus_y", GetQType<double>()}});
  EXPECT_THAT(
      CompileInplaceExprOnStructInput<TestStruct>(compiled_expr,
                                                  "/side_outputs/x_plus_y"),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex(
                   ".*/side_outputs/x_plus_y.*final output.*named output.*")));
}

class TestBoundExpr final : public BoundExpr {
 public:
  TestBoundExpr(FrameLayout::Slot<float> x, FrameLayout::Slot<double> y,
                FrameLayout::Slot<double> x_plus_y,
                FrameLayout::Slot<double> x_times_y)
      : BoundExpr(
            {{"/x", TypedSlot::FromSlot(x)}, {"/y", TypedSlot::FromSlot(y)}},
            TypedSlot::FromSlot(x_plus_y),
            {{"/side_outputs/x_times_y", TypedSlot::FromSlot(x_times_y)}}),
        x_(x),
        y_(y),
        x_plus_y_(x_plus_y),
        x_times_y_(x_times_y) {}

  void InitializeLiterals(EvaluationContext*, FramePtr) const final {}

  void Execute(EvaluationContext*, FramePtr frame) const final {
    frame.Set(x_plus_y_, frame.Get(x_) + frame.Get(y_));
    frame.Set(x_times_y_, frame.Get(x_) * frame.Get(y_));
  }

 private:
  FrameLayout::Slot<float> x_;
  FrameLayout::Slot<double> y_;
  FrameLayout::Slot<double> x_plus_y_;
  FrameLayout::Slot<double> x_times_y_;
};

class TestCompiledExpr : public InplaceCompiledExpr {
 public:
  TestCompiledExpr()
      : InplaceCompiledExpr(
            {{"/x", GetQType<float>()}, {"/y", GetQType<double>()}},
            GetQType<double>(),
            {{"/side_outputs/x_times_y", GetQType<double>()}}) {}
  absl::StatusOr<std::unique_ptr<BoundExpr>> InplaceBind(
      const absl::flat_hash_map<std::string, TypedSlot>& slots,
      TypedSlot output_slot,
      const absl::flat_hash_map<std::string, TypedSlot>& named_output_slots)
      const final {
    RETURN_IF_ERROR(VerifySlotTypes(input_types(), slots));
    return std::make_unique<TestBoundExpr>(
        slots.at("/x").ToSlot<float>().value(),
        slots.at("/y").ToSlot<double>().value(),
        output_slot.ToSlot<double>().value(),
        named_output_slots.at("/side_outputs/x_times_y")
            .ToSlot<double>()
            .value());
  }
};

TEST(CompileInplaceExprOnStructInputTest, SuccessXPlusY) {
  TestCompiledExpr compiled_expr;
  ASSERT_OK_AND_ASSIGN(std::function<absl::Status(TestStruct&)> eval_fn,
                       CompileInplaceExprOnStructInput<TestStruct>(
                           compiled_expr, "/side_outputs/x_plus_y"));
  TestStruct input{
      .x = 5.f,
      .y = 7.,
      .side_outputs = {.x_plus_y = -1, .x_times_y = -1, .unused = -1}};
  ASSERT_OK(eval_fn(input));
  EXPECT_EQ(input.side_outputs.x_plus_y, 12);
  EXPECT_EQ(input.side_outputs.x_times_y, 35.);
  EXPECT_EQ(input.x, 5);
  EXPECT_EQ(input.y, 7);
  EXPECT_EQ(input.side_outputs.unused, -1.);
}

class TestBoundExprWithOptionals final : public BoundExpr {
 public:
  TestBoundExprWithOptionals(FrameLayout::Slot<OptionalValue<float>> x,
                             FrameLayout::Slot<OptionalValue<double>> y,
                             FrameLayout::Slot<OptionalValue<double>> x_plus_y)
      : BoundExpr(
            {{"/x", TypedSlot::FromSlot(x)}, {"/y", TypedSlot::FromSlot(y)}},
            TypedSlot::FromSlot(x_plus_y), {}),
        x_(x),
        y_(y),
        x_plus_y_(x_plus_y) {}

  void InitializeLiterals(EvaluationContext*, FramePtr) const final {}

  void Execute(EvaluationContext*, FramePtr frame) const final {
    if (frame.Get(x_).present && frame.Get(y_).present) {
      frame.Set(x_plus_y_, frame.Get(x_).value + frame.Get(y_).value);
    } else {
      frame.Set(x_plus_y_, std::nullopt);
    }
  }

 private:
  FrameLayout::Slot<OptionalValue<float>> x_;
  FrameLayout::Slot<OptionalValue<double>> y_;
  FrameLayout::Slot<OptionalValue<double>> x_plus_y_;
};

class TestCompiledExprWithOptionals : public InplaceCompiledExpr {
 public:
  TestCompiledExprWithOptionals()
      : InplaceCompiledExpr({{"/x", GetQType<OptionalValue<float>>()},
                             {"/y", GetQType<OptionalValue<double>>()}},
                            GetQType<OptionalValue<double>>(), {}) {}
  absl::StatusOr<std::unique_ptr<BoundExpr>> InplaceBind(
      const absl::flat_hash_map<std::string, TypedSlot>& slots,
      TypedSlot output_slot,
      const absl::flat_hash_map<std::string, TypedSlot>& /*named_output_slots*/)
      const final {
    RETURN_IF_ERROR(VerifySlotTypes(input_types(), slots));
    return std::make_unique<TestBoundExprWithOptionals>(
        slots.at("/x").ToSlot<OptionalValue<float>>().value(),
        slots.at("/y").ToSlot<OptionalValue<double>>().value(),
        output_slot.ToSlot<OptionalValue<double>>().value());
  }
};

TEST(CompileInplaceExprOnStructInputTest, SuccessXPlusYWithOptionals) {
  TestCompiledExprWithOptionals compiled_expr;
  ASSERT_OK_AND_ASSIGN(
      std::function<absl::Status(TestStructWithOptional&)> eval_fn,
      CompileInplaceExprOnStructInput<TestStructWithOptional>(compiled_expr,
                                                              "/x_plus_y"));
  TestStructWithOptional input{.x = 5.f, .y = 7., .x_plus_y = -1};
  ASSERT_OK(eval_fn(input));
  EXPECT_EQ(input.x_plus_y, 12.);
  EXPECT_EQ(input.x, 5.f);
  EXPECT_EQ(input.y, 7.);
}

TEST(CompileDynamicExprOnStructInputTest, TypeError) {
  ASSERT_OK(InitArolla());
  ASSERT_OK_AND_ASSIGN(
      expr::ExprNodePtr expr,
      expr::CallOp("annotation.qtype",
                   {expr::Leaf("/x"), expr::Literal(GetQType<int>())}));
  EXPECT_THAT(CompileDynamicExprOnStructInput(
                  ExprCompiler<TestStruct, std::optional<double>>(), expr)
                  .status(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*inconsistent.*qtype.*INT32.*")));
}

TEST(CompileDynamicExprOnStructInputTest, UnknownLeaf) {
  ASSERT_OK(InitArolla());
  expr::ExprNodePtr expr = expr::Leaf("/unknown");
  EXPECT_THAT(
      CompileDynamicExprOnStructInput(
          ExprCompiler<TestStruct, std::optional<double>>(), expr)
          .status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("unknown inputs: /unknown")));
}

TEST(CompileDynamicExprOnStructInputTest, TypeErrorOnCodegenModel) {
  ASSERT_OK(InitArolla());
  TestCompiledExprWithOptionals compiled_expr;
  EXPECT_THAT(
      CompileDynamicExprOnStructInput(
          ExprCompiler<TestStruct, std::optional<double>>(), compiled_expr)
          .status(),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex(".*slot types mismatch.*")));
}

TEST(CompileDynamicExprOnStructInputTest, Nested) {
  ASSERT_OK(InitArolla());
  ASSERT_OK_AND_ASSIGN(
      expr::ExprNodePtr expr,
      expr::CallOp("math.add",
                   {expr::Leaf("/x"), expr::Leaf("/side_outputs/x_plus_y")}));
  ASSERT_OK_AND_ASSIGN(
      std::function<absl::StatusOr<double>(const TestStruct&)> eval_fn,
      CompileDynamicExprOnStructInput(ExprCompiler<TestStruct, double>(),
                                      expr));
  TestStruct input{
      .x = 5.f,
      .y = -1.,
      .side_outputs = {.x_plus_y = 7., .x_times_y = -1, .unused = -1}};
  EXPECT_THAT(eval_fn(input), IsOkAndHolds(12.));
}

TEST(CompileDynamicExprOnStructInputTest, SuccessXPlusYWithOptionals) {
  ASSERT_OK(InitArolla());
  ASSERT_OK_AND_ASSIGN(
      expr::ExprNodePtr expr,
      expr::CallOp("math.add", {expr::Leaf("/x"), expr::Leaf("/y")}));
  ASSERT_OK_AND_ASSIGN(
      std::function<absl::StatusOr<std::optional<double>>(
          const TestStructWithOptional&)>
          eval_fn,
      CompileDynamicExprOnStructInput(
          ExprCompiler<TestStructWithOptional, std::optional<double>>(), expr));
  TestStructWithOptional input{.x = 5.f, .y = 7., .x_plus_y = -1};
  EXPECT_THAT(eval_fn(input), IsOkAndHolds(12.));
  input.x = std::nullopt;
  EXPECT_THAT(eval_fn(input), IsOkAndHolds(std::nullopt));
}

TEST(CompileDynamicExprOnStructInputTest, ErrorStatus) {
  ASSERT_OK(InitArolla());
  absl::StatusOr<expr::ExprNodePtr> status_or_expr =
      absl::InternalError("input error");
  auto result = CompileDynamicExprOnStructInput(
      ExprCompiler<TestStructWithOptional, std::optional<double>>(),
      status_or_expr);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInternal,
                               MatchesRegex("input error")));
}

TEST(CompileDynamicExprOnStructInputTest, SuccessXPlusYOnCodegenModel) {
  ASSERT_OK(InitArolla());
  TestCompiledExpr compiled_expr;
  ASSERT_OK_AND_ASSIGN(
      std::function<absl::StatusOr<double>(const TestStruct&)> eval_fn,
      CompileDynamicExprOnStructInput(ExprCompiler<TestStruct, double>(),
                                      compiled_expr));
  TestStruct input{.x = 5.f, .y = 7.};
  EXPECT_THAT(eval_fn(input), IsOkAndHolds(12.));
}

TEST(CompileDynamicExprOnStructInputTest, SuccessSideOutputOnCodegenModel) {
  ASSERT_OK(InitArolla());
  TestCompiledExpr compiled_expr;
  ASSERT_OK_AND_ASSIGN(
      std::function<absl::StatusOr<double>(const TestStruct&, TestStruct*)>
          eval_fn,
      CompileDynamicExprOnStructInput(
          ExprCompiler<TestStruct, double, TestStruct>(), compiled_expr));
  TestStruct input{.x = 5.f, .y = 7.};
  EXPECT_THAT(eval_fn(input, nullptr), IsOkAndHolds(12.));
  EXPECT_THAT(eval_fn(input, &input), IsOkAndHolds(12.));
  EXPECT_EQ(input.side_outputs.x_times_y, 35);
}

TEST(CompileDynamicExprOnStructWithBytesInputTest, SuccessUpper) {
  ASSERT_OK(InitArolla());
  ASSERT_OK_AND_ASSIGN(
      expr::ExprNodePtr expr,
      expr::CallOp("strings.upper",
                   {expr::CallOp("strings.decode", {expr::Leaf("/name")})}));
  ASSERT_OK_AND_ASSIGN(
      std::function<absl::StatusOr<std::optional<Text>>(
          const TestStructWithString&)>
          eval_fn,
      CompileDynamicExprOnStructInput(
          ExprCompiler<TestStructWithString, std::optional<Text>>(), expr));
  TestStructWithString input{.name = Bytes("abc")};
  EXPECT_THAT(eval_fn(input), IsOkAndHolds(Text("ABC")));
  input.name = std::nullopt;
  EXPECT_THAT(eval_fn(input), IsOkAndHolds(std::nullopt));
}

}  // namespace
}  // namespace arolla
