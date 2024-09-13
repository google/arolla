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
#include "arolla/expr/eval/prepare_expression.h"

#include <cstdint>
#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_stack_trace.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/optimization/optimizer.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::testing::DummyOp;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::Eq;
using ::testing::HasSubstr;

class IdentityAnnotation final : public AnnotationExprOperatorTag,
                                 public ExprOperatorWithFixedSignature {
 public:
  IdentityAnnotation()
      : ExprOperatorWithFixedSignature(
            "id", ExprOperatorSignature::MakeArgsN(1), "",
            FingerprintHasher("arolla::expr::IdentityAnnotation").Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    return inputs[0];
  }
};

class OperatorWithBadGetOutputQType : public ExprOperatorWithFixedSignature {
 public:
  OperatorWithBadGetOutputQType()
      : ExprOperatorWithFixedSignature(
            "bad_op", ExprOperatorSignature::MakeArgsN(1), "",
            FingerprintHasher("arolla::expr::OperatorWithBadGetOutputQType")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    return ExprAttributes(GetQType<int64_t>());
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    return node->node_deps()[0];
  }
};

class OperatorWithNoInferAttributes final
    : public ExprOperatorWithFixedSignature {
 public:
  OperatorWithNoInferAttributes()
      : ExprOperatorWithFixedSignature(
            "no_infer_attr", ExprOperatorSignature::MakeArgsN(1), "",
            FingerprintHasher("arolla::expr::OperatorWithNoInferAttributes")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    return ExprAttributes{};
  }
};

TEST(PrepareExpressionTest, ExtractQTypeForCompilation) {
  const auto id_annotation = std::make_shared<IdentityAnnotation>();

  auto x = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto id_expr, CallOp(id_annotation, {x}));
  ASSERT_OK_AND_ASSIGN(auto expr,
                       WithQTypeAnnotation(id_expr, GetQType<float>()));

  absl::flat_hash_map<Fingerprint, QTypePtr> types;
  ASSERT_OK_AND_ASSIGN(auto stripped_expr,
                       ExtractQTypesForCompilation(expr, &types));

  EXPECT_THAT(stripped_expr, EqualsExpr(id_expr));
  EXPECT_EQ(types[x->fingerprint()], GetQType<float>());
  EXPECT_EQ(types[id_expr->fingerprint()], GetQType<float>());
}

TEST(PrepareExpressionTest, Optimizations) {
  auto pattern_op = std::make_shared<DummyOp>(
      "pattern_op", ExprOperatorSignature::MakeArgsN(2));
  auto pattern_x = Literal(2);
  // Fake optimizer that transforms `pattern_op(2, ...)` to `57`.
  Optimizer literals_optimizer = [pattern_op, pattern_x](ExprNodePtr node) {
    if (node->op() == pattern_op &&
        node->node_deps()[0]->fingerprint() == pattern_x->fingerprint()) {
      return Literal(57);
    }
    return node;
  };
  // Please note that all test cases are based on the following pattern:
  //
  //   `pattern_op(pattern_x, <leaf>)`
  //
  // The <leaf> is needed to prevent the literal-folding transformation from
  // evaluating `pattern_op`.
  const absl::flat_hash_map<std::string, QTypePtr> input_qtypes = {
      {"u", GetQType<Text>()}, {"v", GetQType<Bytes>()}};
  DynamicEvaluationEngineOptions options{.optimizer = literals_optimizer};
  {
    // Optimizations must be applied before folding literals.
    ASSERT_OK_AND_ASSIGN(
        auto expr,
        CallOp("math.add", {CallOp(pattern_op, {pattern_x, Leaf("u")}),
                            CallOp(pattern_op, {pattern_x, Leaf("v")})}));
    EXPECT_THAT(PrepareExpression(expr, input_qtypes, options),
                IsOkAndHolds(EqualsExpr(Literal(114))));
  }
  {
    // Optimizations must also work after folding literals.
    ASSERT_OK_AND_ASSIGN(
        auto expr,
        CallOp(pattern_op,
               {CallOp("math.add", {Literal(1), Literal(1)}), Leaf("u")}));
    EXPECT_THAT(PrepareExpression(expr, input_qtypes, options),
                IsOkAndHolds(EqualsExpr(Literal(57))));
  }
  ASSERT_OK_AND_ASSIGN(
      auto add_1_lambda,
      MakeLambdaOperator(ExprOperatorSignature{{"x"}},
                         CallOp("math.add", {Placeholder("x"), Literal(1)})));
  {
    // Optimizations must first work before ToLowest.
    ASSERT_OK_AND_ASSIGN(
        auto expr,
        CallOp(add_1_lambda, {CallOp(pattern_op, {pattern_x, Leaf("u")})}));
    EXPECT_THAT(PrepareExpression(expr, input_qtypes, options),
                IsOkAndHolds(EqualsExpr(Literal(58))));
  }
  {
    // Optimizations must also work after ToLowest.
    ASSERT_OK_AND_ASSIGN(
        auto expr,
        CallOp(pattern_op, {CallOp(add_1_lambda, {Literal(1)}), Leaf("u")}));
    EXPECT_THAT(PrepareExpression(expr, input_qtypes, options),
                IsOkAndHolds(EqualsExpr(Literal(57))));
  }
}

TEST(PrepareExpressionTest, DetailedStackTraceBuilding) {
  ASSERT_OK_AND_ASSIGN(
      auto add_2_lambda,
      MakeLambdaOperator("add_2_lambda", ExprOperatorSignature{{"x"}},
                         CallOp("math.add", {Placeholder("x"), Literal(2)})));
  auto pattern_op = std::make_shared<DummyOp>(
      "pattern_op", ExprOperatorSignature::MakeArgsN(2));
  // Dummy optimizer
  Optimizer dummy_optimizer =
      [pattern_op,
       add_2_lambda](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
    if (node->op() == pattern_op &&
        node->node_deps()[0]->fingerprint() == Literal(2)->fingerprint()) {
      return CallOp(add_2_lambda, {node->node_deps()[1]});
    }
    return node;
  };

  DynamicEvaluationEngineOptions options{.optimizer = dummy_optimizer};
  auto stack_trace = std::make_shared<DetailedExprStackTrace>();

  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp(pattern_op,
             {CallOp("math.add", {Literal(1), Literal(1)}), Leaf("u")}));

  ASSERT_OK_AND_ASSIGN(
      auto prepared_expr,
      PrepareExpression(expr, {{"u", GetQType<int>()}}, options, stack_trace));

  EXPECT_EQ(stack_trace->FullTrace(prepared_expr->fingerprint()),
            "ORIGINAL NODE: pattern_op(M.math.add(..., ...):INT32, L.u)\n"
            "COMPILED NODE: M.math.add(annotation.qtype(..., ...), 2):INT32\n"
            "DETAILED STACK TRACE:\n"
            "pattern_op(M.math.add(..., ...):INT32, L.u)\n"
            "  had transformations applied to its children\n"
            "pattern_op(2, L.u)\n"
            "  was optimized to\n"
            "add_2_lambda(annotation.qtype(..., ...)):INT32\n"
            "  was lowered to\n"
            "M.math.add(annotation.qtype(..., ...), 2):INT32");
}

TEST(PrepareExpressionTest, LightweightStackTraceBuilding) {
  ASSERT_OK_AND_ASSIGN(
      auto add_2_lambda,
      MakeLambdaOperator("add_2_lambda", ExprOperatorSignature{{"x"}},
                         CallOp("math.add", {Placeholder("x"), Literal(2)})));
  auto pattern_op = std::make_shared<DummyOp>(
      "pattern_op", ExprOperatorSignature::MakeArgsN(2));
  // Dummy optimizer
  Optimizer dummy_optimizer =
      [pattern_op,
       add_2_lambda](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
    if (node->op() == pattern_op &&
        node->node_deps()[0]->fingerprint() == Literal(2)->fingerprint()) {
      return CallOp(add_2_lambda, {node->node_deps()[1]});
    }
    return node;
  };

  DynamicEvaluationEngineOptions options{.optimizer = dummy_optimizer};
  auto stack_trace = std::make_shared<LightweightExprStackTrace>();

  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp(pattern_op,
             {CallOp("math.add", {Literal(1), Literal(1)}), Leaf("u")}));

  ASSERT_OK_AND_ASSIGN(
      auto prepared_expr,
      PrepareExpression(expr, {{"u", GetQType<int>()}}, options, stack_trace));
  stack_trace->AddRepresentations(prepared_expr, expr);

  EXPECT_EQ(stack_trace->FullTrace(prepared_expr->fingerprint()),
            "ORIGINAL NODE: pattern_op(M.math.add(..., ...):INT32, L.u)\n"
            "COMPILED NODE: M.math.add(annotation.qtype(..., ...), 2):INT32");
}

TEST(PrepareExpressionTest, StackTraceWithErrorNestedUnderLambda) {
  ASSERT_OK_AND_ASSIGN(
      auto lambda_with_nested_error,
      MakeLambdaOperator(
          "lambda_with_nested_error", ExprOperatorSignature{{"x"}, {"y"}},
          CallOp("math.add",
                 {Literal(2.0), CallOp("math.divide", {Placeholder("x"),
                                                       Placeholder("y")})})));
  auto stack_trace = std::make_shared<DetailedExprStackTrace>();

  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp(lambda_with_nested_error, {Leaf("x"), Leaf("y")}));

  ASSERT_OK_AND_ASSIGN(
      auto prepared_expr,
      PrepareExpression(expr,
                        {{"x", GetQType<float>()}, {"y", GetQType<float>()}},
                        DynamicEvaluationEngineOptions{}, stack_trace));

  // A copy of faulty_node will be created during PrepareExpression(expr).
  ASSERT_OK_AND_ASSIGN(auto faulty_node,
                       CallOp("math.divide", {Leaf("x"), Leaf("y")}));

  ASSERT_OK_AND_ASSIGN(
      faulty_node,
      PrepareExpression(faulty_node,
                        {{"x", GetQType<float>()}, {"y", GetQType<float>()}},
                        DynamicEvaluationEngineOptions{}));

  EXPECT_THAT(
      stack_trace->FullTrace(faulty_node->fingerprint()),
      Eq("ORIGINAL NODE: lambda_with_nested_error(L.x, L.y)\n"
         "COMPILED NODE: M.math.divide(annotation.qtype(..., ...), "
         "annotation.qtype(..., ...)):FLOAT32\n"
         "DETAILED STACK TRACE:\n"
         "lambda_with_nested_error(L.x, L.y)\n"
         "  was lowered to\n"
         "M.math.add(float64{2}, M.math.divide(..., ...):FLOAT32):FLOAT64\n"
         "  which contains\n"
         "M.math.divide(annotation.qtype(..., ...),"
         " annotation.qtype(..., ...)):FLOAT32"));
}

TEST(PrepareExpressionTest, StackTraceBuildingNoTransformations) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("edge.from_sizes",
             {CallOp("annotation.qtype",
                     {Leaf("x"), Literal(GetDenseArrayQType<int64_t>())})}));
  auto stack_trace = std::make_shared<DetailedExprStackTrace>();

  ASSERT_OK_AND_ASSIGN(
      auto prepared_expr,
      PrepareExpression(expr, {{"x", GetDenseArrayQType<int64_t>()}},
                        DynamicEvaluationEngineOptions{}, stack_trace));

  BoundExprStackTraceBuilder stack_trace_builder(stack_trace);
  stack_trace_builder.RegisterIp(0, prepared_expr);
  auto bound_stack_trace = stack_trace_builder.Build(/*num_operators=*/10);
  EXPECT_EQ(bound_stack_trace[0], "");
}

TEST(PrepareExpressionTest, StackTraceAnnotationCycle) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      // Leaf will undergo the cycle L.x -> annotation.qtype(L.x, ..) -> L.x
      CallOp("edge.from_sizes", {Leaf("x")}));
  auto stack_trace = std::make_shared<DetailedExprStackTrace>();

  ASSERT_OK_AND_ASSIGN(
      auto prepared_expr,
      PrepareExpression(expr, {{"x", GetDenseArrayQType<int64_t>()}},
                        DynamicEvaluationEngineOptions{}, stack_trace));
  absl::flat_hash_map<Fingerprint, QTypePtr> node_types;
  ASSERT_OK_AND_ASSIGN(prepared_expr,
                       eval_internal::ExtractQTypesForCompilation(
                           prepared_expr, &node_types, stack_trace));

  BoundExprStackTraceBuilder stack_trace_builder(stack_trace);
  stack_trace_builder.RegisterIp(0, prepared_expr);
  auto bound_stack_trace = stack_trace_builder.Build(/*num_operators=*/10);
  EXPECT_EQ(bound_stack_trace[0], "");
}

TEST(PrepareExpressionTest, OperatorWithBadGetOutputQType) {
  // Optimizations must be applied before folding literals.
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp(std::make_shared<OperatorWithBadGetOutputQType>(),
                              {Literal(2.0)}));
  EXPECT_THAT(
      PrepareExpression(expr, {}, DynamicEvaluationEngineOptions{}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               "expression bad_op(float64{2}):INT64 attributes changed in "
               "ToLower from Attr(qtype=INT64) to Attr(qvalue=float64{2}); "
               "this indicates incorrect InferAttributes() or GetOutputType() "
               "of the operator bad_op; while transforming "
               "bad_op(float64{2}):INT64; while doing literal folding; while "
               "transforming bad_op(float64{2}):INT64"));
}

TEST(PrepareExpressionTest, StripAnnotations) {
  const auto id_annotation = std::make_shared<IdentityAnnotation>();

  ASSERT_OK_AND_ASSIGN(auto x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp(id_annotation,
                        {CallOp("math.neg", {CallOp(id_annotation, {x})})}));
  EXPECT_THAT(PrepareExpression(expr, {}, DynamicEvaluationEngineOptions{}),
              IsOkAndHolds(EqualsExpr(CallOp("math.neg", {x}))));
}

TEST(PrepareExpressionTest, SingleLeafExpression) {
  auto expr = Leaf("x");
  EXPECT_THAT(
      PrepareExpression(expr, {{"x", GetQType<float>()}},
                        DynamicEvaluationEngineOptions{}),
      IsOkAndHolds(EqualsExpr(CallOp(
          QTypeAnnotation::Make(), {Leaf("x"), Literal(GetQType<float>())}))));
  EXPECT_THAT(PrepareExpression(expr, {}, DynamicEvaluationEngineOptions{}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing QType information for inputs {x}")));
}

TEST(PrepareExpressionTest, QTypeAnnotations) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.neg", {WithQTypeAnnotation(Leaf("x"), GetQType<float>())}));
  EXPECT_THAT(PrepareExpression(expr, {}, DynamicEvaluationEngineOptions{}),
              IsOkAndHolds(EqualsExpr(expr)));
  EXPECT_THAT(PrepareExpression(expr, {{"x", GetQType<float>()}},
                                DynamicEvaluationEngineOptions{}),
              IsOkAndHolds(EqualsExpr(expr)));
  EXPECT_THAT(PrepareExpression(expr, {{"x", GetQType<double>()}},
                                DynamicEvaluationEngineOptions{}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("inconsistent qtype annotation and input "
                                 "qtype: FLOAT32,FLOAT64")));
  EXPECT_THAT(PrepareExpression(expr, {{"x", nullptr}},
                                DynamicEvaluationEngineOptions{}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("inconsistent qtype annotation and input "
                                 "qtype: FLOAT32,NULL")));
}

TEST(PrepareExpressionTest, QTypeAnnotations_WithPartiallyAnnotatedLeaves) {
  auto x = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto typed_x, CallOp(QTypeAnnotation::Make(),
                                            {x, Literal(GetQType<float>())}));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core.make_tuple",
                                         {typed_x,
                                          // Note that the `x` below is untyped.
                                          x}));
  // The error message below does not come from PrepareExpression because
  // it supports exprs with partially annotated leaves.
  EXPECT_THAT(PrepareExpression(expr, {}, DynamicEvaluationEngineOptions{}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing QType information for inputs {x}")));
  EXPECT_THAT(
      PrepareExpression(expr, {{"x", GetQType<float>()}},
                        DynamicEvaluationEngineOptions{}),
      IsOkAndHolds(EqualsExpr(CallOp("core.make_tuple", {typed_x, typed_x}))));
}

TEST(PrepareExpressionTest, StripExtraQTypeAnnotations) {
  ASSERT_OK_AND_ASSIGN(auto typed_x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto typed_typed_x,
                       WithQTypeAnnotation(typed_x, GetQType<float>()));
  EXPECT_THAT(
      PrepareExpression(typed_typed_x, {}, DynamicEvaluationEngineOptions{}),
      IsOkAndHolds(EqualsExpr(typed_x)));

  ASSERT_OK_AND_ASSIGN(
      auto expr_with_non_deducible_type_annotation,
      WithQTypeAnnotation(
          CallOp(std::make_shared<OperatorWithNoInferAttributes>(), {typed_x}),
          GetQType<float>()));
  EXPECT_THAT(
      PrepareExpression(expr_with_non_deducible_type_annotation, {},
                        DynamicEvaluationEngineOptions{}),
      IsOkAndHolds(EqualsExpr(expr_with_non_deducible_type_annotation)));

  ASSERT_OK_AND_ASSIGN(
      auto expr_with_double_type_annotation,
      WithQTypeAnnotation(expr_with_non_deducible_type_annotation,
                          GetQType<float>()));
  EXPECT_THAT(
      PrepareExpression(expr_with_double_type_annotation, {},
                        DynamicEvaluationEngineOptions{}),
      IsOkAndHolds(EqualsExpr(expr_with_non_deducible_type_annotation)));
}

}  // namespace
}  // namespace arolla::expr::eval_internal
