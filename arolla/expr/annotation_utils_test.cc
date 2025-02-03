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
#include "arolla/expr/annotation_utils.h"

#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsExpr;

class DummyOp : public BasicExprOperator {
 public:
  DummyOp(absl::string_view display_name,
          const ExprOperatorSignature& signature)
      : BasicExprOperator(display_name, signature, "docstring",
                          FingerprintHasher("arolla::expr::DummyOp")
                              .Combine(display_name, signature)
                              .Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override {
    return GetQType<int>();
  }
};

class DummyAnnotation : public AnnotationExprOperatorTag,
                        public BasicExprOperator {
 public:
  DummyAnnotation(absl::string_view display_name,
                  const ExprOperatorSignature& signature)
      : BasicExprOperator(display_name, signature, "docstring",
                          FingerprintHasher("arolla::expr::DummyAnnotation")
                              .Combine(display_name, signature)
                              .Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override {
    return input_qtypes.empty() ? GetQType<int>() : input_qtypes[0];
  }
};

TEST(AnnotationUtilsTest, IsAnnotation) {
  {
    auto op =
        std::make_shared<DummyAnnotation>("id", ExprOperatorSignature{{"x"}});
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x")}));
    EXPECT_THAT(IsAnnotation(expr), IsOkAndHolds(true));
  }
  {
    auto op = RegisterOperator<DummyAnnotation>(
        "annotation_utils_test.is_annotation.registered_annotation", "id",
        ExprOperatorSignature{{"x"}});
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x")}));
    EXPECT_THAT(IsAnnotation(expr), IsOkAndHolds(true));
  }
  {  // no args
    auto op =
        std::make_shared<DummyAnnotation>("stub", ExprOperatorSignature{});
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {}));
    EXPECT_THAT(IsAnnotation(expr), IsOkAndHolds(false));
  }
  {  // not an annotation
    auto op = std::make_shared<DummyOp>("id", ExprOperatorSignature{{"x"}});
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x")}));
    EXPECT_THAT(IsAnnotation(expr), IsOkAndHolds(false));
  }
  {
    auto op = std::make_shared<RegisteredOperator>(
        "annotation_utils_test.is_annotation.missing");
    auto expr =
        ExprNode::UnsafeMakeOperatorNode(std::move(op), {Leaf("x")}, {});
    EXPECT_THAT(IsAnnotation(expr), StatusIs(absl::StatusCode::kNotFound));
  }
}

TEST(AnnotationUtilsTest, StripTopmostAnnotations) {
  // dummy_annotation(
  //     dummy_annotation(
  //         dummy_op(dummy_annotation(x, a), y),
  //         b
  //     ),
  //     c
  // ) -> dummy_op(dummy_annotation(x, a), y)
  auto dummy_annotation = std::make_shared<DummyAnnotation>(
      "dummy_annotation", ExprOperatorSignature{{"x"}, {"y"}});
  auto dummy_op = std::make_shared<DummyOp>(
      "dummy_op", ExprOperatorSignature{{"x"}, {"y"}});
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp(dummy_annotation,
             {CallOp(dummy_annotation,
                     {CallOp(dummy_op,
                             {CallOp(dummy_annotation, {Leaf("x"), Leaf("a")}),
                              Leaf("y")}),
                      Leaf("b")}),
              Leaf("c")}));
  ASSERT_OK_AND_ASSIGN(auto actual, StripTopmostAnnotations(expr));
  ASSERT_OK_AND_ASSIGN(
      auto expected,
      CallOp(dummy_op,
             {CallOp(dummy_annotation, {Leaf("x"), Leaf("a")}), Leaf("y")}));
  EXPECT_THAT(actual, EqualsExpr(expected));
}

TEST(AnnotationUtilsTest, StripAnnotations) {
  // dummy_annotation(
  //     dummy_annotation(
  //         dummy_op(dummy_annotation(x, a), y),
  //         b
  //     ),
  //     c
  // ) -> dummy_op( y)
  auto dummy_annotation = std::make_shared<DummyAnnotation>(
      "dummy_annotation", ExprOperatorSignature{{"x"}, {"y"}});
  auto dummy_op = std::make_shared<DummyOp>(
      "dummy_op", ExprOperatorSignature{{"x"}, {"y"}});
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp(dummy_annotation,
             {CallOp(dummy_annotation,
                     {CallOp(dummy_op,
                             {CallOp(dummy_annotation, {Leaf("x"), Leaf("a")}),
                              Leaf("y")}),
                      Leaf("b")}),
              Leaf("c")}));
  ASSERT_OK_AND_ASSIGN(auto actual, StripAnnotations(expr));
  ASSERT_OK_AND_ASSIGN(auto expected, CallOp(dummy_op, {Leaf("x"), Leaf("y")}));
  EXPECT_THAT(actual, EqualsExpr(expected));
}

TEST(AnnotationUtilsTest, IsQTypeAnnotation) {
  {
    auto op = QTypeAnnotation::Make();
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Placeholder("y")}));
    EXPECT_TRUE(IsQTypeAnnotation(expr));
  }
  {
    auto op = std::make_shared<QTypeAnnotation>("aux_policy");
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Placeholder("y")}));
    EXPECT_TRUE(IsQTypeAnnotation(expr));
  }
  {  // another annotation
    auto op = std::make_shared<DummyAnnotation>(
        "annotation.name", ExprOperatorSignature{{"expr"}, {"qtype"}});
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Placeholder("y")}));
    EXPECT_FALSE(IsQTypeAnnotation(expr));
  }
  {  // wrong arity
    auto op = QTypeAnnotation::Make();
    auto expr =
        ExprNode::UnsafeMakeOperatorNode(std::move(op), {Leaf("x")}, {});
    EXPECT_FALSE(IsQTypeAnnotation(expr));
  }
  EXPECT_FALSE(IsQTypeAnnotation(Leaf("x")));
}

TEST(AnnotationUtilsTest, IsNameAnnotation) {
  {
    auto op = NameAnnotation::Make();
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(Text("name"))}));
    EXPECT_TRUE(IsNameAnnotation(expr));
  }
  {
    auto op = std::make_shared<NameAnnotation>("aux_policy");
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(Text("name"))}));
    EXPECT_TRUE(IsNameAnnotation(expr));
  }
  {  // another annotation
    auto op = std::make_shared<DummyAnnotation>(
        "annotation.name", ExprOperatorSignature{{"expr"}, {"name"}});
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(Text("name"))}));
    EXPECT_FALSE(IsNameAnnotation(expr));
  }
  {  // wrong arity
    auto op = NameAnnotation::Make();
    auto expr =
        ExprNode::UnsafeMakeOperatorNode(std::move(op), {Leaf("x")}, {});
    EXPECT_FALSE(IsNameAnnotation(expr));
  }
  {  // name is not a literal
    auto op = NameAnnotation::Make();
    auto expr = ExprNode::UnsafeMakeOperatorNode(
        std::move(op), {Leaf("x"), Placeholder("y")}, {});
    EXPECT_FALSE(IsNameAnnotation(expr));
  }
  {  // name is a literal of unexpected type
    auto op = NameAnnotation::Make();
    auto expr = ExprNode::UnsafeMakeOperatorNode(
        std::move(op), {Leaf("x"), Literal(Bytes("name"))}, {});
    EXPECT_FALSE(IsNameAnnotation(expr));
  }
  EXPECT_FALSE(IsNameAnnotation(Leaf("x")));
}

TEST(AnnotationUtilsTest, IsExportAnnotation) {
  {
    auto op = ExportAnnotation::Make();
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(Text("tag"))}));
    EXPECT_TRUE(IsExportAnnotation(expr));
  }
  {
    auto op = ExportValueAnnotation::Make();
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Literal(Text("tag")),
                                                Placeholder("value")}));
    EXPECT_TRUE(IsExportAnnotation(expr));
  }
  {  // another annotation
    auto op = std::make_shared<DummyAnnotation>(
        "annotation.export", ExprOperatorSignature{{"expr"}, {"export_tag"}});
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(Text("tag"))}));
    EXPECT_FALSE(IsExportAnnotation(expr));
  }
  {  // annotation.export with wrong arity
    auto op = ExportAnnotation::Make();
    auto expr = ExprNode::UnsafeMakeOperatorNode(
        std::move(op), {Leaf("x"), Literal(Text("tag")), Placeholder("value")},
        {});
    EXPECT_FALSE(IsExportAnnotation(expr));
  }
  {  // annotation.export_value with wrong arity
    auto op = ExportAnnotation::Make();
    auto expr = ExprNode::UnsafeMakeOperatorNode(
        std::move(op), {Leaf("x"), Literal(Text("tag")), Placeholder("value")},
        {});
    EXPECT_FALSE(IsExportAnnotation(expr));
  }
  {  // export_tag is not a literal
    auto op = ExportAnnotation::Make();
    auto expr = ExprNode::UnsafeMakeOperatorNode(
        std::move(op), {Leaf("x"), Placeholder("tag")}, {});
    EXPECT_FALSE(IsExportAnnotation(expr));
  }
  {  // export_tag is a literal of unexpected type
    auto op = ExportAnnotation::Make();
    auto expr = ExprNode::UnsafeMakeOperatorNode(
        std::move(op), {Leaf("x"), Literal(Bytes("tag"))}, {});
    EXPECT_FALSE(IsExportAnnotation(expr));
  }
  EXPECT_FALSE(IsExportAnnotation(Leaf("x")));
}

TEST(AnnotationUtilsTest, ReadQTypeAnnotation) {
  {
    auto op = QTypeAnnotation::Make();
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(GetQTypeQType())}));
    EXPECT_EQ(ReadQTypeAnnotation(expr), GetQTypeQType());
  }
  {  // A non-literal value.
    auto op = QTypeAnnotation::Make();
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Placeholder("y")}));
    EXPECT_EQ(ReadQTypeAnnotation(expr), nullptr);
  }
  {  // another annotation
    auto op = std::make_shared<DummyAnnotation>(
        "annotation.qtype", ExprOperatorSignature{{"expr"}, {"qtype"}});
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(GetQTypeQType())}));
    EXPECT_EQ(ReadQTypeAnnotation(expr), nullptr);
  }
  EXPECT_EQ(ReadQTypeAnnotation(Leaf("x")), nullptr);
}

TEST(AnnotationUtilsTest, ReadNameAnnotation) {
  {
    auto op = NameAnnotation::Make();
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(Text("name"))}));
    EXPECT_EQ(ReadNameAnnotation(expr), "name");
  }
  {  // another annotation
    auto op = std::make_shared<DummyAnnotation>(
        "annotation.name", ExprOperatorSignature{{"expr"}, {"name"}});
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(Text("name"))}));
    EXPECT_EQ(ReadNameAnnotation(expr), "");
  }
  EXPECT_EQ(ReadNameAnnotation(Leaf("x")), "");
}

TEST(AnnotationUtilsTest, ReadExportAnnotation) {
  {
    auto op = ExportAnnotation::Make();
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(Text("tag"))}));
    EXPECT_EQ(ReadExportAnnotationTag(expr), "tag");
    EXPECT_THAT(ReadExportAnnotationValue(expr), EqualsExpr(Leaf("x")));
  }
  {
    auto op = ExportValueAnnotation::Make();
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Literal(Text("tag")),
                                                Placeholder("value")}));
    EXPECT_EQ(ReadExportAnnotationTag(expr), "tag");
    EXPECT_THAT(ReadExportAnnotationValue(expr),
                EqualsExpr(Placeholder("value")));
  }
  {  // another annotation
    auto op = std::make_shared<DummyAnnotation>(
        "annotation.export", ExprOperatorSignature{{"expr"}, {"export_tag"}});
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(op, {Leaf("x"), Literal(Text("tag"))}));
    EXPECT_EQ(ReadExportAnnotationTag(expr), "");
    EXPECT_EQ(ReadExportAnnotationValue(expr), nullptr);
  }
  EXPECT_EQ(ReadExportAnnotationTag(Leaf("x")), "");
  EXPECT_EQ(ReadExportAnnotationValue(Leaf("x")), nullptr);
}

}  // namespace
}  // namespace arolla::expr
