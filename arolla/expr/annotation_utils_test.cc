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
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsExpr;
using ::testing::Eq;
using ::testing::HasSubstr;

class AnnotationOperatorTest : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

class IdentityAnnotation : public AnnotationExprOperatorTag,
                           public BasicExprOperator {
 public:
  IdentityAnnotation()
      : BasicExprOperator(
            "id", ExprOperatorSignature::MakeArgsN(1), "",
            FingerprintHasher("arolla::expr::IdentityAnnotation").Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override {
    return input_qtypes[0];
  }
};

TEST_F(AnnotationOperatorTest, SmokeTest) {
  const auto with_annotation = std::make_shared<IdentityAnnotation>();
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(with_annotation, {Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(auto lower_expr, ToLowerNode(expr));
  EXPECT_THAT(lower_expr, EqualsExpr(expr));

  ASSERT_OK_AND_ASSIGN(auto typed_expr,
                       CallOp(with_annotation, {Literal<float>(1.0)}));
  EXPECT_EQ(typed_expr->qtype(), GetQType<float>());
}

TEST_F(AnnotationOperatorTest, StripAnnotations) {
  const auto id_anno = std::make_shared<IdentityAnnotation>();
  {
    // id_anno(id_anno(x) + y)
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expr,
        CallOp(id_anno, {CallOp("math.add",
                                {CallOp(id_anno, {Leaf("x")}), Leaf("y")})}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr actual, StripAnnotations(expr));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expected,
                         CallOp("math.add", {Leaf("x"), Leaf("y")}));
    EXPECT_THAT(actual, EqualsExpr(expected));
  }
  {
    // id_anno(id_anno(x))
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp(id_anno, {CallOp(id_anno, {Leaf("x")})}));
    ASSERT_OK_AND_ASSIGN(ExprNodePtr actual, StripAnnotations(expr));
    ExprNodePtr expected = Leaf("x");
    EXPECT_THAT(actual, EqualsExpr(expected));
  }
}

TEST_F(AnnotationOperatorTest, StripTopmostAnnotations) {
  const auto id_anno = std::make_shared<IdentityAnnotation>();
  {
    // id_anno(id_anno(x) + y)
    ASSERT_OK_AND_ASSIGN(
        ExprNodePtr expr,
        CallOp(id_anno, {CallOp("math.add",
                                {CallOp(id_anno, {Leaf("x")}), Leaf("y")})}));
    EXPECT_THAT(StripTopmostAnnotations(expr),
                IsOkAndHolds(EqualsExpr(CallOp(
                    "math.add", {CallOp(id_anno, {Leaf("x")}), Leaf("y")}))));
  }
  {
    // id_anno(id_anno(x))
    ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                         CallOp(id_anno, {CallOp(id_anno, {Leaf("x")})}));
    EXPECT_THAT(StripTopmostAnnotations(expr),
                IsOkAndHolds(EqualsExpr(Leaf("x"))));
  }
}

class IdentityAnnotation2 : public AnnotationExprOperatorTag,
                            public BasicExprOperator {
 public:
  IdentityAnnotation2()
      : BasicExprOperator(
            "id2", ExprOperatorSignature::MakeArgsN(1), "",
            FingerprintHasher("arolla::expr::IdentityAnnotation2").Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override {
    return input_qtypes[0];
  }
};

TEST_F(AnnotationOperatorTest, AttachAnnotations) {
  ExprNodePtr expr = Leaf("x");
  EXPECT_THAT(AttachAnnotation(expr, expr),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("not a detached annotation")));

  const auto id_anno = std::make_shared<IdentityAnnotation>();
  const auto id_anno2 = std::make_shared<IdentityAnnotation2>();
  ASSERT_OK_AND_ASSIGN(auto anno1, CallOp(id_anno, {Placeholder("_")}));
  ASSERT_OK_AND_ASSIGN(auto anno2, CallOp(id_anno2, {Placeholder("_")}));
  std::vector<ExprNodePtr> annotations = {anno1, anno2};
  ASSERT_OK_AND_ASSIGN(auto anno_expr, AttachAnnotations(expr, annotations));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expected_anno_expr,
                       CallOp(id_anno2, {CallOp(id_anno, {Leaf("x")})}));
  EXPECT_THAT(anno_expr, EqualsExpr(expected_anno_expr));
  ASSERT_OK_AND_ASSIGN(auto detached, StripAnnotations(anno_expr));
  EXPECT_THAT(detached, EqualsExpr(expr));
}

TEST_F(AnnotationOperatorTest, AnnotationExport) {
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expr,
      CallOp(ExportAnnotation::Make(), {Leaf("a"), Literal(Text{"b"})}));
  ASSERT_TRUE(IsExportAnnotation(expr));
  auto expected_value = Leaf("a");
  EXPECT_THAT(ReadExportAnnotationTag(expr), Eq("b"));
  EXPECT_THAT(ReadExportAnnotationValue(expr), EqualsExpr(expected_value));
}

TEST_F(AnnotationOperatorTest, AnnotationExportValue) {
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                       CallOp(ExportValueAnnotation::Make(),
                              {Leaf("a"), Literal(Text{"b"}), Leaf("c")}));
  ASSERT_TRUE(IsExportAnnotation(expr));
  auto expected_value = Leaf("c");
  EXPECT_THAT(ReadExportAnnotationTag(expr), Eq("b"));
  EXPECT_THAT(ReadExportAnnotationValue(expr), EqualsExpr(expected_value));
}

TEST_F(AnnotationOperatorTest, AnnotationExportArbitraryNode) {
  ExprNodePtr expr = Leaf("a");
  ASSERT_FALSE(IsExportAnnotation(expr));
  EXPECT_EQ(ReadExportAnnotationTag(expr), "");
  EXPECT_EQ(ReadExportAnnotationValue(expr), nullptr);
}

}  // namespace
}  // namespace arolla::expr
