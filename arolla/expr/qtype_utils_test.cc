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
#include "arolla/expr/qtype_utils.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsAttr;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::TypedValueWith;
using ::arolla::testing::WithNameAnnotation;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsNull;
using ::testing::Optional;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using Attr = ::arolla::expr::ExprAttributes;

TEST(QTypeMetadataTest, GetExprQType_LeafWithoutQType) {
  ExprNodePtr leaf = Leaf("a");
  EXPECT_THAT(leaf->qtype(), IsNull());
}

TEST(QTypeMetadataTest, GetExprQType_LeafWithQType) {
  ASSERT_OK_AND_ASSIGN(ExprNodePtr leaf_with_qtype,
                       WithQTypeAnnotation(Leaf("a"), GetQType<float>()));
  EXPECT_EQ(leaf_with_qtype->qtype(), GetQType<float>());
}

TEST(QTypeMetadataTest, GetExprQType_ArgumentlessOperator) {
  ASSERT_OK_AND_ASSIGN(
      auto argumentless_operator,
      MakeLambdaOperator(ExprOperatorSignature{}, Literal(1.f)));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr node, BindOp(argumentless_operator, {}, {}));
  EXPECT_THAT(node->qtype(), GetQType<float>());
}

TEST(QTypeMetadataTest, GetExprQType) {
  ExprNodePtr leaf = Leaf("a");
  ASSERT_OK_AND_ASSIGN(ExprNodePtr leaf_with_qtype,
                       WithQTypeAnnotation(Leaf("a"), GetQType<float>()));
  ExprNodePtr literal = Literal<int64_t>(57);
  EXPECT_THAT(GetExprQTypes({leaf, leaf_with_qtype, literal}),
              ElementsAre(nullptr, GetQType<float>(), GetQType<int64_t>()));
}

TEST(QTypeMetadataTest, GetExprQValues) {
  ExprNodePtr literal = Literal<int64_t>(57);
  ExprNodePtr leaf = Leaf("a");
  ASSERT_OK_AND_ASSIGN(
      auto argumentless_operator,
      MakeLambdaOperator(ExprOperatorSignature{}, Literal(1.f)));
  ASSERT_OK_AND_ASSIGN(ExprNodePtr argumentless_node,
                       BindOp(argumentless_operator, {}, {}));
  EXPECT_THAT(
      GetExprQValues({literal, leaf, argumentless_node}),
      ElementsAre(Optional(TypedValueWith<int64_t>(57)), Eq(std::nullopt),
                  Optional(TypedValueWith<float>(1.f))));
}

TEST(QTypeMetadataTest, CollectLeafQTypes_Basic) {
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expr,
      CallOp("test.power",
             {WithQTypeAnnotation(Leaf("x"), GetQType<float>()),
              WithQTypeAnnotation(WithNameAnnotation(Leaf("y"), "name"),
                                  GetQType<float>())}));
  EXPECT_THAT(CollectLeafQTypes(expr),
              IsOkAndHolds(UnorderedElementsAre(Pair("x", GetQType<float>()),
                                                Pair("y", GetQType<float>()))));
}

TEST(QTypeMetadataTest, CollectLeafQTypes_Partial) {
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expr,
      CallOp("test.power",
             {WithQTypeAnnotation(Leaf("x"), GetQType<float>()), Leaf("y")}));
  EXPECT_THAT(CollectLeafQTypes(expr),
              IsOkAndHolds(UnorderedElementsAre(Pair("x", GetQType<float>()))));
}

TEST(QTypeMetadataTest, CollectLeafQTypes_Duplicate) {
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expr,
      CallOp("test.power",
             {WithQTypeAnnotation(Leaf("x"), GetQType<float>()),
              WithQTypeAnnotation(WithNameAnnotation(Leaf("x"), "x"),
                                  GetQType<float>())}));
  EXPECT_THAT(CollectLeafQTypes(expr),
              IsOkAndHolds(UnorderedElementsAre(Pair("x", GetQType<float>()))));
}

TEST(QTypeMetadataTest, CollectLeafQTypes_Inconsistent) {
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expr,
      CallOp("test.power",
             {WithQTypeAnnotation(Leaf("x"), GetQType<float>()),
              WithQTypeAnnotation(Leaf("x"), GetQType<int32_t>())}));
  EXPECT_THAT(
      CollectLeafQTypes(expr),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "inconsistent qtype annotations for L.x: INT32 != FLOAT32"));
}

TEST(QTypeMetadataTest, CollectLeafQTypes_InconsistentNested) {
  auto leaf_x = Leaf("x");
  auto literal_float32_qtype = Literal(GetQType<float>());
  auto literal_int32_qtype = Literal(GetQType<int32_t>());
  auto sub_expr = ExprNode::UnsafeMakeOperatorNode(
      QTypeAnnotation::Make(), {leaf_x, literal_float32_qtype},
      ExprAttributes{});
  auto expr = ExprNode::UnsafeMakeOperatorNode(QTypeAnnotation::Make(),
                                               {sub_expr, literal_int32_qtype},
                                               ExprAttributes{});
  EXPECT_THAT(CollectLeafQTypes(expr),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "inconsistent qtype annotations for L.x: "
                       "INT32 != FLOAT32"));
}

TEST(QTypeMetadataTest, PopulateQTypes) {
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                       CallOp("test.add3", {Leaf("a"), Leaf("b"), Leaf("a")}));

  EXPECT_THAT(PopulateQTypes(expr, {{"a", GetQType<float>()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("QType for the leaves {b} are missing")));
  EXPECT_THAT(PopulateQTypes(expr, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("QType for the leaves {a, b} are missing")));
  EXPECT_THAT(PopulateQTypes(expr, {{"a", GetQType<float>()}},
                             /*allow_incomplete_type_information=*/true),
              IsOk());

  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr_float,
                       PopulateQTypes(expr, {{"a", GetQType<float>()},
                                             {"b", GetQType<float>()}}));
  EXPECT_EQ(expr_float->qtype(), GetQType<float>());
}

TEST(QTypeMetadataTest, PopulateQTypes_WithGetter) {
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                       CallOp("test.add3", {Leaf("a"), Leaf("b"), Leaf("a")}));

  EXPECT_THAT(PopulateQTypes(expr, [](auto) { return nullptr; }),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("QType for the leaves {a, b} are missing")));
  EXPECT_THAT(PopulateQTypes(expr,
                             [](absl::string_view leaf_name) {
                               return leaf_name == "a" ? GetQType<float>()
                                                       : nullptr;
                             }),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("QType for the leaves {b} are missing")));
  EXPECT_THAT(PopulateQTypes(
                  expr, [](auto) { return nullptr; },
                  /*allow_incomplete_type_information=*/true),
              IsOk());

  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr_float, PopulateQTypes(expr, [](auto) {
                         return GetQType<float>();
                       }));
  EXPECT_EQ(expr_float->qtype(), GetQType<float>());
}

TEST(QTypeMetadataTest, PopulateQTypes_CollectingQTypesFromExpr) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("test.add3", {WithQTypeAnnotation(Leaf("a"), GetQType<float>()),
                           Leaf("b"), Leaf("a")}));
  ASSERT_OK_AND_ASSIGN(auto actual_expr,
                       PopulateQTypes(expr, {{"b", GetQType<double>()}}));
  ASSERT_OK_AND_ASSIGN(
      auto expected_expr,
      CallOp("test.add3", {CallOp(QTypeAnnotation::Make(),
                                  {Leaf("a"), Literal(GetQType<float>())}),
                           CallOp(QTypeAnnotation::Make(),
                                  {Leaf("b"), Literal(GetQType<double>())}),
                           CallOp(QTypeAnnotation::Make(),
                                  {Leaf("a"), Literal(GetQType<float>())})}));
  EXPECT_THAT(actual_expr, EqualsExpr(expected_expr));
}

TEST(QTypeMetadataTest, PopulateQType_QTypeMismatch) {
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                       WithQTypeAnnotation(Leaf("a"), GetQType<int32_t>()));

  EXPECT_THAT(
      PopulateQTypes(expr, {{"a", GetQType<float>()}}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "inconsistent annotation.qtype(expr: FLOAT32, qtype=INT32)")));
}

TEST(QTypeMetadataTest, PopulateQType_TypesUnsupportedByOperator) {
  ASSERT_OK_AND_ASSIGN(ExprNodePtr expr,
                       CallOp("math.add", {Leaf("a"), Leaf("b")}));
  EXPECT_THAT(PopulateQTypes(
                  expr, {{"a", GetQType<int32_t>()}, {"b", GetQType<Bytes>()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected numerics, got y: BYTES")));
}

TEST(QTypeMetadataTest, GetExprAttrs) {
  ASSERT_OK_AND_ASSIGN(ExprNodePtr a,
                       WithQTypeAnnotation(Leaf("a"), GetQType<int32_t>()));
  ExprNodePtr b = Literal(1.0f);
  ExprNodePtr c = Placeholder("c");
  EXPECT_THAT(GetExprAttrs({}), ElementsAre());
  EXPECT_THAT(GetExprAttrs({a, b, c}),
              ElementsAre(EqualsAttr(GetQType<int32_t>()),
                          EqualsAttr(TypedValue::FromValue(1.0f)),
                          EqualsAttr(nullptr)));
}

TEST(QTypeMetadataTest, GetAttrQTypes) {
  EXPECT_THAT(GetAttrQTypes({}), ElementsAre());
  EXPECT_THAT(GetAttrQTypes({Attr{}}), ElementsAre(nullptr));
  EXPECT_THAT(GetAttrQTypes(
                  {Attr(GetQType<int32_t>()), Attr(GetQType<float>()), Attr{}}),
              ElementsAre(GetQType<int32_t>(), GetQType<float>(), nullptr));
}

TEST(QTypeMetadataTest, GetValueQTypes) {
  EXPECT_THAT(GetValueQTypes({}), ElementsAre());
  EXPECT_THAT(GetValueQTypes({GetQType<int32_t>()}), ElementsAre(nullptr));
  EXPECT_THAT(GetValueQTypes({GetOptionalQType<int32_t>(),
                              GetOptionalQType<float>(), GetQType<float>()}),
              ElementsAre(GetQType<int32_t>(), GetQType<float>(), nullptr));
}

TEST(QTypeMetadataTest, HasAllAttrQTypes) {
  EXPECT_TRUE(HasAllAttrQTypes({}));
  EXPECT_TRUE(
      HasAllAttrQTypes({Attr(GetQType<int32_t>()), Attr(GetQType<float>())}));
  EXPECT_FALSE(HasAllAttrQTypes({Attr{}}));
  EXPECT_FALSE(HasAllAttrQTypes(
      {Attr(GetQType<int32_t>()), Attr(GetQType<float>()), Attr{}}));
}

}  // namespace
}  // namespace arolla::expr
