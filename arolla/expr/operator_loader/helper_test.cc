// Copyright 2025 Google LLC
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
#include "arolla/expr/operator_loader/helper.h"

#include <cstddef>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/bytes.h"

namespace arolla::operator_loader {
namespace {

using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::expr::QTypeAnnotation;
using ::arolla::testing::EqualsExpr;

using Attr = ::arolla::expr::ExprAttributes;

absl::StatusOr<ExprNodePtr> AnnotatedInputExpr() {
  return CallOp(arolla::expr::QTypeAnnotation::Make(),
                {Leaf("input_qtype_sequence"),
                 Literal(GetSequenceQType(GetQTypeQType()))});
}

absl::StatusOr<ExprNodePtr> ParsedInputExpr(
    absl::string_view signature_pattern) {
  return CallOp("qtype._parse_input_qtype_sequence",
                {Literal(Bytes(signature_pattern)), AnnotatedInputExpr()});
}

absl::StatusOr<ExprNodePtr> GetNthExpr(absl::StatusOr<ExprNodePtr> expr,
                                       size_t n) {
  return CallOp(arolla::expr::GetNthOperator::Make(n), {std::move(expr)});
}

TEST(ResolvePlaceholdersTest, Basic) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x, y"));
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("core.equal", {Placeholder("x"), Placeholder("y")}));
  ASSERT_OK_AND_ASSIGN(
      auto result, ResolvePlaceholders(sig, expr, GetQType<OptionalUnit>()));
  ASSERT_OK_AND_ASSIGN(auto parsed_input_expr, ParsedInputExpr("PP"));
  ASSERT_OK_AND_ASSIGN(auto readiness_expr, GetNthExpr(parsed_input_expr, 0));
  ASSERT_OK_AND_ASSIGN(
      auto resolved_expr,
      CallOp("core.equal", {GetNthExpr(parsed_input_expr, 1),
                            GetNthExpr(parsed_input_expr, 2)}));
  EXPECT_THAT(result.resolved_expr, EqualsExpr(resolved_expr));
  EXPECT_THAT(result.readiness_expr, EqualsExpr(readiness_expr));
}

TEST(ResolvePlaceholdersTest, InputQTypeSequenceLeaf) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x, *y"));
  auto expr = Leaf("input_qtype_sequence");
  ASSERT_OK_AND_ASSIGN(auto result, ResolvePlaceholders(sig, expr, nullptr));
  ASSERT_OK_AND_ASSIGN(auto annotated_input_expr, AnnotatedInputExpr());
  ASSERT_OK_AND_ASSIGN(auto readiness_expr,
                       GetNthExpr(ParsedInputExpr("pv"), 0));
  EXPECT_THAT(result.resolved_expr, EqualsExpr(annotated_input_expr));
  EXPECT_THAT(result.readiness_expr, EqualsExpr(readiness_expr));
}

TEST(ResolvePlaceholdersTest, Literal) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x, *y"));
  auto expr = Literal(GetQType<int>());
  ASSERT_OK_AND_ASSIGN(auto result,
                       ResolvePlaceholders(sig, expr, GetQTypeQType()));
  ASSERT_OK_AND_ASSIGN(auto readiness_expr,
                       GetNthExpr(ParsedInputExpr("pv"), 0));
  EXPECT_THAT(result.resolved_expr, EqualsExpr(expr));
  EXPECT_THAT(result.readiness_expr, EqualsExpr(readiness_expr));
}

TEST(ResolvePlaceholdersTest, ErrorOnUnexpectedLeaf) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x"));
  auto expr = Leaf("y");
  EXPECT_THAT(ResolvePlaceholders(sig, expr, GetQTypeQType()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expression contains unexpected leaves: L.y; did you "
                       "mean to use placeholders?"));
}

TEST(ResolvePlaceholdersTest, ErrorOnUnexpectedPlaceholder) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x"));
  auto expr = Placeholder("y");
  EXPECT_THAT(ResolvePlaceholders(sig, expr, GetQTypeQType()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expression contains unexpected placeholders: P.y"));
}

TEST(ResolvePlaceholdersTest, FirstPositionalParameterUsed) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x, y, *z"));
  auto expr = Placeholder("x");
  ASSERT_OK_AND_ASSIGN(auto result,
                       ResolvePlaceholders(sig, expr, GetQTypeQType()));
  ASSERT_OK_AND_ASSIGN(auto parsed_input_expr, ParsedInputExpr("Ppv"));
  ASSERT_OK_AND_ASSIGN(auto readiness_expr, GetNthExpr(parsed_input_expr, 0));
  ASSERT_OK_AND_ASSIGN(auto resolved_expr, GetNthExpr(parsed_input_expr, 1));
  EXPECT_THAT(result.resolved_expr, EqualsExpr(resolved_expr));
  EXPECT_THAT(result.readiness_expr, EqualsExpr(readiness_expr));
}

TEST(ResolvePlaceholdersTest, SecondPositionalParameterUsed) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x, y, *z"));
  auto expr = Placeholder("y");
  ASSERT_OK_AND_ASSIGN(auto result,
                       ResolvePlaceholders(sig, expr, GetQTypeQType()));
  ASSERT_OK_AND_ASSIGN(auto parsed_input_expr, ParsedInputExpr("pPv"));
  ASSERT_OK_AND_ASSIGN(auto readiness_expr, GetNthExpr(parsed_input_expr, 0));
  ASSERT_OK_AND_ASSIGN(auto resolved_expr, GetNthExpr(parsed_input_expr, 1));
  EXPECT_THAT(result.resolved_expr, EqualsExpr(resolved_expr));
  EXPECT_THAT(result.readiness_expr, EqualsExpr(readiness_expr));
}

TEST(ResolvePlaceholdersTest, VariadicPositionalParameterUsed) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x, y, *z"));
  auto expr = Placeholder("z");
  ASSERT_OK_AND_ASSIGN(auto result,
                       ResolvePlaceholders(sig, expr, GetQTypeQType()));
  ASSERT_OK_AND_ASSIGN(auto parsed_input_expr, ParsedInputExpr("ppV"));
  ASSERT_OK_AND_ASSIGN(auto readiness_expr, GetNthExpr(parsed_input_expr, 0));
  ASSERT_OK_AND_ASSIGN(auto resolved_expr, GetNthExpr(parsed_input_expr, 1));
  EXPECT_THAT(result.resolved_expr, EqualsExpr(resolved_expr));
  EXPECT_THAT(result.readiness_expr, EqualsExpr(readiness_expr));
}

TEST(ResolvePlaceholdersTest, MixedLeafAndPlaceholder) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x"));
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("core.make_tuple",
                        {Placeholder("x"), Leaf("input_qtype_sequence")}));
  ASSERT_OK_AND_ASSIGN(auto result, ResolvePlaceholders(sig, expr, nullptr));
  ASSERT_OK_AND_ASSIGN(auto parsed_input_expr, ParsedInputExpr("P"));
  ASSERT_OK_AND_ASSIGN(auto annotated_input_expr, AnnotatedInputExpr());
  ASSERT_OK_AND_ASSIGN(auto readiness_expr, GetNthExpr(parsed_input_expr, 0));
  ASSERT_OK_AND_ASSIGN(
      auto resolved_expr,
      CallOp("core.make_tuple",
             {GetNthExpr(parsed_input_expr, 1), annotated_input_expr}));
  EXPECT_THAT(result.resolved_expr, EqualsExpr(resolved_expr));
  EXPECT_THAT(result.readiness_expr, EqualsExpr(readiness_expr));
}

TEST(ResolvePlaceholdersTest, ErrorWhenQTypeInferenceFails) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x"));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core.presence_and",
                                         {Placeholder("x"), Placeholder("x")}));
  EXPECT_THAT(ResolvePlaceholders(sig, expr, GetQType<OptionalUnit>()),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ResolvePlaceholdersTest, ErrorWhenUnexpectedOutputQType) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x"));
  auto expr = Placeholder("x");
  EXPECT_THAT(ResolvePlaceholders(sig, expr, GetQType<OptionalUnit>()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected output qtype OPTIONAL_UNIT, but got QTYPE"));
}

TEST(MakeInputQTypeSequenceTest, FromExprAttributes) {
  {
    ASSERT_OK_AND_ASSIGN(Sequence seq,
                         MakeInputQTypeSequence(absl::Span<const Attr>{}));
    EXPECT_EQ(seq.value_qtype(), GetQTypeQType());
    EXPECT_THAT(seq.UnsafeSpan<QTypePtr>(), ElementsAre());
  }
  {
    ASSERT_OK_AND_ASSIGN(Sequence seq, MakeInputQTypeSequence(
                                           {Attr{GetQType<int>()},
                                            Attr{GetQType<float>()}, Attr{}}));
    EXPECT_EQ(seq.value_qtype(), GetQTypeQType());
    EXPECT_THAT(
        seq.UnsafeSpan<QTypePtr>(),
        ElementsAre(GetQType<int>(), GetQType<float>(), GetNothingQType()));
  }
  {
    auto nothing_qtype = GetNothingQType();
    EXPECT_THAT(
        MakeInputQTypeSequence({Attr{GetQType<int>()}, Attr{nothing_qtype}}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("inputs of type NOTHING are unsupported")));
  }
}

TEST(MakeInputQTypeSequenceTest, FromExprNodes) {
  {
    ASSERT_OK_AND_ASSIGN(
        Sequence seq,
        MakeInputQTypeSequence({Literal(1), Literal(1.5f), Leaf("z")}));
    EXPECT_EQ(seq.value_qtype(), GetQTypeQType());
    EXPECT_THAT(
        seq.UnsafeSpan<QTypePtr>(),
        ElementsAre(GetQType<int>(), GetQType<float>(), GetNothingQType()));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto nothing_node,
                         CallOp(QTypeAnnotation::Make(),
                                {Leaf("_"), Literal(GetNothingQType())}));
    EXPECT_THAT(MakeInputQTypeSequence({Literal(1), nothing_node}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("inputs of type NOTHING are unsupported")));
  }
}

TEST(ReplacePlaceholdersWithLeavesTest, SinglePlaceholder) {
  auto expr = Placeholder("x");
  ASSERT_OK_AND_ASSIGN(auto result, ReplacePlaceholdersWithLeaves(expr));
  EXPECT_TRUE(result->is_leaf());
  EXPECT_EQ(result->leaf_key(), "x");
}

TEST(ReplacePlaceholdersWithLeavesTest, MultiplePlaceholders) {
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp("core.make_tuple",
                                         {Placeholder("a"), Placeholder("b")}));
  ASSERT_OK_AND_ASSIGN(auto result, ReplacePlaceholdersWithLeaves(expr));
  ASSERT_EQ(result->node_deps().size(), 2);
  EXPECT_TRUE(result->node_deps()[0]->is_leaf());
  EXPECT_EQ(result->node_deps()[0]->leaf_key(), "a");
  EXPECT_TRUE(result->node_deps()[1]->is_leaf());
  EXPECT_EQ(result->node_deps()[1]->leaf_key(), "b");
}

TEST(ReplacePlaceholdersWithLeavesTest, NoPlaceholders) {
  auto expr = Literal(42);
  ASSERT_OK_AND_ASSIGN(auto result, ReplacePlaceholdersWithLeaves(expr));
  EXPECT_EQ(result, expr);
}

TEST(ReplacePlaceholdersWithLeavesTest, ErrorWhenLeafAlreadyPresent) {
  auto expr = Leaf("x");
  EXPECT_THAT(
      ReplacePlaceholdersWithLeaves(expr),
      StatusIs(absl::StatusCode::kInvalidArgument,
               AllOf(HasSubstr("expected no leaf nodes"), HasSubstr("L.x"))));
}

}  // namespace
}  // namespace arolla::operator_loader
