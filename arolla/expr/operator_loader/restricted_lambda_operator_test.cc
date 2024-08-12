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
#include "arolla/expr/operator_loader/restricted_lambda_operator.h"

#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::LambdaOperator;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::expr::SuppressUnusedWarning;
using ::arolla::testing::EqualsAttr;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;
using Attr = ::arolla::expr::ExprAttributes;

class RestrictedLambdaOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }

  static absl::StatusOr<std::shared_ptr<LambdaOperator>> MakeBaseLambdaOp() {
    return expr::MakeLambdaOperator(
        "with_name", ExprOperatorSignature{{"x"}, {"name"}},
        SuppressUnusedWarning("name", Placeholder("x")),
        "doc-string-for-lambda");
  }

  static absl::StatusOr<QTypeConstraint> MakeQTypeConstraint() {
    ASSIGN_OR_RETURN(
        auto predicate_expr,
        CallOp("core.equal", {Placeholder("name"), Literal(GetQType<Text>())}));
    return QTypeConstraint{predicate_expr,
                           "expected name to be TEXT, got {name}"};
  }

  static absl::StatusOr<std::shared_ptr<const RestrictedLambdaOperator>>
  MakeOp() {
    ASSIGN_OR_RETURN(auto lambda_op, MakeBaseLambdaOp());
    ASSIGN_OR_RETURN(auto qtype_constraint, MakeQTypeConstraint());
    ASSIGN_OR_RETURN(auto restricted_lambda_op,
                     RestrictedLambdaOperator::Make(
                         std::move(lambda_op), {std::move(qtype_constraint)}));

    return std::dynamic_pointer_cast<const RestrictedLambdaOperator>(
        restricted_lambda_op);
  }
};

}  // namespace

TEST_F(RestrictedLambdaOperatorTest, PublicProperties) {
  ASSERT_OK_AND_ASSIGN(auto base_lambda_op, MakeBaseLambdaOp());
  ASSERT_OK_AND_ASSIGN(auto qtype_constraint, MakeQTypeConstraint());
  ASSERT_OK_AND_ASSIGN(auto op, MakeOp());
  EXPECT_EQ(op->display_name(), "with_name");
  EXPECT_EQ(op->doc(), "doc-string-for-lambda");
  EXPECT_EQ(op->base_lambda_operator()->fingerprint(),
            base_lambda_op->fingerprint());
  EXPECT_EQ(op->qtype_constraints().size(), 1);
  EXPECT_EQ(op->qtype_constraints()[0].error_message,
            qtype_constraint.error_message);
  EXPECT_THAT(op->qtype_constraints()[0].predicate_expr,
              EqualsExpr(qtype_constraint.predicate_expr));
}

TEST_F(RestrictedLambdaOperatorTest, UnexpectedParameter) {
  ASSERT_OK_AND_ASSIGN(auto base_lambda_op, MakeBaseLambdaOp());
  ASSERT_OK_AND_ASSIGN(auto qtype_constraint0, MakeQTypeConstraint());
  auto new_parameter = Placeholder("new_parameter");
  QTypeConstraint qtype_constraint1 = {new_parameter, "new_message"};
  EXPECT_THAT(
      RestrictedLambdaOperator::Make(std::move(base_lambda_op),
                                     {qtype_constraint0, qtype_constraint1}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "unexpected parameters: P.new_parameter"));
}

TEST_F(RestrictedLambdaOperatorTest, InferAttributes) {
  ASSERT_OK_AND_ASSIGN(auto op, MakeOp());
  EXPECT_THAT(op->InferAttributes({Attr{}, Attr{}}),
              IsOkAndHolds(EqualsAttr(nullptr)));
  EXPECT_THAT(op->InferAttributes({Attr{}, Attr(GetQType<Text>())}),
              IsOkAndHolds(EqualsAttr(nullptr)));
  EXPECT_THAT(
      op->InferAttributes({Attr(GetQType<Unit>()), Attr(GetQType<Text>())}),
      IsOkAndHolds(EqualsAttr(GetQType<Unit>())));
  EXPECT_THAT(op->InferAttributes({Attr{}, Attr(GetQType<Bytes>())}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected name to be TEXT, got BYTES"));
}

TEST_F(RestrictedLambdaOperatorTest, ToLowerLevel) {
  auto leaf = Leaf("leaf");
  ASSERT_OK_AND_ASSIGN(auto leaf_with_qtype,
                       WithQTypeAnnotation(Leaf("leaf"), GetQType<float>()));
  auto name_literal = Literal(Text("name"));
  auto name_placeholder = Placeholder("name");

  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(MakeOp(), {leaf, name_placeholder}));
    EXPECT_EQ(expr->qtype(), nullptr);
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(MakeOp(), {leaf, name_literal}));
    EXPECT_EQ(expr->qtype(), nullptr);
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(MakeOp(), {leaf_with_qtype, name_placeholder}));
    EXPECT_EQ(expr->qtype(), nullptr);
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(MakeOp(), {leaf_with_qtype, name_literal}));
    EXPECT_EQ(expr->qtype(), GetQType<float>());
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(leaf_with_qtype)));
  }
}

TEST_F(RestrictedLambdaOperatorTest, QValuePropagation) {
  ASSERT_OK_AND_ASSIGN(const auto expr,
                       CallOp(MakeOp(), {Literal(1), Literal(Text("abc"))}));
  EXPECT_THAT(expr->attr(), EqualsAttr(TypedRef::FromValue(1)));
}

}  // namespace arolla::operator_loader
