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
#include "arolla/expr/operator_loader/dispatch_operator.h"

#include <cstdint>
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
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/expr/operator_loader/restricted_lambda_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/repr_token_eq.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::GetNothingQType;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::LambdaOperator;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::testing::EqualsAttr;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::ReprTokenEq;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::HasSubstr;
using Attr = ::arolla::expr::ExprAttributes;

class DispatchOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }

  static absl::StatusOr<expr::ExprNodePtr> arg_first() {
    return CallOp("core.get_nth", {Leaf("input_tuple_qtype"), Literal(0)});
  }

  static absl::StatusOr<expr::ExprNodePtr> arg_second() {
    return CallOp("core.get_nth", {Leaf("input_tuple_qtype"), Literal(1)});
  }

  static absl::StatusOr<expr::ExprNodePtr> arg_first_qtype() {
    return CallOp("qtype.get_field_qtype",
                  {Leaf("input_tuple_qtype"), Literal(0)});
  }

  static absl::StatusOr<expr::ExprNodePtr> args_from_second_qtype() {
    return CallOp("qtype.slice_tuple_qtype",
                  {Leaf("input_tuple_qtype"), Literal(1), Literal(-1)});
  }

  static absl::StatusOr<std::shared_ptr<const LambdaOperator>>
  MakeBaseBinaryOp() {
    return expr::MakeLambdaOperator(
        "with_name", ExprOperatorSignature{{"x"}, {"name"}},
        SuppressUnusedWarning("name", Placeholder("x")),
        "doc-string-for-lambda");
  }

  static absl::StatusOr<QTypeConstraint> MakeBaseBinaryQTypeConstraint() {
    ASSIGN_OR_RETURN(auto predicate_expr,
                     CallOp("core.equal",
                            {Placeholder("name"), Literal(GetQType<Bytes>())}));
    return QTypeConstraint{predicate_expr,
                           "expected name to be bytes, got {name}"};
  }

  static absl::StatusOr<std::shared_ptr<const RestrictedLambdaOperator>>
  MakeBinaryOp() {
    ASSIGN_OR_RETURN(auto lambda_op, MakeBaseBinaryOp());
    ASSIGN_OR_RETURN(auto qtype_constraint, MakeBaseBinaryQTypeConstraint());
    ASSIGN_OR_RETURN(auto restricted_lambda_op,
                     RestrictedLambdaOperator::Make(
                         std::move(lambda_op), {std::move(qtype_constraint)}));
    return std::dynamic_pointer_cast<const RestrictedLambdaOperator>(
        restricted_lambda_op);
  }

  static absl::StatusOr<std::shared_ptr<const LambdaOperator>>
  MakeBaseUnaryOp() {
    return expr::MakeLambdaOperator("noop", ExprOperatorSignature{{"x"}},
                                    Placeholder("x"),
                                    "doc-string-for-unary-case");
  }

  static absl::StatusOr<QTypeConstraint> MakeBaseUnaryQTypeConstraint() {
    ASSIGN_OR_RETURN(auto predicate_expr,
                     CallOp("qtype.is_numeric_qtype", {Placeholder("x")}));
    return QTypeConstraint{predicate_expr, "expected x to be numeric, got {x}"};
  }

  static absl::StatusOr<std::shared_ptr<const RestrictedLambdaOperator>>
  MakeUnaryOp() {
    ASSIGN_OR_RETURN(auto lambda_op, MakeBaseUnaryOp());
    ASSIGN_OR_RETURN(auto qtype_constraint, MakeBaseUnaryQTypeConstraint());
    ASSIGN_OR_RETURN(auto restricted_lambda_op,
                     RestrictedLambdaOperator::Make(
                         std::move(lambda_op), {std::move(qtype_constraint)}));

    return std::dynamic_pointer_cast<const RestrictedLambdaOperator>(
        restricted_lambda_op);
  }

  // returns a constraint validating that *args tuple has no fields
  static absl::StatusOr<expr::ExprNodePtr> MakeUnaryCondition() {
    auto one_argument =
        CallOp("core.equal",
               {CallOp("qtype.get_field_count", {args_from_second_qtype()}),
                Literal(0)});
    auto is_numeric = CallOp("qtype.is_scalar_qtype", {arg_first_qtype()});
    return CallOp("core.presence_and", {one_argument, is_numeric});
  }

  static absl::StatusOr<expr::ExprNodePtr> MakeDispatchReadinessCondition(
      const std::vector<int64_t> ids) {
    auto expr = CallOp("core.not_equal",
                       {Leaf("input_tuple_qtype"), Literal(GetNothingQType())});
    for (auto id : ids) {
      auto additional_expr = CallOp(
          "core.not_equal", {CallOp("qtype.get_field_qtype",
                                    {Leaf("input_tuple_qtype"), Literal(id)}),
                             Literal(GetNothingQType())});
      expr = CallOp("core.presence_and", {expr, additional_expr});
    }
    return expr;
  }

  static absl::StatusOr<std::shared_ptr<const DispatchOperator>> MakeOp() {
    ASSIGN_OR_RETURN(auto binary_op, MakeBinaryOp());
    ASSIGN_OR_RETURN(auto unary_op, MakeUnaryOp());
    ASSIGN_OR_RETURN(auto unary_condition, MakeUnaryCondition());
    ASSIGN_OR_RETURN(auto not_unary_condition,
                     CallOp("core.presence_not", {unary_condition}));
    ASSIGN_OR_RETURN(auto readiness_condition,
                     MakeDispatchReadinessCondition({0}));
    ASSIGN_OR_RETURN(
        auto dispatch_op,
        DispatchOperator::Make("op.name",
                               expr::ExprOperatorSignature{
                                   {"x"},
                                   {.name = "args",
                                    .kind = ExprOperatorSignature::Parameter::
                                        Kind::kVariadicPositional}},
                               // Add a special character to test escaping in
                               // error messages and repr.
                               {{.name = "unary\tcase",
                                 .op = std::move(unary_op),
                                 .condition = std::move(unary_condition)},
                                {.name = "default",
                                 .op = std::move(binary_op),
                                 .condition = std::move(not_unary_condition)}},
                               readiness_condition));
    return std::dynamic_pointer_cast<const DispatchOperator>(dispatch_op);
  }

  static absl::StatusOr<std::shared_ptr<const DispatchOperator>>
  MakeOpNoDefault() {
    ASSIGN_OR_RETURN(auto no_op, MakeBaseUnaryOp());
    ASSIGN_OR_RETURN(auto unary_op, MakeUnaryOp());
    ASSIGN_OR_RETURN(auto unary_condition, MakeUnaryCondition());
    ASSIGN_OR_RETURN(auto readiness_condition,
                     MakeDispatchReadinessCondition({0}));
    ASSIGN_OR_RETURN(
        auto dispatch_op,
        DispatchOperator::Make("op.name",
                               expr::ExprOperatorSignature{
                                   {"x"},
                                   {.name = "args",
                                    .kind = ExprOperatorSignature::Parameter::
                                        Kind::kVariadicPositional}},
                               // Add a special character to test escaping in
                               // error messages and repr.
                               {{.name = "unary\tcase",
                                 .op = std::move(unary_op),
                                 .condition = std::move(unary_condition)}},
                               readiness_condition));
    return std::dynamic_pointer_cast<const DispatchOperator>(dispatch_op);
  }

  static absl::StatusOr<std::shared_ptr<const DispatchOperator>>
  MakeDuplicatedOp() {
    ASSIGN_OR_RETURN(auto binary_op, MakeBinaryOp());
    ASSIGN_OR_RETURN(auto unary_op_a, MakeUnaryOp());
    ASSIGN_OR_RETURN(auto unary_op_b, MakeUnaryOp());
    ASSIGN_OR_RETURN(auto unary_condition_a, MakeUnaryCondition());
    ASSIGN_OR_RETURN(auto unary_condition_b, MakeUnaryCondition());
    ASSIGN_OR_RETURN(auto not_unary_condition,
                     CallOp("core.presence_not", {unary_condition_a}));
    ASSIGN_OR_RETURN(auto readiness_condition,
                     MakeDispatchReadinessCondition({0}));
    ASSIGN_OR_RETURN(
        auto dispatch_op,
        DispatchOperator::Make("op.name",
                               expr::ExprOperatorSignature{
                                   {"x"},
                                   {.name = "args",
                                    .kind = ExprOperatorSignature::Parameter::
                                        Kind::kVariadicPositional}},
                               {{.name = "unary_case_a",
                                 .op = std::move(unary_op_a),
                                 .condition = std::move(unary_condition_a)},
                                {.name = "unary_case_b",
                                 .op = std::move(unary_op_b),
                                 .condition = std::move(unary_condition_b)},
                                {.name = "binary_case",
                                 .op = std::move(binary_op),
                                 .condition = std::move(not_unary_condition)}},
                               readiness_condition));
    return std::dynamic_pointer_cast<const DispatchOperator>(dispatch_op);
  }
};

}  // namespace

TEST_F(DispatchOperatorTest, PublicProperties) {
  ASSERT_OK_AND_ASSIGN(auto op, MakeOp());
  EXPECT_EQ(op->display_name(), "op.name");
  EXPECT_EQ(op->doc(), "");
}

TEST_F(DispatchOperatorTest, InferAttributes) {
  ASSERT_OK_AND_ASSIGN(auto op, MakeOp());
  EXPECT_THAT(op->InferAttributes({Attr{}}), IsOkAndHolds(EqualsAttr(nullptr)));
  EXPECT_THAT(op->InferAttributes({Attr{}, Attr{}}),
              IsOkAndHolds(EqualsAttr(nullptr)));
  EXPECT_THAT(op->InferAttributes({Attr{}, Attr{}, Attr{}}),
              IsOkAndHolds(EqualsAttr(nullptr)));
  EXPECT_THAT(op->InferAttributes({Attr(GetQType<int>()), Attr{}, Attr{}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "incorrect number of dependencies passed to an "
                       "operator node: expected 2 but got 3; in default "
                       "overload of DispatchOperator"));
  EXPECT_THAT(op->InferAttributes({}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "incorrect number of dependencies passed to an "
                       "operator node: expected 1 but got 0"));
  EXPECT_THAT(op->InferAttributes({Attr(GetQType<Bytes>())}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected x to be numeric, got BYTES; in unary\\tcase "
                       "overload of DispatchOperator"));
  EXPECT_THAT(op->InferAttributes({Attr(GetQType<int>())}),
              IsOkAndHolds(EqualsAttr(GetQType<int>())));
  EXPECT_THAT(op->InferAttributes({Attr(GetQType<int>()), Attr{}}),
              IsOkAndHolds(EqualsAttr(nullptr)));
  EXPECT_THAT(
      op->InferAttributes({Attr(GetQType<int>()), Attr(GetQType<Bytes>())}),
      IsOkAndHolds(EqualsAttr(GetQType<int>())));
}

TEST_F(DispatchOperatorTest, InferAttributesNoDefault) {
  ASSERT_OK_AND_ASSIGN(auto op, MakeOpNoDefault());
  EXPECT_THAT(op->InferAttributes({Attr{}}), IsOkAndHolds(EqualsAttr(nullptr)));
  EXPECT_THAT(op->InferAttributes({Attr{}, Attr{}}),
              IsOkAndHolds(EqualsAttr(nullptr)));
  EXPECT_THAT(
      op->InferAttributes({Attr(GetQType<int>()), Attr{}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "no suitable overload for argument types (INT32,NOTHING)"));
  EXPECT_THAT(op->InferAttributes({}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "incorrect number of dependencies passed to an "
                       "operator node: expected 1 but got 0"));
  EXPECT_THAT(op->InferAttributes({Attr(GetQType<Bytes>())}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected x to be numeric, got BYTES; in unary\\tcase "
                       "overload of DispatchOperator"));
  EXPECT_THAT(op->InferAttributes({Attr(GetQType<int>())}),
              IsOkAndHolds(EqualsAttr(GetQType<int>())));
}

TEST_F(DispatchOperatorTest, SignatureWithDefaultValues) {
  ASSERT_OK_AND_ASSIGN(auto binary_op, MakeBinaryOp());
  ASSERT_OK_AND_ASSIGN(auto unary_op, MakeUnaryOp());
  ASSERT_OK_AND_ASSIGN(auto readiness_condition,
                       MakeDispatchReadinessCondition({}));

  ASSERT_OK_AND_ASSIGN(auto predicate_expr_xx,
                       CallOp("core.equal", {arg_first(), arg_first()}));

  EXPECT_THAT(
      DispatchOperator::Make("op",
                             expr::ExprOperatorSignature{
                                 {.name = "x",
                                  .default_value = TypedValue::FromValue(false),
                                  .kind = ExprOperatorSignature::Parameter::
                                      Kind::kPositionalOrKeyword}},
                             {{.name = "foo",
                               .op = std::move(binary_op),
                               .condition = std::move(predicate_expr_xx)}},
                             readiness_condition),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "signatures with the default values are not supported in "
               "dispatch operator; got signature: x="));
}

TEST_F(DispatchOperatorTest, ToLowerLevel) {
  auto leaf = Leaf("leaf");
  ASSERT_OK_AND_ASSIGN(auto leaf_with_qtype,
                       WithQTypeAnnotation(Leaf("leaf"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto leaf_with_nothing_qtype,
                       WithQTypeAnnotation(Leaf("leaf"), GetNothingQType()));
  auto name_literal = Literal(Bytes("name"));
  auto name_placeholder = Placeholder("name");
  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(MakeOp(), {leaf}));
    ASSERT_OK_AND_ASSIGN(auto noop_leaf, CallOp(MakeUnaryOp(), {leaf}));
    EXPECT_EQ(expr->qtype(), nullptr);
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(MakeOp(), {leaf, name_placeholder}));
    ASSERT_OK_AND_ASSIGN(auto binary_op,
                         CallOp(MakeBinaryOp(), {leaf, name_placeholder}));
    EXPECT_EQ(expr->qtype(), nullptr);
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(MakeOp(), {leaf, name_literal}));
    ASSERT_OK_AND_ASSIGN(auto binary_op,
                         CallOp(MakeBinaryOp(), {leaf, name_literal}));
    EXPECT_EQ(expr->qtype(), nullptr);
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(MakeOp(), {leaf_with_qtype}));
    EXPECT_EQ(expr->qtype(), GetQType<float>());
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(leaf_with_qtype)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(MakeOp(), {leaf_with_qtype, name_placeholder}));
    ASSERT_OK_AND_ASSIGN(
        auto binary_op,
        CallOp(MakeBinaryOp(), {leaf_with_qtype, name_placeholder}));
    EXPECT_EQ(expr->qtype(), nullptr);
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(binary_op)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(MakeOp(), {leaf_with_qtype, name_literal}));
    EXPECT_EQ(expr->qtype(), GetQType<float>());
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(leaf_with_qtype)));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto expr, CallOp(MakeDuplicatedOp(), {leaf_with_qtype, name_literal}));
    EXPECT_EQ(expr->qtype(), GetQType<float>());
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(leaf_with_qtype)));
  }
  {
    EXPECT_THAT(
        CallOp(MakeDuplicatedOp(), {leaf_with_qtype}),
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            HasSubstr("constraints of the multiple overloads (unary_case_a, "
                      "unary_case_b) passed for argument types (FLOAT32)")));
  }
  {
    EXPECT_THAT(CallOp(MakeOp(), {}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "missing 1 required argument: 'x'; while binding "
                         "operator 'op.name'"));
  }
  {
    EXPECT_THAT(
        CallOp(MakeOp(), {leaf_with_nothing_qtype}),
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            HasSubstr("the operator is broken for argument types (NOTHING)")));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(MakeOp(), {leaf_with_nothing_qtype, leaf}));
    ASSERT_OK_AND_ASSIGN(auto binary_op,
                         CallOp(MakeBinaryOp(), {leaf, name_literal}));
    EXPECT_EQ(expr->qtype(), nullptr);
    EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    EXPECT_THAT(CallOp(MakeOp(), {leaf_with_nothing_qtype, leaf_with_qtype}),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         HasSubstr("the operator is broken for argument types "
                                   "(NOTHING,FLOAT32)")));
  }
}

TEST_F(DispatchOperatorTest, Repr) {
  ASSERT_OK_AND_ASSIGN(auto op, MakeOp());
  EXPECT_THAT(op->GenReprToken(),
              ReprTokenEq("<DispatchOperator: name='op.name', signature='x, "
                          "*args', cases=['unary\\tcase', 'default']>"));
}

}  // namespace arolla::operator_loader
