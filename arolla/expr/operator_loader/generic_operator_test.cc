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
#include "arolla/expr/operator_loader/generic_operator.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/util/unit.h"

namespace arolla::operator_loader {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeLambdaOperator;
using ::arolla::expr::Placeholder;
using ::arolla::expr::SuppressUnusedWarning;
using ::arolla::expr::ToLowest;
using ::arolla::expr::testing::DummyOp;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::TypedValueWith;
using ::testing::HasSubstr;
using ::testing::Optional;
using ::testing::Truly;

auto EqualsAttr(const ExprAttributes& expected_attr) {
  return Truly([expected_attr](const ExprAttributes& actual_attr) {
    return actual_attr.IsIdenticalTo(expected_attr);
  });
}

class GenericOperatorOverloadTest : public ::testing::Test {
 protected:
  static absl::StatusOr<ExprOperatorPtr> GetFirstOperator() {
    return MakeLambdaOperator(
        "get_left", ExprOperatorSignature{{"left"}, {"right"}},
        SuppressUnusedWarning("right", Placeholder("left")),
        "doc-string-for-get-left");
  }
};

TEST_F(GenericOperatorOverloadTest, Make) {
  ASSERT_OK_AND_ASSIGN(auto base_operator, GetFirstOperator());
  ASSERT_OK_AND_ASSIGN(
      auto prepared_overload_condition_expr,
      CallOp("core.not_equal",
             {CallOp("core.get_nth", {Leaf("input_tuple_qtype"), Literal(0)}),
              Literal(GetNothingQType())}));
  ASSERT_OK_AND_ASSIGN(
      auto op, GenericOperatorOverload::Make(base_operator,
                                             prepared_overload_condition_expr));
  EXPECT_EQ(op->base_operator(), base_operator);
  EXPECT_EQ(op->prepared_overload_condition_expr().get(),
            prepared_overload_condition_expr.get());
  EXPECT_EQ(op->display_name(), "get_left");
  EXPECT_THAT(op->GetDoc(), IsOkAndHolds("doc-string-for-get-left"));
  EXPECT_THAT(op->InferAttributes({ExprAttributes{}, ExprAttributes{}}),
              IsOkAndHolds(EqualsAttr(ExprAttributes{})));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Leaf("y")}));
  EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(Leaf("x"))));
}

TEST_F(GenericOperatorOverloadTest, Make_ErrorUnexpectedPlaceholder) {
  ASSERT_OK_AND_ASSIGN(auto base_operator, GetFirstOperator());
  ASSERT_OK_AND_ASSIGN(
      auto prepared_overload_condition_expr,
      CallOp("core.not_equal", {Placeholder("left"), Placeholder("right")}));
  EXPECT_THAT(GenericOperatorOverload::Make(base_operator,
                                            prepared_overload_condition_expr),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "prepared overload condition contains unexpected "
                       "placeholders: P.left, P.right"));
}

TEST_F(GenericOperatorOverloadTest, Make_ErrorUnexpectedLeaves) {
  ASSERT_OK_AND_ASSIGN(auto base_operator, GetFirstOperator());
  ASSERT_OK_AND_ASSIGN(
      auto prepared_overload_condition_expr,
      CallOp("core.make_tuple",
             {Leaf("input_tuple_qtype"), Leaf("left"), Leaf("right")}));
  EXPECT_THAT(GenericOperatorOverload::Make(base_operator,
                                            prepared_overload_condition_expr),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "prepared overload condition contains unexpected "
                       "leaves: L.left, L.right"));
}

TEST_F(GenericOperatorOverloadTest, ToLowerLevelOptimization) {
  auto base_operator =
      std::make_shared<DummyOp>("base_op", ExprOperatorSignature{{"x"}, {"y"}});
  ASSERT_OK_AND_ASSIGN(
      auto prepared_overload_condition_expr,
      CallOp("core.not_equal",
             {CallOp("core.get_nth", {Leaf("input_tuple_qtype"), Literal(0)}),
              Literal(GetNothingQType())}));
  ASSERT_OK_AND_ASSIGN(
      auto op, GenericOperatorOverload::Make(base_operator,
                                             prepared_overload_condition_expr));
  auto expr = ExprNode::UnsafeMakeOperatorNode(
      op, {Leaf("x"), Leaf("y")}, ExprAttributes(GetQType<float>()));
  auto expected_expr = ExprNode::UnsafeMakeOperatorNode(
      base_operator, {Leaf("x"), Leaf("y")}, ExprAttributes(GetQType<float>()));
  // Clarification: If ToLowerLevel invoked InferAttributes, the assigned expr
  // attributes would be changed.
  EXPECT_THAT(ToLowest(expr), IsOkAndHolds(EqualsExpr(expected_expr)));
}

TEST_F(GenericOperatorOverloadTest, BadBaseOperatorNullptr) {
  EXPECT_THAT(
      GenericOperatorOverload::Make(nullptr, Literal(OptionalUnit(false))),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(GenericOperatorOverloadTest, BadConditionExprNullptr) {
  ASSERT_OK_AND_ASSIGN(auto op, MakeLambdaOperator(Placeholder("x")));
  EXPECT_THAT(GenericOperatorOverload::Make(op, nullptr),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(GenericOperatorTest, CommonCase) {
  ASSERT_OK_AND_ASSIGN(
      auto base_op_1,
      MakeLambdaOperator("generic_operator_test.common_case.is_unit._.negative",
                         ExprOperatorSignature{{"_x"}},
                         Literal(OptionalUnit(false))));
  ASSERT_OK_AND_ASSIGN(
      auto prepared_condition_1,
      CallOp("core.presence_and",
             {CallOp("core.not_equal",
                     {CallOp("qtype.get_field_qtype",
                             {Leaf("input_tuple_qtype"), Literal(0)}),
                      Literal(GetNothingQType())}),
              CallOp("core.not_equal",
                     {CallOp("qtype.get_field_qtype",
                             {Leaf("input_tuple_qtype"), Literal(0)}),
                      Literal(GetQType<Unit>())})}));
  ASSERT_OK_AND_ASSIGN(auto op_1, GenericOperatorOverload::Make(
                                      base_op_1, prepared_condition_1));
  ASSERT_OK_AND_ASSIGN(
      auto base_op_2,
      MakeLambdaOperator("generic_operator_test.common_case.is_unit._.positive",
                         ExprOperatorSignature{{"_x"}},
                         Literal(OptionalUnit(true))));
  ASSERT_OK_AND_ASSIGN(
      auto prepared_condition_2,
      CallOp("core.equal", {CallOp("qtype.get_field_qtype",
                                   {Leaf("input_tuple_qtype"), Literal(0)}),
                            Literal(GetQType<Unit>())}));
  ASSERT_OK_AND_ASSIGN(auto op_2, GenericOperatorOverload::Make(
                                      base_op_2, prepared_condition_2));
  ASSERT_OK(RegisterOperator(
      "generic_operator_test.common_case.is_unit._.negative", op_1));
  ASSERT_OK(RegisterOperator(
      "generic_operator_test.common_case.is_unit._.positive", op_2));
  ASSERT_OK_AND_ASSIGN(auto op, GenericOperator::Make(
                                    "generic_operator_test.common_case.is_unit",
                                    ExprOperatorSignature{{"x"}},
                                    /*doc=*/"doc-string"));

  EXPECT_EQ(op->display_name(), "generic_operator_test.common_case.is_unit");
  EXPECT_EQ(op->namespace_for_overloads(),
            "generic_operator_test.common_case.is_unit");
  EXPECT_EQ(op->doc(), "doc-string");

  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x")}));
    EXPECT_EQ(expr->qtype(), nullptr);
    EXPECT_THAT(ToLowerNode(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Literal(Unit())}));
    ASSERT_OK_AND_ASSIGN(
        auto expected_lower_node,
        CallOp("generic_operator_test.common_case.is_unit._.positive",
               {Literal(Unit())}));
    EXPECT_THAT(expr->qvalue(),
                Optional(TypedValueWith<OptionalUnit>(OptionalUnit(true))));
    EXPECT_THAT(ToLowerNode(expr),
                IsOkAndHolds(EqualsExpr(expected_lower_node)));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Literal(1)}));
    ASSERT_OK_AND_ASSIGN(
        auto expected_lower_node,
        CallOp("generic_operator_test.common_case.is_unit._.negative",
               {Literal(1)}));
    EXPECT_THAT(expr->qvalue(),
                Optional(TypedValueWith<OptionalUnit>(OptionalUnit(false))));
    EXPECT_THAT(ToLowerNode(expr),
                IsOkAndHolds(EqualsExpr(expected_lower_node)));
  }
}

TEST(GenericOperatorTest, BadSignature) {
  ExprOperatorSignature sig{{"x"}, {"x"}};
  EXPECT_THAT(GenericOperator::Make("foo", sig, ""),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(GenericOperatorTest, BadNamespace) {
  ExprOperatorSignature sig{{"x"}};
  EXPECT_THAT(GenericOperator::Make("///", sig, ""),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(GenericOperatorTest, FailOverloadMatch) {
  ASSERT_OK_AND_ASSIGN(
      auto base_op,
      MakeLambdaOperator("generic_operator_test.fail_overload_match.op._n",
                         Placeholder("x")));
  ASSERT_OK_AND_ASSIGN(
      auto prepared_condition,
      CallOp("core.presence_and",
             {CallOp("core.not_equal",
                     {CallOp("qtype.get_field_qtype",
                             {Leaf("input_tuple_qtype"), Literal(0)}),
                      Literal(GetNothingQType())}),
              CallOp("core.not_equal",
                     {CallOp("qtype.get_field_qtype",
                             {Leaf("input_tuple_qtype"), Literal(0)}),
                      Literal(GetQType<Unit>())})}));
  ASSERT_OK_AND_ASSIGN(
      auto op_n, GenericOperatorOverload::Make(base_op, prepared_condition));
  ASSERT_OK(RegisterOperator("generic_operator_test.fail_overload_match.op._1",
                             op_n));
  ASSERT_OK(RegisterOperator("generic_operator_test.fail_overload_match.op._2",
                             op_n));
  ASSERT_OK(RegisterOperator("generic_operator_test.fail_overload_match.op._3",
                             op_n));
  ASSERT_OK_AND_ASSIGN(
      auto op,
      GenericOperator::Make("generic_operator_test.fail_overload_match.op",
                            ExprOperatorSignature{{"x"}}, /*doc=*/""));
  {
    ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x")}));
    EXPECT_EQ(expr->qtype(), nullptr);
    EXPECT_THAT(ToLowerNode(expr), IsOkAndHolds(EqualsExpr(expr)));
  }
  {
    EXPECT_THAT(CallOp(op, {Literal(Unit())}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("no matching overload [x: UNIT]")));
  }
  {
    EXPECT_THAT(
        CallOp(op, {Literal(1)}),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr(
                "ambiguous overloads: "
                "generic_operator_test.fail_overload_match.op._1, "
                "generic_operator_test.fail_overload_match.op._2, "
                "generic_operator_test.fail_overload_match.op._3 [x: INT32]")));
  }
}

TEST(GenericOperatorTest, BadOverloadOperator) {
  ASSERT_OK_AND_ASSIGN(
      auto op_n, MakeLambdaOperator("generic_operator_test.bad_overload.op._n",
                                    Placeholder("x")));
  ASSERT_OK(RegisterOperator("generic_operator_test.bad_overload.op._n", op_n));
  ASSERT_OK_AND_ASSIGN(
      auto op, GenericOperator::Make("generic_operator_test.bad_overload.op",
                                     ExprOperatorSignature{{"x"}},
                                     /*doc=*/""));
  {
    EXPECT_THAT(
        CallOp(op, {Literal(Unit())}),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr("expected a GenericOperatorOverload, got "
                           "arolla::expr::LambdaOperator: "
                           "generic_operator_test.bad_overload.op._n")));
  }
}

TEST(GenericOperatorTest, FormatSignatureQTypes) {
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x, y, *z"));
  ASSERT_OK_AND_ASSIGN(
      auto op,
      GenericOperator::Make("generic_operator_test.format_sig_qtypes", sig,
                            /*doc=*/""));
  EXPECT_OK(CallOp(op, {Leaf("x"), Leaf("x")}));
  EXPECT_THAT(
      CallOp(op, {Literal(Unit()), Literal(1)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("no matching overload [x: UNIT, y: INT32, *z: ()]")));
  EXPECT_THAT(
      CallOp(op, {Literal(Unit()), Literal(1), Literal(1.5f)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr(
                   "no matching overload [x: UNIT, y: INT32, *z: (FLOAT32)]")));
  EXPECT_THAT(
      CallOp(op, {Literal(Unit()), Literal(1), Literal(1.5f)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr(
                   "no matching overload [x: UNIT, y: INT32, *z: (FLOAT32)]")));
  EXPECT_THAT(
      CallOp(op, {Literal(Unit()), Literal(1), Literal(1.5f), Literal(2.5)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("no matching overload [x: UNIT, y: INT32, *z: "
                         "(FLOAT32, FLOAT64)]")));
}

}  // namespace
}  // namespace arolla::operator_loader
