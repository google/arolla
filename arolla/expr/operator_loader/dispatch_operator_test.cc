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
#include "arolla/expr/operator_loader/dispatch_operator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/testing/repr_token_eq.h"
#include "arolla/util/testing/status_matchers.h"

namespace arolla::operator_loader {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::_;
using ::testing::AllOf;
using ::testing::HasSubstr;
using ::testing::Return;
using ::testing::Truly;

using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::testing::CausedBy;
using ::arolla::testing::EqualsAttr;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::MockExprOperator;
using ::arolla::testing::ReprTokenEq;

using Attr = ::arolla::expr::ExprAttributes;
using Kind = ::arolla::expr::ExprOperatorSignature::Parameter::Kind;

TEST(DispatchOperatorTest, Basics) {
  auto case_1_base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*case_1_base_op, InferAttributes(_))
      .WillRepeatedly(Return(Attr{GetQType<int64_t>()}));
  EXPECT_CALL(*case_1_base_op, ToLowerLevel(Truly([&](const ExprNodePtr& node) {
    return node->op() == case_1_base_op;
  }))).WillRepeatedly([&](const ExprNodePtr& node) { return node; });
  ASSERT_OK_AND_ASSIGN(  // NOTE: Condition refers parameters from the dispatch
      auto case_1_condition_expr,  // operator, not from the base operator!
      CallOp("core.equal", {Placeholder("x"), Literal(GetQType<int32_t>())}));
  DispatchOperator::Overload case_1 = {
      .name = "case_1",
      .op = case_1_base_op,
      .condition_expr = case_1_condition_expr,
  };

  auto case_2_base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*case_2_base_op, InferAttributes(_))
      .WillRepeatedly(Return(Attr{GetQType<double>()}));
  EXPECT_CALL(*case_2_base_op, ToLowerLevel(Truly([&](const ExprNodePtr& node) {
    return node->op() == case_2_base_op;
  }))).WillRepeatedly([&](const ExprNodePtr& node) { return node; });
  ASSERT_OK_AND_ASSIGN(
      auto case_2_condition_expr,
      CallOp("core.equal", {Placeholder("x"), Literal(GetQType<float>())}));
  DispatchOperator::Overload case_2 = {
      .name = "case_2",
      .op = case_2_base_op,
      .condition_expr = case_2_condition_expr,
  };

  ASSERT_OK_AND_ASSIGN(
      auto dispatch_op,
      DispatchOperator::Make("dispatch_op", {{"x"}}, "docstring",
                             {case_1, case_2}, nullptr));
  EXPECT_EQ(dispatch_op->display_name(), "dispatch_op");
  EXPECT_EQ(dispatch_op->doc(), "docstring");
  EXPECT_EQ(dispatch_op->signature().parameters.size(), 1);
  EXPECT_EQ(dispatch_op->signature().parameters[0].name, "x");

  EXPECT_EQ(dispatch_op->py_qvalue_specialization_key(),
            "::arolla::operator_loader::DispatchOperator");

  EXPECT_THAT(dispatch_op->overloads().size(), 2);
  EXPECT_EQ(dispatch_op->overloads()[0].name, "case_1");
  EXPECT_EQ(dispatch_op->overloads()[0].op, case_1_base_op);
  EXPECT_EQ(dispatch_op->overloads()[0].condition_expr, case_1_condition_expr);
  EXPECT_EQ(dispatch_op->overloads()[1].name, "case_2");
  EXPECT_EQ(dispatch_op->overloads()[1].op, case_2_base_op);
  EXPECT_EQ(dispatch_op->overloads()[1].condition_expr, case_2_condition_expr);
  EXPECT_EQ(dispatch_op->default_op(), nullptr);

  ASSERT_OK_AND_ASSIGN(auto expr_0, CallOp(dispatch_op, {Leaf("x")}));
  EXPECT_THAT(expr_0->attr(), EqualsAttr(Attr{}));
  EXPECT_THAT(dispatch_op->ToLowerLevel(expr_0),
              IsOkAndHolds(EqualsExpr(expr_0)));

  ASSERT_OK_AND_ASSIGN(auto expr_1, CallOp(dispatch_op, {Literal(int32_t{1})}));
  EXPECT_THAT(expr_1->attr(), EqualsAttr(GetQType<int64_t>()));
  ASSERT_OK_AND_ASSIGN(auto lowered_expr_1, dispatch_op->ToLowerLevel(expr_1));
  EXPECT_EQ(lowered_expr_1->op(), case_1_base_op);

  ASSERT_OK_AND_ASSIGN(auto expr_2, CallOp(dispatch_op, {Literal(float{2})}));
  EXPECT_THAT(expr_2->attr(), EqualsAttr(GetQType<double>()));
  ASSERT_OK_AND_ASSIGN(auto lowered_expr_2, dispatch_op->ToLowerLevel(expr_2));
  EXPECT_EQ(lowered_expr_2->op(), case_2_base_op);

  EXPECT_THAT(
      dispatch_op->InferAttributes({Attr(GetQType<std::string>())}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               AllOf(HasSubstr("no suitable overload or default operator"),
                     HasSubstr("In dispatch operator: 'dispatch_op'."),
                     HasSubstr("Input qtypes: x: BYTES."))));

  EXPECT_THAT(dispatch_op->ToLowerLevel(ExprNode::UnsafeMakeOperatorNode(
                  ExprOperatorPtr{dispatch_op}, {Leaf("x")},
                  Attr{GetNothingQType()})),  // Manually assign output qtype
                                              // attribute.
              IsOk());

  EXPECT_THAT(
      dispatch_op->ToLowerLevel(ExprNode::UnsafeMakeOperatorNode(
          ExprOperatorPtr{dispatch_op}, {Literal(std::string{"foo"})},
          Attr{GetNothingQType()})),  // Manually assign output qtype
                                      // attribute.
      StatusIs(absl::StatusCode::kInvalidArgument,
               AllOf(HasSubstr("no suitable overload or default operator"),
                     HasSubstr("In dispatch operator: 'dispatch_op'."),
                     HasSubstr("Input qtypes: x: BYTES."))));
}

TEST(DispatchOperatorTest, WithDefaultOperator) {
  auto case_1_base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*case_1_base_op, InferAttributes(_))
      .WillRepeatedly(Return(Attr{GetQType<int64_t>()}));
  EXPECT_CALL(*case_1_base_op, ToLowerLevel(Truly([&](const ExprNodePtr& node) {
    return node->op() == case_1_base_op;
  }))).WillRepeatedly([&](const ExprNodePtr& node) { return node; });
  ASSERT_OK_AND_ASSIGN(  // NOTE: Condition refers parameters from the dispatch
      auto case_1_condition_expr,  // operator, not from the base operator!
      CallOp("core.equal", {Placeholder("x"), Literal(GetQType<int32_t>())}));
  DispatchOperator::Overload case_1 = {
      .name = "case_1",
      .op = case_1_base_op,
      .condition_expr = case_1_condition_expr,
  };

  auto default_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*default_op, InferAttributes(_))
      .WillRepeatedly(Return(Attr{GetQType<double>()}));
  EXPECT_CALL(*default_op, ToLowerLevel(Truly([&](const ExprNodePtr& node) {
    return node->op() == default_op;
  }))).WillRepeatedly([&](const ExprNodePtr& node) { return node; });

  ASSERT_OK_AND_ASSIGN(
      auto dispatch_op,
      DispatchOperator::Make("dispatch_op", {{"x"}}, "docstring", {case_1},
                             default_op));

  EXPECT_THAT(dispatch_op->overloads().size(), 1);
  EXPECT_EQ(dispatch_op->overloads()[0].name, "case_1");
  EXPECT_EQ(dispatch_op->overloads()[0].op, case_1_base_op);
  EXPECT_EQ(dispatch_op->overloads()[0].condition_expr, case_1_condition_expr);
  EXPECT_EQ(dispatch_op->default_op(), default_op);

  ASSERT_OK_AND_ASSIGN(auto expr_0, CallOp(dispatch_op, {Leaf("x")}));
  EXPECT_THAT(expr_0->attr(), EqualsAttr(Attr{}));
  EXPECT_THAT(dispatch_op->ToLowerLevel(expr_0),
              IsOkAndHolds(EqualsExpr(expr_0)));

  ASSERT_OK_AND_ASSIGN(auto expr_1, CallOp(dispatch_op, {Literal(int32_t{1})}));
  EXPECT_THAT(expr_1->attr(), EqualsAttr(GetQType<int64_t>()));
  ASSERT_OK_AND_ASSIGN(auto lowered_expr_1, dispatch_op->ToLowerLevel(expr_1));
  EXPECT_EQ(lowered_expr_1->op(), case_1_base_op);

  ASSERT_OK_AND_ASSIGN(auto expr_2, CallOp(dispatch_op, {Literal(float{2})}));
  EXPECT_THAT(expr_2->attr(), EqualsAttr(GetQType<double>()));
  ASSERT_OK_AND_ASSIGN(auto lowered_expr_2, dispatch_op->ToLowerLevel(expr_2));
  EXPECT_EQ(lowered_expr_2->op(), default_op);
}

TEST(DispatchOperatorTest, OverloadConditionError) {
  auto xy_sig = ExprOperatorSignature{{"x"}, {"y"}};
  ASSERT_OK_AND_ASSIGN(
      auto condition_expr,
      CallOp("core.equal", {Placeholder("x"), Placeholder("z")}));
  DispatchOperator::Overload case_1 = {
      .name = "case_1",
      .op = MockExprOperator::MakeStrict(),
      .condition_expr = condition_expr,
  };
  EXPECT_THAT(
      DispatchOperator::Make("dispatch_op", xy_sig, "doc", {case_1}, nullptr),
      AllOf(
          StatusIs(absl::StatusCode::kInvalidArgument,
                   HasSubstr("problem with an overload condition: 'case_1'")),
          CausedBy(StatusIs(
              absl::StatusCode::kInvalidArgument,
              HasSubstr("expression contains unexpected placeholders: P.z")))));
}

TEST(DispatchOperatorTest, InferAttributesError) {
  auto base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*base_op, InferAttributes(_))
      .WillOnce(Return(absl::InternalError("error in base operator")));
  DispatchOperator::Overload case_1 = {
      .name = "case_1",
      .op = base_op,
      .condition_expr = Literal(kPresent),
  };
  ASSERT_OK_AND_ASSIGN(
      auto dispatch_op,
      DispatchOperator::Make("dispatch_op", {}, "doc", {case_1}, nullptr));
  EXPECT_THAT(dispatch_op->InferAttributes({}),
              StatusIs(absl::StatusCode::kInternal,
                       AllOf(HasSubstr("error in base operator"),
                             HasSubstr("In dispatch operator: 'dispatch_op', "
                                       "case 'case_1'."))));
}

TEST(DispatchOperatorTest, ToLowerLevelError) {
  auto base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*base_op, InferAttributes(_))
      .WillOnce(Return(Attr{GetQType<int64_t>()}));
  EXPECT_CALL(*base_op, ToLowerLevel(_)).WillOnce([](const ExprNodePtr& node) {
    return absl::InternalError("error in base operator");
  });
  DispatchOperator::Overload case_1 = {
      .name = "case_1",
      .op = base_op,
      .condition_expr = Literal(kPresent),
  };
  ASSERT_OK_AND_ASSIGN(
      auto dispatch_op,
      DispatchOperator::Make("dispatch_op", {}, "doc", {case_1}, nullptr));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(dispatch_op, {}));
  EXPECT_THAT(
      dispatch_op->ToLowerLevel(expr),
      StatusIs(
          absl::StatusCode::kInternal,
          AllOf(HasSubstr("error in base operator"),
                HasSubstr(
                    "In dispatch operator: 'dispatch_op', case 'case_1'."))));
}

TEST(DispatchOperatorTest, ConditionEdgeCases) {
  auto base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*base_op, InferAttributes(_))
      .WillRepeatedly(Return(Attr{GetQType<int>()}));

  ASSERT_OK_AND_ASSIGN(
      auto condition_expr_1,
      CallOp("core.equal", {Placeholder("x"), Literal(GetQType<int>())}));
  ASSERT_OK_AND_ASSIGN(
      auto condition_expr_2,
      CallOp("core.equal", {Placeholder("y"), Literal(GetQType<int>())}));

  DispatchOperator::Overload case_1 = {
      .name = "case_1",
      .op = base_op,
      .condition_expr = condition_expr_1,
  };
  DispatchOperator::Overload case_2 = {
      .name = "case_2",
      .op = base_op,
      .condition_expr = condition_expr_2,
  };
  ASSERT_OK_AND_ASSIGN(auto dispatch_op, DispatchOperator::Make(
                                             "dispatch_op", {{"x"}, {"y"}},
                                             "doc", {case_1, case_2}, nullptr));
  EXPECT_THAT(dispatch_op->InferAttributes({Attr{}, Attr{}}),
              IsOkAndHolds(EqualsAttr(Attr{})));
  EXPECT_THAT(dispatch_op->InferAttributes({Attr{GetQType<int>()}, Attr{}}),
              IsOkAndHolds(EqualsAttr(Attr{})));
  EXPECT_THAT(dispatch_op->InferAttributes({Attr{}, Attr{GetQType<int>()}}),
              IsOkAndHolds(EqualsAttr(Attr{})));
  EXPECT_THAT(dispatch_op->InferAttributes(
                  {Attr{GetQType<float>()}, Attr{GetQType<int>()}}),
              IsOkAndHolds(EqualsAttr(Attr{GetQType<int>()})));
  EXPECT_THAT(dispatch_op->InferAttributes(
                  {Attr{GetQType<int>()}, Attr{GetQType<float>()}}),
              IsOkAndHolds(EqualsAttr(Attr{GetQType<int>()})));
  EXPECT_THAT(
      dispatch_op->InferAttributes(
          {Attr{GetQType<int>()}, Attr{GetQType<int>()}}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               AllOf(HasSubstr(
                         "multiple overload cases matched: 'case_1', 'case_2'"),
                     HasSubstr("Input qtypes: x: INT32, y: INT32."),
                     HasSubstr("In dispatch operator: 'dispatch_op'."))));
  EXPECT_THAT(
      dispatch_op->InferAttributes(
          {Attr{GetQType<float>()}, Attr{GetQType<float>()}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               AllOf(HasSubstr("no suitable overload or default operator"),
                     HasSubstr("Input qtypes: x: FLOAT32, y: FLOAT32."),
                     HasSubstr("In dispatch operator: 'dispatch_op'."))));
}

TEST(DispatchOperatorTest, Repr) {
  DispatchOperator::Overload case_1 = {
      .name = "case_1",
      .op = MockExprOperator::MakeStrict(),
      .condition_expr = Literal(kPresent),
  };
  DispatchOperator::Overload case_2 = {
      .name = "case_2",
      .op = MockExprOperator::MakeStrict(),
      .condition_expr = Literal(kMissing),
  };
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x, *args"));
  ASSERT_OK_AND_ASSIGN(auto op_without_default,
                       DispatchOperator::Make("op_without_default", sig, "doc",
                                              {case_1, case_2}, nullptr));
  EXPECT_THAT(op_without_default->GenReprToken(),
              ReprTokenEq("<DispatchOperator: name='op_without_default', "
                          "signature='x, *args', cases=['case_1', 'case_2']>"));
  ASSERT_OK_AND_ASSIGN(
      auto op_with_default,
      DispatchOperator::Make("op_with_default", sig, "doc", {case_1, case_2},
                             MockExprOperator::MakeStrict()));
  EXPECT_THAT(
      op_with_default->GenReprToken(),
      ReprTokenEq(
          "<DispatchOperator: name='op_with_default', "
          "signature='x, *args', cases=['case_1', 'case_2', 'default']>"));
}

}  // namespace
}  // namespace arolla::operator_loader
