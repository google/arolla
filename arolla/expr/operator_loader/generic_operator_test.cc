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

#include "arolla/expr/operator_loader/generic_operator.h"

#include <cstdint>
#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/helper.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/testing/status_matchers.h"

namespace arolla::operator_loader {
namespace {

using ::testing::_;
using ::testing::AllOf;
using ::testing::HasSubstr;
using ::testing::Return;
using ::testing::Truly;

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
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
using ::arolla::testing::QValueWith;

using Attr = ::arolla::expr::ExprAttributes;

template <typename... QTypePtrs>
absl::StatusOr<TypedValue> EvalPreparedExpr(const ExprNodePtr& prepared_expr,
                                            QTypePtrs... qtypes) {
  ASSIGN_OR_RETURN(auto input_qtype_sequence,
                   MakeInputQTypeSequence({Attr(qtypes)...}));
  return Invoke(prepared_expr, absl::flat_hash_map<std::string, TypedValue>{
                                   {"input_qtype_sequence",
                                    MakeSequenceQValue(input_qtype_sequence)}});
}

TEST(GenericOperatorOverloadTest, Basics) {
  auto base_op = MockExprOperator::MakeStrict();
  ASSERT_OK_AND_ASSIGN(  // NOTE: Condition refers parameters from the overload,
      auto condition_expr,  // not from the generic operator / base operator!
      CallOp("core.equal", {Placeholder("x"), Placeholder("y")}));
  ASSERT_OK_AND_ASSIGN(auto overload_op, GenericOperatorOverload::Make(
                                             "overload_case_1", {{"x"}, {"y"}},
                                             condition_expr, base_op));
  EXPECT_EQ(overload_op->base_operator(), base_op);
  EXPECT_EQ(overload_op->condition_expr(), condition_expr);
  EXPECT_EQ(overload_op->display_name(), "overload_case_1");
  EXPECT_EQ(overload_op->doc(),
            "A generic operator overload; not expected to be used directly!");
  EXPECT_EQ(overload_op->signature().parameters.size(), 2);
  EXPECT_EQ(overload_op->signature().parameters[0].name, "x");
  EXPECT_EQ(overload_op->signature().parameters[1].name, "y");
  EXPECT_THAT(EvalPreparedExpr(overload_op->prepared_readiness_expr(),
                               GetQType<int>(), nullptr),
              IsOkAndHolds(QValueWith<OptionalUnit>(kMissing)));
  EXPECT_THAT(EvalPreparedExpr(overload_op->prepared_readiness_expr(),
                               GetQType<int>(), GetQType<float>()),
              IsOkAndHolds(QValueWith<OptionalUnit>(kPresent)));
  EXPECT_THAT(EvalPreparedExpr(overload_op->prepared_condition_expr(),
                               GetQType<int>(), GetQType<float>()),
              IsOkAndHolds(QValueWith<OptionalUnit>(kMissing)));
  EXPECT_THAT(EvalPreparedExpr(overload_op->prepared_condition_expr(),
                               GetQType<int>(), GetQType<int>()),
              IsOkAndHolds(QValueWith<OptionalUnit>(kPresent)));
  EXPECT_THAT(overload_op->InferAttributes(
                  {Attr(GetQType<int>()), Attr(GetQType<float>())}),
              StatusIs(absl::StatusCode::kUnimplemented));
  EXPECT_THAT(
      overload_op->ToLowerLevel(
          /*arbitrary expression*/ overload_op->prepared_condition_expr()),
      StatusIs(absl::StatusCode::kUnimplemented));
  EXPECT_EQ(overload_op->py_qvalue_specialization_key(),
            "::arolla::operator_loader::GenericOperatorOverload");
}

TEST(GenericOperatorOverloadTest, NoDefaultValuesInSignature) {
  auto base_op = MockExprOperator::MakeStrict();
  ASSERT_OK_AND_ASSIGN(auto sig, ExprOperatorSignature::Make("x, y=", 1));
  EXPECT_THAT(
      GenericOperatorOverload::Make("overload_case_1", sig, Literal(kPresent),
                                    base_op),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("default values in generic operator overloads are not "
                         "supported, got signature_spec: 'x, y='")));
}

TEST(GenericOperatorOverloadTest, OverloadConditionError) {
  auto xy_sig = ExprOperatorSignature{{"x"}, {"y"}};
  auto base_op = MockExprOperator::MakeStrict();
  ASSERT_OK_AND_ASSIGN(  // NOTE: Condition refers parameters from the overload,
      auto condition_expr,  // not from the generic operator / base operator!
      CallOp("core.equal", {Placeholder("x"), Placeholder("z")}));
  EXPECT_THAT(
      GenericOperatorOverload::Make("overload_case_1", xy_sig, condition_expr,
                                    base_op),
      AllOf(
          StatusIs(
              absl::StatusCode::kInvalidArgument,
              HasSubstr(
                  "problem with an overload condition: 'overload_case_1'")),
          CausedBy(StatusIs(
              absl::StatusCode::kInvalidArgument,
              HasSubstr("expression contains unexpected placeholders: P.z")))));
}

TEST(GenericOperatorTest, Basics) {
  auto case_1_sig = ExprOperatorSignature{{"a"}};
  auto case_1_base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*case_1_base_op, InferAttributes(_))
      .WillRepeatedly(Return(Attr{GetQType<int64_t>()}));
  EXPECT_CALL(*case_1_base_op, ToLowerLevel(Truly([&](const ExprNodePtr& node) {
    return node->op() == case_1_base_op;
  }))).WillRepeatedly([&](const ExprNodePtr& node) { return node; });
  ASSERT_OK_AND_ASSIGN(
      auto case_1_condition_expr,
      CallOp("core.equal", {Placeholder("a"), Literal(GetQType<int32_t>())}));
  ASSERT_OK_AND_ASSIGN(
      auto case_1_op,
      GenericOperatorOverload::Make("case_1", case_1_sig, case_1_condition_expr,
                                    case_1_base_op));
  ASSERT_OK(RegisterOperator("generic_op_test.basics.op._case_1", case_1_op)
                .status());

  auto case_2_sig = ExprOperatorSignature{{"b"}};
  auto case_2_base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*case_2_base_op, InferAttributes(_))
      .WillRepeatedly(Return(Attr{GetQType<double>()}));
  EXPECT_CALL(*case_2_base_op, ToLowerLevel(Truly([&](const ExprNodePtr& node) {
    return node->op() == case_2_base_op;
  }))).WillRepeatedly([&](const ExprNodePtr& node) { return node; });
  ASSERT_OK_AND_ASSIGN(
      auto case_2_condition_expr,
      CallOp("core.equal", {Placeholder("b"), Literal(GetQType<float>())}));
  ASSERT_OK_AND_ASSIGN(
      auto case_2_op,
      GenericOperatorOverload::Make("case_2", case_2_sig, case_2_condition_expr,
                                    case_2_base_op));
  ASSERT_OK(RegisterOperator("generic_op_test.basics.op._case_2", case_2_op)
                .status());

  ASSERT_OK_AND_ASSIGN(
      auto generic_op,
      GenericOperator::Make("generic_op_test.basics.op", {{"x"}}, "docstring"));
  EXPECT_EQ(generic_op->display_name(), "generic_op_test.basics.op");
  EXPECT_EQ(generic_op->doc(), "docstring");
  EXPECT_EQ(generic_op->signature().parameters.size(), 1);
  EXPECT_EQ(generic_op->signature().parameters[0].name, "x");

  ASSERT_OK_AND_ASSIGN(auto expr_0, CallOp(generic_op, {Leaf("x")}));
  EXPECT_THAT(expr_0->attr(), EqualsAttr(Attr{}));
  EXPECT_THAT(generic_op->ToLowerLevel(expr_0),
              IsOkAndHolds(EqualsExpr(expr_0)));

  ASSERT_OK_AND_ASSIGN(auto expr_1, CallOp(generic_op, {Literal(int32_t{1})}));
  EXPECT_THAT(expr_1->attr(), EqualsAttr(GetQType<int64_t>()));
  ASSERT_OK_AND_ASSIGN(auto lowered_expr_1, generic_op->ToLowerLevel(expr_1));
  EXPECT_EQ(lowered_expr_1->op(), case_1_base_op);

  ASSERT_OK_AND_ASSIGN(auto expr_2, CallOp(generic_op, {Literal(float{2})}));
  EXPECT_THAT(expr_2->attr(), EqualsAttr(GetQType<double>()));
  ASSERT_OK_AND_ASSIGN(auto lowered_expr_2, generic_op->ToLowerLevel(expr_2));
  EXPECT_EQ(lowered_expr_2->op(), case_2_base_op);

  EXPECT_EQ(generic_op->py_qvalue_specialization_key(),
            "::arolla::operator_loader::GenericOperator");

  EXPECT_THAT(
      generic_op->InferAttributes({Attr(GetQType<std::string>())}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          AllOf(HasSubstr("no suitable overload operator"),
                HasSubstr("In generic operator: 'generic_op_test.basics.op'."),
                HasSubstr("Input qtypes: x: BYTES."))));

  EXPECT_THAT(
      generic_op->ToLowerLevel(ExprNode::UnsafeMakeOperatorNode(
          ExprOperatorPtr{generic_op}, {Leaf("x")},
          Attr{GetNothingQType()})),  // Manually assign output qtype attribute.
      IsOk());

  EXPECT_THAT(
      generic_op->ToLowerLevel(ExprNode::UnsafeMakeOperatorNode(
          ExprOperatorPtr{generic_op}, {Literal(std::string{"foo"})},
          Attr{GetNothingQType()})),  // Manually assign output qtype attribute.
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          AllOf(HasSubstr("no suitable overload operator"),
                HasSubstr("In generic operator: 'generic_op_test.basics.op'."),
                HasSubstr("Input qtypes: x: BYTES."))));
}

TEST(GenericOperatorTest, NameError) {
  EXPECT_THAT(GenericOperator::Make("<name>", {{"x"}}, "doc"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected a operator name to be a valid "
                                 "namespace name, got '<name>'")));
}

TEST(GenericOperatorTest, InferAttributesError) {
  auto base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*base_op, InferAttributes(_))
      .WillOnce([](absl::Span<const Attr> inputs) {
        return absl::InternalError("error in base operator");
      });
  ASSERT_OK_AND_ASSIGN(auto case_1_op,
                       GenericOperatorOverload::Make(
                           "<case_1>", {}, Literal(kPresent), base_op));
  ASSERT_OK(
      RegisterOperator("generic_op_test.infer_attr_error.op._case_1", case_1_op)
          .status());
  ASSERT_OK_AND_ASSIGN(
      auto generic_op,
      GenericOperator::Make("generic_op_test.infer_attr_error.op", {}, "doc"));
  EXPECT_THAT(
      generic_op->InferAttributes({}),
      StatusIs(
          absl::StatusCode::kInternal,
          AllOf(
              HasSubstr("error in base operator"),
              HasSubstr(
                  "In generic operator: 'generic_op_test.infer_attr_error.op', "
                  "case '<case_1>'."))));
}

TEST(GenericOperatorTest, ToLowerLevelError) {
  auto base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*base_op, InferAttributes(_))
      .WillOnce(Return(Attr{GetQType<int64_t>()}));
  EXPECT_CALL(*base_op, ToLowerLevel(_)).WillOnce([](const ExprNodePtr& node) {
    return absl::InternalError("error in base operator");
  });
  ASSERT_OK_AND_ASSIGN(auto case_1_op,
                       GenericOperatorOverload::Make(
                           "<case_1>", {}, Literal(kPresent), base_op));
  ASSERT_OK(
      RegisterOperator("generic_op_test.lowering_error.op._case_1", case_1_op)
          .status());
  ASSERT_OK_AND_ASSIGN(
      auto generic_op,
      GenericOperator::Make("generic_op_test.lowering_error.op", {}, "doc"));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(generic_op, {}));
  EXPECT_THAT(
      generic_op->ToLowerLevel(expr),
      StatusIs(
          absl::StatusCode::kInternal,
          AllOf(HasSubstr("error in base operator"),
                HasSubstr(
                    "In generic operator: "
                    "'generic_op_test.lowering_error.op', case '<case_1>'."))));
}

TEST(GenericOperatorTest, ConditionEdgeCases) {
  auto sig = ExprOperatorSignature{{"x"}, {"y"}};
  auto base_op = MockExprOperator::MakeStrict();
  EXPECT_CALL(*base_op, InferAttributes(_))
      .WillRepeatedly(Return(Attr{GetQType<int>()}));

  ASSERT_OK_AND_ASSIGN(
      auto condition_expr_1,
      CallOp("core.equal", {Placeholder("x"), Literal(GetQType<int>())}));
  ASSERT_OK_AND_ASSIGN(
      auto condition_expr_2,
      CallOp("core.equal", {Placeholder("y"), Literal(GetQType<int>())}));

  ASSERT_OK_AND_ASSIGN(
      auto case_1_op, GenericOperatorOverload::Make("<case_1>", sig,
                                                    condition_expr_1, base_op));
  ASSERT_OK_AND_ASSIGN(
      auto case_2_op, GenericOperatorOverload::Make("<case_2>", sig,
                                                    condition_expr_2, base_op));

  ASSERT_OK(RegisterOperator("generic_op_test.cond_edges.op._case_1", case_1_op)
                .status());
  ASSERT_OK(RegisterOperator("generic_op_test.cond_edges.op._case_2", case_2_op)
                .status());
  ASSERT_OK_AND_ASSIGN(
      auto generic_op,
      GenericOperator::Make("generic_op_test.cond_edges.op", sig, "doc"));
  EXPECT_THAT(generic_op->InferAttributes({Attr{}, Attr{}}),
              IsOkAndHolds(EqualsAttr(Attr{})));
  EXPECT_THAT(generic_op->InferAttributes({Attr{GetQType<int>()}, Attr{}}),
              IsOkAndHolds(EqualsAttr(Attr{})));
  EXPECT_THAT(generic_op->InferAttributes({Attr{}, Attr{GetQType<int>()}}),
              IsOkAndHolds(EqualsAttr(Attr{})));
  EXPECT_THAT(generic_op->InferAttributes(
                  {Attr{GetQType<float>()}, Attr{GetQType<int>()}}),
              IsOkAndHolds(EqualsAttr(Attr{GetQType<int>()})));
  EXPECT_THAT(generic_op->InferAttributes(
                  {Attr{GetQType<int>()}, Attr{GetQType<float>()}}),
              IsOkAndHolds(EqualsAttr(Attr{GetQType<int>()})));
  EXPECT_THAT(
      generic_op->InferAttributes(
          {Attr{GetQType<int>()}, Attr{GetQType<int>()}}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          AllOf(HasSubstr(
                    "multiple overload cases matched: '<case_1>', '<case_2>'"),
                HasSubstr("Input qtypes: x: INT32, y: INT32."),
                HasSubstr(
                    "In generic operator: 'generic_op_test.cond_edges.op'."))));
  EXPECT_THAT(generic_op->InferAttributes(
                  {Attr{GetQType<float>()}, Attr{GetQType<float>()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       AllOf(HasSubstr("no suitable overload operator"),
                             HasSubstr("Input qtypes: x: FLOAT32, y: FLOAT32."),
                             HasSubstr("In generic operator: "
                                       "'generic_op_test.cond_edges.op'."))));
}

TEST(GenericOperatorTest, SnapshotUpdate) {
  auto sig = ExprOperatorSignature{{"x"}};
  ASSERT_OK_AND_ASSIGN(
      auto generic_op,
      GenericOperator::Make("generic_op_test.snapshot_updates.op", sig, "doc"));

  {  // case_1: INT32
    EXPECT_THAT(generic_op->InferAttributes({Attr{GetQType<int>()}}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("no suitable overload operator")));
    auto base_op = MockExprOperator::MakeStrict();
    EXPECT_CALL(*base_op, InferAttributes(_))
        .WillRepeatedly(Return(Attr{GetQType<int>()}));
    ASSERT_OK_AND_ASSIGN(
        auto cond_expr,
        CallOp("core.equal", {Placeholder("x"), Literal(GetQType<int>())}));
    ASSERT_OK_AND_ASSIGN(
        auto overload_op,
        GenericOperatorOverload::Make("case_1", sig, cond_expr, base_op));
    ASSERT_OK(RegisterOperator("generic_op_test.snapshot_updates.op._case_1",
                               overload_op)
                  .status());
    EXPECT_THAT(generic_op->InferAttributes({Attr{GetQType<int>()}}),
                IsOkAndHolds(EqualsAttr(Attr{GetQType<int>()})));
  }
  {  // case_2: FLOAT32
    EXPECT_THAT(generic_op->InferAttributes({Attr{GetQType<float>()}}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("no suitable overload operator")));
    auto base_op = MockExprOperator::MakeStrict();
    EXPECT_CALL(*base_op, InferAttributes(_))
        .WillRepeatedly(Return(Attr{GetQType<float>()}));
    ASSERT_OK_AND_ASSIGN(
        auto cond_expr,
        CallOp("core.equal", {Placeholder("x"), Literal(GetQType<float>())}));
    ASSERT_OK_AND_ASSIGN(
        auto overload_op,
        GenericOperatorOverload::Make("case_2", sig, cond_expr, base_op));
    ASSERT_OK(RegisterOperator("generic_op_test.snapshot_updates.op._case_2",
                               overload_op)
                  .status());
    EXPECT_THAT(generic_op->InferAttributes({Attr{GetQType<float>()}}),
                IsOkAndHolds(EqualsAttr(Attr{GetQType<float>()})));
  }
  {  // Unregister case_2.
    arolla::expr::ExprOperatorRegistry::GetInstance()->UnsafeUnregister(
        "generic_op_test.snapshot_updates.op._case_1");
    EXPECT_THAT(generic_op->InferAttributes({Attr{GetQType<int>()}}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("no suitable overload operator")));
  }
}

TEST(GenericOperatorTest, IgnoreNonGenericOverloads) {
  ASSERT_OK_AND_ASSIGN(
      auto generic_op,
      GenericOperator::Make("generic_op_test.bad_overload.op", {{"x"}}, "doc"));
  ASSERT_OK(RegisterOperator("generic_op_test.bad_overload.op._unrelated_op",
                             MockExprOperator::MakeStrict())
                .status());
  EXPECT_THAT(generic_op->InferAttributes({Attr{GetQType<int>()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("no suitable overload operator")));
}

}  // namespace
}  // namespace arolla::operator_loader
