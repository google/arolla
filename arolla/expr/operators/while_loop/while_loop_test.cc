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
#include "arolla/expr/operators/while_loop/while_loop.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"

namespace arolla::expr_operators {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::LambdaOperator;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::testing::EqualsAttr;
using ::arolla::testing::EqualsExpr;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::NotNull;
using Attr = ::arolla::expr::ExprAttributes;

class WhileLoopTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(WhileLoopTest, WhileLoopOperatorMake) {
  ASSERT_OK_AND_ASSIGN(auto body, MakeLambdaOperator(Placeholder("param")));
  ASSERT_OK_AND_ASSIGN(
      auto condition,
      MakeLambdaOperator(
          CallOp("core.equal", {Placeholder("param"), Placeholder("param")})));

  ASSERT_OK_AND_ASSIGN(auto good_loop_operator,
                       WhileLoopOperator::Make(
                           condition->GetSignature().value(), condition, body));
  EXPECT_THAT(good_loop_operator->display_name(), Eq("anonymous.while_loop"));
  EXPECT_THAT(good_loop_operator->condition(), Eq(condition));
  EXPECT_THAT(good_loop_operator->body(), Eq(body));
  EXPECT_THAT(good_loop_operator->InferAttributes({Attr(GetQType<int64_t>())}),
              IsOkAndHolds(EqualsAttr(GetQType<int64_t>())));
  EXPECT_THAT(good_loop_operator->InferAttributes(
                  {Attr(GetQType<int64_t>()), Attr(GetQType<int64_t>())}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("incorrect number of dependencies passed to "
                                 "an operator node: expected 1 but got 2")));
}

TEST_F(WhileLoopTest, WhileLoopOperatorMakeValidation) {
  ASSERT_OK_AND_ASSIGN(
      auto condition,
      MakeLambdaOperator(
          CallOp("core.equal", {Placeholder("param"), Placeholder("param")})));
  ASSERT_OK_AND_ASSIGN(
      auto too_many_args_body,
      MakeLambdaOperator(
          ExprOperatorSignature::Make("x, y"),
          CallOp("math.add", {Placeholder("x"), Placeholder("y")})));
  EXPECT_THAT(WhileLoopOperator::Make(condition->GetSignature().value(),
                                      condition, too_many_args_body),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("loop signature does not match its body "
                                 "signature: `param` vs `x, y`")));
}

TEST_F(WhileLoopTest, WhileLoopOperatorWrongCondition) {
  ASSERT_OK_AND_ASSIGN(auto good_body,
                       MakeLambdaOperator(Placeholder("param")));
  const auto& wrong_type_condition = good_body;
  ASSERT_OK_AND_ASSIGN(
      auto wrong_condition_operator,
      WhileLoopOperator::Make(wrong_type_condition->GetSignature().value(),
                              wrong_type_condition, good_body));
  EXPECT_THAT(
      wrong_condition_operator->InferAttributes({Attr(GetQType<int64_t>())}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("incorrect return type of the condition of "
                         "`anonymous.while_loop` while loop for input types "
                         "(INT64): expected OPTIONAL_UNIT, got INT64")));
}

TEST_F(WhileLoopTest, WhileLoopOperatorWrongBody) {
  ASSERT_OK_AND_ASSIGN(
      auto condition,
      MakeLambdaOperator(
          CallOp("core.equal", {Placeholder("param"), Placeholder("param")})));
  ASSERT_OK_AND_ASSIGN(
      auto wrong_type_body,
      MakeLambdaOperator(CallOp("core.to_float64", {Placeholder("param")})));
  ASSERT_OK_AND_ASSIGN(
      auto wrong_body_operator,
      WhileLoopOperator::Make(condition->GetSignature().value(), condition,
                              wrong_type_body));
  EXPECT_THAT(
      wrong_body_operator->InferAttributes({Attr(GetQType<int64_t>())}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("incorrect return type of the body of "
                         "`anonymous.while_loop` while loop for input types "
                         "(INT64): expected INT64, got FLOAT64")));
}

TEST_F(WhileLoopTest, MakeWhileLoop) {
  auto init_x = Leaf("x");
  auto init_y = Leaf("y");

  ASSERT_OK_AND_ASSIGN(
      auto loop_condition,
      CallOp("core.not_equal", {Placeholder("y"), Literal<int64_t>(0)}));

  auto new_x = Placeholder("y");
  ASSERT_OK_AND_ASSIGN(
      auto new_y, CallOp("math.mod", {Placeholder("x"), Literal<int64_t>(57)}));

  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr while_loop,
      MakeWhileLoop({{"x", init_x}, {"y", init_y}}, loop_condition,
                    {{"x", new_x}, {"y", new_y}}));
  EXPECT_THAT(
      while_loop->node_deps(),
      ElementsAre(EqualsExpr(CallOp(
          "namedtuple.make",
          {Literal(Text("x,y")),
           CallOp("core.cast",
                  {Leaf("x"), CallOp("qtype.qtype_of", {Leaf("y")}),
                   Literal(true)}),
           CallOp(
               "core.cast",
               {Leaf("y"),
                CallOp("qtype.qtype_of",
                       {CallOp("math.mod", {Leaf("x"), Literal<int64_t>(57)})}),
                Literal(true)})}))));

  auto while_loop_op =
      dynamic_cast<const WhileLoopOperator*>(while_loop->op().get());
  ASSERT_THAT(while_loop_op, NotNull());

  ASSERT_OK_AND_ASSIGN(
      auto state_field_0,
      CallOp("core.get_nth", {Placeholder("loop_state"), Literal<int64_t>(0)}));
  ASSERT_OK_AND_ASSIGN(
      auto state_field_1,
      CallOp("core.get_nth", {Placeholder("loop_state"), Literal<int64_t>(1)}));

  auto condition_op =
      dynamic_cast<const LambdaOperator*>(while_loop_op->condition().get());
  ASSERT_THAT(condition_op, NotNull());
  EXPECT_THAT(condition_op->lambda_body(),
              EqualsExpr(CallOp("core.not_equal",
                                {state_field_1, Literal<int64_t>(0)})));

  auto body_op =
      dynamic_cast<const LambdaOperator*>(while_loop_op->body().get());
  ASSERT_THAT(body_op, NotNull());
  EXPECT_THAT(
      body_op->lambda_body(),
      EqualsExpr(
          CallOp("namedtuple.make",
                 {Literal(Text("x,y")), state_field_1,
                  CallOp("math.mod", {state_field_0, Literal<int64_t>(57)})})));

  ASSERT_OK_AND_ASSIGN(
      QTypePtr good_state_type,
      MakeNamedTupleQType({"x", "y"}, MakeTupleQType({GetQType<int64_t>(),
                                                      GetQType<int64_t>()})));
  EXPECT_THAT(while_loop_op->InferAttributes({Attr(good_state_type)}),
              IsOkAndHolds(EqualsAttr(good_state_type)));

  // The type is OK for the loop itself, but the loop condition's
  // GetOutputQType returns an error.
  ASSERT_OK_AND_ASSIGN(
      QTypePtr wrong_state_type,
      MakeNamedTupleQType({"x", "y"}, MakeTupleQType({GetQType<int64_t>(),
                                                      GetQType<Bytes>()})));
  EXPECT_THAT(while_loop_op->InferAttributes({Attr(wrong_state_type)}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("in condition of `anonymous.while_loop` "
                                 "while loop")));
}

TEST_F(WhileLoopTest, MakeWhileLoopErrors) {
  auto leaf_x = Leaf("x");

  ASSERT_OK_AND_ASSIGN(
      auto condition_with_x,
      CallOp("core.not_equal", {Placeholder("x"), Literal<int64_t>(0)}));

  auto placeholder_x = Placeholder("x");
  auto placeholder_y = Placeholder("y");

  EXPECT_THAT(
      MakeWhileLoop({{"x", leaf_x}}, condition_with_x,
                    {{"x", placeholder_x}, {"y", placeholder_x}}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("no initial value given for the loop state variable `y`")));

  EXPECT_THAT(
      MakeWhileLoop({{"x", leaf_x}}, condition_with_x, {{"x", placeholder_y}}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("no initial value given for the loop state variable `y`")));

  ASSERT_OK_AND_ASSIGN(
      auto condition_with_y,
      CallOp("core.not_equal", {Placeholder("y"), Literal<int64_t>(0)}));
  EXPECT_THAT(
      MakeWhileLoop({{"x", leaf_x}}, condition_with_y, {{"x", placeholder_x}}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("no initial value given for the loop state variable `y`")));
}

}  // namespace
}  // namespace arolla::expr_operators
