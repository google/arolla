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
#include "arolla/expr/operator_loader/generic_operator_overload_condition.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/unit.h"

namespace arolla::operator_loader {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;

class GenericOperatorOverloadConditionTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }

  static absl::StatusOr<ExprNodePtr> Arg(int n) {
    return CallOp("qtype.get_field_qtype",
                  {Leaf("input_tuple_qtype"), Literal(n)});
  }
  static absl::StatusOr<ExprNodePtr> Equal(absl::StatusOr<ExprNodePtr> lhs,
                                           absl::StatusOr<ExprNodePtr> rhs) {
    return CallOp("core.equal", {lhs, rhs});
  }
  static absl::StatusOr<ExprNodePtr> NotEqual(absl::StatusOr<ExprNodePtr> lhs,
                                              absl::StatusOr<ExprNodePtr> rhs) {
    return CallOp("core.not_equal", {lhs, rhs});
  }
  static absl::StatusOr<ExprNodePtr> And(absl::StatusOr<ExprNodePtr> lhs,
                                         absl::StatusOr<ExprNodePtr> rhs) {
    return CallOp("core.presence_and", {lhs, rhs});
  }
};

TEST_F(GenericOperatorOverloadConditionTest, Empty) {
  ASSERT_OK_AND_ASSIGN(auto condition_fn,
                       MakeGenericOperatorOverloadConditionFn({}));
  EXPECT_THAT(condition_fn(MakeTupleQType({})),
              IsOkAndHolds(std::vector<bool>()));
}

TEST_F(GenericOperatorOverloadConditionTest, SingleCondition) {
  ASSERT_OK_AND_ASSIGN(auto condition_expr,
                       NotEqual(Arg(0), Literal(GetNothingQType())));
  ASSERT_OK_AND_ASSIGN(
      auto condition_fn,
      MakeGenericOperatorOverloadConditionFn({condition_expr}));
  EXPECT_THAT(condition_fn(MakeTupleQType({})),
              IsOkAndHolds(std::vector({false})));
  EXPECT_THAT(condition_fn(MakeTupleQType({GetNothingQType()})),
              IsOkAndHolds(std::vector({false})));
  EXPECT_THAT(condition_fn(MakeTupleQType({GetQType<Unit>()})),
              IsOkAndHolds(std::vector({true})));
}

TEST_F(GenericOperatorOverloadConditionTest, MultipleConditions) {
  ASSERT_OK_AND_ASSIGN(auto condition_expr_1,
                       And(And(NotEqual(Arg(0), Literal(GetNothingQType())),
                               NotEqual(Arg(1), Literal(GetNothingQType()))),
                           NotEqual(Arg(0), Arg(1))));
  ASSERT_OK_AND_ASSIGN(auto condition_expr_2,
                       And(And(NotEqual(Arg(0), Literal(GetNothingQType())),
                               NotEqual(Arg(1), Literal(GetNothingQType()))),
                           Equal(Arg(0), Arg(1))));
  ASSERT_OK_AND_ASSIGN(auto condition_fn,
                       MakeGenericOperatorOverloadConditionFn(
                           {condition_expr_1, condition_expr_2}));
  EXPECT_THAT(condition_fn(MakeTupleQType({})),
              IsOkAndHolds(std::vector({false, false})));
  EXPECT_THAT(condition_fn(MakeTupleQType({GetNothingQType()})),
              IsOkAndHolds(std::vector({false, false})));
  EXPECT_THAT(
      condition_fn(MakeTupleQType({GetQType<Unit>(), GetQType<Unit>()})),
      IsOkAndHolds(std::vector({false, true})));
  EXPECT_THAT(condition_fn(MakeTupleQType({GetQType<Unit>(), GetQType<int>()})),
              IsOkAndHolds(std::vector({true, false})));
}

TEST_F(GenericOperatorOverloadConditionTest, UnexpectedReturnQType) {
  ASSERT_OK_AND_ASSIGN(auto condition_expr_1,
                       NotEqual(Arg(0), Literal(GetNothingQType())));
  ASSERT_OK_AND_ASSIGN(auto condition_expr_2, Arg(1));
  EXPECT_THAT(MakeGenericOperatorOverloadConditionFn(
                  {condition_expr_1, condition_expr_2}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "unexpected return qtype: expected "
                       "tuple<OPTIONAL_UNIT,OPTIONAL_UNIT>, got "
                       "tuple<OPTIONAL_UNIT,QTYPE>"));
}

}  // namespace
}  // namespace arolla::operator_loader
