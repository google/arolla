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
#include "arolla/expr/tuple_expr_operator.h"

#include <cstdint>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::InvokeExprOperator;

TEST(TupleExprOperatorTest, Basics) {
  ASSERT_OK_AND_ASSIGN(auto tuple,
                       CallOp(MakeTupleOperator::Make(),
                              {Literal<float>(2.f), Literal<int64_t>(3)}));
  ASSERT_OK_AND_ASSIGN(auto first,
                       CallOp(std::make_shared<GetNthOperator>(0), {tuple}));
  ASSERT_OK_AND_ASSIGN(auto second,
                       CallOp(std::make_shared<GetNthOperator>(1), {tuple}));
  EXPECT_EQ(first->qtype(), GetQType<float>());
  EXPECT_EQ(second->qtype(), GetQType<int64_t>());
}

TEST(TupleExprOperatorTest, InvokeMakeTuple) {
  ASSERT_OK_AND_ASSIGN(
      auto tuple, InvokeExprOperator<TypedValue>(MakeTupleOperator::Make(), 2.f,
                                                 int64_t{3}));
  EXPECT_EQ(tuple.GetType(),
            MakeTupleQType({GetQType<float>(), GetQType<int64_t>()}));
  EXPECT_EQ(tuple.GetFieldCount(), 2);
  EXPECT_THAT(tuple.GetField(0).As<float>(), IsOkAndHolds(2.f));
  EXPECT_THAT(tuple.GetField(1).As<int64_t>(), IsOkAndHolds(3));
}

}  // namespace
}  // namespace arolla::expr
