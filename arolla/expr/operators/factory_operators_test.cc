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
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla::expr_operators {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::WithQTypeAnnotation;
using ::testing::Eq;

TEST(FactoryTest, EmptyLike) {
  ASSERT_OK_AND_ASSIGN(auto scalar_leaf,
                       WithQTypeAnnotation(Leaf("scalar"), GetQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto empty_like_scalar,
                       CallOp("core.empty_like", {scalar_leaf}));
  EXPECT_THAT(empty_like_scalar->qtype(), Eq(GetOptionalQType<float>()));
  EXPECT_THAT(
      ToLowerNode(empty_like_scalar),
      IsOkAndHolds(EqualsExpr(
          CallOp("core.const_like",
                 {scalar_leaf, Literal<OptionalValue<float>>(std::nullopt)}))));

  ASSERT_OK_AND_ASSIGN(
      auto array_leaf,
      WithQTypeAnnotation(Leaf("array"), GetDenseArrayQType<float>()));
  ASSERT_OK_AND_ASSIGN(auto empty_like_array,
                       CallOp("core.empty_like", {array_leaf}));
  EXPECT_THAT(empty_like_array->qtype(), Eq(GetDenseArrayQType<float>()));
  EXPECT_THAT(ToLowerNode(empty_like_array),
              IsOkAndHolds(EqualsExpr(CallOp(
                  "core.const_like",
                  {array_leaf, Literal<OptionalValue<float>>(std::nullopt)}))));
}

}  // namespace
}  // namespace arolla::expr_operators
