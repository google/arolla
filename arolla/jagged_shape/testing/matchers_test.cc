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
#include "arolla/jagged_shape/testing/matchers.h"

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"

namespace arolla {
namespace {

using ::arolla::testing::IsEquivalentTo;
using ::testing::Eq;
using ::testing::Not;
using ::testing::StringMatchResultListener;

template <typename MatcherType, typename Value>
std::string Explain(const MatcherType& m, const Value& x) {
  StringMatchResultListener listener;
  ExplainMatchResult(m, x, &listener);
  return listener.str();
}

TEST(QTypeTest, JaggedShapeIsEquivalentTo) {
  ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 2})));
  ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 1, 3})));
  ASSERT_OK_AND_ASSIGN(auto shape1,
                       JaggedDenseArrayShape::FromEdges({edge1, edge2}));
  ASSERT_OK_AND_ASSIGN(auto edge3, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 1, 4})));
  ASSERT_OK_AND_ASSIGN(auto shape2,
                       JaggedDenseArrayShape::FromEdges({edge1, edge3}));

  EXPECT_THAT(shape1, IsEquivalentTo(shape1));
  EXPECT_THAT(shape1, Not(IsEquivalentTo(shape2)));

  auto m = IsEquivalentTo(shape1);
  EXPECT_THAT(::testing::DescribeMatcher<JaggedDenseArrayShapePtr>(m),
              Eq("is equivalent to JaggedShape(2, [1, 2])"));
  EXPECT_THAT(::testing::DescribeMatcher<JaggedDenseArrayShapePtr>(
                  m, /*negation=*/true),
              Eq("is not equivalent to JaggedShape(2, [1, 2])"));
  EXPECT_THAT(Explain(m, nullptr), Eq("is null"));
  EXPECT_THAT(Explain(m, *shape1),
              Eq("JaggedShape(2, [1, 2]) which is equivalent"));
  EXPECT_THAT(Explain(m, shape1),
              Eq("pointing to JaggedShape(2, [1, 2]) which is equivalent"));
  EXPECT_THAT(Explain(m, *shape2),
              Eq("JaggedShape(2, [1, 3]) which is not equivalent"));
  EXPECT_THAT(Explain(m, shape2),
              Eq("pointing to JaggedShape(2, [1, 3]) which is not equivalent"));
}

}  // namespace
}  // namespace arolla
