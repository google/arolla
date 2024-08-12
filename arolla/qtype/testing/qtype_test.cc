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
#include "arolla/qtype/testing/qtype.h"

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::testing {
namespace {

using ::testing::Eq;
using ::testing::Gt;
using ::testing::StringMatchResultListener;

template <typename MatcherType, typename Value>
std::string Explain(const MatcherType& m, const Value& x) {
  StringMatchResultListener listener;
  ExplainMatchResult(m, x, &listener);
  return listener.str();
}

TEST(QTypeTest, TypedValueWith) {
  EXPECT_THAT(TypedValue::FromValue(57), TypedValueWith<int>(57));
  EXPECT_THAT(TypedValue::FromValue(57), TypedValueWith<int>(Eq(57)));
  EXPECT_THAT(TypedValue::FromValue(57), TypedValueWith<int>(Gt(50)));
  EXPECT_THAT(TypedValue::FromValue<int64_t>(57), TypedValueWith<int64_t>(57));

  {
    StringMatchResultListener listener;
    auto m = TypedValueWith<int32_t>(57);
    EXPECT_THAT(::testing::DescribeMatcher<TypedValue>(m),
                Eq("stores value of type `int` that is equal to 57"));
    EXPECT_THAT(::testing::DescribeMatcher<TypedValue>(m, /*negation=*/true),
                Eq("doesn't store a value of type `int` or stores a value that "
                   "isn't equal to 57"));
    EXPECT_THAT(Explain(m, TypedValue::FromValue(2.39)),
                Eq("stores a value with QType FLOAT64 which does not match C++ "
                   "type `int`"));
    EXPECT_THAT(Explain(m, TypedValue::FromValue(239)), Eq("the value is 239"));
  }
  {
    StringMatchResultListener listener;
    auto m = TypedValueWith<int32_t>(Eq(57));
    EXPECT_THAT(::testing::DescribeMatcher<TypedValue>(m),
                Eq("stores value of type `int` that is equal to 57"));
    EXPECT_THAT(::testing::DescribeMatcher<TypedValue>(m, /*negation=*/true),
                Eq("doesn't store a value of type `int` or stores a value that "
                   "isn't equal to 57"));
    EXPECT_THAT(Explain(m, TypedValue::FromValue(2.39)),
                Eq("stores a value with QType FLOAT64 which does not match C++ "
                   "type `int`"));
    EXPECT_THAT(Explain(m, TypedValue::FromValue(239)), Eq("the value is 239"));
  }
}

}  // namespace
}  // namespace arolla::testing
