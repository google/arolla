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
#include "arolla/qexpr/operators/strings/join.h"

#include <tuple>
#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

using ::testing::HasSubstr;

namespace arolla {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;

namespace {

template <typename TypeParam>
class JoinStringsTest : public ::testing::Test {
 public:
  using StringType = std::tuple_element_t<0, TypeParam>;
  using EvalAsFunctor = std::tuple_element_t<1, TypeParam>;

  template <typename... Args>
  absl::StatusOr<StringType> InvokeOperator(const Args&... args) {
    if constexpr (EvalAsFunctor::value) {
      auto result = JoinOperatorFamily{}(args...);
      static_assert(std::is_same_v<decltype(result), StringType>);
      return result;
    } else {
      return ::arolla::InvokeOperator<StringType>(
          "strings._join_with_separator", args...);
    }
  }

  template <typename... Args>
  absl::StatusOr<OptionalValue<StringType>> InvokeOperatorOptional(
      const Args&... args) {
    if constexpr (EvalAsFunctor::value) {
      auto result = JoinOperatorFamily{}(args...);
      static_assert(
          std::is_same_v<decltype(result), OptionalValue<StringType>>);
      return result;
    } else {
      return ::arolla::InvokeOperator<OptionalValue<StringType>>(
          "strings._join_with_separator", args...);
    }
  }
};

TYPED_TEST_SUITE_P(JoinStringsTest);

TYPED_TEST_P(JoinStringsTest, JoinScalars) {
  using StringType = typename JoinStringsTest<TypeParam>::StringType;
  StringType first_part("first");
  StringType second_part("second");
  StringType third_part("third");
  StringType delimiter("/");

  EXPECT_THAT(this->InvokeOperator(delimiter, first_part),
              IsOkAndHolds(StringType("first")));

  EXPECT_THAT(
      this->InvokeOperator(delimiter, first_part, second_part, third_part),
      IsOkAndHolds(StringType("first/second/third")));

  EXPECT_THAT(
      this->InvokeOperator(delimiter, first_part, second_part, third_part),
      IsOkAndHolds(StringType("first/second/third")));
}

TYPED_TEST_P(JoinStringsTest, JoinScalarsErrors) {
  if constexpr (!JoinStringsTest<TypeParam>::EvalAsFunctor::value) {
    // skip error tests since it cause compilation error
    using StringType = typename JoinStringsTest<TypeParam>::StringType;
    using OtherType =
        std::conditional_t<std::is_same_v<StringType, Text>, Bytes, Text>;
    StringType first_part("first");
    StringType second_part("second");
    StringType delimiter("/");
    EXPECT_THAT(this->InvokeOperator(delimiter),
                StatusIs(absl::StatusCode::kNotFound,
                         HasSubstr("expected at least 2 arguments.")));
    EXPECT_THAT(this->InvokeOperator(delimiter, first_part, second_part,
                                     OtherType("third")),
                StatusIs(absl::StatusCode::kNotFound,
                         HasSubstr("joined parts must have same type.")));

    EXPECT_THAT(this->InvokeOperator(0, 1, 2),
                StatusIs(absl::StatusCode::kNotFound,
                         HasSubstr("first argument must be")));
  }
}

TYPED_TEST_P(JoinStringsTest, JoinOptionalScalars) {
  using StringType = typename JoinStringsTest<TypeParam>::StringType;
  OptionalValue<StringType> first_part(StringType("first"));
  OptionalValue<StringType> second_part(StringType("second"));
  StringType third_part("third");
  StringType delimiter("/");

  // All present
  OptionalValue<StringType> expected1(StringType("first/second/third"));
  EXPECT_THAT(this->InvokeOperatorOptional(delimiter, first_part, second_part,
                                           third_part),
              IsOkAndHolds(expected1));

  // One missing
  OptionalValue<StringType> expected2;
  EXPECT_THAT(
      this->InvokeOperatorOptional(delimiter, first_part,
                                   OptionalValue<StringType>{}, third_part),
      IsOkAndHolds(expected2));
}

// Every test should be registered.
REGISTER_TYPED_TEST_SUITE_P(JoinStringsTest, JoinScalars, JoinOptionalScalars,
                            JoinScalarsErrors);

// Instantiate test suite for each string type.
using BytesEvalNormally = std::tuple<Bytes, std::bool_constant<false>>;
INSTANTIATE_TYPED_TEST_SUITE_P(Bytes, JoinStringsTest, BytesEvalNormally);
using TextEvalNormally = std::tuple<Bytes, std::bool_constant<false>>;
INSTANTIATE_TYPED_TEST_SUITE_P(Text, JoinStringsTest, TextEvalNormally);
using BytesEvalFunctor = std::tuple<Bytes, std::bool_constant<true>>;
INSTANTIATE_TYPED_TEST_SUITE_P(BytesFunctor, JoinStringsTest, BytesEvalFunctor);
using TextEvalFunctor = std::tuple<Bytes, std::bool_constant<true>>;
INSTANTIATE_TYPED_TEST_SUITE_P(TextFunctor, JoinStringsTest, TextEvalFunctor);

}  // namespace
}  // namespace arolla
