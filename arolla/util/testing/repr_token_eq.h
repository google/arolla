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
#ifndef AROLLA_UTIL_TESTING_EQUALS_REPR_TOKEN_H_
#define AROLLA_UTIL_TESTING_EQUALS_REPR_TOKEN_H_

#include <ostream>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//strings/escaping.h"
#include "arolla/util/repr.h"

namespace testing::internal {

template <>
struct UniversalPrinter<::arolla::ReprToken> {
  static void Print(const ::arolla::ReprToken& repr_result,
                    std::ostream* ostream) {
    *ostream << "ReprToken{\"" << absl::Utf8SafeCHexEscape(repr_result.str)
             << "\", {" << static_cast<int>(repr_result.precedence.left) << ", "
             << static_cast<int>(repr_result.precedence.right) << "}}";
  }
};

}  // namespace testing::internal

namespace arolla::testing {

class ReprTokenEqMatcher {
 public:
  using is_gtest_matcher = void;

  explicit ReprTokenEqMatcher(ReprToken expected_repr_result)
      : expected_repr_result_(expected_repr_result) {}

  bool MatchAndExplain(const ReprToken& actual_repr_result,
                       std::ostream* /* listener */) const {
    return actual_repr_result.str == expected_repr_result_.str &&
           actual_repr_result.precedence.left ==
               expected_repr_result_.precedence.left &&
           actual_repr_result.precedence.right ==
               expected_repr_result_.precedence.right;
  }

  void DescribeTo(std::ostream* os) const {
    *os << "equals to " << ::testing::PrintToString(expected_repr_result_);
  }

  void DescribeNegationTo(std::ostream* os) const {
    *os << "does not equal " << ::testing::PrintToString(expected_repr_result_);
  }

 private:
  const ReprToken expected_repr_result_;
};

// gMock matcher for arolla::ReprToken type
//
//   EXPECT_THAT(actual_repr_result, ReprTokenEq(expected_repr_result));
//
inline ::testing::Matcher<const ReprToken&> ReprTokenEq(
    ReprToken expected_repr_result) {
  return ReprTokenEqMatcher(std::move(expected_repr_result));
}

// gMock matcher for arolla::ReprToken type
//
//   EXPECT_THAT(actual_repr_result, ReprTokenEq(str, precedence));
//
inline ::testing::Matcher<const ReprToken&> ReprTokenEq(
    std::string expected_str,
    ReprToken::Precedence expected_precedence = ReprToken::kHighest) {
  return ReprTokenEqMatcher(
      ReprToken{std::move(expected_str), expected_precedence});
}

}  // namespace arolla::testing

#endif  // AROLLA_UTIL_TESTING_EQUALS_REPR_RESULT_H_
