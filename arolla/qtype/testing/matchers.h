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
#ifndef AROLLA_QTYPE_TESTING_MATCHERS_H_
#define AROLLA_QTYPE_TESTING_MATCHERS_H_

#include <ostream>
#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/nullability.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/demangle.h"

namespace arolla::testing {
namespace qvalue_with_matcher_internal {

// Implementation of QValueWith matcher.
template <typename T>
class QValueWithMatcher {
 public:
  using is_gtest_matcher = void;

  explicit QValueWithMatcher(::testing::Matcher<T> value_matcher)
      : value_matcher_(value_matcher) {}

  bool MatchAndExplain(TypedRef v, std::ostream* absl_nullable ostream) const {
    if (v.GetType() != GetQType<std::decay_t<T>>()) {
      if (ostream != nullptr) {
        *ostream << "stores a value with QType " << v.GetType()->name()
                 << " which does not match C++ type `" << TypeName<T>() << "`";
      }
      return false;
    }
    ::testing::StringMatchResultListener listener;
    bool matched = value_matcher_.MatchAndExplain(v.UnsafeAs<std::decay_t<T>>(),
                                                  &listener);
    if (!matched && ostream != nullptr) {
      *ostream << listener.str() << "the value is " << v.Repr();
    }
    return matched;
  }

  bool MatchAndExplain(TypedValue v,
                       std::ostream* absl_nullable ostream) const {
    return MatchAndExplain(v.AsRef(), ostream);
  }

  void DescribeTo(std::ostream* absl_nonnull ostream) const {
    *ostream << "stores value of type `" << TypeName<T>() << "` that ";
    value_matcher_.DescribeTo(ostream);
  }

  void DescribeNegationTo(std::ostream* absl_nonnull ostream) const {
    *ostream << "doesn't store a value of type `" << TypeName<T>()
             << "` or stores a value that ";
    value_matcher_.DescribeNegationTo(ostream);
  }

 private:
  ::testing::Matcher<T> value_matcher_;
};

}  // namespace qvalue_with_matcher_internal

// Returns GMock matcher for TypedValue/TypedRef content.
//
// Usage:
//   EXPECT_THAT(my_typed_value, QValueWith<value_type>(value_matcher));
//
template <typename T, typename ValueMatcher>
qvalue_with_matcher_internal::QValueWithMatcher<T> QValueWith(
    ValueMatcher value_matcher) {
  return qvalue_with_matcher_internal::QValueWithMatcher<T>(
      ::testing::MatcherCast<T>(value_matcher));
}

}  // namespace arolla::testing

#endif  // AROLLA_QTYPE_TESTING_MATCHERS_H_
