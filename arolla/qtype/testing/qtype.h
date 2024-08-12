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
#ifndef AROLLA_QTYPE_TESTING_QTYPE_H_
#define AROLLA_QTYPE_TESTING_QTYPE_H_

#include <ostream>
#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/demangle.h"

namespace arolla::testing {

namespace qtype_impl {

template <typename T>
class TypedValueWithMatcher : public ::testing::MatcherInterface<TypedValue> {
 public:
  explicit TypedValueWithMatcher(::testing::Matcher<T> value_matcher)
      : value_matcher_(value_matcher) {}

  bool MatchAndExplain(
      TypedValue v, ::testing::MatchResultListener* listener) const override {
    const auto& stored = v.As<std::decay_t<T>>();
    if (!stored.ok()) {
      *listener << "stores a value with QType " << v.GetType()->name()
                << " which does not match C++ type `"
                << TypeName<std::decay_t<T>>() << "`";
      return false;
    }
    bool matched = ExplainMatchResult(value_matcher_, *stored, listener);
    if (!matched) {
      *listener << "the value is " << v.Repr();
    }
    return matched;
  }

  void DescribeTo(::std::ostream* os) const override {
    *os << "stores value of type `" << TypeName<T>() << "` that ";
    value_matcher_.DescribeTo(os);
  }

  void DescribeNegationTo(::std::ostream* os) const override {
    *os << "doesn't store a value of type `" << TypeName<T>()
        << "` or stores a value that ";
    value_matcher_.DescribeNegationTo(os);
  }

 private:
  ::testing::Matcher<T> value_matcher_;
};

}  // namespace qtype_impl

// Returns GMock matcher for TypedValue content.
//
// Usage:
//   EXPECT_THAT(my_typed_value, TypedValueWith<value_type>(value_matcher));
//
template <typename T>
inline ::testing::Matcher<TypedValue> TypedValueWith(
    ::testing::Matcher<T> value_matcher) {
  return MakeMatcher(new qtype_impl::TypedValueWithMatcher<T>(value_matcher));
}

// Same as above, but matches polymorphic inner matchers like Pointee.
// When using this overload, type T must be specified, as it cannot be deduced
// from the context.
template <typename T, typename ValueMatcher>
inline ::testing::Matcher<TypedValue> TypedValueWith(
    ValueMatcher value_matcher) {
  return MakeMatcher(new qtype_impl::TypedValueWithMatcher<T>(
      ::testing::MatcherCast<T>(value_matcher)));
}

}  // namespace arolla::testing

#endif  // AROLLA_QTYPE_TESTING_QTYPE_H_
