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
#ifndef AROLLA_JAGGED_SHAPE_TESTING_MATCHERS_H_
#define AROLLA_JAGGED_SHAPE_TESTING_MATCHERS_H_

#include <ostream>

#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "arolla/jagged_shape/jagged_shape.h"
#include "arolla/util/repr.h"

namespace arolla::testing {

namespace matchers_impl {

template <typename Edge>
class JaggedShapeEquivalentToMatcher {
 public:
  using is_gtest_matcher = void;

  explicit JaggedShapeEquivalentToMatcher(JaggedShapePtr<Edge> expected_shape)
      : expected_shape_(std::move(expected_shape)) {
    DCHECK(expected_shape_ != nullptr);
  }

  bool MatchAndExplain(const JaggedShapePtr<Edge>& shape,
                       ::testing::MatchResultListener* listener) const {
    if (shape == nullptr) {
      *listener << "is null";
      return false;
    }
    *listener << "pointing to ";
    return MatchAndExplain(*shape, listener);
  }

  bool MatchAndExplain(const JaggedShape<Edge>& shape,
                       ::testing::MatchResultListener* listener) const {
    bool is_equivalent = shape.IsEquivalentTo(*expected_shape_);
    *listener << Repr(shape)
              << (is_equivalent ? " which is equivalent"
                                : " which is not equivalent");
    return is_equivalent;
  }

  void DescribeTo(::std::ostream* os) const {
    *os << "is equivalent to " << Repr(*expected_shape_);
  }

  void DescribeNegationTo(::std::ostream* os) const {
    *os << "is not equivalent to " << Repr(*expected_shape_);
  }

 private:
  JaggedShapePtr<Edge> expected_shape_;
};

}  // namespace matchers_impl

// Returns GMock matcher for JaggedShapePtr.
//
// Usage:
//   EXPECT_THAT(my_shape, IsEquivalentTo(value_matcher));
//
template <typename Edge>
auto IsEquivalentTo(const JaggedShapePtr<Edge>& expected_shape) {
  return matchers_impl::JaggedShapeEquivalentToMatcher<Edge>(expected_shape);
}
template <typename Edge>
auto IsEquivalentTo(const JaggedShape<Edge>& expected_shape) {
  return matchers_impl::JaggedShapeEquivalentToMatcher<Edge>(
      JaggedShapePtr<Edge>::NewRef(&expected_shape));
}

}  // namespace arolla::testing

#endif  // AROLLA_JAGGED_SHAPE_TESTING_MATCHERS_H_
