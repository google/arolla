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
#ifndef AROLLA_UTIL_TESTING_STATUS_H_
#define AROLLA_UTIL_TESTING_STATUS_H_

#include <ostream>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/util/status.h"

namespace arolla::testing {
namespace status_internal {

inline const absl::Status& ReadStatus(const absl::Status& status) {
  return status;
}
template <typename T>
const absl::Status& ReadStatus(const absl::StatusOr<T>& v_or) {
  return v_or.status();
}

class CausedByMatcher {
 public:
  using is_gtest_matcher = void;

  explicit CausedByMatcher(::testing::Matcher<absl::Status> status_matcher)
      : status_matcher_(std::move(status_matcher)) {}

  void DescribeTo(std::ostream* os) const {
    *os << "has a cause which ";
    status_matcher_.DescribeTo(os);
  }

  void DescribeNegationTo(std::ostream* os) const {
    *os << "does not have a cause, or has a cause which ";
    status_matcher_.DescribeNegationTo(os);
  }

  template <typename StatusOrT>
  bool MatchAndExplain(const StatusOrT& status,
                       ::testing::MatchResultListener* result_listener) const {
    const absl::Status* cause = arolla::GetCause(ReadStatus(status));
    if (cause == nullptr) {
      *result_listener << "which has no cause";
      return false;
    }
    *result_listener << "has a cause " << *cause << " ";
    return status_matcher_.MatchAndExplain(*cause, result_listener);
  }

 private:
  ::testing::Matcher<absl::Status> status_matcher_;
};

}  // namespace status_internal

// Matches arolla::GetCause of the given Status or StatusOr using
// status_matcher.
//
// Example:
//
//   EXPECT_THAT(
//       arolla::WithCause(absl::InvalidArgumentError("status"),
//                         absl::FailedPreconditionError("cause")),
//       arolla::testing::CausedBy(
//           StatusIs(absl::StatusCode::kFailedPrecondition, "cause")));
//
template <typename StatusMatcherT>
status_internal::CausedByMatcher CausedBy(StatusMatcherT status_matcher) {
  return status_internal::CausedByMatcher(
      ::testing::MatcherCast<absl::Status>(std::move(status_matcher)));
}

}  // namespace arolla::testing

#endif  // AROLLA_UTIL_TESTING_STATUS_H_
