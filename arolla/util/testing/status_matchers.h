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
#ifndef AROLLA_UTIL_TESTING_STATUS_H_
#define AROLLA_UTIL_TESTING_STATUS_H_

#include <any>
#include <ostream>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/util/demangle.h"
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

template <typename T>
class PayloadIsMatcher {
 public:
  using is_gtest_matcher = void;

  explicit PayloadIsMatcher(::testing::Matcher<T> payload_matcher)
      : payload_matcher_(std::move(payload_matcher)) {}

  void DescribeTo(std::ostream* os) const {
    *os << "has a payload of type " << arolla::TypeName<T>() << " which ";
    payload_matcher_.DescribeTo(os);
  }

  void DescribeNegationTo(std::ostream* os) const {
    *os << "does not have a payload of type " << TypeName<T>()
        << ", or has it but ";
    payload_matcher_.DescribeNegationTo(os);
  }

  template <typename StatusOrT>
  bool MatchAndExplain(const StatusOrT& status,
                       ::testing::MatchResultListener* result_listener) const {
    const std::any* any_payload = GetPayload(ReadStatus(status));
    if (any_payload == nullptr) {
      *result_listener << "which has no payload";
      return false;
    }
    const T* payload = std::any_cast<const T>(any_payload);
    if (payload == nullptr) {
      *result_listener << "has a payload of type "
                       << arolla::TypeName(any_payload->type());
      return false;
    }
    *result_listener << "has a payload " << ::testing::PrintToString(*payload)
                     << " of type " << arolla::TypeName(any_payload->type())
                     << " ";
    return payload_matcher_.MatchAndExplain(*payload, result_listener);
  }

 private:
  ::testing::Matcher<T> payload_matcher_;
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
//       CausedBy(
//           StatusIs(absl::StatusCode::kFailedPrecondition, "cause")));
//
template <typename StatusMatcherT>
status_internal::CausedByMatcher CausedBy(StatusMatcherT status_matcher) {
  return status_internal::CausedByMatcher(
      ::testing::MatcherCast<absl::Status>(std::move(status_matcher)));
}

// Matches arolla::GetPayload<T> of the given Status or StatusOr using
// payload_matcher.
//
// Example:
//
//   struct MyPayload {
//     std::string value;
//   };
//
//   EXPECT_THAT(
//       arolla::WithPayload(absl::InvalidArgumentError("status"),
//                           MyPayload{.value = "payload"}),
//       PayloadIs<MyPayload>(Field(&MyPayload::value, "payload")));
//
template <typename T, typename PayloadMatcherT>
status_internal::PayloadIsMatcher<T> PayloadIs(
    PayloadMatcherT payload_matcher) {
  return status_internal::PayloadIsMatcher<T>(
      ::testing::MatcherCast<T>(std::move(payload_matcher)));
}

// Matches Status or StatusOr to have a payload of type T.
template <typename T>
status_internal::PayloadIsMatcher<T> PayloadIs() {
  return PayloadIs<T>(::testing::_);
}

}  // namespace arolla::testing

#endif  // AROLLA_UTIL_TESTING_STATUS_H_
