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

#include <ostream>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/util/demangle.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/status.h"

namespace arolla::testing {
namespace status_internal {

inline const absl::Status& ReadStatus(
    const absl::Status& status ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return status;
}

template <typename T>
const absl::Status& ReadStatus(
    const absl::StatusOr<T>& v_or ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  return v_or.status();
}

class CauseIsMatcher {
 public:
  using is_gtest_matcher = void;

  explicit CauseIsMatcher(::testing::Matcher<absl::Status> status_matcher)
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

template <arolla::status_internal::ErrorPayload T>
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
    const auto* structured_error =
        arolla::status_internal::ReadStructuredError(ReadStatus(status));
    if (structured_error == nullptr ||
        structured_error->payload_type() == typeid(void)) {
      *result_listener << "which has no payload";
      return false;
    }
    const auto* typed_structured_error = fast_dynamic_downcast_final<
        const arolla::status_internal::StructuredError<T>*>(structured_error);
    if (typed_structured_error == nullptr) {
      *result_listener << "has a payload of type "
                       << arolla::TypeName(structured_error->payload_type());
      return false;
    }
    *result_listener << "has a payload "
                     << ::testing::PrintToString(
                            typed_structured_error->payload())
                     << " of type " << arolla::TypeName<T>() << " ";
    return payload_matcher_.MatchAndExplain(typed_structured_error->payload(),
                                            result_listener);
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
//       arolla::Error(
//           absl::InvalidArgumentError("status"),
//           arolla::CausedBy(absl::FailedPreconditionError("cause"))),
//       CauseIs(StatusIs(absl::StatusCode::kFailedPrecondition, "cause")));
//
template <typename StatusMatcherT>
status_internal::CauseIsMatcher CauseIs(StatusMatcherT status_matcher) {
  return status_internal::CauseIsMatcher(
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
//       arolla::Error(absl::InvalidArgumentError("status"),
//                     MyPayload{.value = "payload"}),
//       PayloadIs<MyPayload>(Field(&MyPayload::value, "payload")));
//
template <arolla::status_internal::ErrorPayload T, typename PayloadMatcherT>
status_internal::PayloadIsMatcher<T> PayloadIs(
    PayloadMatcherT payload_matcher) {
  return status_internal::PayloadIsMatcher<T>(
      ::testing::MatcherCast<T>(std::move(payload_matcher)));
}

// Matches Status or StatusOr to have a payload of type T.
template <arolla::status_internal::ErrorPayload T>
status_internal::PayloadIsMatcher<T> PayloadIs() {
  return PayloadIs<T>(::testing::_);
}

}  // namespace arolla::testing

#endif  // AROLLA_UTIL_TESTING_STATUS_H_
