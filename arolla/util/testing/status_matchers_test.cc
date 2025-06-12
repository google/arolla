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
#include "arolla/util/testing/status_matchers.h"

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/util/status.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::CausedBy;
using ::arolla::testing::PayloadIs;
using ::testing::_;
using ::testing::AllOf;
using ::testing::DescribeMatcher;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::MatchesRegex;
using ::testing::Not;

template <typename MatcherType, typename Value>
std::string Explain(const MatcherType& m, const Value& x) {
  ::testing::StringMatchResultListener listener;
  ExplainMatchResult(m, x, &listener);
  return listener.str();
}

TEST(StatusTest, CausedBy_Status) {
  EXPECT_THAT(WithCause(absl::InvalidArgumentError("status"),
                        absl::InternalError("cause")),
              CausedBy(_));
  EXPECT_THAT(WithCause(absl::InvalidArgumentError("status"),
                        absl::InternalError("cause")),
              CausedBy(StatusIs(absl::StatusCode::kInternal, "cause")));
  EXPECT_THAT(
      WithCause(absl::InvalidArgumentError("status"),
                WithCause(absl::FailedPreconditionError("cause"),
                          absl::InternalError("cause of cause"))),
      AllOf(StatusIs(absl::StatusCode::kInvalidArgument, "status"),
            CausedBy(StatusIs(absl::StatusCode::kFailedPrecondition, "cause")),
            CausedBy(CausedBy(
                StatusIs(absl::StatusCode::kInternal, "cause of cause")))));

  EXPECT_THAT(absl::OkStatus(), Not(CausedBy(_)));
  EXPECT_THAT(absl::InternalError("status"), Not(CausedBy(_)));
  EXPECT_THAT(absl::InternalError("status"),
              Not(CausedBy(StatusIs(absl::StatusCode::kInvalidArgument,
                                    "not a cause"))));

  auto m = CausedBy(StatusIs(absl::StatusCode::kInternal, "cause"));
  EXPECT_THAT(DescribeMatcher<absl::Status>(m),
              MatchesRegex("has a cause which .* \"cause\""));
  EXPECT_THAT(
      DescribeMatcher<absl::Status>(m, /*negation=*/true),
      MatchesRegex("does not have a cause, or has a cause which .* \"cause\""));
  EXPECT_THAT(Explain(m, absl::InvalidArgumentError("status")),
              Eq("which has no cause"));
  EXPECT_THAT(Explain(m, WithCause(absl::InvalidArgumentError("status"),
                                   absl::InternalError("not a cause"))),
              AllOf(HasSubstr("has a cause"),
                    HasSubstr("whose error message is wrong")));
}

TEST(StatusTest, CausedBy_StatusOr) {
  using S = absl::StatusOr<int>;

  EXPECT_THAT(S(WithCause(absl::InvalidArgumentError("status"),
                          absl::InternalError("cause"))),
              CausedBy(_));
  EXPECT_THAT(S(WithCause(absl::InvalidArgumentError("status"),
                          absl::InternalError("cause"))),
              CausedBy(StatusIs(absl::StatusCode::kInternal, "cause")));
  EXPECT_THAT(
      S(WithCause(absl::InvalidArgumentError("status"),
                  WithCause(absl::FailedPreconditionError("cause"),
                            absl::InternalError("cause of cause")))),
      AllOf(StatusIs(absl::StatusCode::kInvalidArgument, "status"),
            CausedBy(StatusIs(absl::StatusCode::kFailedPrecondition, "cause")),
            CausedBy(CausedBy(
                StatusIs(absl::StatusCode::kInternal, "cause of cause")))));

  EXPECT_THAT(S(57), Not(CausedBy(_)));
  EXPECT_THAT(S(absl::InternalError("status")), Not(CausedBy(_)));
  EXPECT_THAT(S(absl::InternalError("status")),
              Not(CausedBy(StatusIs(absl::StatusCode::kInvalidArgument,
                                    "not a cause"))));

  auto m = CausedBy(StatusIs(absl::StatusCode::kInternal, "cause"));
  EXPECT_THAT(DescribeMatcher<S>(m),
              MatchesRegex("has a cause which .* \"cause\""));
  EXPECT_THAT(
      DescribeMatcher<S>(m, /*negation=*/true),
      MatchesRegex("does not have a cause, or has a cause which .* \"cause\""));
  EXPECT_THAT(Explain(m, absl::InvalidArgumentError("status")),
              Eq("which has no cause"));
  EXPECT_THAT(Explain(m, WithCause(absl::InvalidArgumentError("status"),
                                   absl::InternalError("not a cause"))),
              AllOf(HasSubstr("has a cause"),
                    HasSubstr("whose error message is wrong")));
}

TEST(StatusTest, PayloadIs_Status) {
  EXPECT_THAT(
      WithPayload(absl::InvalidArgumentError("status"), std::string("payload")),
      PayloadIs<std::string>());
  EXPECT_THAT(
      WithPayload(absl::InvalidArgumentError("status"), std::string("payload")),
      PayloadIs<std::string>(_));
  EXPECT_THAT(
      WithPayload(absl::InvalidArgumentError("status"), std::string("payload")),
      PayloadIs<std::string>(Eq("payload")));

  EXPECT_THAT(absl::InvalidArgumentError("status"),
              Not(PayloadIs<std::string>()));
  EXPECT_THAT(WithPayload(absl::InvalidArgumentError("status"), 57),
              Not(PayloadIs<std::string>()));

  auto m = PayloadIs<std::string>(Eq("payload"));
  EXPECT_THAT(DescribeMatcher<absl::Status>(m),
              MatchesRegex("has a payload of type .* which is equal to.*"));
  EXPECT_THAT(DescribeMatcher<absl::Status>(m, /*negation=*/true),
              MatchesRegex("does not have a payload of type .*, or has it but "
                           "isn't equal to.*"));
  EXPECT_THAT(Explain(m, absl::InvalidArgumentError("status")),
              Eq("which has no payload"));
  EXPECT_THAT(Explain(m, WithPayload(absl::InvalidArgumentError("status"),
                                     int32_t{57})),
              Eq("has a payload of type int"));
  EXPECT_THAT(Explain(m, WithPayload(absl::InvalidArgumentError("status"),
                                     std::string("another payload"))),
              HasSubstr("has a payload \"another payload\" of type"));
}

TEST(StatusTest, PayloadIs_StatusOr) {
  using S = absl::StatusOr<int>;

  EXPECT_THAT(S(75), Not(PayloadIs<std::string>(_)));
  EXPECT_THAT(S(WithPayload(absl::InvalidArgumentError("status"),
                            std::string("payload"))),
              PayloadIs<std::string>("payload"));
}

}  // namespace
}  // namespace arolla
