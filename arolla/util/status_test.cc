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
#include "arolla/util/status.h"

#include <initializer_list>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::AnyOf;
using ::testing::Eq;
using ::testing::Test;

TEST(StatusTest, CheckInputStatus) {
  EXPECT_OK(CheckInputStatus());
  EXPECT_OK(CheckInputStatus(13));
  EXPECT_OK(CheckInputStatus(13, "a"));
  EXPECT_OK(CheckInputStatus(absl::StatusOr<int>(13), "a"));
  EXPECT_OK(CheckInputStatus(13, absl::StatusOr<std::string>("a")));
  EXPECT_OK(CheckInputStatus(absl::StatusOr<int>(13),
                             absl::StatusOr<std::string>("a")));

  absl::Status bad_status1{absl::StatusCode::kInvalidArgument, "bad 1"};
  absl::Status bad_status2{absl::StatusCode::kDataLoss, "bad 2"};
  EXPECT_EQ(CheckInputStatus(absl::StatusOr<int>(bad_status1)), bad_status1);
  EXPECT_EQ(CheckInputStatus(13, absl::StatusOr<int>(bad_status1)),
            bad_status1);
  EXPECT_EQ(CheckInputStatus(absl::StatusOr<int>(13),
                             absl::StatusOr<int>(bad_status1)),
            bad_status1);
  EXPECT_EQ(CheckInputStatus(absl::StatusOr<int>(bad_status1),
                             absl::StatusOr<int>(bad_status2)),
            bad_status1);
}

TEST(StatusTest, LiftStatusUpSuccess) {
  // empty tuple.
  std::tuple<> empty = LiftStatusUp().value();
  EXPECT_THAT(empty, Eq(std::make_tuple()));

  // 1-tuple
  std::tuple<int> one = LiftStatusUp(absl::StatusOr<int>(1)).value();
  EXPECT_THAT(one, Eq(std::make_tuple(1)));

  // 2-tuple
  std::tuple<std::string, int> two =
      LiftStatusUp(absl::StatusOr<std::string>("one"), absl::StatusOr<int>(2))
          .value();
  EXPECT_THAT(two, Eq(std::make_tuple(std::string("one"), 2)));

  // span
  ASSERT_OK_AND_ASSIGN(
      std::vector<int> vec,
      LiftStatusUp(absl::Span<const absl::StatusOr<int>>{1, 2}));
  EXPECT_THAT(vec, ::testing::ElementsAre(1, 2));

  // flat_hash_map
  absl::flat_hash_map<int, int> fhm =
      LiftStatusUp(std::initializer_list<std::pair<int, absl::StatusOr<int>>>{
                       {1, 2}, {3, 4}})
          .value();
  EXPECT_THAT(fhm, Eq(absl::flat_hash_map<int, int>{{1, 2}, {3, 4}}));

  absl::flat_hash_map<int, int> fhm1 =
      LiftStatusUp(std::initializer_list<
                       std::pair<absl::StatusOr<int>, absl::StatusOr<int>>>{
                       {1, 2}, {3, 4}})
          .value();
  EXPECT_THAT(fhm, Eq(absl::flat_hash_map<int, int>{{1, 2}, {3, 4}}));
}

TEST(StatusTest, LiftStatusUpErrors) {
  // 1-tuple
  absl::Status bad_status1{absl::StatusCode::kInvalidArgument, "bad 1"};
  absl::Status bad_status2{absl::StatusCode::kDataLoss, "bad 2"};
  EXPECT_EQ(LiftStatusUp(absl::StatusOr<int>(bad_status1)).status(),
            bad_status1);

  // 2-tuple
  EXPECT_EQ(LiftStatusUp(absl::StatusOr<std::string>("one"),
                         absl::StatusOr<int>(bad_status2))
                .status(),
            bad_status2);

  // multiple errors
  EXPECT_EQ(LiftStatusUp(absl::StatusOr<std::string>("one"),
                         absl::StatusOr<int>(bad_status1),
                         absl::StatusOr<float>(bad_status2))
                .status(),
            bad_status1);
  EXPECT_EQ(LiftStatusUp(absl::StatusOr<float>(bad_status2),
                         absl::StatusOr<std::string>("one"),
                         absl::StatusOr<int>(bad_status1))
                .status(),
            bad_status2);

  // span
  EXPECT_THAT(LiftStatusUp(absl::Span<const absl::StatusOr<int>>{bad_status1,
                                                                 bad_status2})
                  .status(),
              bad_status1);

  // map
  EXPECT_THAT(
      LiftStatusUp(std::initializer_list<std::pair<int, absl::StatusOr<int>>>{
                       {1, bad_status1}, {2, 3}, {4, bad_status2}})
          .status(),
      AnyOf(bad_status1, bad_status2));
  EXPECT_THAT(
      LiftStatusUp(std::initializer_list<
                       std::pair<absl::StatusOr<int>, absl::StatusOr<int>>>{
                       {bad_status1, 1}, {2, 3}, {4, bad_status2}})
          .status(),
      AnyOf(bad_status1, bad_status2));
  EXPECT_THAT(
      LiftStatusUp(std::initializer_list<
                       std::pair<absl::StatusOr<int>, absl::StatusOr<int>>>{
                       {1, bad_status1}, {2, 3}, {4, bad_status2}})
          .status(),
      AnyOf(bad_status1, bad_status2));
}

TEST(StatusTest, UnStatus) {
  using T = std::unique_ptr<int>;
  using StatusOrT = absl::StatusOr<T>;
  {
    StatusOrT status_or_t = std::make_unique<int>(1);
    const T& value = UnStatus(status_or_t);
    EXPECT_EQ(*value, 1);
    EXPECT_EQ(value.get(), status_or_t.value().get());
  }
  {
    StatusOrT status_or_t = std::make_unique<int>(1);
    T value = UnStatus(std::move(status_or_t));
    EXPECT_EQ(*value, 1);
  }
  {
    T original_value = std::make_unique<int>(1);
    const T& value = UnStatus(original_value);
    EXPECT_EQ(*value, 1);
    EXPECT_EQ(value.get(), original_value.get());
  }
  {
    T original_value = std::make_unique<int>(1);
    T value = UnStatus(std::move(original_value));
    EXPECT_EQ(*value, 1);
  }
}

TEST(StatusTest, FirstErrorStatus) {
  absl::Status ok_status = absl::OkStatus();
  absl::Status failed_precondition = absl::FailedPreconditionError("msg1");
  absl::Status internal_error = absl::InternalError("msg2");

  // No errors
  EXPECT_OK(FirstErrorStatus({}));
  EXPECT_OK(FirstErrorStatus({ok_status, ok_status}));

  // One error
  EXPECT_EQ(FirstErrorStatus({failed_precondition}), failed_precondition);
  EXPECT_EQ(FirstErrorStatus({ok_status, failed_precondition}),
            failed_precondition);
  EXPECT_EQ(FirstErrorStatus({failed_precondition, ok_status}),
            failed_precondition);

  // Multiple errors
  EXPECT_EQ(FirstErrorStatus({failed_precondition, internal_error}),
            failed_precondition);
  EXPECT_EQ(FirstErrorStatus({internal_error, failed_precondition}),
            internal_error);
}

TEST(StatusTest, GetStatusOrOk) {
  absl::Status ok_status = absl::OkStatus();

  // No errors
  EXPECT_OK(GetStatusOrOk(5));
  EXPECT_OK(GetStatusOrOk(ok_status));
  EXPECT_OK(GetStatusOrOk(absl::StatusOr<int>(5)));

  // Error
  absl::Status failed_precondition = absl::FailedPreconditionError("msg1");
  EXPECT_EQ(GetStatusOrOk(failed_precondition), failed_precondition);
  EXPECT_EQ(GetStatusOrOk(absl::StatusOr<int>(failed_precondition)),
            failed_precondition);
}

TEST(StatusTest, IsOkStatus) {
  absl::Status ok_status = absl::OkStatus();

  // No errors
  EXPECT_TRUE(IsOkStatus(5));
  EXPECT_TRUE(IsOkStatus(ok_status));
  EXPECT_TRUE(IsOkStatus(absl::StatusOr<int>(5)));

  // Error
  absl::Status failed_precondition = absl::FailedPreconditionError("msg1");
  EXPECT_FALSE(IsOkStatus(failed_precondition));
  EXPECT_FALSE(IsOkStatus(absl::StatusOr<int>(failed_precondition)));
}

TEST(StatusTest, UnStatusCaller) {
  absl::Status failed_precondition = absl::FailedPreconditionError("msg1");
  absl::Status failed_precondition2 = absl::FailedPreconditionError("msg2");

  auto add_op = [](int a, int b) { return a + b; };
  UnStatusCaller<decltype(add_op)> add_op_wrap{add_op};
  auto add_op_with_status = [](int a, int b) -> absl::StatusOr<int> {
    return a + b;
  };
  auto add_op_with_status_wrap = MakeUnStatusCaller(add_op_with_status);
  auto add_op_always_error = [&](int a, int b) -> absl::StatusOr<int> {
    return failed_precondition;
  };
  auto add_op_always_error_wrap = MakeUnStatusCaller(add_op_always_error);

  // No input errors
  EXPECT_THAT(add_op_wrap(5, 7), IsOkAndHolds(12));
  EXPECT_THAT(add_op_with_status_wrap(5, 7), IsOkAndHolds(12));
  EXPECT_EQ(add_op_always_error_wrap(5, 7).status(), failed_precondition);

  // Input errors
  EXPECT_EQ(add_op_wrap(5, absl::StatusOr<int>(failed_precondition)).status(),
            failed_precondition);
  EXPECT_EQ(add_op_wrap(absl::StatusOr<int>(failed_precondition),
                        absl::StatusOr<int>(failed_precondition2))
                .status(),
            failed_precondition);
  EXPECT_EQ(
      add_op_always_error_wrap(5, absl::StatusOr<int>(failed_precondition2))
          .status(),
      failed_precondition2);
}

}  // namespace
}  // namespace arolla
