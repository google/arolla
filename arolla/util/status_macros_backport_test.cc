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
// Test for external status definitions.

#include "arolla/util/status_macros_backport.h"

#include <tuple>

#include "gtest/gtest-spi.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "arolla/util/status_macros_backport.h"

namespace {

// A version of ASSERT_EQ that can run outside of TEST(){...} macro.
#define INTERNAL_ASSERT_EQ(lhs, rhs)                                  \
  [](auto lhs_, auto rhs_) {                                          \
    if (lhs_ != rhs_) {                                               \
      LOG(FATAL) << "assertion " #lhs " == " #rhs " failed: " << lhs_ \
                 << " != " << rhs_;                                   \
    }                                                                 \
  }(lhs, rhs)

template <typename T>
absl::StatusOr<T> ReturnStatusOrValue(T v) {
  return v;
}

absl::StatusOr<int> ReturnStatusOrError(absl::string_view msg) {
  return absl::Status(absl::StatusCode::kUnknown, msg);
}

absl::Status ReturnError(absl::string_view msg) {
  return absl::Status(absl::StatusCode::kUnknown, msg);
}

absl::Status ReturnOk() { return absl::Status(); }

TEST(ExternalStatusTest, ReturnIfError) {
  auto func = []() -> absl::StatusOr<int> {
    RETURN_IF_ERROR(ReturnOk()) << "UNEXPECTED";
    RETURN_IF_ERROR(ReturnError("EXPECTED")) << "ALSO " << "EXPECTED";
    return 5;
  };

  ASSERT_EQ(func().status().message(), "EXPECTED; ALSO EXPECTED");
}

TEST(ExternalStatusTest, ReturnIfErrorAnnotateEmpty) {
  auto err = [] { return absl::InvalidArgumentError(""); };
  auto func = [&]() -> absl::Status {
    RETURN_IF_ERROR(err()) << "suffix";
    return absl::OkStatus();
  };
  ASSERT_EQ(func().message(), "suffix");
}

TEST(ExternalStatusTest, ReturnIfErrorPayload) {
  auto err = [] {
    auto status = absl::InvalidArgumentError("message");
    status.SetPayload("url", absl::Cord("payload"));
    return status;
  };
  auto func = [&]() -> absl::Status {
    RETURN_IF_ERROR(err()) << "suffix";
    return absl::OkStatus();
  };
  ASSERT_EQ(func().message(), "message; suffix");
  ASSERT_EQ(func().GetPayload("url"), "payload");
}

TEST(ExternalStatusTest, AssignOrReturn) {
  auto func = []() -> absl::StatusOr<int> {
    ASSIGN_OR_RETURN(int value1, ReturnStatusOrValue(1));
    INTERNAL_ASSERT_EQ(1, value1);
    ASSIGN_OR_RETURN(const int value2, ReturnStatusOrValue(2));
    INTERNAL_ASSERT_EQ(2, value2);
    ASSIGN_OR_RETURN(const int& value3, ReturnStatusOrValue(3));
    INTERNAL_ASSERT_EQ(3, value3);
    ASSIGN_OR_RETURN((const auto& [tuple1, tuple2]),
                     ReturnStatusOrValue(std::make_tuple(1, 2)));
    INTERNAL_ASSERT_EQ(1, tuple1);
    INTERNAL_ASSERT_EQ(2, tuple2);
    ASSIGN_OR_RETURN(int value4, ReturnStatusOrError("EXPECTED"));
    return value4;
  };
  ASSERT_EQ(func().status().message(), "EXPECTED");
}

TEST(ExternalStatusTest, AssignOrReturn3) {
  auto func1 = []() -> absl::StatusOr<int> {
    ASSIGN_OR_RETURN(int value1, ReturnStatusOrValue(1), _ << "NOT EXPECTED");
    INTERNAL_ASSERT_EQ(1, value1);
    ASSIGN_OR_RETURN((const auto& [tuple1, tuple2]),
                     ReturnStatusOrValue(std::make_tuple(1, 2)),
                     _ << "NOT EXPECTED");
    INTERNAL_ASSERT_EQ(1, tuple1);
    INTERNAL_ASSERT_EQ(2, tuple2);
    ASSIGN_OR_RETURN(int value2, ReturnStatusOrError("EXPECTED"),
                     _ << "ALSO " << "EXPECTED");
    return value2;
  };

  ASSERT_EQ(func1().status().message(), "EXPECTED; ALSO EXPECTED");

  auto func2 = []() -> void {
    ASSIGN_OR_RETURN(int value, absl::StatusOr<int>(5), (void)_);
    INTERNAL_ASSERT_EQ(value, 5);
  };
  func2();
}

TEST(ExternalStatusTest, AssignOrReturnAnnotateEmpty) {
  auto err = [] { return absl::StatusOr<int>(absl::InvalidArgumentError("")); };
  auto func = [&]() -> absl::StatusOr<int> {
    ASSIGN_OR_RETURN(auto result, err(), _ << "suffix");
    return result;
  };
  ASSERT_EQ(func().status().message(), "suffix");
}

TEST(ExternalStatusTest, AssignOrReturn3Payload) {
  auto err = [] {
    auto status = absl::InvalidArgumentError("message");
    status.SetPayload("url", absl::Cord("payload"));
    return absl::StatusOr<int>(status);
  };
  auto func = [&]() -> absl::StatusOr<int> {
    ASSIGN_OR_RETURN(auto result, err(), _ << "suffix");
    return result;
  };
  ASSERT_EQ(func().status().message(), "message; suffix");
  ASSERT_EQ(func().status().GetPayload("url"), "payload");
}

TEST(ExternalStatusTest, AssertOkAndAssign) {
  ASSERT_OK_AND_ASSIGN(auto value, ReturnStatusOrValue(1));
  ASSERT_EQ(1, value);
  ASSERT_OK_AND_ASSIGN((const auto& [tuple1, tuple2]),
                       ReturnStatusOrValue(std::make_tuple(1, 2)));
  ASSERT_EQ(1, tuple1);
  ASSERT_EQ(2, tuple2);
  EXPECT_FATAL_FAILURE(
      []() {
        ASSERT_OK_AND_ASSIGN(auto x, ReturnStatusOrError("Expected error"));
        (void)x;
      }(),
      "Expected error");
}

TEST(ExternalStatusTest, AssertOk) {
  ASSERT_OK(ReturnOk());
  ASSERT_OK(ReturnStatusOrValue(1));
  EXPECT_FATAL_FAILURE(ASSERT_OK(ReturnStatusOrError("Expected error")),
                       "Expected error");
}

TEST(ExternalStatusTest, ExpectOk) {
  EXPECT_OK(ReturnOk());
  EXPECT_OK(ReturnStatusOrValue(1));
  EXPECT_NONFATAL_FAILURE(EXPECT_OK(ReturnStatusOrError("Expected error")),
                          "Expected error");
}

}  // namespace
