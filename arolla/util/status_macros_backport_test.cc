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
// Test for external status definitions.

#include "arolla/util/status_macros_backport.h"

#include <tuple>

#include "gtest/gtest-spi.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace {

template <typename T>
absl::StatusOr<T> ReturnStatusOrValue(T v) {
  return v;
}

absl::StatusOr<int> ReturnStatusOrError(absl::string_view msg) {
  return absl::Status(absl::StatusCode::kUnknown, msg);
}

TEST(ExternalStatusTest, AssertOkAndAssign) {
  ASSERT_OK_AND_ASSIGN(auto value, ReturnStatusOrValue(1));
  ASSERT_EQ(1, value);
  ASSERT_OK_AND_ASSIGN((const auto& [tuple1, tuple2]),
                       ReturnStatusOrValue(std::make_tuple(1, 2)));
  ASSERT_EQ(1, tuple1);
  ASSERT_EQ(2, tuple2);
  EXPECT_FATAL_FAILURE(
      [] {
        ASSERT_OK_AND_ASSIGN(auto x, ReturnStatusOrError("Expected error"));
        (void)x;
      }(),
      "Expected error");
}

}  // namespace
