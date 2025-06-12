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
#include "py/arolla/py_utils/error_converter_registry.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/status.h"

namespace arolla::python {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::Eq;
using ::testing::IsNull;
using ::testing::MatchesRegex;
using ::testing::NotNull;

struct TestPayload1 {
  std::string value;
};

struct TestPayload2 {
  std::string value;
};

struct TestPayload3 {
  std::string value;
};

TEST(StatusPayloadHandlerTest, TestRegisterAndGet) {
  int converter1_called = 0;
  auto converter1 = [&](const absl::Status& status) { converter1_called++; };
  ASSERT_OK(RegisterErrorConverter<TestPayload1>(converter1));

  int converter2_called = 0;
  auto converter2 = [&](const absl::Status& status) { converter2_called++; };
  ASSERT_OK(RegisterErrorConverter<TestPayload2>(converter2));

  {
    const auto status =
        WithPayload(absl::InternalError("error"), TestPayload1{"test"});
    const auto* converter = GetRegisteredErrorConverter(status);
    ASSERT_THAT(converter, NotNull());
    (*converter)(status);
    EXPECT_THAT(converter1_called, Eq(1));
    EXPECT_THAT(converter2_called, Eq(0));
  }
  {
    const auto status =
        WithPayload(absl::InternalError("error"), TestPayload2{"test"});
    const auto* converter = GetRegisteredErrorConverter(status);
    ASSERT_THAT(converter, NotNull());
    (*converter)(status);
    EXPECT_THAT(converter1_called, Eq(1));
    EXPECT_THAT(converter2_called, Eq(1));
  }
  {
    const auto status = absl::InternalError("error");
    const auto* converter = GetRegisteredErrorConverter(status);
    EXPECT_THAT(converter, IsNull());
  }
  {
    const auto status =
        WithPayload(absl::InternalError("error"), TestPayload3{"test"});
    const auto* converter = GetRegisteredErrorConverter(status);
    EXPECT_THAT(converter, IsNull());
  }
}

struct TestPayload4 {
  std::string value;
};

TEST(StatusPayloadHandlerTest, TestRegisterErrors) {
  EXPECT_THAT(RegisterErrorConverter<TestPayload4>(nullptr),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(RegisterErrorConverter<TestPayload4>([](absl::Status) {}),
              IsOk());
  EXPECT_THAT(
      RegisterErrorConverter<TestPayload4>([](absl::Status) {}),
      StatusIs(absl::StatusCode::kInternal,
               MatchesRegex("error converter for .*TestPayload4 payload "
                            "already registered")));
}

}  // namespace
}  // namespace arolla::python
