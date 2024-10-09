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
#include "py/arolla/py_utils/status_payload_handler_registry.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/cord.h"

namespace arolla::python {
namespace {

using ::testing::IsNull;
using ::testing::NotNull;
using ::absl_testing::StatusIs;

void Handler(absl::Cord payload, const absl::Status& status) {}

TEST(StatusPayloadHandlerTest, TestRegisterAndGet) {
  EXPECT_TRUE(RegisterStatusHandler("type_url", Handler).ok());
  auto handler = GetStatusHandlerOrNull("type_url");
  EXPECT_THAT(handler, NotNull());
  EXPECT_EQ(*handler.target<void (*)(absl::Cord, const absl::Status&)>(),
            Handler);
}

TEST(StatusPayloadHandlerTest, TestRegisterDuplicated) {
  EXPECT_THAT(RegisterStatusHandler(
                  "type_url", [](absl::Cord payload,
                                 const absl::Status& status) {}),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST(StatusPayloadHandlerTest, TestGetEmpty) {
  EXPECT_THAT(GetStatusHandlerOrNull("non_existing_type_url"), IsNull());
}

TEST(StatusPayloadHandlerTest, TestRegisterNull) {
  EXPECT_THAT(RegisterStatusHandler("abc", nullptr),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace arolla::python
