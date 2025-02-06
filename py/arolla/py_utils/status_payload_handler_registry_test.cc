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

namespace arolla::python {
namespace {

using ::absl_testing::StatusIs;

TEST(StatusPayloadHandlerTest, TestRegisterAndGet) {
  int handler1_called = 0;
  auto handler1 = [&](const absl::Status& status) {
    return handler1_called++ % 2 == 0;
  };
  ASSERT_TRUE(RegisterStatusHandler(handler1).ok());

  int handler2_called = 0;
  auto handler2 = [&](const absl::Status& status) {
    return handler2_called++ % 3 == 0;
  };
  ASSERT_TRUE(RegisterStatusHandler(handler2).ok());

  EXPECT_TRUE(CallStatusHandlers(absl::InternalError("error")));
  EXPECT_EQ(handler1_called, 1);
  EXPECT_EQ(handler2_called, 0);

  EXPECT_TRUE(CallStatusHandlers(absl::InternalError("error")));
  EXPECT_EQ(handler1_called, 2);
  EXPECT_EQ(handler2_called, 1);

  EXPECT_TRUE(CallStatusHandlers(absl::InternalError("error")));
  EXPECT_EQ(handler1_called, 3);
  EXPECT_EQ(handler2_called, 1);

  EXPECT_FALSE(CallStatusHandlers(absl::InternalError("error")));
  EXPECT_EQ(handler1_called, 4);
  EXPECT_EQ(handler2_called, 2);
}

TEST(StatusPayloadHandlerTest, TestRegisterNull) {
  EXPECT_THAT(RegisterStatusHandler(nullptr),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace arolla::python
