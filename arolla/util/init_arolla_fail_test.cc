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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace arolla {
namespace {

using ::arolla::testing::StatusIs;
using ::testing::HasSubstr;

bool foo_ok = false;

bool baz_ok = false;

AROLLA_REGISTER_INITIALIZER(kHighest, Foo, []() -> absl::Status {
  foo_ok = true;
  return absl::OkStatus();
})

AROLLA_REGISTER_INITIALIZER(kRegisterExprOperatorsLowest, Bar,
                            []() -> absl::Status {
                              return absl::InvalidArgumentError("bar: fails");
                            })

AROLLA_REGISTER_INITIALIZER(kLowest, Baz, []() -> absl::Status {
  baz_ok = true;
  return absl::OkStatus();
})

// There is only one test for this subsystem because only the first
// InitArolla() call makes the difference per process life-time.

TEST(InitArollaFailTest, Fail) {
  ASSERT_THAT(InitArolla(), StatusIs(absl::StatusCode::kInvalidArgument,
                                      HasSubstr("bar: fails")));
  ASSERT_TRUE(foo_ok);
  ASSERT_FALSE(baz_ok);
}

}  // namespace
}  // namespace arolla
