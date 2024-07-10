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
#include "arolla/util/init_arolla.h"

#include <string>
#include <tuple>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace arolla {
namespace {

using ::arolla::testing::IsOk;

std::string& GetBuffer() {
  static Indestructible<std::string> result;
  return *result;
}

AROLLA_REGISTER_INITIALIZER(kHighest, Foo, [] { GetBuffer() += "Hello"; })

AROLLA_REGISTER_INITIALIZER(kLowest, Bar, []() -> absl::Status {
  GetBuffer() += "World";
  return absl::OkStatus();
})

AROLLA_REGISTER_ANONYMOUS_INITIALIZER(kLowest, [] { GetBuffer() += "!"; })

AROLLA_REGISTER_INITIALIZER(kLowest, Baz, []() -> absl::Status {
  // Expect a statement with ',' to trigger no compilation error.
  std::tuple<int, int>();
  return absl::OkStatus();
})

// There is only one test for this subsystem because only the first
// InitArolla() call makes the difference per process life-time.

TEST(InitArollaTest, Complex) {
  {  // Before init.
    EXPECT_EQ(GetBuffer(), "");
  }
  {  // After init.
    ASSERT_THAT(InitArolla(), IsOk());
    EXPECT_EQ(GetBuffer(), "HelloWorld!");
  }
  {  // The following calls do nothing.
    ASSERT_THAT(InitArolla(), IsOk());
    EXPECT_EQ(GetBuffer(), "HelloWorld!");
  }
}

}  // namespace
}  // namespace arolla
