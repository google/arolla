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

#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "absl/status/status.h"

namespace arolla {
namespace {

struct Buffer {
  std::string result;
};

Buffer& buffer() {
  static absl::NoDestructor<Buffer> result;
  return *result;
}

AROLLA_INITIALIZER(.name = "Foo", .init_fn = [] { buffer().result += "Hello"; })

AROLLA_INITIALIZER(
        .name = "Bar", .deps = {"Foo"}, .init_fn = [] {
          buffer().result += "World";
          return absl::OkStatus();
        })

AROLLA_INITIALIZER(.deps = {"Bar"}, .init_fn = [] { buffer().result += "!"; })

// There is only one test for this subsystem because only the first
// InitArolla() call makes the difference per process life-time.

TEST(InitArollaTest, Complex) {
  {  // Before init.
    EXPECT_EQ(buffer().result, "");
  }
  {  // After init.
    InitArolla();
    EXPECT_EQ(buffer().result, "HelloWorld!");
    CheckInitArolla();  // no crash
  }
  {  // The following calls do nothing.
    InitArolla();
    EXPECT_EQ(buffer().result, "HelloWorld!");
    CheckInitArolla();  // no crash
  }
  {  // Manually trigger the secondary initialization.
    static constexpr arolla::init_arolla_internal::Initializer
        secondary_initializer = {.init_fn = [] { buffer().result += "!!"; }};
    [[maybe_unused]] static const arolla::init_arolla_internal::Registration
        registration(secondary_initializer);
    arolla::init_arolla_internal::InitArollaSecondary();
    EXPECT_EQ(buffer().result, "HelloWorld!!!");
  }
}

}  // namespace
}  // namespace arolla
