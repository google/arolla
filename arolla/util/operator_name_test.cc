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
#include "arolla/util/operator_name.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace arolla {
namespace {

using ::testing::Eq;
using ::testing::StrEq;

TEST(OperatorNameTest, Macro) {
  const char* name1 = AROLLA_OPERATOR_NAME("foo.bar");
  EXPECT_THAT(name1, StrEq("foo.bar"));
  absl::string_view name2 = AROLLA_OPERATOR_NAME("foo.bar");
  EXPECT_THAT(name2, Eq("foo.bar"));
  std::string name3 = AROLLA_OPERATOR_NAME("foo.bar");
  EXPECT_THAT(name1, Eq("foo.bar"));
}

}  // namespace
}  // namespace arolla
