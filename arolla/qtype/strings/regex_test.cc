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
#include "arolla/qtype/strings/regex.h"

#include <sstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "arolla/util/testing/status_matchers_backport.h"
#include "re2/re2.h"

using ::arolla::testing::StatusIs;
using ::testing::HasSubstr;

namespace arolla {
namespace {

TEST(Regex, DefaultConstructor) {
  Regex regex;
  EXPECT_FALSE(regex.has_value());

  std::stringstream out;
  out << regex;
  EXPECT_EQ(out.str(), "Regex{}");

  // Default constructed Regex has a valid value which fails to match
  // any string.
  EXPECT_FALSE(RE2::PartialMatch("some string", regex.value()));
}

TEST(Regex, FromPattern) {
  ASSERT_OK_AND_ASSIGN(auto regex, Regex::FromPattern("\\d+ bottles of beer"));
  EXPECT_TRUE(regex.has_value());
  EXPECT_TRUE(RE2::FullMatch("100 bottles of beer", regex.value()));
  std::stringstream out;
  out << regex;
  EXPECT_EQ(out.str(), "Regex{\"\\d+ bottles of beer\"}");

  EXPECT_THAT(Regex::FromPattern("ab\\αcd"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid regular expression: \"ab\\αcd\"; ")));
}

}  // namespace
}  // namespace arolla
