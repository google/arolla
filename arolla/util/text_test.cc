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
#include "arolla/util/text.h"

#include <string>
#include <type_traits>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;
using ::testing::Eq;
using ::testing::MatchesRegex;

TEST(TextTest, Constructor) {
  EXPECT_THAT(Text("Hello").view(), Eq("Hello"));
  std::string hello = "Hello";
  EXPECT_THAT(Text(hello).view(), Eq("Hello"));
  absl::string_view hello_view = hello;
  EXPECT_THAT(Text(hello_view).view(), Eq("Hello"));
  absl::Cord hello_cord(hello);
  EXPECT_THAT(Text(hello_cord).view(), Eq("Hello"));
}

TEST(TextTest, CopyAndMoveConstructors) {
  static_assert(std::is_nothrow_move_constructible<Text>::value);

  Text src("Google");
  Text copied(src);
  EXPECT_THAT(copied, Eq(src));

  Text moved(std::move(src));
  EXPECT_THAT(moved, Eq(copied));
}

TEST(TextTest, CopyAndMoveAssignment) {
  static_assert(std::is_nothrow_move_assignable<Text>::value);

  Text src("Google");
  Text copied = src;
  EXPECT_THAT(copied, Eq(src));

  Text moved = std::move(src);
  EXPECT_THAT(moved, Eq(copied));
}

TEST(TextTest, AssignmentFromString) {
  std::string google = "Google";
  {
    Text val("x");
    val = "Google";
    EXPECT_THAT(val.view(), Eq(google));
  }
  {
    Text val("x");
    val = google;
    EXPECT_THAT(val.view(), Eq(google));
  }
  {
    absl::string_view google_view = google;
    Text val("x");
    val = google_view;
    EXPECT_THAT(val.view(), Eq("Google"));
  }
  {
    absl::Cord google_cord(google);
    Text val("x");
    val = google_cord;
    EXPECT_THAT(val.view(), Eq("Google"));
  }
  {
    Text val("x");
    val = std::move(google);
    EXPECT_THAT(val.view(), Eq("Google"));
  }
}

TEST(TextTest, Repr) {
  EXPECT_THAT(
      GenReprToken(
          Text("\"\xe8\xb0\xb7\xe6\xad\x8c\" is Google\'s Chinese name\n")),
      ReprTokenEq(
          "'\\\"\xe8\xb0\xb7\xe6\xad\x8c\\\" is Google\\'s Chinese name\\n'"));

  const std::string pattern =
      "A"             // Three code-points
      "\xc3\x86"      // of different
      "\xe0\xa0\x80"  // byte length.
      "\xf0\x90\x80\x80";
  std::string data = pattern;
  for (int i = 0; i < 8; ++i) {
    data += data;
  }
  EXPECT_THAT(Repr(Text(data)),
              MatchesRegex("'(" + pattern +
                           "){30}[.]{3} \\(TEXT of 2560 bytes total\\)'"));
}

}  // namespace
}  // namespace arolla
