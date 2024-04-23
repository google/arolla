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
#include "arolla/util/bytes.h"

#include <string>
#include <type_traits>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;
using ::testing::Eq;
using ::testing::MatchesRegex;

TEST(BytesTest, Constructor) {
  EXPECT_THAT(Bytes("Hello"), Eq("Hello"));
  std::string hello = "Hello";
  EXPECT_THAT(Bytes(hello), Eq("Hello"));
  absl::string_view hello_view = hello;
  EXPECT_THAT(Bytes(hello_view), Eq("Hello"));
}

TEST(BytesTest, CopyAndMoveConstructors) {
  static_assert(std::is_nothrow_move_constructible<Bytes>::value);

  Bytes src("Google");
  Bytes copied(src);
  EXPECT_THAT(copied, Eq(src));

  Bytes moved(std::move(src));
  EXPECT_THAT(moved, Eq(copied));
}

TEST(BytesTest, CopyAndMoveAssignment) {
  static_assert(std::is_nothrow_move_assignable<Bytes>::value);

  Bytes src("Google");
  Bytes copied = src;
  EXPECT_THAT(copied, Eq(src));

  Bytes moved = std::move(src);
  EXPECT_THAT(moved, Eq(copied));
}

TEST(BytesTest, AssignmentFromString) {
  std::string google = "Google";
  {
    Bytes val("x");
    val = "Google";
    EXPECT_THAT(val, Eq(google));
  }
  {
    Bytes val("x");
    val = google;
    EXPECT_THAT(val, Eq(google));
  }
  {
    absl::string_view google_view = google;
    Bytes val("x");
    val = google_view;
    EXPECT_THAT(val, Eq("Google"));
  }
  {
    Bytes val("x");
    val = std::move(google);
    EXPECT_THAT(val, Eq("Google"));
  }
}

TEST(BytesTest, Repr) {
  EXPECT_THAT(GenReprToken(Bytes("G'\"\t\xff")),
              ReprTokenEq(R"(b'G\'\"\t\xff')"));
  EXPECT_THAT(Repr(Bytes(std::string(1024, 'x'))),
              MatchesRegex(R"(b'x{120}[.]{3} \(1024 bytes total\)')"));
}

}  // namespace
}  // namespace arolla
