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
#include "arolla/util/string.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace arolla {
namespace {

using ::testing::Eq;

TEST(StringTest, Truncate) {
  EXPECT_THAT(Truncate("", 7), Eq(""));
  EXPECT_THAT(Truncate("fifty seven", 7), Eq("fift..."));
  EXPECT_THAT(Truncate("fifty seven", 10), Eq("fifty s..."));
  EXPECT_THAT(Truncate("fifty seven", 11), Eq("fifty seven"));
  EXPECT_THAT(Truncate("fifty seven", 20), Eq("fifty seven"));
}

TEST(StringTest, IsQualifiedIdentifier) {
  // Single token names are allowed.
  static_assert(IsQualifiedIdentifier("foo"));

  // Validate first token's heading character
  static_assert(!IsQualifiedIdentifier(".bar"));
  static_assert(!IsQualifiedIdentifier("0.bar"));
  static_assert(!IsQualifiedIdentifier("9.bar"));
  static_assert(!IsQualifiedIdentifier("-.bar"));
  static_assert(IsQualifiedIdentifier("_.bar"));
  static_assert(IsQualifiedIdentifier("A.bar"));
  static_assert(IsQualifiedIdentifier("Z.bar"));
  static_assert(IsQualifiedIdentifier("a.bar"));
  static_assert(IsQualifiedIdentifier("z.bar"));

  // Validate first token's trailing character
  static_assert(IsQualifiedIdentifier("_0.bar"));
  static_assert(IsQualifiedIdentifier("_9.bar"));
  static_assert(!IsQualifiedIdentifier("_-.bar"));
  static_assert(IsQualifiedIdentifier("__.bar"));
  static_assert(IsQualifiedIdentifier("_A.bar"));
  static_assert(IsQualifiedIdentifier("_Z.bar"));
  static_assert(IsQualifiedIdentifier("_a.bar"));
  static_assert(IsQualifiedIdentifier("_z.bar"));

  // Validate non-first token's heading character
  static_assert(!IsQualifiedIdentifier("foo..bar"));
  static_assert(!IsQualifiedIdentifier("foo.0.bar"));
  static_assert(!IsQualifiedIdentifier("foo.9.bar"));
  static_assert(!IsQualifiedIdentifier("foo.-.bar"));
  static_assert(IsQualifiedIdentifier("foo._.bar"));
  static_assert(IsQualifiedIdentifier("foo.A.bar"));
  static_assert(IsQualifiedIdentifier("foo.Z.bar"));
  static_assert(IsQualifiedIdentifier("foo.a.bar"));
  static_assert(IsQualifiedIdentifier("foo.z.bar"));

  // Validate first token's trailing character
  static_assert(IsQualifiedIdentifier("foo._0.bar"));
  static_assert(IsQualifiedIdentifier("foo._9.bar"));
  static_assert(!IsQualifiedIdentifier("foo._-.bar"));
  static_assert(IsQualifiedIdentifier("foo.__.bar"));
  static_assert(IsQualifiedIdentifier("foo._A.bar"));
  static_assert(IsQualifiedIdentifier("foo._Z.bar"));
  static_assert(IsQualifiedIdentifier("foo._a.bar"));
  static_assert(IsQualifiedIdentifier("foo._z.bar"));

  // Empty trailing token
  static_assert(!IsQualifiedIdentifier("foo.bar."));

  // Trivials
  static_assert(IsQualifiedIdentifier("test.add"));
  static_assert(IsQualifiedIdentifier("test.subtest.add"));
}

TEST(StringTest, NonFirstComma) {
  bool first_call = true;
  EXPECT_EQ(NonFirstComma(first_call), "");
  EXPECT_FALSE(first_call);
  EXPECT_EQ(NonFirstComma(first_call), ", ");
  EXPECT_FALSE(first_call);
}

TEST(StringTest, ContainerAccessString) {
  EXPECT_EQ(ContainerAccessString("bar"), ".bar");
  EXPECT_EQ(ContainerAccessString("bar.baz"), "['bar.baz']");
  EXPECT_EQ(ContainerAccessString(""), "['']");
}

TEST(StringTest, starts_with) {
  constexpr bool compile_time_true = starts_with("", "");
  EXPECT_TRUE(compile_time_true);
  constexpr bool compile_time_false = starts_with("foo", "bar");
  EXPECT_FALSE(compile_time_false);
  EXPECT_TRUE(starts_with("", ""));
  EXPECT_TRUE(starts_with("Hello, World!", "Hello"));
  EXPECT_TRUE(starts_with("Hello, World!", "Hello, World!"));
  EXPECT_FALSE(starts_with("Hello, World!", "Hello, World! "));
}

}  // namespace
}  // namespace arolla
