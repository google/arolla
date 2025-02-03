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

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

namespace arolla {
namespace {

TEST(Regex, NoCapturingGroups) {
  ASSERT_OK_AND_ASSIGN(auto regex, CompileRegex("\\d+ bottles of beer"));
  ASSERT_NE(regex, nullptr);
  EXPECT_EQ(regex->NumberOfCapturingGroups(), 0);
  EXPECT_TRUE(regex->PartialMatch("100 bottles of beer"));
  std::string match;
  EXPECT_FALSE(regex->PartialMatch("100 bottles of beer", &match));
}

TEST(Regex, OneCapturingGroup) {
  ASSERT_OK_AND_ASSIGN(auto regex, CompileRegex("(\\d+) bottles of beer"));
  ASSERT_NE(regex, nullptr);
  EXPECT_EQ(regex->NumberOfCapturingGroups(), 1);
  EXPECT_TRUE(regex->PartialMatch("100 bottles of beer"));
  std::string match;
  EXPECT_TRUE(regex->PartialMatch("100 bottles of beer", &match));
  EXPECT_EQ(match, "100");
}

TEST(Regex, ManyCapturingGroup) {
  ASSERT_OK_AND_ASSIGN(auto regex, CompileRegex("(\\d+) (bottles) (of) beer"));
  ASSERT_NE(regex, nullptr);
  EXPECT_EQ(regex->NumberOfCapturingGroups(), 3);
  EXPECT_TRUE(regex->PartialMatch("100 bottles of beer"));
  std::string match;
  EXPECT_TRUE(regex->PartialMatch("100 bottles of beer", &match));
  EXPECT_EQ(match, "100");
}

TEST(Regex, Repr) {
  ASSERT_OK_AND_ASSIGN(auto regex1, CompileRegex("abc"));
  ASSERT_OK_AND_ASSIGN(auto regex2, CompileRegex("a.c"));
  EXPECT_EQ(regex1->pattern(), "abc");
  EXPECT_EQ(regex2->pattern(), "a.c");
  EXPECT_EQ(Repr(RegexPtr{}), "regex{}");
  EXPECT_EQ(Repr(regex1), "regex{`abc`}");
  EXPECT_EQ(Repr(regex2), "regex{`a.c`}");
}

TEST(Regex, Fingerprint) {
  ASSERT_OK_AND_ASSIGN(auto regex1_1, CompileRegex("abc"));
  ASSERT_OK_AND_ASSIGN(auto regex1_2, CompileRegex("abc"));
  ASSERT_OK_AND_ASSIGN(auto regex2_1, CompileRegex("a.c"));
  ASSERT_OK_AND_ASSIGN(auto regex2_2, CompileRegex("a.c"));
  auto fingerprint0_1 = FingerprintHasher("salt").Combine(RegexPtr{}).Finish();
  auto fingerprint0_2 = FingerprintHasher("salt").Combine(RegexPtr{}).Finish();
  auto fingerprint1_1 = FingerprintHasher("salt").Combine(regex1_1).Finish();
  auto fingerprint1_2 = FingerprintHasher("salt").Combine(regex1_2).Finish();
  auto fingerprint2_1 = FingerprintHasher("salt").Combine(regex2_1).Finish();
  auto fingerprint2_2 = FingerprintHasher("salt").Combine(regex2_2).Finish();
  EXPECT_EQ(fingerprint0_1, fingerprint0_2);
  EXPECT_EQ(fingerprint1_1, fingerprint1_2);
  EXPECT_EQ(fingerprint2_1, fingerprint2_2);
  EXPECT_NE(fingerprint0_1, fingerprint1_1);
  EXPECT_NE(fingerprint1_1, fingerprint2_1);
  EXPECT_NE(fingerprint2_1, fingerprint0_1);
}

TEST(Regex, QType) {
  EXPECT_EQ(GetQType<RegexPtr>()->name(), "REGEX");
  EXPECT_EQ(GetQType<RegexPtr>()->type_info(), typeid(RegexPtr));
  ASSERT_OK_AND_ASSIGN(auto regex, CompileRegex("a.c"));
  auto qvalue = TypedValue::FromValue(regex);
  EXPECT_EQ(qvalue.Repr(), "regex{`a.c`}");
}

TEST(Regex, CompilationError) {
  EXPECT_THAT(CompileRegex("ab\\αcd"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression: `ab\\αcd`;")));
}

}  // namespace
}  // namespace arolla
