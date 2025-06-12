// Copyright 2025 Google LLC
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
#include "arolla/util/repr.h"

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;

TEST(ReprTest, Bool) {
  EXPECT_THAT(GenReprToken(true), ReprTokenEq("true"));
  EXPECT_THAT(GenReprToken(false), ReprTokenEq("false"));
}

TEST(ReprTest, VectorBoolConstReference) {
  const std::vector<bool> vector = {true};
  EXPECT_THAT(GenReprToken(vector[0]), ReprTokenEq("true"));
}

TEST(ReprTest, int32_t) {
  EXPECT_THAT(GenReprToken(int32_t{-1}),
              ReprTokenEq("-1", ReprToken::kSafeForArithmetic));
  EXPECT_THAT(GenReprToken(int32_t{0}),
              ReprTokenEq("0", ReprToken::kSafeForNegation));
  EXPECT_THAT(GenReprToken(int32_t{1}),
              ReprTokenEq("1", ReprToken::kSafeForNegation));
}

TEST(ReprTest, int64_t) {
  EXPECT_THAT(GenReprToken(int64_t{-1}), ReprTokenEq("int64{-1}"));
  EXPECT_THAT(GenReprToken(int64_t{0}), ReprTokenEq("int64{0}"));
  EXPECT_THAT(GenReprToken(int64_t{1}), ReprTokenEq("int64{1}"));
}

TEST(ReprTest, float32) {
  EXPECT_THAT(GenReprToken(float{-1.}),
              ReprTokenEq("-1.", ReprToken::kSafeForArithmetic));
  EXPECT_THAT(GenReprToken(float{-0.}),
              ReprTokenEq("-0.", ReprToken::kSafeForArithmetic));
  EXPECT_THAT(GenReprToken(float{0.}),
              ReprTokenEq("0.", ReprToken::kSafeForNegation));
  EXPECT_THAT(GenReprToken(float{1}),
              ReprTokenEq("1.", ReprToken::kSafeForNegation));
  EXPECT_THAT(GenReprToken(float{0.2}),
              ReprTokenEq("0.2", ReprToken::kSafeForNegation));
  EXPECT_THAT(GenReprToken(float{1e30}),
              ReprTokenEq("1e30", ReprToken::kSafeForNegation));
  EXPECT_THAT(GenReprToken(float{1e-30}),
              ReprTokenEq("1e-30", ReprToken::kSafeForNegation));
  EXPECT_THAT(GenReprToken(std::numeric_limits<float>::infinity()),
              ReprTokenEq("inf", ReprToken::kSafeForNegation));
  EXPECT_THAT(GenReprToken(-std::numeric_limits<float>::infinity()),
              ReprTokenEq("-inf", ReprToken::kSafeForArithmetic));
  EXPECT_THAT(GenReprToken(std::numeric_limits<float>::quiet_NaN()),
              ReprTokenEq("nan", ReprToken::kSafeForNegation));
  EXPECT_THAT(GenReprToken(-std::numeric_limits<float>::quiet_NaN()),
              ReprTokenEq("nan", ReprToken::kSafeForNegation));
  EXPECT_THAT(GenReprToken(std::numeric_limits<float>::signaling_NaN()),
              ReprTokenEq("nan", ReprToken::kSafeForNegation));
  EXPECT_THAT(GenReprToken(-std::numeric_limits<float>::signaling_NaN()),
              ReprTokenEq("nan", ReprToken::kSafeForNegation));
}

TEST(ReprTest, float64) {
  EXPECT_THAT(GenReprToken(double{-1.}), ReprTokenEq("float64{-1}"));
  EXPECT_THAT(GenReprToken(double{-0.}), ReprTokenEq("float64{-0}"));
  EXPECT_THAT(GenReprToken(double{0.}), ReprTokenEq("float64{0}"));
  EXPECT_THAT(GenReprToken(double{1}), ReprTokenEq("float64{1}"));
  EXPECT_THAT(GenReprToken(double{0.2}), ReprTokenEq("float64{0.2}"));
  EXPECT_THAT(GenReprToken(double{1e30}), ReprTokenEq("float64{1e30}"));
  EXPECT_THAT(GenReprToken(double{1e-30}), ReprTokenEq("float64{1e-30}"));
  EXPECT_THAT(GenReprToken(std::numeric_limits<double>::infinity()),
              ReprTokenEq("float64{inf}"));
  EXPECT_THAT(GenReprToken(-std::numeric_limits<double>::infinity()),
              ReprTokenEq("float64{-inf}"));
  EXPECT_THAT(GenReprToken(std::numeric_limits<double>::quiet_NaN()),
              ReprTokenEq("float64{nan}"));
  EXPECT_THAT(GenReprToken(-std::numeric_limits<double>::quiet_NaN()),
              ReprTokenEq("float64{nan}"));
  EXPECT_THAT(GenReprToken(double{0.2f}),
              ReprTokenEq("float64{0.20000000298023224}"));
}

TEST(ReprTest, weak_float) {
  EXPECT_THAT(GenReprTokenWeakFloat(double{-1.}),
              ReprTokenEq("weak_float{-1}"));
  EXPECT_THAT(GenReprTokenWeakFloat(double{-0.}),
              ReprTokenEq("weak_float{-0}"));
  EXPECT_THAT(GenReprTokenWeakFloat(double{0.}), ReprTokenEq("weak_float{0}"));
  EXPECT_THAT(GenReprTokenWeakFloat(double{1}), ReprTokenEq("weak_float{1}"));
  EXPECT_THAT(GenReprTokenWeakFloat(double{0.2}),
              ReprTokenEq("weak_float{0.2}"));
  EXPECT_THAT(GenReprTokenWeakFloat(double{1e30}),
              ReprTokenEq("weak_float{1e30}"));
  EXPECT_THAT(GenReprTokenWeakFloat(double{1e-30}),
              ReprTokenEq("weak_float{1e-30}"));
  EXPECT_THAT(GenReprTokenWeakFloat(std::numeric_limits<double>::infinity()),
              ReprTokenEq("weak_float{inf}"));
  EXPECT_THAT(GenReprTokenWeakFloat(-std::numeric_limits<double>::infinity()),
              ReprTokenEq("weak_float{-inf}"));
  EXPECT_THAT(GenReprTokenWeakFloat(std::numeric_limits<double>::quiet_NaN()),
              ReprTokenEq("weak_float{nan}"));
  EXPECT_THAT(GenReprTokenWeakFloat(-std::numeric_limits<double>::quiet_NaN()),
              ReprTokenEq("weak_float{nan}"));
  EXPECT_THAT(GenReprTokenWeakFloat(double{0.2f}),
              ReprTokenEq("weak_float{0.20000000298023224}"));
}

TEST(ReprTest, fingerprint) {
  auto fp = [](const auto& value) {
    return FingerprintHasher("").Combine(value).Finish();
  };
  ReprToken::Precedence p01{0, 1};
  {
    // ReprToken::Precedence.
    auto p01_hash = fp(p01);
    EXPECT_EQ(p01_hash, fp(p01));
    EXPECT_NE(p01_hash, fp(ReprToken::Precedence{0, 0}));
    EXPECT_NE(p01_hash, fp(ReprToken::Precedence{1, 1}));
  }
  {
    // ReprToken.
    ReprToken abc{.str = "abc"};
    auto abc_hash = fp(abc);
    EXPECT_EQ(abc_hash, fp(abc));
    EXPECT_NE(abc_hash, fp(ReprToken{.str = "foo"}));
    auto abc_p01_hash = fp(ReprToken{.str = "abc", .precedence = p01});
    EXPECT_EQ(abc_p01_hash, fp(ReprToken{.str = "abc", .precedence = p01}));
    EXPECT_NE(
        abc_p01_hash,
        fp(ReprToken{.str = "abc", .precedence = ReprToken::Precedence{0, 0}}));
  }
}

struct ClassWithArollaReprToken {
  std::string v;
  ReprToken ArollaReprToken() const { return {v}; }
};

TEST(ReprTest, ClassWithArollaReprToken) {
  ReprTraits<ClassWithArollaReprToken> traits;
  EXPECT_EQ(traits(ClassWithArollaReprToken{"x"}).str, "x");
  EXPECT_EQ(GenReprToken(ClassWithArollaReprToken{"x"}).str, "x");
  EXPECT_EQ(Repr(ClassWithArollaReprToken{"x"}), "x");
}

}  // namespace
}  // namespace arolla
