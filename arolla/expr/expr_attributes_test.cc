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
#include "arolla/expr/expr_attributes.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::PrintToString;

using Attr = ::arolla::expr::ExprAttributes;

TEST(ExprAttributesTest, Default) {
  const Attr attr;
  EXPECT_EQ(attr.qtype(), nullptr);
  EXPECT_EQ(attr.qvalue(), std::nullopt);
  EXPECT_EQ(PrintToString(attr), "Attr{}");
}

TEST(ExprAttributesTest, QTypeNullptr) {
  const Attr attr(nullptr);
  EXPECT_EQ(attr.qtype(), nullptr);
  EXPECT_EQ(attr.qvalue(), std::nullopt);
  EXPECT_EQ(PrintToString(attr), "Attr{}");
}

TEST(ExprAttributesTest, QType) {
  const Attr attr(GetQTypeQType());
  EXPECT_EQ(attr.qtype(), GetQTypeQType());
  EXPECT_EQ(attr.qvalue(), std::nullopt);
  EXPECT_EQ(PrintToString(attr), "Attr(qtype=QTYPE)");
}

TEST(ExprAttributesTest, QValue) {
  const Attr attr(TypedValue::FromValue(GetNothingQType()));
  EXPECT_EQ(attr.qtype(), GetQTypeQType());
  EXPECT_THAT(attr.qvalue()->As<QTypePtr>(), IsOkAndHolds(GetNothingQType()));
  EXPECT_EQ(PrintToString(attr), "Attr(qvalue=NOTHING)");
}

TEST(ExprAttributesTest, NoQTypeNoQValue) {
  const Attr attr(nullptr, std::nullopt);
  EXPECT_EQ(attr.qtype(), nullptr);
  EXPECT_EQ(attr.qvalue(), std::nullopt);
  EXPECT_EQ(PrintToString(attr), "Attr{}");
}

TEST(ExprAttributesTest, QTypeNoQValue) {
  const Attr attr(GetQTypeQType(), std::nullopt);
  EXPECT_EQ(attr.qtype(), GetQTypeQType());
  EXPECT_EQ(attr.qvalue(), std::nullopt);
  EXPECT_EQ(PrintToString(attr), "Attr(qtype=QTYPE)");
}

TEST(ExprAttributesTest, QValueQValue) {
  std::optional<TypedValue> qvalue = TypedValue::FromValue(GetNothingQType());
  const Attr attr(GetQTypeQType(), qvalue);
  EXPECT_EQ(attr.qtype(), GetQTypeQType());
  EXPECT_THAT(attr.qvalue()->As<QTypePtr>(), IsOkAndHolds(GetNothingQType()));
  EXPECT_EQ(PrintToString(attr), "Attr(qvalue=NOTHING)");
}

TEST(ExprAttributesTest, Fingerprints) {
  absl::flat_hash_set<Fingerprint> fingerprints;
  EXPECT_TRUE(
      fingerprints
          .insert(FingerprintHasher("").Combine(ExprAttributes()).Finish())
          .second);
  EXPECT_FALSE(
      fingerprints
          .insert(FingerprintHasher("").Combine(ExprAttributes()).Finish())
          .second);
  EXPECT_TRUE(fingerprints
                  .insert(FingerprintHasher("")
                              .Combine(ExprAttributes(GetQType<int64_t>()))
                              .Finish())
                  .second);
  EXPECT_FALSE(fingerprints
                   .insert(FingerprintHasher("")
                               .Combine(ExprAttributes(GetQType<int64_t>()))
                               .Finish())
                   .second);
  EXPECT_TRUE(fingerprints
                  .insert(FingerprintHasher("")
                              .Combine(ExprAttributes(
                                  TypedValue::FromValue<int64_t>(57)))
                              .Finish())
                  .second);
  EXPECT_FALSE(fingerprints
                   .insert(FingerprintHasher("")
                               .Combine(ExprAttributes(
                                   TypedValue::FromValue<int64_t>(57)))
                               .Finish())
                   .second);
}

TEST(ExprAttributesTest, IsIdenticalToEmpty) {
  const Attr attr1;
  const Attr attr2;
  EXPECT_TRUE(attr1.IsIdenticalTo(attr1));
  EXPECT_TRUE(attr1.IsIdenticalTo(attr2));
  EXPECT_TRUE(attr2.IsIdenticalTo(attr2));
}

TEST(ExprAttributesTest, IsIdenticalToGeneral) {
  const Attr attr0;
  const Attr attr1(GetQTypeQType());
  EXPECT_FALSE(attr0.IsIdenticalTo(attr1));
  const Attr attr2(TypedValue::FromValue(GetNothingQType()));
  EXPECT_FALSE(attr0.IsIdenticalTo(attr2));
  EXPECT_FALSE(attr1.IsIdenticalTo(attr2));
  const Attr attr3(GetQTypeQType(), TypedValue::FromValue(GetNothingQType()));
  EXPECT_FALSE(attr0.IsIdenticalTo(attr3));
  EXPECT_FALSE(attr1.IsIdenticalTo(attr3));
  EXPECT_TRUE(attr2.IsIdenticalTo(attr3));
  const Attr attr4(TypedValue::FromValue(GetQType<int64_t>()));
  EXPECT_FALSE(attr0.IsIdenticalTo(attr4));
  EXPECT_FALSE(attr1.IsIdenticalTo(attr4));
  EXPECT_FALSE(attr2.IsIdenticalTo(attr4));
  EXPECT_FALSE(attr3.IsIdenticalTo(attr4));
}

TEST(ExprAttributesTest, IsSubsetOfEmpty) {
  const Attr attr1;
  const Attr attr2;
  EXPECT_TRUE(attr1.IsSubsetOf(attr1));
  EXPECT_TRUE(attr1.IsSubsetOf(attr2));
  EXPECT_TRUE(attr2.IsSubsetOf(attr2));
}

TEST(ExprAttributesTest, IsSubsetOf) {
  const Attr attr0;
  const Attr attr1(GetQTypeQType());
  const Attr attr2(TypedValue::FromValue(GetNothingQType()));
  const Attr attr3(TypedValue::FromValue(GetQTypeQType()));
  EXPECT_TRUE(attr0.IsSubsetOf(attr0));
  EXPECT_TRUE(attr0.IsSubsetOf(attr1));
  EXPECT_TRUE(attr0.IsSubsetOf(attr2));
  EXPECT_TRUE(attr0.IsSubsetOf(attr3));

  EXPECT_FALSE(attr1.IsSubsetOf(attr0));
  EXPECT_TRUE(attr1.IsSubsetOf(attr1));
  EXPECT_TRUE(attr1.IsSubsetOf(attr2));
  EXPECT_TRUE(attr1.IsSubsetOf(attr3));

  EXPECT_FALSE(attr2.IsSubsetOf(attr0));
  EXPECT_FALSE(attr2.IsSubsetOf(attr1));
  EXPECT_TRUE(attr2.IsSubsetOf(attr2));
  EXPECT_FALSE(attr2.IsSubsetOf(attr3));

  EXPECT_FALSE(attr3.IsSubsetOf(attr0));
  EXPECT_FALSE(attr3.IsSubsetOf(attr1));
  EXPECT_FALSE(attr3.IsSubsetOf(attr2));
  EXPECT_TRUE(attr3.IsSubsetOf(attr3));
}

}  // namespace
}  // namespace arolla::expr
