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
#include "arolla/qtype/simple_qtype.h"

#include <cstdint>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/struct_field.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::MatchesRegex;

struct TypeWithRepr {};
struct TypeWithoutRepr {};

struct FullFeaturedType {
  int32_t state;
};

struct TypeWithNamedFields {
  float x;
  double y;
  constexpr static auto ArollaStructFields() {
    using CppType = TypeWithNamedFields;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(x),
        AROLLA_DECLARE_STRUCT_FIELD(y),
    };
  }
  void ArollaFingerprint(FingerprintHasher* hasher) const {
    CombineStructFields(hasher, *this);
  }
};

}  // namespace

AROLLA_DECLARE_QTYPE(TypeWithRepr);
AROLLA_DECLARE_QTYPE(TypeWithoutRepr);
AROLLA_DECLARE_QTYPE(FullFeaturedType);
AROLLA_DECLARE_QTYPE(TypeWithNamedFields);

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(TypeWithRepr);
void FingerprintHasherTraits<TypeWithRepr>::operator()(
    FingerprintHasher*, const TypeWithRepr&) const {}

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(TypeWithoutRepr);
void FingerprintHasherTraits<TypeWithoutRepr>::operator()(
    FingerprintHasher*, const TypeWithoutRepr&) const {}

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(FullFeaturedType);
void FingerprintHasherTraits<FullFeaturedType>::operator()(
    FingerprintHasher* hasher, const FullFeaturedType& value) const {
  hasher->Combine(value.state);
}

AROLLA_DECLARE_REPR(TypeWithRepr);
ReprToken ReprTraits<TypeWithRepr>::operator()(const TypeWithRepr&) const {
  return ReprToken{"type_with_repr", {10, 50}};
}

AROLLA_DECLARE_REPR(FullFeaturedType);
ReprToken ReprTraits<FullFeaturedType>::operator()(
    const FullFeaturedType& value) const {
  return ReprToken{absl::StrFormat("FullFeaturedType{%d}", value.state),
                   {31, 27}};
}

AROLLA_DEFINE_SIMPLE_QTYPE(TYPE_WITH_REPR, TypeWithRepr);
AROLLA_DEFINE_SIMPLE_QTYPE(TYPE_WITHOUT_REPR, TypeWithoutRepr);
AROLLA_DEFINE_SIMPLE_QTYPE(TYPE_WITH_NAMED_FIELDS, TypeWithNamedFields);

QTypePtr QTypeTraits<FullFeaturedType>::type() {
  struct FullFeaturedTypeQType final : SimpleQType {
    FullFeaturedTypeQType()
        : SimpleQType(
              meta::type<FullFeaturedType>(), "FullFeaturedType",
              /*value_qtype=*/GetQType<TypeWithoutRepr>(),
              /*qtype_specialization_key=*/"::arolla::FullFeaturedQType") {}
    absl::string_view UnsafePyQValueSpecializationKey(
        const void* /*source*/) const final {
      return "::arolla::FullFeaturedQValue";
    }
  };
  static const absl::NoDestructor<FullFeaturedTypeQType> result;
  return result.get();
}

namespace {

TEST(SimpleQType, TypeWithRepr) {
  TypeWithRepr x;
  EXPECT_THAT(GetQType<TypeWithRepr>()->UnsafeReprToken(&x),
              ReprTokenEq("type_with_repr", {10, 50}));
}

TEST(SimpleQType, TypeWithoutRepr) {
  TypeWithoutRepr x;
  const auto repr_result = GetQType<TypeWithoutRepr>()->UnsafeReprToken(&x);
  EXPECT_THAT(repr_result.str,
              MatchesRegex("<value of TYPE_WITHOUT_REPR at 0x[0-9a-f]+>"));
  EXPECT_THAT(repr_result.precedence.left, -1);
  EXPECT_THAT(repr_result.precedence.right, -1);
}

TEST(SimpleQType, FullFeaturedQType) {
  auto qtype = GetQType<FullFeaturedType>();
  const FullFeaturedType x{4};
  EXPECT_EQ(qtype->value_qtype(), GetQType<TypeWithoutRepr>());
  EXPECT_EQ(qtype->qtype_specialization_key(), "::arolla::FullFeaturedQType");
  EXPECT_THAT(qtype->UnsafeReprToken(&x),
              ReprTokenEq("FullFeaturedType{4}", {31, 27}));
  EXPECT_EQ(qtype->UnsafePyQValueSpecializationKey(&x),
            "::arolla::FullFeaturedQValue");
  FingerprintHasher hx("salt");
  FingerprintHasher hy("salt");
  const FullFeaturedType y{3};
  qtype->UnsafeCombineToFingerprintHasher(&x, &hx);
  qtype->UnsafeCombineToFingerprintHasher(&y, &hy);
  EXPECT_NE(std::move(hx).Finish(), std::move(hy).Finish());
}

TEST(SimpleQType, TypeWithNames) {
  QTypePtr qtype = GetQType<TypeWithNamedFields>();
  EXPECT_THAT(GetFieldNames(qtype), ElementsAre("x", "y"));
  EXPECT_EQ(GetFieldIndexByName(qtype, "x"), 0);
  EXPECT_EQ(GetFieldIndexByName(qtype, "y"), 1);
  EXPECT_EQ(GetFieldIndexByName(qtype, "z"), std::nullopt);
}

TEST(SimpleQType, TypeWithNamesErrors) {
  QTypePtr qtype = GetQType<int>();
  EXPECT_THAT(GetFieldNames(qtype), IsEmpty());
  EXPECT_EQ(GetFieldIndexByName(qtype, "y"), std::nullopt);
  EXPECT_EQ(GetFieldIndexByName(qtype, "x"), std::nullopt);
}

}  // namespace
}  // namespace arolla
