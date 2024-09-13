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
#include "arolla/qtype/derived_qtype.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;

// An example of a derived qtype.
struct PointQType final : BasicDerivedQType {
  PointQType()
      : BasicDerivedQType(ConstructorArgs{
            .name = "POINT",
            .base_qtype =
                MakeTupleQType({GetQType<double>(), GetQType<double>()}),
            .value_qtype = GetQType<double>(),
            .qtype_specialization_key = "::arolla::PointQType",
        }) {}

  static QTypePtr get() {
    static const absl::NoDestructor<PointQType> result;
    return result.get();
  }
};

TEST(BasicDerivedQTypeTest, QTypeProperties) {
  const auto point_qtype = PointQType::get();
  EXPECT_EQ(point_qtype->name(), "POINT");
  EXPECT_EQ(point_qtype->value_qtype(), GetQType<double>());
  EXPECT_EQ(point_qtype->qtype_specialization_key(), "::arolla::PointQType");
  const auto tuple_qtype =
      MakeTupleQType({GetQType<double>(), GetQType<double>()});
  EXPECT_EQ(point_qtype->type_info(), tuple_qtype->type_info());
  EXPECT_EQ(point_qtype->type_layout().AllocSize(),
            tuple_qtype->type_layout().AllocSize());
  EXPECT_EQ(point_qtype->type_layout().AllocAlignment().value,
            tuple_qtype->type_layout().AllocAlignment().value);
  EXPECT_EQ(point_qtype->type_fields().size(), 2);
}

TEST(BasicDerivedQTypeTest, DefaultRepr) {
  const auto tuple_qvalue = MakeTupleFromFields(1., 2.);
  const auto point_qvalue =
      UnsafeDowncastDerivedQValue(PointQType::get(), tuple_qvalue.AsRef());
  EXPECT_THAT(point_qvalue.GenReprToken(),
              ReprTokenEq("POINT{(float64{1}, float64{2})}"));
}

TEST(BasicDerivedQTypeTest, UnsafeCombineToFingerprintHasher) {
  const auto tuple_qvalue = MakeTupleFromFields(1., 2.);
  const auto* tuple_qtype = tuple_qvalue.GetType();
  const auto* point_qtype = PointQType::get();
  FingerprintHasher hasher1("seed");
  FingerprintHasher hasher2("seed");
  tuple_qtype->UnsafeCombineToFingerprintHasher(tuple_qvalue.GetRawPointer(),
                                                &hasher1);
  point_qtype->UnsafeCombineToFingerprintHasher(tuple_qvalue.GetRawPointer(),
                                                &hasher2);
  EXPECT_EQ(std::move(hasher1).Finish(), std::move(hasher2).Finish());
}

TEST(BasicDerivedQTypeTest, DecayDerivedQType) {
  const auto point_qtype = PointQType::get();
  const auto tuple_qtype =
      MakeTupleQType({GetQType<double>(), GetQType<double>()});
  EXPECT_NE(point_qtype, tuple_qtype);
  EXPECT_EQ(DecayDerivedQType(point_qtype), tuple_qtype);
  EXPECT_EQ(DecayDerivedQType(tuple_qtype), tuple_qtype);
  EXPECT_EQ(DecayDerivedQType(nullptr), nullptr);
}

TEST(BasicDerivedQTypeTest, UnsafeDowncastDerivedQRef) {
  const auto tuple_qvalue = MakeTupleFromFields(1., 2.);
  const auto point_qvalue = TypedValue(
      UnsafeDowncastDerivedQValue(PointQType::get(), tuple_qvalue.AsRef()));
  EXPECT_EQ(point_qvalue.GetType(), PointQType::get());
  EXPECT_NE(point_qvalue.GetFingerprint(), tuple_qvalue.GetFingerprint());
}

TEST(BasicDerivedQTypeTest, UnsafeDowncastDerivedQValue) {
  const auto tuple_qvalue = MakeTupleFromFields(1., 2.);
  const auto point_qvalue =
      UnsafeDowncastDerivedQValue(PointQType::get(), tuple_qvalue);
  EXPECT_EQ(point_qvalue.GetType(), PointQType::get());
  EXPECT_NE(point_qvalue.GetFingerprint(), tuple_qvalue.GetFingerprint());
}

TEST(BasicDerivedQTypeTest, DecayDerivedQRef) {
  const auto tuple_qvalue = MakeTupleFromFields(1., 2.);
  const auto point_qvalue = TypedValue(
      UnsafeDowncastDerivedQValue(PointQType::get(), tuple_qvalue.AsRef()));
  EXPECT_NE(point_qvalue.GetFingerprint(), tuple_qvalue.GetFingerprint());
  EXPECT_EQ(
      TypedValue(DecayDerivedQValue(point_qvalue.AsRef())).GetFingerprint(),
      tuple_qvalue.GetFingerprint());
  EXPECT_EQ(
      TypedValue(DecayDerivedQValue(tuple_qvalue.AsRef())).GetFingerprint(),
      tuple_qvalue.GetFingerprint());
}

TEST(BasicDerivedQTypeTest, DecayDerivedQValue) {
  const auto tuple_qvalue = MakeTupleFromFields(1., 2.);
  const auto point_qvalue =
      UnsafeDowncastDerivedQValue(PointQType::get(), tuple_qvalue);
  EXPECT_NE(point_qvalue.GetFingerprint(), tuple_qvalue.GetFingerprint());
  EXPECT_EQ(DecayDerivedQValue(point_qvalue).GetFingerprint(),
            tuple_qvalue.GetFingerprint());
  EXPECT_EQ(TypedValue(DecayDerivedQValue(tuple_qvalue)).GetFingerprint(),
            tuple_qvalue.GetFingerprint());
}

}  // namespace
}  // namespace arolla
