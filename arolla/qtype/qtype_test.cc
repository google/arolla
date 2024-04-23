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
#include "arolla/qtype/qtype.h"

#include <cstdint>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash_testing.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_test_utils.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/testing/status_matchers_backport.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

struct DummyType {};

}  // namespace

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(DummyType);
void FingerprintHasherTraits<DummyType>::operator()(
    FingerprintHasher* hasher, const DummyType& value) const {
  hasher->Combine(absl::string_view("dummy_value"));
}

// Define a type with no corresponding OptionalType.
AROLLA_DECLARE_SIMPLE_QTYPE(DUMMY, DummyType);
AROLLA_DEFINE_SIMPLE_QTYPE(DUMMY, DummyType);

namespace testing {
namespace {

using ::arolla::testing::StatusIs;
using ::testing::HasSubstr;
using ::testing::StrEq;

TEST(TypeTest, IsScalarQType) {
  EXPECT_TRUE(IsScalarQType(GetQType<Unit>()));
  EXPECT_TRUE(IsScalarQType(GetQType<bool>()));
  EXPECT_TRUE(IsScalarQType(GetQType<Bytes>()));
  EXPECT_TRUE(IsScalarQType(GetQType<Text>()));
  EXPECT_TRUE(IsScalarQType(GetQType<int32_t>()));
  EXPECT_TRUE(IsScalarQType(GetQType<int64_t>()));
  EXPECT_TRUE(IsScalarQType(GetQType<uint64_t>()));
  EXPECT_TRUE(IsScalarQType(GetQType<float>()));
  EXPECT_TRUE(IsScalarQType(GetQType<double>()));
  EXPECT_FALSE(IsScalarQType(GetOptionalQType<double>()));
}

TEST(TypeTest, IsIntegralScalarQType) {
  EXPECT_FALSE(IsIntegralScalarQType(GetQType<Unit>()));
  EXPECT_FALSE(IsIntegralScalarQType(GetQType<bool>()));
  EXPECT_FALSE(IsIntegralScalarQType(GetQType<Bytes>()));
  EXPECT_FALSE(IsIntegralScalarQType(GetQType<Text>()));
  EXPECT_TRUE(IsIntegralScalarQType(GetQType<int32_t>()));
  EXPECT_TRUE(IsIntegralScalarQType(GetQType<int64_t>()));
  EXPECT_FALSE(IsIntegralScalarQType(GetQType<uint64_t>()));
  EXPECT_FALSE(IsIntegralScalarQType(GetQType<float>()));
  EXPECT_FALSE(IsIntegralScalarQType(GetQType<double>()));
  EXPECT_FALSE(IsIntegralScalarQType(GetOptionalQType<int32_t>()));
}

TEST(TypeTest, IsFloatingPointScalarQType) {
  EXPECT_FALSE(IsFloatingPointScalarQType(GetQType<Unit>()));
  EXPECT_FALSE(IsFloatingPointScalarQType(GetQType<bool>()));
  EXPECT_FALSE(IsFloatingPointScalarQType(GetQType<Bytes>()));
  EXPECT_FALSE(IsFloatingPointScalarQType(GetQType<Text>()));
  EXPECT_FALSE(IsFloatingPointScalarQType(GetQType<int32_t>()));
  EXPECT_FALSE(IsFloatingPointScalarQType(GetQType<int64_t>()));
  EXPECT_FALSE(IsFloatingPointScalarQType(GetQType<uint64_t>()));
  EXPECT_TRUE(IsFloatingPointScalarQType(GetQType<float>()));
  EXPECT_TRUE(IsFloatingPointScalarQType(GetQType<double>()));
  EXPECT_FALSE(IsFloatingPointScalarQType(GetOptionalQType<double>()));
}

TEST(TypeTest, IsNumericScalarQType) {
  EXPECT_FALSE(IsNumericScalarQType(GetQType<Unit>()));
  EXPECT_FALSE(IsNumericScalarQType(GetQType<bool>()));
  EXPECT_FALSE(IsNumericScalarQType(GetQType<Bytes>()));
  EXPECT_FALSE(IsNumericScalarQType(GetQType<Text>()));
  EXPECT_TRUE(IsNumericScalarQType(GetQType<int32_t>()));
  EXPECT_TRUE(IsNumericScalarQType(GetQType<int64_t>()));
  EXPECT_FALSE(IsNumericScalarQType(GetQType<uint64_t>()));
  EXPECT_TRUE(IsNumericScalarQType(GetQType<float>()));
  EXPECT_TRUE(IsNumericScalarQType(GetQType<double>()));
  EXPECT_FALSE(IsNumericScalarQType(GetOptionalQType<double>()));
}

TEST(TypeTest, QTypeQType) {
  TestPrimitiveTraits<QTypePtr>("QTYPE", GetQTypeQType());
  EXPECT_EQ(GetQTypeQType(), GetQType<QTypePtr>());
}

TEST(TypeTest, NothingQType) {
  const auto nothing_qtype = GetNothingQType();
  EXPECT_EQ(nothing_qtype->name(), "NOTHING");
  EXPECT_EQ(nothing_qtype->type_layout().AllocSize(), 0);
  EXPECT_EQ(nothing_qtype->type_layout().AllocAlignment().value, 1);
  EXPECT_TRUE(nothing_qtype->type_fields().empty());
  EXPECT_EQ(nothing_qtype->value_qtype(), nullptr);
}

TEST(TypeTest, StdStringIsBytesQType) {
  TestPrimitiveTraits<std::string>("BYTES", std::string("Hello!"));
  EXPECT_TRUE(IsScalarQType(GetQType<std::string>()));
}

// Calls TestPrimitiveTraits for variety of numeric types.
TEST(TypeTest, NumericTypeTraits) {
  TestPrimitiveTraits<int32_t>("INT32", 19);
  TestPrimitiveTraits<int64_t>("INT64", 20);
  TestPrimitiveTraits<float>("FLOAT32", 13.3);
  TestPrimitiveTraits<double>("FLOAT64", 14.5);
  TestPrimitiveTraits<bool>("BOOLEAN", true);
  TestPrimitiveTraits<Bytes>("BYTES", Bytes("google"));
}

TEST(TypeTest, OptionalNumericTypeTraits) {
  TestPrimitiveTraits<OptionalValue<int32_t>>("OPTIONAL_INT32",
                                              OptionalValue<int32_t>());
  TestPrimitiveTraits<OptionalValue<int64_t>>("OPTIONAL_INT64",
                                              OptionalValue<int64_t>(20));
  TestPrimitiveTraits<OptionalValue<float>>("OPTIONAL_FLOAT32",
                                            OptionalValue<float>());
  TestPrimitiveTraits<OptionalValue<double>>("OPTIONAL_FLOAT64",
                                             OptionalValue<double>(14.5));
  TestPrimitiveTraits<OptionalValue<bool>>("OPTIONAL_BOOLEAN",
                                           OptionalValue<bool>(true));
}

TEST(TypeTest, GlobalConstants) {
  EXPECT_EQ(GetQType<float>(), GetQType<float>());
  EXPECT_EQ(GetOptionalQType<Bytes>(), GetOptionalQType<Bytes>());
}

TEST(TypeTest, VerifyQTypeTypeInfo) {
  EXPECT_OK(VerifyQTypeTypeInfo(GetQType<float>(), typeid(float)));
  EXPECT_OK(VerifyQTypeTypeInfo(GetQType<double>(), typeid(double)));
  EXPECT_THAT(VerifyQTypeTypeInfo(GetQType<float>(), typeid(double)),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("type mismatch: expected C++ type `float` "
                                 "(FLOAT32), got `double`")));
}

TEST(TypeTest, Hash) {
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(
      {GetQType<int32_t>(), GetQType<bool>(), GetQType<double>(),
       GetOptionalQType<int64_t>()}));
}

TEST(TypeTest, DebugPrintQType) {
  std::stringstream buffer;
  buffer << *(GetQType<float>());
  EXPECT_THAT(buffer.str(), StrEq("QType{FLOAT32}"));
}

TEST(TypeTest, DebugPrintQTypePtr) {
  std::stringstream buffer;
  buffer << GetQType<float>();
  EXPECT_THAT(buffer.str(), StrEq("QTypePtr{FLOAT32}"));
}

TEST(TypeTest, FormatTypeVector) {
  EXPECT_EQ("(FLOAT32,NULL)", FormatTypeVector({GetQType<float>(), nullptr}));
}

TEST(OptionalQTypeTest, ToOptionalType) {
  QTypePtr int32_qtype = GetQType<int32_t>();
  QTypePtr opt_int32_qtype = GetOptionalQType<int32_t>();
  QTypePtr dummy_qtype = GetQType<DummyType>();

  EXPECT_THAT(ToOptionalQType(int32_qtype), IsOkAndHolds(opt_int32_qtype));
  EXPECT_THAT(ToOptionalQType(opt_int32_qtype), IsOkAndHolds(opt_int32_qtype));
  EXPECT_THAT(ToOptionalQType(dummy_qtype).status(),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(OptionalQTypeTest, ValueQType) {
  EXPECT_EQ(GetOptionalQType<bool>()->value_qtype(), GetQType<bool>());
  EXPECT_EQ(GetOptionalQType<int32_t>()->value_qtype(), GetQType<int32_t>());
  EXPECT_EQ(GetOptionalQType<float>()->value_qtype(), GetQType<float>());
}

TEST(OptionalQTypeTest, DecayOptionalQType) {
  EXPECT_EQ(DecayOptionalQType(GetQType<bool>()), GetQType<bool>());
  EXPECT_EQ(DecayOptionalQType(GetQType<int32_t>()), GetQType<int32_t>());
  EXPECT_EQ(DecayOptionalQType(GetQType<float>()), GetQType<float>());
  EXPECT_EQ(DecayOptionalQType(GetOptionalQType<bool>()), GetQType<bool>());
  EXPECT_EQ(DecayOptionalQType(GetOptionalQType<int32_t>()),
            GetQType<int32_t>());
  EXPECT_EQ(DecayOptionalQType(GetOptionalQType<float>()), GetQType<float>());
}

#ifndef NDEBUG
// QType performs runtime type checks in debug builds only.

TEST(TypedSlotDeathTest, TypeMismatchOnCopy) {
  FrameLayout::Builder builder1;
  auto slot1 = builder1.AddSlot<float>();
  auto slot2 = builder1.AddSlot<int>();
  auto descriptor = std::move(builder1).Build();
  MemoryAllocation alloc(&descriptor);
  // attempting to copy slots from different types.
  EXPECT_DEATH(TypedSlot::FromSlot(slot1).CopyTo(
                   alloc.frame(), TypedSlot::FromSlot(slot2), alloc.frame()),
               "Type mismatch");
}

#endif

}  // namespace
}  // namespace testing
}  // namespace arolla
