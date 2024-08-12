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
#include "arolla/qtype/typed_value.h"

#include <cstdint>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"

// A struct without QTypeTraits<T> specialization.
struct WithoutQTypeTraits {};

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::Eq;
using ::testing::HasSubstr;

TEST(TypedValueTest, ReprBasic) {
  EXPECT_EQ(TypedValue::FromValue<bool>(true).Repr(), "true");
  EXPECT_EQ(TypedValue::FromValue<int32_t>(5).Repr(), "5");
  EXPECT_EQ(TypedValue::FromValue<int64_t>(5).Repr(), "int64{5}");
  EXPECT_EQ(TypedValue::FromValue<uint64_t>(5).Repr(), "uint64{5}");
  EXPECT_EQ(TypedValue::FromValue<float>(5.0f).Repr(), "5.");
  EXPECT_EQ(TypedValue::FromValue<double>(5.0).Repr(), "float64{5}");
}

TEST(TypedValueTest, FromValue) {
  auto tval = TypedValue::FromValue<int64_t>(1);
  EXPECT_THAT(tval.GetType(), Eq(GetQType<int64_t>()));
  EXPECT_THAT(tval.As<int64_t>(), IsOkAndHolds(int64_t{1}));
  EXPECT_THAT(tval.As<float>().status(),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST(TypedValueTest, As) {
  auto int_value = TypedValue::FromValue<double>(1.0);
  EXPECT_THAT(int_value.As<double>(), IsOkAndHolds(1.0));
  EXPECT_THAT(int_value.As<float>().status(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("type mismatch: expected C++ type `double` "
                                 "(FLOAT64), got `float`")));
  EXPECT_THAT(int_value.As<WithoutQTypeTraits>().status(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("type mismatch: expected C++ type `double` "
                                 "(FLOAT64), got `WithoutQTypeTraits`")));
}

TEST(TypedValueTest, FromValueWithQType) {
  auto f64 = GetQType<double>();
  absl::StatusOr<TypedValue> tmp = TypedValue::FromValueWithQType(1.0, f64);
  auto tval = std::move(tmp).value();
  EXPECT_THAT(tval.GetType(), Eq(f64));
  EXPECT_THAT(tval.As<double>(), IsOkAndHolds(1.0));
  EXPECT_THAT(tval.As<float>(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("type mismatch: expected C++ type `double` "
                                 "(FLOAT64), got `float`")));

  // Value must match explicit type.
  EXPECT_THAT(TypedValue::FromValueWithQType(1.0f, f64).status(),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST(TypedValueTest, UnsafeFromTypeDefaultConstructed) {
  {
    auto f64 = GetQType<double>();
    auto tval = TypedValue::UnsafeFromTypeDefaultConstructed(f64);
    EXPECT_THAT(tval.GetType(), Eq(GetQType<double>()));
    EXPECT_THAT(tval.As<double>(), IsOkAndHolds(double{0.}));
    EXPECT_THAT(tval.As<float>().status(),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    auto bytes = GetQType<Bytes>();
    auto tval = TypedValue::UnsafeFromTypeDefaultConstructed(bytes);
    EXPECT_THAT(tval.GetType(), Eq(GetQType<Bytes>()));
    ASSERT_OK_AND_ASSIGN(auto val_ref, tval.As<Bytes>());
    EXPECT_THAT(val_ref.get(), Eq(Bytes()));
    EXPECT_THAT(tval.As<float>().status(),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {
    auto of64 = GetQType<OptionalValue<double>>();
    auto tval = TypedValue::UnsafeFromTypeDefaultConstructed(of64);
    EXPECT_THAT(tval.GetType(), Eq(GetQType<OptionalValue<double>>()));
    ASSERT_OK_AND_ASSIGN(auto val_ref, tval.As<OptionalValue<double>>());
    EXPECT_THAT(val_ref.get(), Eq(OptionalValue<double>()));
    EXPECT_THAT(tval.As<OptionalValue<float>>().status(),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

TEST(TypedValueTest, FromSlot) {
  FrameLayout::Builder builder;
  QTypePtr f32 = GetQType<float>();
  TypedSlot tslot = AddSlot(f32, &builder);
  auto layout = std::move(builder).Build();

  MemoryAllocation alloc(&layout);
  FramePtr ptr = alloc.frame();

  EXPECT_OK(TypedValue::FromValue(7.5f).CopyToSlot(tslot, ptr));
  auto tval = TypedValue::FromSlot(tslot, ptr);

  EXPECT_THAT(tval.GetType(), Eq(f32));
  EXPECT_THAT(tval.As<float>(), IsOkAndHolds(7.5f));
}

TEST(TypedValueTest, ToSlot) {
  FrameLayout::Builder builder;
  QTypePtr f64 = GetQType<double>();
  TypedSlot tslot = AddSlot(f64, &builder);
  auto layout = std::move(builder).Build();

  MemoryAllocation alloc(&layout);
  FramePtr ptr = alloc.frame();

  auto tval = TypedValue::FromValue(double{1.0});
  EXPECT_OK(tval.CopyToSlot(tslot, ptr));

  auto slot = tslot.ToSlot<double>().value();
  EXPECT_THAT(ptr.Get(slot), Eq(1.0));
}

TEST(TypedValueTest, Copy) {
  auto tval = TypedValue::FromValue(double{1.0});
  auto tval_copy = tval;
  EXPECT_THAT(tval_copy.As<double>(), IsOkAndHolds(1.0));
}

TEST(TypedValueTest, FingerprintUniqueness) {
  absl::flat_hash_set<Fingerprint> fingerprints;
  EXPECT_TRUE(
      fingerprints.insert(TypedValue::FromValue(int32_t{0}).GetFingerprint())
          .second);
  EXPECT_TRUE(
      fingerprints.insert(TypedValue::FromValue(int64_t{0}).GetFingerprint())
          .second);
  EXPECT_TRUE(
      fingerprints.insert(TypedValue::FromValue(uint64_t{0}).GetFingerprint())
          .second);
  EXPECT_TRUE(
      fingerprints.insert(TypedValue::FromValue(double{0}).GetFingerprint())
          .second);
  EXPECT_TRUE(
      fingerprints.insert(TypedValue::FromValue(float{0}).GetFingerprint())
          .second);
}

TEST(TypedValueTest, FingerprintReproducibility) {
  EXPECT_EQ(TypedValue::FromValue(int32_t{0}).GetFingerprint(),
            TypedValue::FromValue(int32_t{0}).GetFingerprint());
  EXPECT_EQ(TypedValue::FromValue(int64_t{0}).GetFingerprint(),
            TypedValue::FromValue(int64_t{0}).GetFingerprint());
  EXPECT_EQ(TypedValue::FromValue(uint64_t{0}).GetFingerprint(),
            TypedValue::FromValue(uint64_t{0}).GetFingerprint());
  EXPECT_EQ(TypedValue::FromValue(float{0}).GetFingerprint(),
            TypedValue::FromValue(float{0}).GetFingerprint());
  EXPECT_EQ(TypedValue::FromValue(double{0}).GetFingerprint(),
            TypedValue::FromValue(double{0}).GetFingerprint());
}

TEST(TypedValueTest, UnsafeAs) {
  auto tval = TypedValue::FromValue<int64_t>(1);
  ASSERT_THAT(tval.GetType(), Eq(GetQType<int64_t>()));
  EXPECT_THAT(tval.UnsafeAs<int64_t>(), Eq(int64_t{1}));
}

TEST(TypedValueTest, CopyConstructor) {
  TypedValue x = TypedValue::FromValue<int64_t>(1);
  TypedValue y = x;
  EXPECT_EQ(x.GetType(), y.GetType());
  EXPECT_EQ(x.GetRawPointer(), y.GetRawPointer());
}

TEST(TypedValueTest, CopyOperator) {
  TypedValue x = TypedValue::FromValue<int64_t>(1);
  TypedValue y = TypedValue::FromValue<int64_t>(2);
  y = x;
  EXPECT_EQ(x.GetType(), y.GetType());
  EXPECT_EQ(x.GetRawPointer(), y.GetRawPointer());
}

TEST(TypedValueTest, MoveConstructor) {
  TypedValue x = TypedValue::FromValue<int64_t>(1);
  auto* x_type = x.GetType();
  auto* x_raw_ptr = x.GetRawPointer();
  TypedValue y = std::move(x);
  EXPECT_EQ(y.GetType(), x_type);
  EXPECT_EQ(y.GetRawPointer(), x_raw_ptr);
}

TEST(TypedValueTest, MoveOperator) {
  TypedValue x = TypedValue::FromValue<int64_t>(1);
  TypedValue y = TypedValue::FromValue<int64_t>(2);
  auto* x_type = x.GetType();
  auto* x_raw_ptr = x.GetRawPointer();
  y = std::move(x);
  EXPECT_EQ(y.GetType(), x_type);
  EXPECT_EQ(y.GetRawPointer(), x_raw_ptr);
}

TEST(TypedValueTest, CopyFromValue) {
  const Bytes bytes("data");
  TypedValue x = TypedValue::FromValue(bytes);
  ASSERT_OK_AND_ASSIGN(Bytes x_bytes, x.As<Bytes>());
  EXPECT_THAT(x_bytes, Eq(bytes));
}

TEST(TypedValueTest, CopyFromValueT) {
  const Bytes bytes("data");
  TypedValue x = TypedValue::FromValue<Bytes>(bytes);
  ASSERT_OK_AND_ASSIGN(Bytes x_bytes, x.As<Bytes>());
  EXPECT_THAT(x_bytes, Eq(bytes));
}

TEST(TypedValueTest, MoveFromValueT) {
  Bytes bytes("a long string literal to ensure memory allocation");
  auto* data_raw_ptr = bytes.data();
  TypedValue x = TypedValue::FromValue<Bytes>(std::move(bytes));
  EXPECT_EQ(x.UnsafeAs<Bytes>().data(), data_raw_ptr);
}

TEST(TypedValueTest, CopyFromValueWithQType) {
  const Bytes bytes("data");
  ASSERT_OK_AND_ASSIGN(
      TypedValue x, TypedValue::FromValueWithQType(bytes, GetQType<Bytes>()));
  ASSERT_OK_AND_ASSIGN(Bytes x_bytes, x.As<Bytes>());
  EXPECT_THAT(x_bytes, Eq(bytes));
}

TEST(TypedValueTest, MoveFromValueWithQType) {
  Bytes bytes("a long string literal to ensure memory allocation");
  auto* data_raw_ptr = bytes.data();
  ASSERT_OK_AND_ASSIGN(TypedValue x, TypedValue::FromValueWithQType(
                                         std::move(bytes), GetQType<Bytes>()));
  EXPECT_EQ(x.UnsafeAs<Bytes>().data(), data_raw_ptr);
}

}  // namespace
}  // namespace arolla
