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
#include "arolla/memory/optional_value.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/util/bytes.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/view_types.h"

namespace arolla {
namespace testing {
namespace {

using absl_testing::IsOkAndHolds;
using absl_testing::StatusIs;
using ::testing::HasSubstr;
using ::testing::Test;

TEST(OptionalValueTest, TestEmptyValues) {
  // Default, empty.
  OptionalValue<float> v1;
  EXPECT_FALSE(v1.present);

  // Construct from empty std::optional.
  OptionalValue<float> v2(std::optional<float>{});
  EXPECT_FALSE(v2.present);

  // Construct from std::nullopt.
  OptionalValue<float> v3(std::nullopt);
  EXPECT_FALSE(v3.present);

  // Empty values are equal.
  EXPECT_EQ(v1, v2);
  EXPECT_EQ(v1, v3);

  // Values are ignored in equality test of empty OptionalValues.
  v1.value = 1.0f;
  v2.value = 2.0f;
  EXPECT_EQ(v1, v2);

  // Convert back to std::optional<>.
  auto absl_v = v2.AsOptional();
  EXPECT_FALSE(absl_v.has_value());
}

TEST(OptionalValueTest, TestConstExpr) {
  static_assert(!OptionalValue<int>().present);
  static_assert(OptionalValue<int>(5).present);
  static_assert(OptionalValue<int>(5).value == 5);
  static_assert(MakeOptionalValue(5).present);
  static_assert(MakeOptionalValue(5).value == 5);
}

TEST(OptionalValueTest, TestPresentValues) {
  // Construct from T
  OptionalValue<float> v1(1.0f);
  EXPECT_TRUE(v1.present);
  EXPECT_EQ(1.0f, v1.value);
  EXPECT_EQ(Repr(v1), "optional_float32{1.}");

  // MakeOptionalValue
  auto v_auto = MakeOptionalValue(1.0f);
  EXPECT_TRUE(v_auto.present);
  EXPECT_EQ(1.0f, v_auto.value);
  EXPECT_EQ(Repr(v_auto), "optional_float32{1.}");

  OptionalValue<float> v2(std::optional<float>{2.0f});
  EXPECT_TRUE(v2.present);
  EXPECT_EQ(2.0f, v2.value);
  EXPECT_EQ(Repr(v2), "optional_float32{2.}");

  // Both present, values differ.
  EXPECT_NE(v1, v2);

  v1.value = 2.0f;

  // Both present, values same.
  EXPECT_EQ(v1, v2);
}

TEST(OptionalValueTest, TestAssignment) {
  // Construct from T
  OptionalValue<float> v1;
  v1 = 1.0f;
  EXPECT_TRUE(v1.present);
  EXPECT_EQ(v1.value, 1.0f);

  v1 = std::nullopt;
  EXPECT_FALSE(v1.present);
}

TEST(OptionalValueTest, MakeStatusOrOptionalValue) {
  absl::StatusOr<OptionalValue<float>> v =
      MakeStatusOrOptionalValue(absl::StatusOr<float>(1.0f));
  ASSERT_OK(v.status());
  EXPECT_TRUE(v.value().present);
  EXPECT_EQ(v.value().value, 1.0f);

  absl::StatusOr<OptionalValue<float>> v_error = MakeStatusOrOptionalValue(
      absl::StatusOr<float>(absl::InternalError("fake")));
  EXPECT_THAT(v_error.status(),
              StatusIs(absl::StatusCode::kInternal, HasSubstr("fake")));
}

TEST(OptionalValueTest, OptionalUnit) {
  EXPECT_EQ(OptionalUnit(), kMissing);
  EXPECT_EQ(OptionalUnit(false), kMissing);
  EXPECT_FALSE(kMissing);
  EXPECT_FALSE(kMissing.present);
  EXPECT_EQ(Repr(kMissing), "missing");

  EXPECT_EQ(OptionalUnit(true), kPresent);
  EXPECT_TRUE(kPresent);
  EXPECT_TRUE(kPresent.present);
  EXPECT_EQ(Repr(kPresent), "present");
}

TEST(OptionalValueTest, Comparison) {
  OptionalValue<float> v0;
  v0.value = 1.0f;  // ignored
  OptionalValue<float> v1(1.0f);
  OptionalValue<float> v2(2.0f);
  {  // optionals
    EXPECT_TRUE(v1 == v1);
    EXPECT_TRUE(v0 == v0);
    EXPECT_FALSE(v1 == v2);
    EXPECT_FALSE(v1 == v0);
    EXPECT_FALSE(v1 != v1);
    EXPECT_FALSE(v0 != v0);
    EXPECT_TRUE(v1 != v2);
    EXPECT_TRUE(v1 != v0);
    OptionalValue<float> v0_2;
    v0_2.value = 2.0f;  // ignored
    EXPECT_TRUE(v0 == v0_2);
    EXPECT_FALSE(v0 != v0_2);
  }
  {  // scalar
    EXPECT_TRUE(v1 == 1.0f);
    EXPECT_TRUE(1.0f == v1);
    EXPECT_FALSE(v1 != 1.0f);
    EXPECT_FALSE(1.0f != v1);
    EXPECT_FALSE(v1 == 2.0f);
    EXPECT_FALSE(2.0f == v1);
    EXPECT_TRUE(v1 != 2.0f);
    EXPECT_TRUE(2.0f != v1);
  }
  {  // nullopt
    EXPECT_FALSE(v1 == std::nullopt);
    EXPECT_FALSE(std::nullopt == v1);
    EXPECT_TRUE(v0 == std::nullopt);
    EXPECT_TRUE(std::nullopt == v0);
    EXPECT_TRUE(v1 != std::nullopt);
    EXPECT_TRUE(std::nullopt != v1);
    EXPECT_FALSE(v0 != std::nullopt);
    EXPECT_FALSE(std::nullopt != v0);
  }
}

TEST(OptionalValueTest, TestImplicitConstructors) {
  OptionalValue<float> v = {};
  EXPECT_EQ(v, OptionalValue<float>());
  v = 3.5;
  EXPECT_EQ(v, OptionalValue<float>(3.5));
  v = std::optional<float>(2.5);
  EXPECT_EQ(v, OptionalValue<float>(2.5));
}

TEST(OptionalValueTest, TestMoves) {
  // Pass around unique_ptr, which requires move operators.
  auto ptr = std::make_unique<std::string>("Hello!");
  OptionalValue<std::unique_ptr<std::string>> v1(std::move(ptr));

  // Verify data was copied in.
  EXPECT_TRUE(v1.present);
  EXPECT_EQ("Hello!", *(v1.value));

  // Convert to std::optional
  std::optional<std::unique_ptr<std::string>> v2(std::move(v1).AsOptional());
  EXPECT_TRUE(v2.has_value());
  EXPECT_EQ("Hello!", **v2);
}

template <typename T>
using Slot = FrameLayout::Slot<T>;

TEST(OptionalValueTest, TestFrameLayout) {
  FrameLayout::Builder builder;

  // Add a couple slots so we have a non-zero offset.
  builder.AddSlot<double>();
  builder.AddSlot<int32_t>();

  // Add an optional slot.
  auto optional_slot = builder.AddSlot<OptionalValue<float>>();

  // Extract it's sub-slots.
  Slot<bool> presence_slot = optional_slot.GetSubslot<0>();
  Slot<float> value_slot = optional_slot.GetSubslot<1>();

  // Create layout.
  FrameLayout layout = std::move(builder).Build();

  // Allocate layout.
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  // Load a value into the optional slot.
  frame.Set(optional_slot, OptionalValue<float>{1.0f});

  // Verify we can read it using the sub-slots.
  EXPECT_EQ(true, frame.Get(presence_slot));
  EXPECT_EQ(1.0f, frame.Get(value_slot));

  // Modify the value via the sub-slot.
  frame.Set(value_slot, 2.0f);

  // And access via the original optional slot.
  EXPECT_EQ(2.0, frame.Get(optional_slot).value);
}

TEST(OptionalValue, IsBZeroConstructible) {
  EXPECT_TRUE(is_bzero_constructible<OptionalValue<float>>());
  EXPECT_TRUE(is_bzero_constructible<OptionalValue<int>>());
  EXPECT_FALSE(is_bzero_constructible<OptionalValue<std::string>>());
}

TEST(OptionalValue, BZeroStateIsEmptyValue) {
  using T = OptionalValue<float>;
  std::aligned_storage_t<sizeof(T), alignof(T)> storage;
  memset(&storage, 0, sizeof(storage));
  EXPECT_FALSE(std::launder(reinterpret_cast<const T*>(&storage))->present);
}

TEST(OptionalValue, StructuredBindings) {
  {
    OptionalValue<float> f;
    auto [present, value] = f;
    EXPECT_FALSE(present);
  }
  {
    OptionalValue<float> f = 17.0;
    auto [present, value] = f;
    EXPECT_TRUE(present);
    EXPECT_EQ(value, 17.0);
  }
}

TEST(OptionalValue, ViewType) {
  static_assert(std::is_same_v<view_type_t<OptionalValue<int64_t>>,
                               OptionalValue<int64_t>>);
  static_assert(std::is_same_v<view_type_t<OptionalValue<Bytes>>,
                               OptionalValue<absl::string_view>>);
  auto fn = [](OptionalValue<absl::string_view> v) -> char {
    return (v.present && !v.value.empty()) ? v.value[0] : 'X';
  };
  EXPECT_EQ(fn(OptionalValue<Text>(Text("Hello"))), 'H');
  EXPECT_EQ(fn(std::nullopt), 'X');
}

TEST(OptionalValue, WrapFnToAcceptOptionalArgs) {
  {
    auto fn = [](int a, OptionalValue<int64_t> b, int64_t c) -> int {
      return a + c + (b.present ? b.value : 10);
    };
    auto opt_fn = WrapFnToAcceptOptionalArgs(fn);
    EXPECT_EQ(opt_fn(1, 2, 3), OptionalValue<int>(6));
    EXPECT_EQ(opt_fn(std::nullopt, 2, 3), OptionalValue<int>());
    EXPECT_EQ(opt_fn(1, std::nullopt, 3), OptionalValue<int>(14));
    EXPECT_EQ(opt_fn(1, 2, std::nullopt), OptionalValue<int>());
  }
  {  // const reference
    auto fn = [](const Bytes& v) -> const Bytes& { return v; };
    auto opt_fn = WrapFnToAcceptOptionalArgs(fn);
    EXPECT_EQ(opt_fn(Bytes("123")), OptionalValue<Bytes>("123"));
  }
  {  // view type
    auto fn = [](absl::string_view v) { return v; };
    auto opt_fn = WrapFnToAcceptOptionalArgs(fn);
    EXPECT_EQ(opt_fn(MakeOptionalValue(Bytes("123"))),
              MakeOptionalValue(absl::string_view("123")));
  }
  {  // status
    auto fn = [](int a, OptionalValue<int64_t> b,
                 int64_t c) -> absl::StatusOr<int> {
      if (c < 0) {
        return absl::InvalidArgumentError("c < 0");
      } else {
        return a + c + (b.present ? b.value : 10);
      }
    };
    auto opt_fn = WrapFnToAcceptOptionalArgs(fn);
    EXPECT_THAT(opt_fn(1, 2, 3), IsOkAndHolds(OptionalValue<int>(6)));
    EXPECT_THAT(opt_fn(1, 2, -3),
                StatusIs(absl::StatusCode::kInvalidArgument, "c < 0"));
    EXPECT_THAT(opt_fn(std::nullopt, 2, -3),
                IsOkAndHolds(OptionalValue<int>()));
  }
}

TEST(OptionalValueReprTest, bool) {
  EXPECT_EQ(Repr(OptionalValue<bool>(true)), "optional_boolean{true}");
  EXPECT_EQ(Repr(OptionalValue<bool>()), "optional_boolean{NA}");
}

TEST(OptionalValueReprTest, int32_t) {
  EXPECT_EQ(Repr(OptionalValue<int32_t>(1)), "optional_int32{1}");
  EXPECT_EQ(Repr(OptionalValue<int32_t>()), "optional_int32{NA}");
}

TEST(OptionalValueReprTest, int64_t) {
  EXPECT_EQ(Repr(OptionalValue<int64_t>(1)), "optional_int64{1}");
  EXPECT_EQ(Repr(OptionalValue<int64_t>()), "optional_int64{NA}");
}

TEST(OptionalValueReprTest, uint64_t) {
  EXPECT_EQ(Repr(OptionalValue<uint64_t>(1)), "optional_uint64{1}");
  EXPECT_EQ(Repr(OptionalValue<uint64_t>()), "optional_uint64{NA}");
}

TEST(OptionalValueReprTest, float) {
  EXPECT_EQ(Repr(OptionalValue<float>(1.5)), "optional_float32{1.5}");
  EXPECT_EQ(Repr(OptionalValue<float>()), "optional_float32{NA}");
}

TEST(OptionalValueReprTest, double) {
  EXPECT_EQ(Repr(OptionalValue<double>(1.5)), "optional_float64{1.5}");
  EXPECT_EQ(Repr(OptionalValue<double>()), "optional_float64{NA}");
}

TEST(OptionalValueReprTest, Bytes) {
  EXPECT_EQ(Repr(OptionalValue<Bytes>("abc")), "optional_bytes{b'abc'}");
  EXPECT_EQ(Repr(OptionalValue<Bytes>()), "optional_bytes{NA}");
}

TEST(OptionalValueReprTest, Text) {
  EXPECT_EQ(Repr(OptionalValue<Text>("abc")), "optional_text{'abc'}");
  EXPECT_EQ(Repr(OptionalValue<Text>()), "optional_text{NA}");
}

TEST(OptionalValueReprTest, StreamOp) {
  {
    std::ostringstream oss;
    oss << OptionalValue<float>(1.5);
    EXPECT_EQ(oss.str(), "optional_float32{1.5}");
  }
  {
    std::ostringstream oss;
    oss << OptionalValue<float>();
    EXPECT_EQ(oss.str(), "optional_float32{NA}");
  }
}

}  // namespace
}  // namespace testing
}  // namespace arolla
