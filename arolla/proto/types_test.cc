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
#include "arolla/proto/types.h"

#include <cstdint>
#include <string>
#include <type_traits>

#include "gtest/gtest.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bytes.h"

namespace arolla::proto {
namespace {

TEST(arolla_single_value_t, primitive) {
  static_assert(std::is_same_v<arolla_single_value_t<int>, int>);
  static_assert(std::is_same_v<arolla_single_value_t<int&>, int>);
  static_assert(std::is_same_v<arolla_single_value_t<const int&>, int>);
  static_assert(std::is_same_v<arolla_single_value_t<const int8_t&>, int>);
  static_assert(std::is_same_v<arolla_single_value_t<uint8_t>, int>);
  static_assert(std::is_same_v<arolla_single_value_t<int16_t>, int>);
  static_assert(std::is_same_v<arolla_single_value_t<uint16_t>, int>);
  static_assert(std::is_same_v<arolla_single_value_t<uint32_t>, int64_t>);
  static_assert(std::is_same_v<arolla_single_value_t<int*>, int*>);
  static_assert(std::is_same_v<arolla_single_value_t<const int*>, const int*>);
}

enum OldEnum { kFirst = 0 };

enum class NewEnum { kFirst = 0 };

enum class NewEnumByte : char { kFirst = 0 };

TEST(arolla_single_value_t, Enum) {
  static_assert(std::is_same_v<arolla_single_value_t<OldEnum>, int>);
  static_assert(std::is_same_v<arolla_single_value_t<NewEnum>, int>);
  static_assert(std::is_same_v<arolla_single_value_t<NewEnumByte>, int>);
}

TEST(arolla_single_value_t, string_conversion) {
  static_assert(
      std::is_same_v<arolla_single_value_t<std::string>, ::arolla::Bytes>);
  static_assert(
      std::is_same_v<arolla_single_value_t<std::string&>, ::arolla::Bytes>);
  static_assert(std::is_same_v<arolla_single_value_t<const std::string&>,
                               ::arolla::Bytes>);
}

TEST(arolla_single_value_t, string_view_conversion) {
  static_assert(std::is_same_v<arolla_single_value_t<absl::string_view>,
                               ::arolla::Bytes>);
  static_assert(std::is_same_v<arolla_single_value_t<absl::string_view&>,
                               ::arolla::Bytes>);
  static_assert(std::is_same_v<arolla_single_value_t<const absl::string_view&>,
                               ::arolla::Bytes>);
}

TEST(arolla_single_value_t, cord_conversion) {
  static_assert(
      std::is_same_v<arolla_single_value_t<absl::Cord>, ::arolla::Bytes>);
  static_assert(
      std::is_same_v<arolla_single_value_t<absl::Cord&>, ::arolla::Bytes>);
  static_assert(std::is_same_v<arolla_single_value_t<const absl::Cord&>,
                               ::arolla::Bytes>);
}

TEST(arolla_optional_value_t, primitive) {
  static_assert(std::is_same_v<arolla_optional_value_t<bool>,
                               ::arolla::OptionalValue<bool>>);
  static_assert(std::is_same_v<arolla_optional_value_t<int>,
                               ::arolla::OptionalValue<int>>);
  static_assert(std::is_same_v<arolla_optional_value_t<uint32_t>,
                               ::arolla::OptionalValue<int64_t>>);
  static_assert(std::is_same_v<arolla_optional_value_t<const int&>,
                               ::arolla::OptionalValue<int>>);
  static_assert(std::is_same_v<arolla_optional_value_t<const float&>,
                               ::arolla::OptionalValue<float>>);
}

}  // namespace
}  // namespace arolla::proto

// Test outside of the namespace
namespace {

struct ProtoFake {
  bool has_abc32() const { return res; }
  int abc32() const { return 1; }
  void clear_abc32() {}

  int xyz32() const { return 1; }
  void clear_xyz32() {}

  bool res = false;
};

TEST(CompatibleHas, simple) {
  {
    const ProtoFake proto{true};
    bool res = AROLLA_PROTO3_COMPATIBLE_HAS(proto, abc32);
    EXPECT_TRUE(res);
  }
  {
    const ProtoFake proto{false};
    bool res = AROLLA_PROTO3_COMPATIBLE_HAS(proto, abc32);
    EXPECT_FALSE(res);
  }
  {
    const ProtoFake proto;
    bool res = AROLLA_PROTO3_COMPATIBLE_HAS(proto, xyz32);
    EXPECT_TRUE(res);
  }
}

}  // namespace
