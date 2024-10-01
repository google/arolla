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
#include "arolla/io/proto_types/types.h"

#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/optional_value.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

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

TEST(CastTest, Numeric) {
  EXPECT_EQ(ToArollaCompatibleType(5), 5);
  EXPECT_EQ(ToArollaCompatibleType(int8_t{5}), 5);
  EXPECT_EQ(ToArollaCompatibleType(int16_t{5}), 5);
  EXPECT_EQ(ToArollaCompatibleType(int32_t{5}), 5);
  EXPECT_EQ(ToArollaCompatibleType(int64_t{5}), 5);
  EXPECT_EQ(ToArollaCompatibleType(5.7), 5.7);
  EXPECT_EQ(ToArollaCompatibleType(5.7f), 5.7f);
}

TEST(CastTest, Cord) {
  {
    arolla::Bytes x;
    x = ToArollaCompatibleType(absl::Cord("hello"));
    EXPECT_EQ(absl::string_view(x), "hello");
  }
  {
    arolla::Text x;
    x = ToArollaCompatibleType(absl::Cord("hello"));
    EXPECT_EQ(absl::string_view(x), "hello");
  }
}

TEST(CastTest, String) {
  {
    arolla::Bytes x;
    x = ToArollaCompatibleType(std::string("hello"));
    EXPECT_EQ(absl::string_view(x), "hello");
  }
  {
    arolla::Text x;
    x = ToArollaCompatibleType(std::string("hello"));
    EXPECT_EQ(absl::string_view(x), "hello");
  }
}

TEST(CastTest, StringView) {
  {
    arolla::Bytes x;
    x = ToArollaCompatibleType(absl::string_view("hello"));
    EXPECT_EQ(absl::string_view(x), "hello");
  }
  {
    arolla::Text x;
    x = ToArollaCompatibleType(absl::string_view("hello"));
    EXPECT_EQ(absl::string_view(x), "hello");
  }
}

TEST(CastTest, StringLiteral) {
  {
    arolla::Bytes x;
    x = ToArollaCompatibleType("hello");
    EXPECT_EQ(absl::string_view(x), "hello");
  }
  {
    arolla::Text x;
    x = ToArollaCompatibleType("hello");
    EXPECT_EQ(absl::string_view(x), "hello");
  }
}

TEST(CastTest, StringIsForwarded) {
  EXPECT_TRUE((std::is_same_v<decltype(ToArollaCompatibleType(
                                  std::declval<std::string&&>())),
                              std::string&&>));
  EXPECT_TRUE((std::is_same_v<decltype(ToArollaCompatibleType(
                                  std::declval<std::string>())),
                              std::string&&>));
  EXPECT_TRUE((std::is_same_v<decltype(ToArollaCompatibleType(
                                  std::declval<const std::string&>())),
                              const std::string&>));
}

TEST(ResizeContainer, RepeatedPtrField) {
  testing_namespace::Root root;
  // increase from 0
  ResizeContainer(*root.mutable_inners(), 5);
  EXPECT_EQ(root.inners_size(), 5);
  EXPECT_FALSE(root.inners(0).has_a());
  root.mutable_inners(0)->set_a(13);

  // increase from non 0
  ResizeContainer(*root.mutable_inners(), 7);
  EXPECT_EQ(root.inners_size(), 7);
  EXPECT_TRUE(root.inners(0).has_a());
  EXPECT_EQ(root.inners(0).a(), 13);

  // reduce
  ResizeContainer(*root.mutable_inners(), 3);
  EXPECT_EQ(root.inners_size(), 3);
  EXPECT_TRUE(root.inners(0).has_a());
  EXPECT_EQ(root.inners(0).a(), 13);

  // no resize
  ResizeContainer(*root.mutable_inners(), 3);
  EXPECT_EQ(root.inners_size(), 3);
  EXPECT_TRUE(root.inners(0).has_a());
  EXPECT_EQ(root.inners(0).a(), 13);
}

TEST(ResizeContainer, Vector) {
  std::vector<int> v;
  // increase from 0
  ResizeContainer(v, 5);
  EXPECT_EQ(v.size(), 5);
  v[0] = 13;

  // increase from non 0
  ResizeContainer(v, 7);
  EXPECT_EQ(v.size(), 7);
  EXPECT_EQ(v[0], 13);

  // reduce
  ResizeContainer(v, 3);
  EXPECT_EQ(v.size(), 3);
  EXPECT_EQ(v[0], 13);

  // no resize
  ResizeContainer(v, 3);
  EXPECT_EQ(v.size(), 3);
  EXPECT_EQ(v[0], 13);
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
