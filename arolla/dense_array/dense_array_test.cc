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
#include "arolla/dense_array/dense_array.h"

#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;

TEST(DenseArrayTest, BaseType) {
  static_assert(std::is_same_v<int, DenseArray<int>::base_type>);
  static_assert(std::is_same_v<float, DenseArray<float>::base_type>);
}

struct TryCompileAssignment {
  template <typename T>
  auto operator()(T v) -> decltype(v[0] = std::nullopt) {
    return v[0];
  }
};

struct TryCompileMutation {
  template <typename A, typename V>
  auto operator()(A a, V v) -> decltype(a[0].value = V()) {
    return a[0].value;
  }
};

// Test that `array[i] = v` and `array[i].value = v` doesn't compile
// (DenseArray is immutable).
static_assert(!std::is_invocable_v<TryCompileAssignment, DenseArray<int>>);
static_assert(!std::is_invocable_v<TryCompileMutation, DenseArray<Text>,
                                   absl::string_view>);

TEST(DenseArrayTest, Builder) {
  DenseArrayBuilder<float> builder(5);
  EXPECT_TRUE((std::is_same_v<DenseArrayBuilder<float>::base_type, float>));
  builder.Set(1, 3.0);
  builder.Set(2, std::optional<float>());
  builder.Set(3, OptionalValue<float>(2.0));
  builder.Set(4, std::nullopt);
  DenseArray<float> res = std::move(builder).Build();
  EXPECT_THAT(res,
              ElementsAre(std::nullopt, 3.0, std::nullopt, 2.0, std::nullopt));
  EXPECT_THAT(res.ToMask(), ElementsAre(std::nullopt, kUnit, std::nullopt,
                                        kUnit, std::nullopt));
}

TEST(DenseArrayTest, BuilderResizing) {
  DenseArrayBuilder<float> builder(10);
  builder.Set(1, 3.0);
  builder.Set(2, std::optional<float>());
  builder.Set(3, OptionalValue<float>(2.0));
  builder.Set(4, std::nullopt);
  DenseArray<float> res = std::move(builder).Build(5);
  EXPECT_THAT(res,
              ElementsAre(std::nullopt, 3.0, std::nullopt, 2.0, std::nullopt));
}

TEST(DenseArrayTest, ForEach) {
  auto arr = CreateDenseArray<int>({2, 3, std::nullopt, 7});
  int64_t expected_id = 0;
  arr.ForEach([&](int64_t id, bool present, int v) {
    EXPECT_EQ(id, expected_id);
    EXPECT_EQ((OptionalValue<int>{present, v}), arr[id]);
    EXPECT_EQ(present, arr.present(id));
    if (present) {
      EXPECT_EQ(v, arr.values[id]);
    }
    expected_id++;
  });
  EXPECT_EQ(expected_id, arr.size());
}

TEST(DenseArrayTest, ForEachPresent) {
  auto arr = CreateDenseArray<int>({2, 3, std::nullopt, 7});
  std::vector<int64_t> ids;
  std::vector<int> values;
  arr.ForEachPresent([&](int64_t id, int v) {
    ids.push_back(id);
    values.push_back(v);
  });
  EXPECT_THAT(ids, ElementsAre(0, 1, 3));
  EXPECT_THAT(values, ElementsAre(2, 3, 7));
}

TEST(DenseArrayTest, ForEachByGroups) {
  constexpr int size = 100;
  auto gen_fn = [](int i) { return OptionalValue<int>{i % 2 == 1, i * 2}; };
  DenseArrayBuilder<int> bldr(size);
  for (int i = 0; i < size; ++i) bldr.Set(i, gen_fn(i));
  DenseArray<int> arr = std::move(bldr).Build();
  int64_t expected_id = 0;
  int64_t expected_offset = 0;
  arr.ForEachByGroups([&](int64_t offset) {
    EXPECT_EQ(offset, expected_offset);
    expected_offset += 32;
    return [&, offset](int64_t id, bool present, int v) {
      EXPECT_EQ(id, expected_id);
      EXPECT_LT(id - offset, 32);
      EXPECT_EQ((OptionalValue<int>{present, v}), gen_fn(id));
      expected_id++;
    };
  });
  EXPECT_EQ(expected_id, arr.size());
}

TEST(DenseArrayTest, RangeFor) {
  auto arr = CreateDenseArray<int>({2, 3, std::nullopt, 7});

  std::vector<OptionalValue<int>> res;
  res.reserve(arr.size());
  for (const auto& v : arr) res.push_back(v);

  EXPECT_EQ(res.size(), arr.size());
  for (int64_t i = 0; i < arr.size(); ++i) {
    EXPECT_EQ(res[i], arr[i]);
  }
}

TEST(DenseArrayTest, ElementsAre) {
  auto test_array = CreateDenseArray<float>({10.0, 20.0, 30.0, std::nullopt});
  EXPECT_THAT(test_array, ElementsAre(10.0, 20.0, 30.0, std::nullopt));
}

TEST(DenseArrayTest, IsFull) {
  {
    DenseArray<float> array;
    EXPECT_TRUE(array.IsFull());
    array.values = CreateBuffer<float>({1, 2, 3});
    EXPECT_TRUE(array.IsFull());
    array.bitmap = CreateBuffer<uint32_t>({1});
    EXPECT_FALSE(array.IsFull());
    array.bitmap = CreateBuffer<uint32_t>({7});
    EXPECT_TRUE(array.IsFull());
  }
  {
    auto array = CreateDenseArray<float>({10.0, 20.0, 30.0, std::nullopt});
    EXPECT_FALSE(array.IsFull());
  }
  {
    auto array = CreateDenseArray<float>({10.0, 20.0, 30.0});
    EXPECT_TRUE(array.IsFull());
  }
  {
    DenseArray<float> array;
    array.values = CreateBuffer<float>({1.0, 2.0, 3.0});
    array.bitmap = CreateBuffer<uint32_t>({7});
    EXPECT_TRUE(array.IsFull());
    array.bitmap_bit_offset = 1;
    EXPECT_FALSE(array.IsFull());
  }
}

TEST(DenseArrayTest, AllMissing) {
  {
    DenseArray<float> array;
    EXPECT_TRUE(array.IsAllMissing());
    array.values = CreateBuffer<float>({1, 2, 3});
    EXPECT_FALSE(array.IsAllMissing());
    array.bitmap = CreateBuffer<uint32_t>({1});
    EXPECT_FALSE(array.IsAllMissing());
    array.bitmap = CreateBuffer<uint32_t>({0});
    EXPECT_TRUE(array.IsAllMissing());
  }
  {
    auto array = CreateDenseArray<float>({std::nullopt, std::nullopt});
    EXPECT_TRUE(array.IsAllMissing());
  }
  {
    auto array = CreateDenseArray<float>({10.0, 20.0, std::nullopt});
    EXPECT_FALSE(array.IsAllMissing());
  }
  {
    DenseArray<float> array;
    array.values = CreateBuffer<float>({1.0, 2.0, 3.0});
    array.bitmap = CreateBuffer<uint32_t>({5});
    EXPECT_FALSE(array.IsAllMissing());
    array.bitmap_bit_offset = 3;
    EXPECT_TRUE(array.IsAllMissing());
  }
}

TEST(DenseArrayTest, AllPresent) {
  {
    DenseArray<float> array;
    EXPECT_TRUE(array.IsAllPresent());
    array.values = CreateBuffer<float>({1, 2, 3});
    EXPECT_TRUE(array.IsAllPresent());
    array.bitmap = CreateBuffer<uint32_t>({1});
    EXPECT_FALSE(array.IsAllPresent());
    array.bitmap = CreateBuffer<uint32_t>({0});
    EXPECT_FALSE(array.IsAllPresent());
    array.bitmap = CreateBuffer<uint32_t>({0b111});
    EXPECT_TRUE(array.IsAllPresent());
  }
  {
    auto array = CreateDenseArray<float>({1.0f, 2.0f});
    EXPECT_TRUE(array.IsAllPresent());
  }
  {
    auto array = CreateDenseArray<float>({10.0, 20.0, std::nullopt});
    EXPECT_FALSE(array.IsAllPresent());
  }
  {
    DenseArray<float> array;
    array.values = CreateBuffer<float>({1.0, 2.0, 3.0});
    array.bitmap = CreateBuffer<uint32_t>({0b111});
    EXPECT_TRUE(array.IsAllPresent());
    array.bitmap_bit_offset = 1;
    EXPECT_FALSE(array.IsAllPresent());
  }
}

TEST(DenseArrayTest, PresentCount) {
  {
    DenseArray<float> array;
    EXPECT_EQ(array.PresentCount(), 0);
    array.values = CreateBuffer<float>({1, 2, 3});
    EXPECT_EQ(array.PresentCount(), 3);
    array.bitmap = CreateBuffer<uint32_t>({1});
    EXPECT_EQ(array.PresentCount(), 1);
    array.bitmap = CreateBuffer<uint32_t>({4});
    EXPECT_EQ(array.PresentCount(), 1);
    array.bitmap = CreateBuffer<uint32_t>({5});
    EXPECT_EQ(array.PresentCount(), 2);
    array.bitmap = CreateBuffer<uint32_t>({3});
    EXPECT_EQ(array.PresentCount(), 2);
    array.bitmap = CreateBuffer<uint32_t>({7});
    EXPECT_EQ(array.PresentCount(), 3);
  }
  {
    auto array = CreateDenseArray<float>({10.0, 20.0, 30.0, std::nullopt});
    EXPECT_EQ(array.PresentCount(), 3);
  }
  {
    auto array = CreateDenseArray<float>({10.0, 20.0, 30.0});
    EXPECT_EQ(array.PresentCount(), 3);
  }
  {
    DenseArray<float> array;
    array.values = CreateBuffer<float>({1.0, 2.0, 3.0});
    array.bitmap = CreateBuffer<uint32_t>({7});
    EXPECT_EQ(array.PresentCount(), 3);
    array.bitmap_bit_offset = 1;
    EXPECT_EQ(array.PresentCount(), 2);
  }
}

TEST(DenseArrayTest, MakeOwned) {
  std::vector<int> values{1, 2, 3, 4, 5};
  std::vector<uint32_t> bitmap{0x15};
  DenseArray<int> array{Buffer<int>(nullptr, values),
                        Buffer<uint32_t>(nullptr, bitmap)};
  EXPECT_FALSE(array.is_owned());

  array = ArenaTraits<DenseArray<int>>::MakeOwned(std::move(array),
                                                  GetHeapBufferFactory());
  EXPECT_TRUE(array.is_owned());
  EXPECT_THAT(array.values, ElementsAre(1, 2, 3, 4, 5));
  EXPECT_THAT(array.bitmap, ElementsAre(0x15));
}

TEST(DenseArrayTest, MakeUnowned) {
  DenseArray<int> arr = CreateDenseArray<int>({1, 2, std::nullopt, 3});
  DenseArray<int> arr2 = arr.MakeUnowned();
  EXPECT_TRUE(arr.is_owned());
  EXPECT_FALSE(arr2.is_owned());
  EXPECT_FALSE(arr2.values.is_owner());
  EXPECT_FALSE(arr2.bitmap.is_owner());
  EXPECT_THAT(arr2, ElementsAre(1, 2, std::nullopt, 3));
}

TEST(DenseArrayTest, Slice) {
  DenseArray<int> full = CreateDenseArray<int>({5, 1, 3, 4, 5});
  DenseArray<int> dense = CreateDenseArray<int>({5, 1, {}, {}, 5});
  EXPECT_THAT(full.Slice(1, 3), ElementsAre(1, 3, 4));
  EXPECT_THAT(dense.Slice(1, 4), ElementsAre(1, std::nullopt, std::nullopt, 5));
  EXPECT_THAT(dense.Slice(1, 4).Slice(2, 2), ElementsAre(std::nullopt, 5));
  EXPECT_THAT(dense.Slice(3, 0), ElementsAre());
  EXPECT_THAT(dense.Slice(0, 3), ElementsAre(5, 1, std::nullopt));
}

TEST(DenseArrayTest, ForceNoBitmapBitOffset) {
  DenseArray<int> with_offset =
      CreateDenseArray<int>({5, 1, {}, {}, 5}).Slice(1, 4);
  ASSERT_THAT(with_offset.bitmap_bit_offset, 1);
  ASSERT_THAT(with_offset, ElementsAre(1, std::nullopt, std::nullopt, 5));
  DenseArray<int> without_offset = with_offset.ForceNoBitmapBitOffset();
  EXPECT_THAT(without_offset.bitmap_bit_offset, 0);
  EXPECT_THAT(without_offset, ElementsAre(1, std::nullopt, std::nullopt, 5));
}

TEST(DenseArrayTest, CreateDenseArray) {
  {
    // Conversion from int to int64.
    std::vector<std::optional<int>> v{1, std::nullopt, 2, 3, std::nullopt};
    auto test_array = CreateDenseArray<int64_t>(v.begin(), v.end());
    EXPECT_THAT(test_array, ElementsAre(1, std::nullopt, 2, 3, std::nullopt));
    EXPECT_TRUE(test_array.is_owned());
  }
  {
    // Conversion from std::string to absl::string_view.
    std::vector<std::optional<std::string>> v{"Hello", "world", std::nullopt};
    auto test_array = CreateDenseArray<Bytes>(v.begin(), v.end());
    EXPECT_THAT(test_array, ElementsAre("Hello", "world", std::nullopt));
  }
  {
    // No conversion.
    std::vector<std::optional<absl::string_view>> v{"Hello", "world",
                                                    std::nullopt};
    auto test_array = CreateDenseArray<Bytes>(v.begin(), v.end());
    EXPECT_THAT(test_array, ElementsAre("Hello", "world", std::nullopt));
  }
  {
    // Conversion from bool to arolla::Unit.
    std::vector<std::optional<bool>> v{false, true, std::nullopt};
    auto test_array = CreateDenseArray<Unit>(v.begin(), v.end());
    EXPECT_THAT(test_array, ElementsAre(kUnit, kUnit, std::nullopt));
  }

  {
    // Using RawBufferFactory argument.
    UnsafeArenaBufferFactory factory(1000);
    std::vector<std::optional<int>> v{1, std::nullopt, 2, 3, std::nullopt};
    auto test_array = CreateDenseArray<int64_t>(v.begin(), v.end(), &factory);
    EXPECT_THAT(test_array, ElementsAre(1, std::nullopt, 2, 3, std::nullopt));
    EXPECT_FALSE(test_array.is_owned());
  }
}

TEST(DenseArrayTest, CreateFullDenseArray) {
  {
    // No conversion, type is deduced.
    std::vector<int64_t> v{1, 2, 3};
    DenseArray<int64_t> test_array = CreateFullDenseArray(v);
    EXPECT_THAT(test_array, ElementsAre(1, 2, 3));
    EXPECT_TRUE(test_array.is_owned());
    EXPECT_TRUE(test_array.IsFull());
  }
  {
    // Conversion from int to int64.
    std::vector<int> v{1, 2, 3};
    auto test_array = CreateFullDenseArray<int64_t>(v.begin(), v.end());
    EXPECT_THAT(test_array, ElementsAre(1, 2, 3));
    EXPECT_TRUE(test_array.is_owned());
    EXPECT_TRUE(test_array.IsFull());
  }
  {
    // Conversion from std::string to absl::string_view.
    std::vector<std::string> v{"Hello", "world"};
    auto test_array = CreateFullDenseArray<Bytes>(v.begin(), v.end());
    EXPECT_THAT(test_array, ElementsAre("Hello", "world"));
    EXPECT_TRUE(test_array.IsFull());
  }
  {
    // No conversion.
    std::vector<absl::string_view> v{"Hello", "world"};
    auto test_array = CreateFullDenseArray<Bytes>(v.begin(), v.end());
    EXPECT_THAT(test_array, ElementsAre("Hello", "world"));
    EXPECT_TRUE(test_array.IsFull());
  }
  {
    // Conversion from bool to arolla::Unit.
    std::vector<bool> v{false, true};
    auto test_array = CreateFullDenseArray<Unit>(v.begin(), v.end());
    EXPECT_THAT(test_array, ElementsAre(kUnit, kUnit));
    EXPECT_TRUE(test_array.IsFull());
  }
  {
    // Using RawBufferFactory argument.
    UnsafeArenaBufferFactory factory(1000);
    std::vector<int64_t> v{1, 2, 3};
    auto test_array = CreateFullDenseArray<int64_t>(v, &factory);
    EXPECT_THAT(test_array, ElementsAre(1, 2, 3));
    EXPECT_FALSE(test_array.is_owned());
    EXPECT_TRUE(test_array.IsFull());
  }
  {
    // Moved from vector.
    std::vector<int64_t> v{1, 2, 3};
    const int64_t* data = v.data();
    auto test_array = CreateFullDenseArray<int64_t>(std::move(v));
    EXPECT_THAT(test_array, ElementsAre(1, 2, 3));
    EXPECT_EQ(test_array.values.span().data(), data);
    EXPECT_TRUE(test_array.is_owned());
    EXPECT_TRUE(test_array.IsFull());
  }
  {
    // From initializer_list.
    auto test_array = CreateFullDenseArray({Bytes("Hello"), Bytes("world")});
    static_assert(std::is_same_v<decltype(test_array), DenseArray<Bytes>>);
    EXPECT_THAT(test_array, ElementsAre("Hello", "world"));
    EXPECT_TRUE(test_array.IsFull());
  }
}

TEST(DenseArrayTest, ArraysAreEquivalent) {
  {
    // Empty trivial.
    DenseArray<int32_t> array1;
    DenseArray<int32_t> array2;
    EXPECT_TRUE(ArraysAreEquivalent(array1, array2));
  }
  {
    // Equal -> True.
    auto array1 = CreateDenseArray<int32_t>({1, 2, std::nullopt});
    auto array2 = CreateDenseArray<int32_t>({1, 2, std::nullopt});
    EXPECT_TRUE(ArraysAreEquivalent(array1, array2));
  }
  {
    // Not equal values -> False.
    auto array1 = CreateDenseArray<int32_t>({1, 2, std::nullopt});
    auto array2 = CreateDenseArray<int32_t>({1, 3, std::nullopt});
    EXPECT_FALSE(ArraysAreEquivalent(array1, array2));
  }
  {
    // Different lengths -> False.
    auto array1 = CreateDenseArray<int32_t>({1, 2, std::nullopt});
    auto array2 = CreateDenseArray<int32_t>({1, 2});
    EXPECT_FALSE(ArraysAreEquivalent(array1, array2));
  }
  {
    // Different masks -> False.
    auto array1 = CreateDenseArray<int32_t>({1, 2, std::nullopt});
    auto array2 = CreateDenseArray<int32_t>({1, 2, 3});
    EXPECT_FALSE(ArraysAreEquivalent(array1, array2));
  }
  {
    // With different offsets.
    DenseArray<int32_t> array1;
    array1.values = CreateBuffer<int32_t>({1, 2, 3});
    array1.bitmap = CreateBuffer<uint32_t>({7});  // 0111.
    DenseArray<int32_t> array2;
    array2.values = CreateBuffer<int32_t>({1, 2, 3});
    array2.bitmap = CreateBuffer<uint32_t>({14});  // 1110.
    // Different bitmaps -> False.
    EXPECT_FALSE(ArraysAreEquivalent(array1, array2));
    // After aligning bitmaps -> True.
    array2.bitmap_bit_offset = 1;
    EXPECT_TRUE(ArraysAreEquivalent(array1, array2));
  }
  {
    // Empty bitmaps.
    DenseArray<int32_t> array1;
    array1.values = CreateBuffer<int32_t>({1, 2, 3});
    EXPECT_TRUE(array1.bitmap.empty());
    auto array2 = array1;
    EXPECT_TRUE(ArraysAreEquivalent(array1, array2));
    // One not empty and has missing.
    array1.bitmap = CreateBuffer<uint32_t>({6});
    EXPECT_FALSE(ArraysAreEquivalent(array1, array2));
    // One not empty and full.
    array1.bitmap = CreateBuffer<uint32_t>({7});
    EXPECT_TRUE(ArraysAreEquivalent(array1, array2));
    // Both not empty and full.
    array2.bitmap = CreateBuffer<uint32_t>({7});
    EXPECT_TRUE(ArraysAreEquivalent(array1, array2));
  }
}

template <typename T>
class DenseArrayTypedTest : public ::testing::Test {
 public:
  typedef T value_type;
};
using ArrayTypes =
    testing::Types<int, float, int64_t, double, uint64_t, Bytes, Text, Unit>;
TYPED_TEST_SUITE(DenseArrayTypedTest, ArrayTypes);

TYPED_TEST(DenseArrayTypedTest, CreateEmptyDenseArray) {
  using T = typename TestFixture::value_type;
  for (int64_t size = 0; size < (1ull << 20); size = size * 2 + 1) {
    DenseArray<T> test_array = CreateEmptyDenseArray<T>(size);
    EXPECT_THAT(test_array, ElementsAreArray(std::vector<std::nullopt_t>(
                                size, std::nullopt)));
    EXPECT_TRUE(test_array.IsAllMissing());
  }
}

}  // namespace
}  // namespace arolla
