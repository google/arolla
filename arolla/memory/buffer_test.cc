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
#include "arolla/memory/buffer.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/inlined_vector.h"
#include "absl/hash/hash_testing.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/preallocated_buffers.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::IsEmpty;

class BufferTest : public ::testing::Test {
 public:
  Buffer<float> CreateTestBuffer(int num_rows) {
    std::vector<float> values(num_rows);
    for (int i = 0; i < num_rows; i++) {
      values[i] = i;
    }
    return Buffer<float>::Create(values.begin(), values.end());
  }
};

TEST_F(BufferTest, Simple) {
  // Create a buffer from iterators.
  size_t num_rows = 5;
  Buffer<float> buffer = CreateTestBuffer(num_rows);

  // Verify its properties.
  EXPECT_TRUE(buffer.is_owner());
  EXPECT_THAT(buffer, ElementsAre(0.0f, 1.0f, 2.0f, 3.0f, 4.0f));
  EXPECT_EQ(buffer[0], 0.0f);
  EXPECT_EQ(buffer[4], 4.0f);
}

struct ComplicatedType {
  ComplicatedType() : ComplicatedType(0) {}
  explicit ComplicatedType(int x) {
    v = x;
    count++;
  }
  ComplicatedType(const ComplicatedType& c) {
    v = c.v;
    count++;
  }
  ~ComplicatedType() { count--; }

  bool operator==(const ComplicatedType& other) const { return v == other.v; }

  int v = 0;
  static int64_t count;
};

int64_t ComplicatedType::count = 0;

TEST_F(BufferTest, ComplicatedType) {
  ASSERT_EQ(ComplicatedType::count, 0);
  {
    Buffer<ComplicatedType>::Builder bldr(10);
    for (int i = 0; i < 5; ++i) {
      bldr.Set(i, ComplicatedType(i * i));
    }
    Buffer<ComplicatedType> buf = std::move(bldr).Build(5);
    EXPECT_EQ(ComplicatedType::count, 10);
    EXPECT_EQ(buf[4].v, 16);
  }
  ASSERT_EQ(ComplicatedType::count, 0);
  {
    Buffer<ComplicatedType>::Builder bldr(1000);
    for (int i = 0; i < 5; ++i) {
      bldr.Set(i, ComplicatedType(-i * i));
    }
    Buffer<ComplicatedType> buf = std::move(bldr).Build(5);
    EXPECT_EQ(ComplicatedType::count, 5);
    EXPECT_EQ(buf[4].v, -16);
  }
  ASSERT_EQ(ComplicatedType::count, 0);
}

template <typename T>
class BufferUninitializedTest : public ::testing::Test {
 public:
  typedef T value_type;
};
using BufferUninitializedTestTypes =
    testing::Types<char, int, float, int64_t, double, uint64_t, QTypePtr,
                   ComplicatedType, Unit, Bytes, Text>;
TYPED_TEST_SUITE(BufferUninitializedTest, BufferUninitializedTestTypes);

TYPED_TEST(BufferUninitializedTest, CreateUninitializedSmall) {
  using T = typename TestFixture::value_type;
  Buffer<T> buffer = Buffer<T>::CreateUninitialized(100);

  // We use global buffer for trivially destructible types and do not own.
  bool expected_to_use_global_buffer = std::is_trivially_destructible_v<T> ||
                                       // Text and Bytes have special handling.
                                       std::is_same_v<T, Text> ||
                                       std::is_same_v<T, Bytes>;
  EXPECT_EQ(buffer.is_owner(),
            // Buffer for Unit always own.
            (!expected_to_use_global_buffer || std::is_same_v<T, Unit>));
  // T() is zero for listed primitives
  // For Text and Bytes it also should be empty strings.
  EXPECT_THAT(buffer, ElementsAreArray(std::vector<T>(100, T())));
}

TYPED_TEST(BufferUninitializedTest, CreateUninitializedBig) {
  using T = typename TestFixture::value_type;
  size_t size = kZeroInitializedBufferSize / sizeof(T) + 1;
  if (std::is_same_v<T, Text> || std::is_same_v<T, Bytes>) {
    size = kZeroInitializedBufferSize / sizeof(StringsBuffer::Offsets) + 1;
  }
  Buffer<T> buffer = Buffer<T>::CreateUninitialized(size);

  // We use global buffer for trivially destructible types and do not own.
  EXPECT_TRUE(buffer.is_owner());
  EXPECT_EQ(buffer.size(), size);

  if (!std::is_trivially_destructible_v<T>) {
    // For non trivially destructible types we have to initialize.
    // We assume that we need to initialize for Text and Bytes as well
    // for safety.
    EXPECT_THAT(buffer, ElementsAreArray(std::vector<T>(size, T())));
  }
}

TEST_F(BufferTest, InitializerListCreate) {
  // Create a buffer from std::initializer_list.
  Buffer<float> buffer = Buffer<float>::Create({0.0f, 2.0f});

  // Verify its properties.
  EXPECT_TRUE(buffer.is_owner());
  EXPECT_THAT(buffer, ElementsAre(0.0f, 2.0f));
  EXPECT_EQ(buffer[0], 0.0f);
  EXPECT_EQ(buffer[1], 2.0f);
}

TEST_F(BufferTest, TransferOwnershipFromVector) {
  std::vector<int> v{1, 2, 3};
  int* data = v.data();
  auto buf = Buffer<int>::Create(std::move(v));
  EXPECT_THAT(buf, ElementsAre(1, 2, 3));
  EXPECT_EQ(buf.span().data(), data);
}

TEST_F(BufferTest, TransferOwnershipFromInlinedVector) {
  {
    SCOPED_TRACE("small");
    Buffer<int> buf;
    int* data;
    {
      absl::InlinedVector<int, 24> v{1, 2, 3};
      data = v.data();
      buf = Buffer<int>::Create(std::move(v));
    }
    EXPECT_THAT(buf, ElementsAre(1, 2, 3));
    EXPECT_NE(buf.span().data(), data);
  }
  {
    SCOPED_TRACE("big");
    int* data;
    Buffer<int> buf;
    {
      absl::InlinedVector<int, 2> v{1, 2, 3, 4, 5};
      data = v.data();
      buf = Buffer<int>::Create(std::move(v));
    }
    EXPECT_THAT(buf, ElementsAre(1, 2, 3, 4, 5));
    EXPECT_EQ(buf.span().data(), data);
  }
  EXPECT_THAT(Buffer<bool>::Create(absl::InlinedVector<bool, 1>(1)),
              ElementsAre(false));
}

TEST_F(BufferTest, Empty) {
  Buffer<float> buffer1 = CreateTestBuffer(0);
  EXPECT_THAT(buffer1, IsEmpty());

  Buffer<float> buffer2 = buffer1.DeepCopy();
  EXPECT_THAT(buffer2, IsEmpty());

  Buffer<float> buffer3;
  EXPECT_THAT(buffer3, IsEmpty());
}

TEST_F(BufferTest, Move) {
  // Create a buffer from iterators.
  size_t num_rows = 5;
  Buffer<float> buffer = CreateTestBuffer(num_rows);
  EXPECT_TRUE(buffer.is_owner());

  // Move via constructor.
  Buffer<float> buffer2 = std::move(buffer);

  // Ownership transferred to new buffer.
  EXPECT_TRUE(buffer2.is_owner());
  EXPECT_FALSE(buffer.is_owner());  // NOLINT misc-use-after-move

  EXPECT_THAT(buffer2, ElementsAre(0.0f, 1.0f, 2.0f, 3.0f, 4.0f));

  // Move via assignment.
  Buffer<float> buffer3;

  // empty buffer is owned.
  EXPECT_TRUE(buffer3.is_owner());

  // Ownership transfers from buffer2 to buffer3
  buffer3 = std::move(buffer2);
  EXPECT_TRUE(buffer3.is_owner());

  // buffer2 loses ownership.
  EXPECT_FALSE(buffer2.is_owner());  // NOLINT misc-use-after-move
  EXPECT_THAT(buffer3, ElementsAre(0.0f, 1.0f, 2.0f, 3.0f, 4.0f));
}

TEST_F(BufferTest, MemoryUsage) {
  EXPECT_EQ(sizeof(Buffer<float>), 4 * sizeof(void*));

  for (size_t sz = 1; sz < 50; sz += 7) {
    Buffer<float> buffer = CreateTestBuffer(sz);
    const size_t floats = sz * sizeof(float);
    EXPECT_EQ(floats, buffer.memory_usage());
  }
}

TEST_F(BufferTest, MoveSlice) {
  // Create a buffer from iterators.
  size_t num_rows = 10;
  Buffer<float> buffer = CreateTestBuffer(num_rows);
  EXPECT_TRUE(buffer.is_owner());

  // Shrink buffer in place
  buffer = std::move(buffer).Slice(0, 5);
  EXPECT_TRUE(buffer.is_owner());
  EXPECT_THAT(buffer, ElementsAre(0.0f, 1.0f, 2.0f, 3.0f, 4.0f));

  // Slice buffer.
  Buffer<float> buffer2 = std::move(buffer).Slice(2, 3);
  EXPECT_TRUE(buffer2.is_owner());
  EXPECT_FALSE(buffer.is_owner());  // NOLINT misc-use-after-move
  EXPECT_THAT(buffer2, ElementsAre(2.0f, 3.0f, 4.0f));
}

TEST_F(BufferTest, ShallowCopy) {
  // Create a buffer from iterators.
  size_t num_rows = 10;
  Buffer<float> buffer = CreateTestBuffer(num_rows);

  // Copy without offset.
  Buffer<float> buffer_copy1 = buffer.ShallowCopy();

  // Verify its properties.
  EXPECT_FALSE(buffer_copy1.is_owner());
  EXPECT_EQ(buffer.begin(), buffer_copy1.begin());
  EXPECT_EQ(buffer.end(), buffer_copy1.end());
  EXPECT_EQ(buffer, buffer_copy1);

  // Copy with offset.
  Buffer<float> buffer_copy2 = buffer.Slice(5, 5);

  // Verify its properties.
  EXPECT_TRUE(buffer_copy2.is_owner());
  EXPECT_EQ(buffer.begin() + 5, buffer_copy2.begin());
  EXPECT_EQ(buffer.span().subspan(5, 5), buffer_copy2.span());
}

TEST_F(BufferTest, DeepCopy) {
  size_t num_rows = 5;
  Buffer<float> buffer = CreateTestBuffer(num_rows);

  // Deep copy full buffer.
  Buffer<float> buffer_copy = buffer.DeepCopy();

  // Deep copy buffer slice.
  Buffer<float> buffer_slice_copy = buffer.Slice(1, 3).DeepCopy();

  // Release original buffer.
  buffer = Buffer<float>();

  // Verify contents of copies.
  EXPECT_TRUE(buffer_copy.is_owner());
  EXPECT_THAT(buffer_copy, ElementsAre(0.0f, 1.0f, 2.0f, 3.0f, 4.0f));

  EXPECT_TRUE(buffer_slice_copy.is_owner());
  EXPECT_THAT(buffer_slice_copy, ElementsAre(1.0f, 2.0f, 3.0f));

  // Make a deep copy of an empty buffer.
  buffer_copy = buffer.DeepCopy();
  EXPECT_THAT(buffer_copy, IsEmpty());
}

TEST_F(BufferTest, EmptySlice) {
  size_t num_rows = 10;
  Buffer<float> buffer = CreateTestBuffer(num_rows);

  // Empty slice without ownership transfer.
  Buffer<float> copy = buffer.Slice(3, 0);
  EXPECT_THAT(copy, IsEmpty());
  EXPECT_EQ(copy.begin(), nullptr);

  // Shrink buffer to size zero.
  buffer = std::move(buffer).Slice(3, 0);
  EXPECT_THAT(buffer, IsEmpty());
  EXPECT_EQ(buffer.begin(), nullptr);

  // Allow empty slice of empty buffer.
  copy = buffer.Slice(0, 0);
  EXPECT_THAT(copy, IsEmpty());
  EXPECT_EQ(copy.begin(), nullptr);
}

TEST_F(BufferTest, UnownedBuffer) {
  float values[] = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
  Buffer<float> buffer(nullptr, values);
  EXPECT_THAT(buffer.begin(), Eq(&values[0]));
  EXPECT_THAT(buffer, ElementsAre(1.0f, 2.0f, 3.0f, 4.0f, 5.0f));
  EXPECT_FALSE(buffer.is_owner());

  auto slice = buffer.Slice(1, 3);
  EXPECT_THAT(slice, ElementsAre(2.0f, 3.0f, 4.0f));
  EXPECT_THAT(slice.begin(), Eq(&values[1]));
  EXPECT_FALSE(slice.is_owner());

  auto deep_copy = buffer.DeepCopy();
  std::fill(values, values + 5, 0.0f);
  EXPECT_TRUE(deep_copy.is_owner());
  EXPECT_THAT(deep_copy, ElementsAre(1.0f, 2.0f, 3.0f, 4.0f, 5.0f));
}

TEST_F(BufferTest, SupportsAbslHash) {
  Buffer<float> empty;
  std::array<float, 5> values = {1, 2, 3, 4, 5};
  Buffer<float> test1 = Buffer<float>::Create(values.begin(), values.end());
  Buffer<float> test2 = Buffer<float>::Create(values.rbegin(), values.rend());
  EXPECT_TRUE(
      absl::VerifyTypeImplementsAbslHashCorrectly({empty, test1, test2}));
}

TEST(BufferBuilder, ReshuffleBuilder) {
  auto b = CreateBuffer<int>({5, 4, 3, 2, 1});
  Buffer<int>::ReshuffleBuilder bldr(8, b, 0);
  bldr.CopyValueToRange(6, 8, 3);
  bldr.CopyValue(3, 1);
  bldr.CopyValue(1, 2);
  bldr.CopyValue(2, 0);
  auto res = std::move(bldr).Build();
  EXPECT_THAT(res, ElementsAre(0, 3, 5, 4, 0, 0, 2, 2));
}

TEST(BufferBuilder, MutableSpan) {
  Buffer<float>::Builder builder(10);
  auto span = builder.GetMutableSpan();
  for (int i = 0; i < 10; ++i) span[i] = i;
  auto buffer = std::move(builder).Build(5);

  EXPECT_THAT(buffer, ElementsAre(0.0f, 1.0f, 2.0f, 3.0f, 4.0f));
}

TEST(BufferBuilder, Inserter) {
  Buffer<float>::Builder builder(10);
  auto inserter = builder.GetInserter(1);
  for (int i = 0; i < 4; ++i) inserter.Add(i);
  builder.Set(0, 5);
  auto buffer = std::move(builder).Build(inserter);

  EXPECT_THAT(buffer, ElementsAre(5.f, 0.0f, 1.0f, 2.0f, 3.0f));
}

TEST(BufferBuilder, Generator) {
  Buffer<int>::Builder builder(10);
  builder.SetNConst(0, 10, -1);
  int i = 0;
  builder.SetN(2, 3, [&]() { return ++i; });
  auto buffer = std::move(builder).Build(6);

  EXPECT_THAT(buffer, ElementsAre(-1, -1, 1, 2, 3, -1));
}

TEST(BufferBuilder, RandomAccess) {
  Buffer<float>::Builder builder(10);
  builder.Set(4, 1);
  builder.Set(2, 2);
  builder.Set(1, 3);
  builder.Set(0, 4);
  builder.Set(3, 5);
  builder.Set(1, 6);
  auto buffer = std::move(builder).Build(5);

  EXPECT_THAT(buffer, ElementsAre(4.f, 6.f, 2.f, 5.f, 1.f));
}

TEST(BufferBuilder, Tuple) {
  Buffer<std::tuple<int, float>>::Builder builder(10);
  for (int i = 0; i < 5; ++i) {
    builder.Set(i, {i * 2, i * 0.5});
  }
  auto buffer = std::move(builder).Build(5);

  EXPECT_THAT(buffer, ElementsAre(std::tuple<int, float>{0, 0.0},
                                  std::tuple<int, float>{2, 0.5},
                                  std::tuple<int, float>{4, 1.0},
                                  std::tuple<int, float>{6, 1.5},
                                  std::tuple<int, float>{8, 2.0}));
}

}  // namespace
}  // namespace arolla
