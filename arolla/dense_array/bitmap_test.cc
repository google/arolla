// Copyright 2025 Google LLC
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
#include "arolla/dense_array/bitmap.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/types/span.h"
#include "arolla/memory/buffer.h"

namespace arolla::bitmap {
namespace {

TEST(BitmapTest, BitmapSize) {
  EXPECT_EQ(BitmapSize(0), 0);
  EXPECT_EQ(BitmapSize(1), 1);
  EXPECT_EQ(BitmapSize(32), 1);
  EXPECT_EQ(BitmapSize(33), 2);
  EXPECT_EQ(BitmapSize(320), 10);
  EXPECT_EQ(BitmapSize(351), 11);
}

TEST(BitmapTest, SetBit) {
  Word bitmap[3] = {0, kFullWord, 0};
  SetBit(bitmap, 3);
  UnsetBit(bitmap, 32);
  SetBit(bitmap, 64);
  UnsetBit(bitmap, 65);
  EXPECT_EQ(bitmap[0], 8);
  EXPECT_EQ(bitmap[1], kFullWord - 1);
  EXPECT_EQ(bitmap[2], 1);
}

TEST(BitmapTest, GetBit) {
  Word bitmap[3] = {8, kFullWord - 1, 1};
  EXPECT_TRUE(GetBit(bitmap, 3));
  EXPECT_FALSE(GetBit(bitmap, 32));
  EXPECT_TRUE(GetBit(bitmap, 64));
  EXPECT_FALSE(GetBit(bitmap, 65));
}

TEST(BitmapTest, AreAllBitsSet) {
  Word bitmap[4] = {kFullWord, kFullWord, 3, kFullWord};
  EXPECT_TRUE(AreAllBitsSet(bitmap, 64));
  EXPECT_TRUE(AreAllBitsSet(bitmap, 65));
  EXPECT_TRUE(AreAllBitsSet(bitmap, 66));
  EXPECT_FALSE(AreAllBitsSet(bitmap, 67));
  EXPECT_FALSE(AreAllBitsSet(bitmap, 128));
}

TEST(BitmapTest, AreAllBitsUnset) {
  Word bitmap[4] = {0, 0, 12};
  EXPECT_TRUE(AreAllBitsUnset(bitmap, 0));
  EXPECT_TRUE(AreAllBitsUnset(bitmap, 64));
  EXPECT_TRUE(AreAllBitsUnset(bitmap, 65));
  EXPECT_TRUE(AreAllBitsUnset(bitmap, 66));
  EXPECT_FALSE(AreAllBitsUnset(bitmap, 67));
  EXPECT_FALSE(AreAllBitsUnset(bitmap, 95));
  EXPECT_FALSE(AreAllBitsUnset(bitmap, 96));
}

TEST(BitmapTest, Empty) {
  Bitmap bitmap;
  EXPECT_EQ(GetWord(bitmap, 0), kFullWord);
  EXPECT_EQ(GetWord(bitmap, 13), kFullWord);
  EXPECT_EQ(GetWordWithOffset(bitmap, 0, 7), kFullWord);
  EXPECT_EQ(GetWordWithOffset(bitmap, 13, 7), kFullWord);
  EXPECT_TRUE(GetBit(bitmap, 0));
  EXPECT_TRUE(GetBit(bitmap, 1));
  EXPECT_TRUE(GetBit(bitmap, 999));
  int64_t count = 0;
  auto check_fn = [&](bool v) {
    count++;
    EXPECT_TRUE(v);
  };
  Iterate(bitmap, 0, 0, check_fn);
  EXPECT_EQ(count, 0);
  Iterate(bitmap, 2, 17, check_fn);
  EXPECT_EQ(count, 17);
  count = 0;
  Iterate(bitmap, 99, 138, check_fn);
  EXPECT_EQ(count, 138);
}

TEST(BitmapTest, CreateEmpty) {
  for (int64_t size = 0; size < (1 << 20); size = (size + 1) * 2) {
    Bitmap bitmap = CreateEmptyBitmap(size);
    for (int64_t i = 0; i < BitmapSize(size); ++i) {
      EXPECT_EQ(GetWord(bitmap, i), 0);
    }
    for (int64_t i = 0; i < size; ++i) {
      EXPECT_FALSE(GetBit(bitmap, i));
    }
    EXPECT_TRUE(AreAllBitsUnset(bitmap.span().data(), size));
  }
}

TEST(BitmapTest, Iterate) {
  Bitmap bitmap = CreateBuffer<Word>({0xffff4321, 0x0, 0xf0f0f0f0, 0xffffffff});
  EXPECT_EQ(GetWord(bitmap, 0), 0xffff4321);
  EXPECT_EQ(GetWord(bitmap, 2), 0xf0f0f0f0);
  EXPECT_EQ(GetWordWithOffset(bitmap, 0, 0), 0xffff4321);
  EXPECT_EQ(GetWordWithOffset(bitmap, 0, 31), 0x1);
  EXPECT_EQ(GetWordWithOffset(bitmap, 2, 8), 0xfff0f0f0);
  EXPECT_TRUE(GetBit(bitmap, 0));
  EXPECT_FALSE(GetBit(bitmap, 1));
  EXPECT_TRUE(GetBit(bitmap, 31));
  EXPECT_FALSE(GetBit(bitmap, 32));
  EXPECT_FALSE(GetBit(bitmap, 67));
  EXPECT_TRUE(GetBit(bitmap, 68));
  EXPECT_TRUE(GetBit(bitmap, 127));
  int64_t bit = 0;
  std::unique_ptr<int> x;  // Needed to check that the function is not copied.
  auto check_fn = [&, x(std::move(x))](bool v) {
    EXPECT_EQ(v, GetBit(bitmap, bit));
    bit++;
  };
  Iterate(bitmap, 0, 0, check_fn);
  EXPECT_EQ(bit, 0);
  Iterate(bitmap, 0, 17, check_fn);
  EXPECT_EQ(bit, 17);
  Iterate(bitmap, 17, 32, check_fn);
  EXPECT_EQ(bit, 17 + 32);
  Iterate(bitmap, 17 + 32, 69, check_fn);
  EXPECT_EQ(bit, 17 + 32 + 69);
}

TEST(BitmapTest, Intersect) {
  Bitmap b1 = CreateBuffer<Word>({0xffff4321, 0x0, 0xf0f0f0f0, 0xffffffff});
  Bitmap b2 = CreateBuffer<Word>({0x43214321, 0x1, 0x0f0ff0f0, 0xffffffff});
  Bitmap b3 =
      CreateBuffer<Word>({0x43214321, 0x1, 0x0f0ff0f0, 0xffffffff, 0x8});

  {
    std::vector<Word> res(4);
    Intersect(b1, b2, {res.data(), res.size()});
    EXPECT_THAT(res, testing::ElementsAre(0x43214321, 0x0, 0xf0f0, 0xffffffff));
  }
  {
    std::vector<Word> res(4);
    Intersect(b1, b2, 5, 5, {res.data(), res.size()});
    EXPECT_THAT(res, testing::ElementsAre(0x43214321, 0x0, 0xf0f0, 0xffffffff));
  }
  {
    std::vector<Word> res(4);
    Intersect(b1, b3, 4, 8, {res.data(), res.size()});
    EXPECT_THAT(res,
                testing::ElementsAre(0x14320020, 0x0, 0xf0f0f000, 0x8fffffff));
  }
  {
    std::vector<Word> res(4);
    Intersect(b3, b1, 8, 4, {res.data(), res.size()});
    EXPECT_THAT(res,
                testing::ElementsAre(0x14320020, 0x0, 0xf0f0f000, 0x8fffffff));
  }
}

TEST(CountBits, Trivial) {
  const std::vector<uint32_t> bitmap = {1664460009U, 1830791933U, 2649253042U,
                                        1615775603U};  // random values
  const auto bit = [&](int64_t i) { return (bitmap[i / 32] >> (i % 32)) & 1; };
  const auto bitmap_buffer = CreateBuffer(bitmap);
  const int64_t n = 32 * bitmap.size();
  for (int64_t i = 0; i <= n; ++i) {
    int64_t count = 0;
    for (int64_t j = i; j < n; ++j) {
      ASSERT_EQ(count, CountBits(bitmap_buffer, i, j - i)) << i << ' ' << j;
      count += bit(j);
    }
    ASSERT_EQ(count, CountBits(bitmap_buffer, i, n - i));
  }
}

TEST(CountBits, OutOfRange) {
  const auto bitmap_buffer = CreateBuffer({0xffff0000});
  ASSERT_EQ(CountBits(bitmap_buffer, -30, 24), 24);
  ASSERT_EQ(CountBits(bitmap_buffer, -20, 24), 20);
  ASSERT_EQ(CountBits(bitmap_buffer, -10, 24), 10);
  ASSERT_EQ(CountBits(bitmap_buffer, -5, 24), 8);
  ASSERT_EQ(CountBits(bitmap_buffer, 0, 24), 8);
  ASSERT_EQ(CountBits(bitmap_buffer, 5, 24), 13);
  ASSERT_EQ(CountBits(bitmap_buffer, 10, 24), 18);
  ASSERT_EQ(CountBits(bitmap_buffer, 20, 24), 24);
  ASSERT_EQ(CountBits(bitmap_buffer, 30, 24), 24);
  ASSERT_EQ(CountBits(bitmap_buffer, 40, 24), 24);
}

TEST(BuilderTest, AddByGroups) {
  int64_t size = 16384;
  absl::BitGen gen;
  std::vector<bool> bits;
  Builder bldr(size);
  auto add_fn = [&](int) {
    bool v = absl::Bernoulli(gen, 0.5);
    bits.push_back(v);
    return v;
  };
  for (int64_t remaining_count = size; remaining_count > 0;) {
    int64_t count =
        std::min(remaining_count, absl::Uniform<int64_t>(gen, 0, 256));
    remaining_count -= count;
    bldr.AddByGroups(count, [&](int64_t) { return add_fn; });
  }
  Bitmap bitmap = std::move(bldr).Build();
  EXPECT_EQ(size, bits.size());
  for (int64_t i = 0; i < bits.size(); ++i) {
    EXPECT_EQ(GetBit(bitmap, i), bits[i]);
  }
}

TEST(BuilderTest, AddForEachNeverCopyAFunction) {
  int cont[1]{0};
  {  // temporary function
    std::unique_ptr<int> x;
    Builder b(1);
    b.AddForEach(cont, [x(std::move(x))](int) { return true; });
  }
  {  // const function
    std::unique_ptr<int> x;
    Builder b(1);
    const auto fn = [x(std::move(x))](int) { return true; };
    b.AddForEach(cont, fn);
  }
  {  // mutable function
    std::unique_ptr<int> x;
    int cnt = 0;
    Builder b(1);
    auto fn = [&cnt, x(std::move(x))](int) mutable {
      ++cnt;
      return true;
    };
    b.AddForEach(cont, fn);
    EXPECT_EQ(cnt, 1);
  }
}

// Test that bits equal to fn(id). Macro to have lines in error messages.
#define TEST_BITS(bitmap_expr, fn, N)                        \
  Bitmap bitmap = bitmap_expr;                               \
  ASSERT_EQ(bitmap.size(), BitmapSize(N));                   \
  for (int i = 0; i < (N); ++i) {                            \
    ASSERT_EQ(GetBit(bitmap, i), fn(i)) << i << " of " << N; \
  }

TEST(BuilderTest, AddForEachSingle) {
  constexpr int kMaxN = 1000;
  std::vector<int> v(kMaxN);
  for (int n = 0; n < kMaxN; ++n) {
    v[n] = n;
  }
  auto is_5_divisible = [](int x) { return x % 5 == 0; };
  for (int n = 2; n < kMaxN; ++n) {
    {  // vector
      Builder b(n);
      b.AddForEach(std::vector(v.begin(), v.begin() + n), is_5_divisible);
      TEST_BITS(std::move(b).Build(), is_5_divisible, n);
    }
    {  // span
      Builder b(n);
      b.AddForEach(absl::MakeConstSpan(v.data(), n), is_5_divisible);
      TEST_BITS(std::move(b).Build(), is_5_divisible, n);
    }
  }
}

TEST(BuilderTest, AddForEachMany) {
  constexpr int kMaxN = 4027;
  std::vector<int> v(kMaxN);
  for (int n = 0; n < kMaxN; ++n) {
    v[n] = n;
  }
  auto is_5_divisible = [](int x) { return x % 5 == 0; };
  Builder b(kMaxN);
  int beg = 0;
  for (int cnt : {2, 3, 4, 6, 9, 13, 18, 27, 47, 94, 188, 376, 752, kMaxN}) {
    b.AddForEach(
        absl::MakeConstSpan(v.data() + beg, std::min(cnt, kMaxN - beg)),
        is_5_divisible);
    beg += cnt;
  }
  TEST_BITS(std::move(b).Build(), is_5_divisible, kMaxN);
}

TEST(BuilderTest, Full) {
  Builder builder(10);
  builder.AddForEach(std::vector<int>(10), [](int) { return true; });
  EXPECT_TRUE(std::move(builder).Build().empty());
}

TEST(AlmostFullBuilderTest, Full) {
  AlmostFullBuilder builder(555);
  EXPECT_TRUE(std::move(builder).Build().empty());
}

TEST(AlmostFullBuilderTest, Empty) {
  int64_t size = 555;
  AlmostFullBuilder builder(size);
  for (int64_t i = 0; i < size; ++i) {
    builder.AddMissed(i);
  }
  auto bitmap = std::move(builder).Build();
  ASSERT_EQ(bitmap.size(), BitmapSize(size));
  // Be sure that we are consistent, e.g., doesn't have 1s in unused bits.
  EXPECT_TRUE(AreAllBitsUnset(bitmap.span().data(), size));
  for (int64_t i = 0; i < size; ++i) {
    EXPECT_EQ(GetBit(bitmap, i), 0);
  }
}

TEST(AlmostFullBuilderTest, NotFull) {
  int64_t size = 555;
  AlmostFullBuilder builder(size);
  for (int64_t i = 0; i < size; ++i) {
    if (i % 5 == 1) builder.AddMissed(i);
  }
  auto bitmap = std::move(builder).Build();
  EXPECT_EQ(bitmap.size(), BitmapSize(size));
  for (int64_t i = 0; i < size; ++i) {
    EXPECT_EQ(GetBit(bitmap, i), i % 5 != 1);
  }
}

TEST(AlmostFullBuilderTest, EmptyThanFull) {
  int64_t size = 155;
  for (int64_t split_point = 1; split_point < size; ++split_point) {
    AlmostFullBuilder builder(size);
    for (int64_t i = 0; i < split_point; ++i) {
      builder.AddMissed(i);
    }
    auto bitmap = std::move(builder).Build();
    EXPECT_EQ(bitmap.size(), BitmapSize(size));
    for (int64_t i = 0; i < size; ++i) {
      ASSERT_EQ(GetBit(bitmap, i), i >= split_point) << i << " " << split_point;
    }
  }
}

TEST(AlmostFullBuilderTest, EmptyConsequentlyAtStartAndAFewMissed) {
  int64_t size = 155;
  int64_t split_point = 71;
  AlmostFullBuilder builder(size);
  for (int64_t i = 0; i < split_point; ++i) {
    builder.AddMissed(i);
  }
  builder.AddMissed(93);
  builder.AddMissed(107);
  auto bitmap = std::move(builder).Build();
  EXPECT_EQ(bitmap.size(), BitmapSize(size));
  for (int64_t i = 0; i < size; ++i) {
    bool present = (i >= split_point) && (i != 93) && (i != 107);
    ASSERT_EQ(GetBit(bitmap, i), present) << i;
  }
}

}  // namespace
}  // namespace arolla::bitmap
