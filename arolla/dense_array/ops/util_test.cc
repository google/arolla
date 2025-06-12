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
#include "arolla/dense_array/ops/util.h"

#include <cstdint>
#include <cstring>
#include <optional>
#include <utility>

#include "gtest/gtest.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"

namespace arolla {
namespace {

TEST(DenseOpsUtil, Getter) {
  using Getter = dense_ops_internal::Getter<int, DenseArray<int>, false>;
  DenseArrayBuilder<int> bldr(200);
  for (int i = 0; i < 200; ++i) bldr.Set(i, i * i);
  auto array = std::move(bldr).Build();
  auto g = Getter(array, 0);
  for (int i = 0; i < 32; ++i) {
    EXPECT_EQ(g(i), i * i);
  }
  g = Getter(array, 2);
  for (int i = 0; i < 32; ++i) {
    EXPECT_EQ(g(i), (i + 64) * (i + 64));
  }
}

TEST(DenseOpsUtil, OptionalGetter) {
  {  // full
    using Getter =
        dense_ops_internal::Getter<OptionalValue<int>, DenseArray<int>, false>;
    DenseArrayBuilder<int> bldr(200);
    for (int i = 0; i < 200; ++i) bldr.Set(i, i * i);
    auto array = std::move(bldr).Build();
    auto g = Getter(array, 0);
    for (int i = 0; i < 32; ++i) {
      EXPECT_EQ(g(i), i * i);
    }
    g = Getter(array, 2);
    for (int i = 0; i < 32; ++i) {
      EXPECT_EQ(g(i), (i + 64) * (i + 64));
    }
  }
  {  // dense
    using Getter =
        dense_ops_internal::Getter<OptionalValue<int>, DenseArray<int>, false>;
    DenseArrayBuilder<int> bldr(200);
    for (int i = 0; i < 200; ++i) {
      if (i % 3 == 0) bldr.Set(i, i * i);
    }
    auto array = std::move(bldr).Build();
    auto g = Getter(array, 0);
    for (int i = 0; i < 32; ++i) {
      if (i % 3 == 0) {
        EXPECT_EQ(g(i), i * i);
      } else {
        EXPECT_EQ(g(i), std::nullopt);
      }
    }
    g = Getter(array, 2);
    for (int i = 0; i < 32; ++i) {
      if ((i + 64) % 3 == 0) {
        EXPECT_EQ(g(i), (i + 64) * (i + 64));
      } else {
        EXPECT_EQ(g(i), std::nullopt);
      }
    }
  }
  {  // bitmap with offset
    using Getter =
        dense_ops_internal::Getter<OptionalValue<int>, DenseArray<int>, true>;
    Buffer<int>::Builder values_bldr(200);
    Buffer<bitmap::Word>::Builder bitmap_bldr(bitmap::BitmapSize(200 + 2));
    auto bitmap = bitmap_bldr.GetMutableSpan();
    std::memset(bitmap.begin(), 0, bitmap.size() * sizeof(bitmap::Word));
    for (int i = 0; i < 200; ++i) {
      values_bldr.Set(i, i * i);
      if (i % 3 == 0) SetBit(bitmap.begin(), i + 2);
    }
    DenseArray<int> array{std::move(values_bldr).Build(),
                          std::move(bitmap_bldr).Build(), 2};
    auto g = Getter(array, 0);
    for (int i = 0; i < 32; ++i) {
      if (i % 3 == 0) {
        EXPECT_EQ(g(i), i * i);
      } else {
        EXPECT_EQ(g(i), std::nullopt);
      }
    }
    g = Getter(array, 2);
    for (int i = 0; i < 32; ++i) {
      if ((i + 64) % 3 == 0) {
        EXPECT_EQ(g(i), (i + 64) * (i + 64));
      } else {
        EXPECT_EQ(g(i), std::nullopt);
      }
    }
  }
}

// We intentionally reading uninitialized memory, which is considered and error
// under UBSAN.
// We are testing on bool since UBSAN detecting error like:
// runtime error: load of value 190, which is not a valid value for type
// 'const bool'
TEST(DenseOpsUtil, OptionalGetterBoolUninitializedMemoryRead) {
  using Getter =
      dense_ops_internal::Getter<OptionalValue<bool>, DenseArray<bool>, false>;
  DenseArrayBuilder<bool> bldr(200);
  for (int i = 0; i < 200; ++i) {
    if (i % 3 == 0) bldr.Set(i, i % 2 == 0);
  }
  auto array = std::move(bldr).Build();
  auto g = Getter(array, 0);
  for (int i = 0; i < 32; ++i) {
    if (i % 3 == 0) {
      EXPECT_EQ(g(i), i % 2 == 0);
    } else {
      EXPECT_EQ(g(i), std::nullopt);
    }
  }
  g = Getter(array, 2);
  for (int i = 0; i < 32; ++i) {
    if ((i + 64) % 3 == 0) {
      EXPECT_EQ(g(i), (i + 64) % 2 == 0);
    } else {
      EXPECT_EQ(g(i), std::nullopt);
    }
  }
}
constexpr int MAX_SIZE = 199;

template <class Fn>
void StressTestRanges(Fn&& fn) {
  for (int from = 0; from < 64; ++from) {
    for (int count = 0; count < MAX_SIZE - from; ++count) {
      fn(from, from + count);
    }
  }
}

TEST(DenseOpsUtil, IterateEmpty) {
  using Util = dense_ops_internal::DenseOpsUtil<meta::type_list<>>;

  int counter;
  auto count_fn = [&counter](int64_t, bool valid) {
    EXPECT_TRUE(valid);
    counter++;
  };

  StressTestRanges([&](int from, int to) {
    counter = 0;
    Util::Iterate(count_fn, from, to);
    EXPECT_EQ(counter, to - from);
  });
}

TEST(DenseOpsUtil, Iterate) {
  using Util = dense_ops_internal::DenseOpsUtil<
      meta::type_list<OptionalValue<int>, Bytes, float, OptionalValue<Bytes>>>;

  DenseArrayBuilder<int> bldr_a(MAX_SIZE);
  DenseArrayBuilder<Bytes> bldr_b(MAX_SIZE);
  DenseArrayBuilder<float> bldr_c(MAX_SIZE);
  DenseArrayBuilder<Bytes> bldr_d(MAX_SIZE);

  for (int i = 0; i < MAX_SIZE; ++i) {
    if (i % 5 < 2) bldr_a.Set(i, i + 123);
    if (i % 7 > 2) bldr_b.Set(i, absl::StrFormat("b %d", i));
    if (i % 11 < 7) bldr_c.Set(i, i + 1.5f);
    if (i % 13 < 4) bldr_d.Set(i, absl::StrFormat("d %d", i));
  }

  auto a = std::move(bldr_a).Build();
  auto b = std::move(bldr_b).Build();
  auto c = std::move(bldr_c).Build();
  auto d = std::move(bldr_d).Build();

  EXPECT_EQ(Util::IntersectMasks(0, a, b, c, d), 0x0F023878);
  EXPECT_EQ(Util::IntersectMasks(1, a, b, c, d), 0x3881E0C6);
  EXPECT_EQ(Util::IntersectMasks(2, a, b, c, d), 0x470F003C);
  EXPECT_EQ(Util::IntersectMasks(3, a, b, c, d), 0x3C18C1E0);

  int index;
  bool error = false;
  auto fn = [&](int64_t id, bool valid, OptionalValue<int> va,
                absl::string_view vb, float vc,
                OptionalValue<absl::string_view> vd) {
    if (error)
      return;  // Show only the first error, otherwise it can take
               // an hour to create an error log.
    EXPECT_EQ(index, id);
    EXPECT_EQ(valid, b.present(index) && c.present(index));
    error = error || (valid != b.present(index) && c.present(index));
    if (valid) {
      EXPECT_EQ(va, a[index]);
      EXPECT_EQ(vb, b.values[index]);
      EXPECT_EQ(vc, c.values[index]);
      EXPECT_EQ(vd, d[index]);
      error = error || (va != a[index]);
      error = error || (vb != b.values[index]);
      error = error || (vc != c.values[index]);
      error = error || (vd != d[index]);
    }
    index++;
  };

  StressTestRanges([&](int from, int to) {
    index = from;
    Util::Iterate(fn, from, to, a, b, c, d);
    if (!error) {
      EXPECT_EQ(index, to);
    }
    if (from == 0) {
      index = from;
      Util::IterateFromZero(fn, to, a, b, c, d);
      if (!error) {
        EXPECT_EQ(index, to);
      }
    }
  });
}

}  // namespace
}  // namespace arolla
