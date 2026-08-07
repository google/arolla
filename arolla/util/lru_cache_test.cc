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
#include "arolla/util/lru_cache.h"

#include <functional>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"

namespace arolla {
namespace {

using ::testing::IsNull;
using ::testing::Pointee;

TEST(LruCache, BasicBehaviuor) {
  LruCache<int, double, std::hash<int>, std::equal_to<int>> cache(2);
  ASSERT_THAT(cache.LookupOrNull(1), IsNull());
  ASSERT_THAT(cache.LookupOrNull(2), IsNull());
  ASSERT_THAT(cache.LookupOrNull(3), IsNull());
  cache.Put(1, 1.5);
  ASSERT_THAT(cache.LookupOrNull(1), Pointee(1.5));
  ASSERT_THAT(cache.LookupOrNull(2), IsNull());
  ASSERT_THAT(cache.LookupOrNull(3), IsNull());
  cache.Put(2, 2.5);
  ASSERT_THAT(cache.LookupOrNull(1), Pointee(1.5));
  ASSERT_THAT(cache.LookupOrNull(2), Pointee(2.5));
  ASSERT_THAT(cache.LookupOrNull(3), IsNull());
  cache.Put(3, 3.5);
  ASSERT_THAT(cache.LookupOrNull(1), IsNull());
  ASSERT_THAT(cache.LookupOrNull(2), Pointee(2.5));
  ASSERT_THAT(cache.LookupOrNull(3), Pointee(3.5));
}

TEST(LruCache, TransparentKeyType) {
  LruCache<std::string, int, absl::Hash<absl::string_view>, std::equal_to<>>
      cache(3);
  cache.Put("1", 1);
  cache.Put(absl::string_view("2"), 2);
  cache.Put(std::string("3"), 3);
  ASSERT_THAT(cache.LookupOrNull("1"), Pointee(1));
  ASSERT_THAT(cache.LookupOrNull("2"), Pointee(2));
  ASSERT_THAT(cache.LookupOrNull("3"), Pointee(3));
  ASSERT_THAT(cache.LookupOrNull(absl::string_view("1")), Pointee(1));
  ASSERT_THAT(cache.LookupOrNull(absl::string_view("2")), Pointee(2));
  ASSERT_THAT(cache.LookupOrNull(absl::string_view("3")), Pointee(3));
  ASSERT_THAT(cache.LookupOrNull(std::string("1")), Pointee(1));
  ASSERT_THAT(cache.LookupOrNull(std::string("2")), Pointee(2));
  ASSERT_THAT(cache.LookupOrNull(std::string("3")), Pointee(3));
}

TEST(LruCache, Clear) {
  LruCache<int, double> cache(2);
  ASSERT_THAT(cache.LookupOrNull(1), IsNull());
  cache.Put(1, 1.5);
  ASSERT_THAT(cache.LookupOrNull(1), Pointee(1.5));
  cache.Clear();
  ASSERT_THAT(cache.LookupOrNull(1), IsNull());
}

TEST(LruCache, Overwrite) {
  LruCache<int, double> cache(2);
  cache.Put(1, 1.5);
  ASSERT_THAT(cache.LookupOrNull(1), Pointee(1.5));
  cache.Put(1, 2.5);
  ASSERT_THAT(cache.LookupOrNull(1), Pointee(2.5));
}

TEST(LruCache, EvictionOrder) {
  {
    LruCache<int, double> cache(2);
    cache.Put(1, 1.0);
    cache.Put(2, 2.0);
    cache.Put(3, 3.0);
    EXPECT_THAT(cache.LookupOrNull(1), IsNull());
    EXPECT_THAT(cache.LookupOrNull(2), Pointee(2.0));
    EXPECT_THAT(cache.LookupOrNull(3), Pointee(3.0));
  }
  {
    LruCache<int, double> cache(2);
    cache.Put(1, 1.0);
    cache.Put(2, 2.0);
    (void)cache.LookupOrNull(1);
    cache.Put(3, 3.0);
    EXPECT_THAT(cache.LookupOrNull(1), Pointee(1.0));
    EXPECT_THAT(cache.LookupOrNull(2), IsNull());
    EXPECT_THAT(cache.LookupOrNull(3), Pointee(3.0));
  }
  {
    LruCache<int, double> cache(2);
    cache.Put(1, 1.0);
    cache.Put(2, 2.0);
    cache.Put(1, 1.1);
    cache.Put(3, 3.0);
    EXPECT_THAT(cache.LookupOrNull(1), Pointee(1.1));
    EXPECT_THAT(cache.LookupOrNull(2), IsNull());
    EXPECT_THAT(cache.LookupOrNull(3), Pointee(3.0));
  }
}

TEST(LruCache, LookupPointerStability) {
  LruCache<int, double> cache(3);
  cache.Put(1, 1.0);
  cache.Put(2, 2.0);
  cache.Put(3, 3.0);
  auto* p0 = cache.LookupOrNull(0);
  auto* p1 = cache.LookupOrNull(1);
  auto* p2 = cache.LookupOrNull(2);
  auto* q0 = cache.LookupOrNull(0);
  auto* q1 = cache.LookupOrNull(1);
  auto* q2 = cache.LookupOrNull(2);
  EXPECT_EQ(p0, q0);
  EXPECT_EQ(p1, q1);
  EXPECT_EQ(p2, q2);
}

TEST(LruCache, WeightedEviction) {
  LruCache<int, double> cache(10);
  cache.Put(1, 1.0, /*weight=*/4);
  cache.Put(2, 2.0, /*weight=*/5);
  EXPECT_THAT(cache.LookupOrNull(1), Pointee(1.0));
  EXPECT_THAT(cache.LookupOrNull(2), Pointee(2.0));

  // Adding key=3 with weight 3 exceeds capacity 10 (4 + 5 + 3 = 12).
  // Key 1 (oldest) should be evicted.
  cache.Put(3, 3.0, /*weight=*/3);
  EXPECT_THAT(cache.LookupOrNull(1), IsNull());
  EXPECT_THAT(cache.LookupOrNull(2), Pointee(2.0));
  EXPECT_THAT(cache.LookupOrNull(3), Pointee(3.0));
}

TEST(LruCache, EvictMultipleItemsForHeavyEntry) {
  LruCache<int, double> cache(10);
  cache.Put(1, 1.0, /*weight=*/2);
  cache.Put(2, 2.0, /*weight=*/2);
  cache.Put(3, 3.0, /*weight=*/2);
  cache.Put(4, 4.0, /*weight=*/2);

  // Total weight is 8. Adding key 5 with weight 7 brings total to 15.
  // Evicting key 1 (weight 2) -> 13.
  // Evicting key 2 (weight 2) -> 11.
  // Evicting key 3 (weight 2) -> 9 <= 10.
  // Keys 1, 2, 3 should be evicted; keys 4, 5 remain.
  cache.Put(5, 5.0, /*weight=*/7);
  EXPECT_THAT(cache.LookupOrNull(1), IsNull());
  EXPECT_THAT(cache.LookupOrNull(2), IsNull());
  EXPECT_THAT(cache.LookupOrNull(3), IsNull());
  EXPECT_THAT(cache.LookupOrNull(4), Pointee(4.0));
  EXPECT_THAT(cache.LookupOrNull(5), Pointee(5.0));
}

TEST(LruCache, OverwriteWithNewWeight) {
  LruCache<int, double> cache(10);
  cache.Put(1, 1.0, /*weight=*/3);
  cache.Put(2, 2.0, /*weight=*/4);

  // Overwrite key 1 with larger weight 8.
  // Total weight becomes 8 + 4 = 12 > 10.
  // Key 2 (least recently used) is evicted. Key 1 is kept.
  cache.Put(1, 1.5, /*weight=*/8);
  EXPECT_THAT(cache.LookupOrNull(1), Pointee(1.5));
  EXPECT_THAT(cache.LookupOrNull(2), IsNull());

  // Overwrite key 1 with smaller weight 2. Used weight becomes 2.
  cache.Put(1, 1.0, /*weight=*/2);
  cache.Put(3, 3.0, /*weight=*/7);
  EXPECT_THAT(cache.LookupOrNull(1), Pointee(1.0));
  EXPECT_THAT(cache.LookupOrNull(3), Pointee(3.0));
}

TEST(LruCache, SingleEntryExceedsCapacity) {
  LruCache<int, double> cache(5);
  // Entry with weight larger than capacity is retained if it's the only entry.
  cache.Put(1, 1.0, /*weight=*/10);
  EXPECT_THAT(cache.LookupOrNull(1), Pointee(1.0));

  // Adding another entry causes the heavy entry (oldest) to be evicted.
  cache.Put(2, 2.0, /*weight=*/1);
  EXPECT_THAT(cache.LookupOrNull(1), IsNull());
  EXPECT_THAT(cache.LookupOrNull(2), Pointee(2.0));
}

TEST(LruCache, ClearWithWeights) {
  LruCache<int, double> cache(10);
  cache.Put(1, 1.0, /*weight=*/6);
  cache.Put(2, 2.0, /*weight=*/4);
  cache.Clear();

  // After Clear(), total used capacity should be 0.
  cache.Put(3, 3.0, /*weight=*/7);
  cache.Put(4, 4.0, /*weight=*/3);
  EXPECT_THAT(cache.LookupOrNull(3), Pointee(3.0));
  EXPECT_THAT(cache.LookupOrNull(4), Pointee(4.0));
}

}  // namespace
}  // namespace arolla
