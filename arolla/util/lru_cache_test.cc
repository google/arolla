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
  (void)cache.Put(1, 1.5);
  ASSERT_THAT(cache.LookupOrNull(1), Pointee(1.5));
  ASSERT_THAT(cache.LookupOrNull(2), IsNull());
  ASSERT_THAT(cache.LookupOrNull(3), IsNull());
  (void)cache.Put(2, 2.5);
  ASSERT_THAT(cache.LookupOrNull(1), Pointee(1.5));
  ASSERT_THAT(cache.LookupOrNull(2), Pointee(2.5));
  ASSERT_THAT(cache.LookupOrNull(3), IsNull());
  (void)cache.Put(3, 3.5);
  ASSERT_THAT(cache.LookupOrNull(1), IsNull());
  ASSERT_THAT(cache.LookupOrNull(2), Pointee(2.5));
  ASSERT_THAT(cache.LookupOrNull(3), Pointee(3.5));
}

TEST(LruCache, TransparentKeyType) {
  LruCache<std::string, int, absl::Hash<absl::string_view>, std::equal_to<>>
      cache(3);
  (void)cache.Put("1", 1);
  (void)cache.Put(absl::string_view("2"), 2);
  (void)cache.Put(std::string("3"), 3);
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
  (void)cache.Put(1, 1.5);
  ASSERT_THAT(cache.LookupOrNull(1), Pointee(1.5));
  cache.Clear();
  ASSERT_THAT(cache.LookupOrNull(1), IsNull());
}

TEST(LruCache, Overwrite) {
  LruCache<int, double> cache(2);
  (void)cache.Put(1, 1.5);
  ASSERT_THAT(cache.LookupOrNull(1), Pointee(1.5));
  (void)cache.Put(1, 2.5);
  ASSERT_THAT(cache.LookupOrNull(1), Pointee(1.5));
}

TEST(LruCache, EvictionOrder) {
  {
    LruCache<int, double> cache(2);
    (void)cache.Put(1, 1.0);
    (void)cache.Put(2, 2.0);
    (void)cache.Put(3, 3.0);
    EXPECT_THAT(cache.LookupOrNull(1), IsNull());
    EXPECT_THAT(cache.LookupOrNull(2), Pointee(2.0));
    EXPECT_THAT(cache.LookupOrNull(3), Pointee(3.0));
  }
  {
    LruCache<int, double> cache(2);
    (void)cache.Put(1, 1.0);
    (void)cache.Put(2, 2.0);
    (void)cache.LookupOrNull(1);
    (void)cache.Put(3, 3.0);
    EXPECT_THAT(cache.LookupOrNull(1), Pointee(1.0));
    EXPECT_THAT(cache.LookupOrNull(2), IsNull());
    EXPECT_THAT(cache.LookupOrNull(3), Pointee(3.0));
  }
  {
    LruCache<int, double> cache(2);
    (void)cache.Put(1, 1.0);
    (void)cache.Put(2, 2.0);
    (void)cache.Put(1, 1.1);
    (void)cache.Put(3, 3.0);
    EXPECT_THAT(cache.LookupOrNull(1), Pointee(1.0));
    EXPECT_THAT(cache.LookupOrNull(2), IsNull());
    EXPECT_THAT(cache.LookupOrNull(3), Pointee(3.0));
  }
}

TEST(LruCache, LookupPointerStability) {
  LruCache<int, double> cache(3);
  (void)cache.Put(1, 1.0);
  (void)cache.Put(2, 2.0);
  (void)cache.Put(3, 3.0);
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

}  // namespace
}  // namespace arolla
