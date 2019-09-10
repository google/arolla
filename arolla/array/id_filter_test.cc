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
#include "arolla/array/id_filter.h"

#include <cstdint>
#include <tuple>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/raw_buffer_factory.h"

namespace arolla {
namespace {

TEST(IdFilterTest, UpperBoundIntersect) {
  IdFilter empty(IdFilter::kEmpty);
  IdFilter full(IdFilter::kFull);
  IdFilter a = IdFilter(5, CreateBuffer<int64_t>({3, 4}));
  IdFilter b = IdFilter(5, CreateBuffer<int64_t>({0, 2, 3}));
  IdFilter c = IdFilter(5, CreateBuffer<int64_t>({0, 1, 3, 4}));

  EXPECT_TRUE(IdFilter::UpperBoundIntersect(a).IsSame(a));

  EXPECT_TRUE(IdFilter::UpperBoundIntersect(a, empty).IsSame(empty));
  EXPECT_TRUE(IdFilter::UpperBoundIntersect(empty, a).IsSame(empty));
  EXPECT_TRUE(IdFilter::UpperBoundIntersect(a, full).IsSame(a));
  EXPECT_TRUE(IdFilter::UpperBoundIntersect(full, a).IsSame(a));
  EXPECT_TRUE(IdFilter::UpperBoundIntersect(a, b).IsSame(a));
  EXPECT_TRUE(IdFilter::UpperBoundIntersect(b, a).IsSame(a));

  EXPECT_TRUE(IdFilter::UpperBoundIntersect(a, b, c).IsSame(a));
  EXPECT_TRUE(IdFilter::UpperBoundIntersect(c, b, a).IsSame(a));
  EXPECT_TRUE(IdFilter::UpperBoundIntersect(a, empty, c).IsSame(empty));
  EXPECT_TRUE(IdFilter::UpperBoundIntersect(full, b, c).IsSame(b));
}

TEST(IdFilterTest, UpperBoundMerge) {
  IdFilter empty(IdFilter::kEmpty);
  IdFilter full(IdFilter::kFull);
  IdFilter a = IdFilter(5, CreateBuffer<int64_t>({3, 4}));
  IdFilter b = IdFilter(5, CreateBuffer<int64_t>({0, 2, 3}));
  RawBufferFactory* bf = GetHeapBufferFactory();

  EXPECT_TRUE(IdFilter::UpperBoundMerge(5, bf, a).IsSame(a));
  EXPECT_TRUE(IdFilter::UpperBoundMerge(5, bf, a, empty).IsSame(a));
  EXPECT_TRUE(IdFilter::UpperBoundMerge(5, bf, empty, a).IsSame(a));
  EXPECT_TRUE(IdFilter::UpperBoundMerge(25, bf, a, full).IsSame(full));
  EXPECT_TRUE(IdFilter::UpperBoundMerge(25, bf, a, full, b).IsSame(full));
  EXPECT_TRUE(IdFilter::UpperBoundMerge(5, bf, a, b).IsSame(full));
  EXPECT_THAT(IdFilter::UpperBoundMerge(25, bf, a, b).ids(),
              testing::ElementsAre(0, 2, 3, 4));
}

TEST(IdFilterTest, IntersectPartial_ForEach) {
  IdFilter a = IdFilter(5, CreateBuffer<int64_t>({3, 4}));
  IdFilter b = IdFilter(5, CreateBuffer<int64_t>({0, 2, 3}));
  IdFilter c = IdFilter(5, CreateBuffer<int64_t>({0, 1, 3, 4}));

  using FnArgs = std::tuple<int64_t, int64_t, int64_t>;
  std::vector<FnArgs> res;
  auto fn = [&](int64_t id, int64_t offset1, int64_t offset2) {
    res.push_back({id, offset1, offset2});
  };

  IdFilter::IntersectPartial_ForEach(a, b, fn);
  EXPECT_EQ(res, (std::vector<FnArgs>{{3, 0, 2}}));
  res.clear();

  IdFilter::IntersectPartial_ForEach(b, a, fn);
  EXPECT_EQ(res, (std::vector<FnArgs>{{3, 2, 0}}));
  res.clear();

  IdFilter::IntersectPartial_ForEach(a, c, fn);
  EXPECT_EQ(res, (std::vector<FnArgs>{{3, 0, 2}, {4, 1, 3}}));
  res.clear();

  IdFilter::IntersectPartial_ForEach(c, a, fn);
  EXPECT_EQ(res, (std::vector<FnArgs>{{3, 2, 0}, {4, 3, 1}}));
  res.clear();

  IdFilter::IntersectPartial_ForEach(b, c, fn);
  EXPECT_EQ(res, (std::vector<FnArgs>{{0, 0, 0}, {3, 2, 2}}));
  res.clear();

  IdFilter::IntersectPartial_ForEach(c, b, fn);
  EXPECT_EQ(res, (std::vector<FnArgs>{{0, 0, 0}, {3, 2, 2}}));
  res.clear();
}

}  // namespace
}  // namespace arolla
