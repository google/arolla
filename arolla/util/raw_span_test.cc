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
#include "arolla/util/raw_span.h"

#include <array>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/types/span.h"

namespace arolla {
namespace {

using ::testing::Eq;

TEST(SpanTest, SpanConstructor) {
  std::vector<int> v = {1, 2, 3};
  absl::Span<const int> absl_span(v);

  RawSpan<const int> span1(v);
  EXPECT_THAT(span1.data(), Eq(v.data()));
  EXPECT_THAT(span1.size(), Eq(3));

  RawSpan<const int> span2(absl_span);
  EXPECT_THAT(span2.data(), Eq(v.data()));
  EXPECT_THAT(span2.size(), Eq(3));
}

TEST(SpanTest, PointerAndSizeConstructor) {
  std::array<int, 3> arr = {4, 5, 6};
  RawSpan<int> span(arr.data(), arr.size());

  EXPECT_THAT(span.data(), Eq(arr.data()));
  EXPECT_THAT(span.size(), Eq(3));

  RawSpan<int> empty_span(nullptr, 0);
  EXPECT_THAT(empty_span.data(), Eq(nullptr));
  EXPECT_THAT(empty_span.size(), Eq(0));
}

TEST(SpanTest, Accessors) {
  std::array<int, 4> arr = {10, 20, 30, 40};
  RawSpan<const int> span(arr.data(), arr.size());

  EXPECT_THAT(span.begin(), Eq(arr.data()));
  EXPECT_THAT(span.data(), Eq(arr.data()));
  EXPECT_THAT(span.end(), Eq(arr.data() + 4));
  EXPECT_THAT(span.size(), Eq(4));
}

TEST(SpanTest, Indexing) {
  std::array<int, 3> arr = {100, 200, 300};
  RawSpan<int> span(arr.data(), arr.size());

  EXPECT_THAT(span[0], Eq(100));
  EXPECT_THAT(span[1], Eq(200));
  EXPECT_THAT(span[2], Eq(300));

  span[1] = 999;
  EXPECT_THAT(arr[1], Eq(999));
}

TEST(SpanTest, SubspanOffset) {
  std::array<int, 5> arr = {1, 2, 3, 4, 5};
  RawSpan<int> span(arr.data(), arr.size());

  RawSpan<int> sub = span.subspan(2);
  EXPECT_THAT(sub.data(), Eq(arr.data() + 2));
  EXPECT_THAT(sub.size(), Eq(3));
  EXPECT_THAT(sub[0], Eq(3));
  EXPECT_THAT(sub[2], Eq(5));

  RawSpan<int> full = span.subspan(0);
  EXPECT_THAT(full.data(), Eq(arr.data()));
  EXPECT_THAT(full.size(), Eq(5));

  RawSpan<int> empty = span.subspan(5);
  EXPECT_THAT(empty.data(), Eq(arr.data() + 5));
  EXPECT_THAT(empty.size(), Eq(0));
}

TEST(SpanTest, SubspanOffsetAndSize) {
  std::array<int, 5> arr = {10, 20, 30, 40, 50};
  RawSpan<int> span(arr.data(), arr.size());

  RawSpan<int> sub = span.subspan(1, 3);
  EXPECT_THAT(sub.data(), Eq(arr.data() + 1));
  EXPECT_THAT(sub.size(), Eq(3));
  EXPECT_THAT(sub[0], Eq(20));
  EXPECT_THAT(sub[2], Eq(40));

  RawSpan<int> empty = span.subspan(2, 0);
  EXPECT_THAT(empty.data(), Eq(arr.data() + 2));
  EXPECT_THAT(empty.size(), Eq(0));
}

}  // namespace
}  // namespace arolla
