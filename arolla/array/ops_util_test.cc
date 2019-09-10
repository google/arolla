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
#include "arolla/array/ops_util.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/array/array.h"
#include "arolla/array/id_filter.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"

using ::testing::ElementsAre;

namespace arolla::array_ops_internal {

TEST(ArrayOpsUtilTest, IterateConst) {
  ArrayOpsUtil<false, meta::type_list<int, int>> util(6, Array<int>(6, 3),
                                                      Array<int>(6, 7));
  {
    std::vector<int64_t> ids;
    std::vector<int> vx;
    std::vector<int> vy;
    util.Iterate(1, 4, [&](int64_t id, int x, int y) {
      ids.push_back(id);
      vx.push_back(x);
      vy.push_back(y);
    });
    EXPECT_THAT(ids, ElementsAre(1, 2, 3));
    EXPECT_THAT(vx, ElementsAre(3, 3, 3));
    EXPECT_THAT(vy, ElementsAre(7, 7, 7));
  }
  {
    int fn_count = 0;
    int missing_fn_count = 0;
    int repeated_fn_count = 0;
    auto fn = [&](int64_t, int, int) { fn_count++; };
    auto missing_fn = [&](int64_t, int64_t) { missing_fn_count++; };
    auto repeated_fn = [&](int64_t first_id, int64_t count, int x, int y) {
      EXPECT_EQ(first_id, 1);
      EXPECT_EQ(count, 3);
      EXPECT_EQ(x, 3);
      EXPECT_EQ(y, 7);
      repeated_fn_count++;
    };
    util.Iterate(1, 4, fn, missing_fn, repeated_fn);
    EXPECT_EQ(fn_count, 0);
    EXPECT_EQ(missing_fn_count, 0);
    EXPECT_EQ(repeated_fn_count, 1);
  }
}

TEST(ArrayOpsUtilTest, IterateSparse) {
  auto q1 = CreateArray<int>(20, {3, 7, 8, 10}, {1, 2, 3, 4});
  auto q2 = CreateArray<int>(20, {4, 8, 10, 11}, {1, 2, 3, 4});
  std::vector<std::string> res;
  auto fn = [&](int64_t id, int x, int y) {
    res.push_back(absl::StrFormat("single(%d, %d, %d)", id, x, y));
  };
  auto missing_fn = [&](int64_t id, int64_t count) {
    res.push_back(absl::StrFormat("missing(%d, %d)", id, count));
  };
  auto repeated_fn = [&](int64_t id, int64_t count, int x, int y) {
    res.push_back(absl::StrFormat("repeated(%d, %d, %d, %d)", id, count, x, y));
  };
  ArrayOpsUtil<false, meta::type_list<int, int>> util(20, q1, q2);
  util.Iterate(0, 15, fn, missing_fn, repeated_fn);
  EXPECT_THAT(
      res, ElementsAre("missing(0, 4)", "missing(4, 1)", "missing(5, 3)",
                       "single(8, 3, 2)", "missing(9, 1)", "single(10, 4, 3)",
                       "missing(11, 1)", "missing(12, 3)"));
}

TEST(ArrayOpsUtilTest, IterateSparseWithMissedIdValue) {
  Array<int> q1(20, IdFilter(20, CreateBuffer<int64_t>({3, 7, 8, 10})),
                CreateDenseArray<int>({1, 2, 3, 4}), 9);
  auto q2 = CreateArray<int>(20, {4, 8, 10, 11}, {1, 2, 3, 4});
  std::vector<std::string> res;
  auto fn = [&](int64_t id, int x, OptionalValue<int> y) {
    if (y.present) {
      res.push_back(absl::StrFormat("single(%d, %d, %d)", id, x, y.value));
    } else {
      res.push_back(absl::StrFormat("single(%d, %d, NA)", id, x));
    }
  };
  auto missing_fn = [&](int64_t id, int64_t count) {
    res.push_back(absl::StrFormat("missing(%d, %d)", id, count));
  };
  auto repeated_fn = [&](int64_t id, int64_t count, int x,
                         OptionalValue<int> y) {
    if (y.present) {
      res.push_back(
          absl::StrFormat("repeated(%d, %d, %d, %d)", id, count, x, y.value));
    } else {
      res.push_back(absl::StrFormat("repeated(%d, %d, %d, NA)", id, count, x));
    }
  };
  ArrayOpsUtil<false, meta::type_list<int, OptionalValue<int>>> util(20, q1,
                                                                     q2);
  util.Iterate(0, 15, fn, missing_fn, repeated_fn);
  EXPECT_THAT(res, ElementsAre("repeated(0, 3, 9, NA)", "single(3, 1, NA)",
                               "single(4, 9, 1)", "repeated(5, 2, 9, NA)",
                               "single(7, 2, NA)", "single(8, 3, 2)",
                               "repeated(9, 1, 9, NA)", "single(10, 4, 3)",
                               "single(11, 9, 4)", "repeated(12, 3, 9, NA)"));
}

TEST(ArrayOpsUtilTest, ArraysIterate) {
  Array<int> array_x = CreateArray<int>({5, 4, std::nullopt, 2, 1});
  Array<int> array_y =
      CreateArray<int>({3, std::nullopt, 3, 1, 3}).ToSparseForm();
  std::vector<int64_t> ids;
  std::vector<OptionalValue<int>> vx;
  std::vector<int> vy;
  ArraysIterate(
      [&](int64_t id, OptionalValue<int> x, int y) {
        ids.push_back(id);
        vx.push_back(x);
        vy.push_back(y);
      },
      array_x, array_y);
  EXPECT_THAT(ids, ElementsAre(0, 2, 3, 4));
  EXPECT_THAT(vx, ElementsAre(5, std::nullopt, 2, 1));
  EXPECT_THAT(vy, ElementsAre(3, 3, 1, 3));
}

// The only difference from the previous test is the ArraysIterate is replaced
// with ArraysIterateDense. Both functions have the same semantics, only binary
// size and performance can change.
TEST(ArrayOpsUtilTest, ArraysIterateDense) {
  Array<int> array_x = CreateArray<int>({5, 4, std::nullopt, 2, 1});
  Array<int> array_y =
      CreateArray<int>({3, std::nullopt, 3, 1, 3}).ToSparseForm();
  std::vector<int64_t> ids;
  std::vector<OptionalValue<int>> vx;
  std::vector<int> vy;
  ArraysIterateDense(
      [&](int64_t id, OptionalValue<int> x, int y) {
        ids.push_back(id);
        vx.push_back(x);
        vy.push_back(y);
      },
      array_x, array_y);
  EXPECT_THAT(ids, ElementsAre(0, 2, 3, 4));
  EXPECT_THAT(vx, ElementsAre(5, std::nullopt, 2, 1));
  EXPECT_THAT(vy, ElementsAre(3, 3, 1, 3));
}

TEST(ArrayOpsUtilTest, ArraysIterateStrings) {
  Array<Text> array_x = CreateArray<Text>(
      {Text("5"), Text("4"), std::nullopt, Text("2"), Text("1")});
  Array<Bytes> array_y =
      CreateArray<Bytes>(
          {Bytes("3"), std::nullopt, Bytes("3"), Bytes("1"), Bytes("3")})
          .ToSparseForm();
  std::vector<int64_t> ids;
  std::vector<OptionalValue<absl::string_view>> vx;
  std::vector<absl::string_view> vy;
  ArraysIterate(
      [&](int64_t id, OptionalValue<absl::string_view> x, absl::string_view y) {
        ids.push_back(id);
        vx.push_back(x);
        vy.push_back(y);
      },
      array_x, array_y);
  EXPECT_THAT(ids, ElementsAre(0, 2, 3, 4));
  EXPECT_THAT(vx, ElementsAre("5", std::nullopt, "2", "1"));
  EXPECT_THAT(vy, ElementsAre("3", "3", "1", "3"));
}

}  // namespace arolla::array_ops_internal
