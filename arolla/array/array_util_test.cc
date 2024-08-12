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
#include "arolla/array/array_util.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/unit.h"

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;

namespace arolla::testing {

TEST(ArrayUtilTest, ToArrayMask) {
  Array<int> array =
      CreateArray<int>({1, 2, 3, std::nullopt, 1, 1, std::nullopt, 2})
          .ToSparseForm(1);
  array = array.Slice(1, 7);  // leads to non-zero bimap_bit_offset
  Array<Unit> mask = ToArrayMask(array);

  EXPECT_THAT(mask, ElementsAre(kUnit, kUnit, std::nullopt, kUnit, kUnit,
                                std::nullopt, kUnit));
}

TEST(ArrayUtilTest, ToDenseArray) {
  Array<int> array =
      CreateArray<int>({1, 2, 3, std::nullopt, 1, 1, std::nullopt, 2})
          .ToSparseForm(1);
  array = array.Slice(1, 7);  // leads to non-zero bimap_bit_offset
  DenseArray<int> dense_array = ToDenseArray(array);

  EXPECT_THAT(dense_array,
              ElementsAre(2, 3, std::nullopt, 1, 1, std::nullopt, 2));
}

TEST(ArrayUtilTest, ToBuffer) {
  EXPECT_THAT(
      ToBuffer(CreateArray<int>({1, 2, std::nullopt})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::HasSubstr(
              "Array with missing values can not be converted to a Buffer")));
  EXPECT_THAT(ToBuffer(CreateArray<int>({1, 2, 3})),
              IsOkAndHolds(ElementsAre(1, 2, 3)));
}

TEST(ArrayUtilTest, FirstAndLastPresentIds) {
  {  // const, all missing
    Array<Unit> array(5, std::nullopt);
    EXPECT_THAT(ArrayFirstPresentIds(array, 3), ElementsAre());
    EXPECT_THAT(ArrayLastPresentIds(array, 3), ElementsAre());
  }
  {  // const, all present
    Array<Unit> array(5, kUnit);
    EXPECT_THAT(ArrayFirstPresentIds(array, 3), ElementsAre(0, 1, 2));
    EXPECT_THAT(ArrayLastPresentIds(array, 3), ElementsAre(4, 3, 2));
    EXPECT_THAT(ArrayFirstPresentIds(array, 10), ElementsAre(0, 1, 2, 3, 4));
    EXPECT_THAT(ArrayLastPresentIds(array, 10), ElementsAre(4, 3, 2, 1, 0));
  }
  {  // full
    Array<Unit> array = CreateArray<Unit>({kUnit, kUnit, kUnit});
    EXPECT_THAT(ArrayFirstPresentIds(array, 2), ElementsAre(0, 1));
    EXPECT_THAT(ArrayLastPresentIds(array, 2), ElementsAre(2, 1));
  }
  {  // dense
    Array<Unit> array = CreateArray<Unit>({kUnit, std::nullopt, kUnit});
    EXPECT_THAT(ArrayFirstPresentIds(array, 2), ElementsAre(0, 2));
    EXPECT_THAT(ArrayLastPresentIds(array, 2), ElementsAre(2, 0));
  }
  {  // sparse
    Array<Unit> array =
        CreateArray<Unit>({kUnit, std::nullopt, kUnit, std::nullopt, kUnit})
            .ToSparseForm();
    EXPECT_THAT(ArrayFirstPresentIds(array, 2), ElementsAre(0, 2));
    EXPECT_THAT(ArrayLastPresentIds(array, 2), ElementsAre(4, 2));
  }
  {  // sparse with missing_id_value
    Array<Unit> array =
        CreateArray<Unit>({kUnit, std::nullopt, kUnit, std::nullopt, kUnit})
            .ToSparseForm(kUnit);
    EXPECT_THAT(ArrayFirstPresentIds(array, 2), ElementsAre(0, 2));
    EXPECT_THAT(ArrayLastPresentIds(array, 2), ElementsAre(4, 2));
  }
  {  // regression (do not crash for negative max_size)
    Array<Unit> array;
    EXPECT_THAT(ArrayFirstPresentIds(array, -1), ElementsAre());
    EXPECT_THAT(ArrayLastPresentIds(array, -1), ElementsAre());
  }
}

TEST(ArrayUtilTest, ArrayForEachInSubset) {
  using V = std::pair<int64_t, OptionalValue<float>>;
  std::vector<V> res;
  auto fn = [&res](int64_t id, bool present, float v) {
    res.push_back({id, {present, v}});
  };
  auto check = [&res](absl::Span<const V> expected) {
    ASSERT_EQ(res.size(), expected.size());
    for (int i = 0; i < res.size(); ++i) {
      EXPECT_EQ(res[i].first, expected[i].first);
      EXPECT_EQ(res[i].second, expected[i].second);
    }
    res.clear();
  };
  {  // const, all missing
    Array<float> array(5, std::nullopt);
    ArrayForEachInSubset<float, int>(array, {40, 41, 43}, 40, fn);
    check({{0, std::nullopt}, {1, std::nullopt}, {3, std::nullopt}});
  }
  {  // const, all present
    Array<float> array(5, 7.0);
    ArrayForEachInSubset<float, int>(array, {40, 41, 43}, 40, fn);
    check({{0, 7.0}, {1, 7.0}, {3, 7.0}});
  }
  {  // full
    Array<float> array = CreateArray<float>({7.0, 8.0, 7.5, 8.5, 9.0});
    ArrayForEachInSubset<float, int>(array, {40, 41, 43}, 40, fn);
    check({{0, 7.0}, {1, 8.0}, {3, 8.5}});
  }
  {  // dense
    Array<float> array =
        CreateArray<float>({7.0, std::nullopt, std::nullopt, 8.5, 9.0});
    ArrayForEachInSubset<float, int>(array, {40, 41, 43}, 40, fn);
    check({{0, 7.0}, {1, std::nullopt}, {3, 8.5}});
  }
  {  // sparse
    Array<float> array =
        CreateArray<float>({7.0, std::nullopt, std::nullopt, 8.5, 9.0})
            .ToSparseForm();
    ArrayForEachInSubset<float, int>(array, {40, 41, 43}, 40, fn);
    check({{0, 7.0}, {1, std::nullopt}, {3, 8.5}});
  }
  {  // sparse with missing_id_value
    Array<float> array =
        CreateArray<float>({7.0, std::nullopt, std::nullopt, 8.5, 9.0})
            .ToSparseForm(7.0);
    ArrayForEachInSubset<float, int>(array, {40, 41, 43}, 40, fn);
    check({{0, 7.0}, {1, std::nullopt}, {3, 8.5}});
  }
}

}  // namespace arolla::testing
