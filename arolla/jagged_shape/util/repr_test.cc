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
#include "arolla/jagged_shape/util/repr.h"

#include "gtest/gtest.h"

namespace arolla {
namespace {

TEST(ReprTest, CompactSplitPointsAsSizesRepr) {
  {
    // No elements.
    EXPECT_EQ(CompactSplitPointsAsSizesRepr({}, /*max_part_size=*/0), "[]");
    EXPECT_EQ(CompactSplitPointsAsSizesRepr({}, /*max_part_size=*/2), "[]");
  }
  {
    // Single element.
    EXPECT_EQ(CompactSplitPointsAsSizesRepr({0}, /*max_part_size=*/0), "[]");
    EXPECT_EQ(CompactSplitPointsAsSizesRepr({0}, /*max_part_size=*/2), "[]");
  }
  {
    // Uniform splits.
    EXPECT_EQ(
        CompactSplitPointsAsSizesRepr({0, 1, 2, 3, 4}, /*max_part_size=*/0),
        "1");
    EXPECT_EQ(CompactSplitPointsAsSizesRepr({0, 2, 4}, /*max_part_size=*/1),
              "2");
    EXPECT_EQ(CompactSplitPointsAsSizesRepr({0, 0, 0}, /*max_part_size=*/1),
              "0");
  }
  {
    // Non-uniform splits.
    EXPECT_EQ(
        CompactSplitPointsAsSizesRepr({0, 2, 3, 4, 5, 8}, /*max_part_size=*/0),
        "[...]");
    EXPECT_EQ(
        CompactSplitPointsAsSizesRepr({0, 2, 3, 4, 5, 8}, /*max_part_size=*/1),
        "[2, ..., 3]");
    EXPECT_EQ(
        CompactSplitPointsAsSizesRepr({0, 2, 3, 4, 5, 8}, /*max_part_size=*/2),
        "[2, 1, ..., 1, 3]");
    EXPECT_EQ(
        CompactSplitPointsAsSizesRepr({0, 2, 3, 4, 5, 8}, /*max_part_size=*/3),
        "[2, 1, 1, 1, 3]");
  }
}

}  // namespace
}  // namespace arolla
