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
#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;

TEST(FactoryOpsTest, ArrayShapeOfOp) {
  EXPECT_THAT(
      InvokeOperator<ArrayShape>("core._array_shape_of", Array<Unit>(3)),
      IsOkAndHolds(ArrayShape{3}));
}

TEST(FactoryOpsTest, ArrayConstWithShapeOp) {
  ASSERT_OK_AND_ASSIGN(
      auto res, InvokeOperator<Array<int>>("core.const_with_shape._array_shape",
                                           ArrayShape{3}, 57));
  EXPECT_THAT(res, ElementsAre(57, 57, 57));
}

TEST(FactoryOpsTest, ArrayShapeSize_Array) {
  EXPECT_THAT(InvokeOperator<int64_t>("array.array_shape_size", ArrayShape{3}),
              IsOkAndHolds(Eq(3)));
}

TEST(FactoryOpsTest, ResizeArrayShape_Array) {
  EXPECT_THAT(InvokeOperator<ArrayShape>("array.resize_array_shape",
                                         ArrayShape{3}, int64_t{5}),
              IsOkAndHolds(ArrayShape{5}));

  EXPECT_THAT(InvokeOperator<ArrayShape>("array.resize_array_shape",
                                         ArrayShape{3}, int64_t{-1}),
              StatusIs(absl::StatusCode::kInvalidArgument, "bad size: -1"));
}

TEST(FactoryOpsTest, AsDenseArray) {
  auto qblock = CreateArray<int>({1, 2, 3, std::nullopt, 5}).Slice(1, 4);
  ASSERT_TRUE(qblock.IsDenseForm());
  ASSERT_GT(qblock.dense_data().bitmap_bit_offset, 0);
  ASSERT_OK_AND_ASSIGN(
      DenseArray<int> res,
      InvokeOperator<DenseArray<int>>("array._as_dense_array", qblock));
  EXPECT_EQ(res.bitmap_bit_offset, 0);
  EXPECT_THAT(res, ElementsAre(2, 3, std::nullopt, 5));
}

TEST(FactoryOpsTest, AsArray) {
  auto dense_array = CreateDenseArray<int>({1, 2, 3, std::nullopt, 5});
  ASSERT_OK_AND_ASSIGN(Array<int> res, InvokeOperator<Array<int>>(
                                           "array._as_array", dense_array));
  EXPECT_THAT(res, ElementsAre(1, 2, 3, std::nullopt, 5));
}

}  // namespace
}  // namespace arolla
