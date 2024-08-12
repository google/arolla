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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;

class FactoryOpsTest : public ::testing::Test {
 protected:
  void SetUp() final { InitArolla(); }
};

TEST_F(FactoryOpsTest, DenseArrayShapeOfOp) {
  EXPECT_THAT(InvokeOperator<DenseArrayShape>("core._array_shape_of",
                                              DenseArray<Unit>{VoidBuffer(3)}),
              IsOkAndHolds(DenseArrayShape{3}));
}

TEST_F(FactoryOpsTest, DenseArrayConstWithShapeOp) {
  ASSERT_OK_AND_ASSIGN(auto res, InvokeOperator<DenseArray<int>>(
                                     "core.const_with_shape._array_shape",
                                     DenseArrayShape{3}, 57));
  EXPECT_THAT(res, ElementsAre(57, 57, 57));
}

TEST_F(FactoryOpsTest, ArrayShapeSize_DenseArray) {
  EXPECT_THAT(
      InvokeOperator<int64_t>("array.array_shape_size", DenseArrayShape{3}),
      IsOkAndHolds(Eq(3)));
}

TEST_F(FactoryOpsTest, ResizeArrayShape_DenseArray) {
  EXPECT_THAT(InvokeOperator<DenseArrayShape>("array.resize_array_shape",
                                              DenseArrayShape{3}, int64_t{5}),
              IsOkAndHolds(DenseArrayShape{5}));

  EXPECT_THAT(InvokeOperator<DenseArrayShape>("array.resize_array_shape",
                                              DenseArrayShape{3}, int64_t{-1}),
              StatusIs(absl::StatusCode::kInvalidArgument, "bad size: -1"));
}

}  // namespace
}  // namespace arolla
