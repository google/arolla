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
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"  // IWYU pragma: keep
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/init_arolla.h"

namespace arolla::testing {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::ElementsAre;

class LogicOpsTest : public ::testing::Test {
  void SetUp() final { InitArolla(); }
};

TEST_F(LogicOpsTest, ArrayPresenceOrOp) {
  using QI = Array<int>;
  auto arr_empty = Array<int>(4, std::nullopt);
  auto arr_const = Array<int>(4, 7);
  auto arr_full = CreateArray<int>({1, 2, 3, 4});
  auto arr_dense = CreateArray<int>({2, 3, std::nullopt, 1});
  auto arr_sparse =
      CreateArray<int>({std::nullopt, 4, std::nullopt, 2}).ToSparseForm();
  EXPECT_THAT(InvokeOperator<QI>("core.presence_or", arr_empty, arr_sparse),
              IsOkAndHolds(ElementsAre(std::nullopt, 4, std::nullopt, 2)));
  EXPECT_THAT(InvokeOperator<QI>("core.presence_or", arr_const, arr_sparse),
              IsOkAndHolds(ElementsAre(7, 7, 7, 7)));
  EXPECT_THAT(InvokeOperator<QI>("core.presence_or", arr_full, arr_sparse),
              IsOkAndHolds(ElementsAre(1, 2, 3, 4)));
  EXPECT_THAT(InvokeOperator<QI>("core.presence_or", arr_dense, arr_empty),
              IsOkAndHolds(ElementsAre(2, 3, std::nullopt, 1)));
  EXPECT_THAT(InvokeOperator<QI>("core.presence_or", arr_sparse, arr_dense),
              IsOkAndHolds(ElementsAre(2, 4, std::nullopt, 2)));
  EXPECT_THAT(InvokeOperator<QI>("core.presence_or", arr_dense, arr_const),
              IsOkAndHolds(ElementsAre(2, 3, 7, 1)));
  EXPECT_THAT(InvokeOperator<QI>("core.presence_or", arr_sparse, arr_const),
              IsOkAndHolds(ElementsAre(7, 4, 7, 2)));
}

}  // namespace
}  // namespace arolla::testing
