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
#include "arolla/qexpr/operators/math/batch_arithmetic.h"

#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//types/span.h"

namespace arolla {
namespace {

TEST(BatchArithmetic, BatchAdd) {
  std::vector<float> arg1{1., 3., 2.};
  std::vector<float> arg2{3.5, 1.5, 2.};
  std::vector<float> res(3);
  BatchAdd<float>()(absl::Span<float>(res), arg1, arg2);
  EXPECT_THAT(res, testing::ElementsAre(4.5, 4.5, 4.0));
}

TEST(BatchArithmetic, BatchSub) {
  std::vector<int64_t> arg1{1, 3, 2};
  std::vector<int64_t> arg2{3, 1, 2};
  std::vector<int64_t> res(3);
  BatchSub<int64_t>()(absl::Span<int64_t>(res), arg1, arg2);
  EXPECT_THAT(res, testing::ElementsAre(-2, 2, 0));
}

TEST(BatchArithmetic, BatchProd) {
  std::vector<double> arg1{1., 3., 2.};
  std::vector<double> arg2{3.5, 1.5, 2.};
  std::vector<double> res(3);
  BatchProd<double>()(absl::Span<double>(res), arg1, arg2);
  EXPECT_THAT(res, testing::ElementsAre(3.5, 4.5, 4.0));
}

TEST(BatchArithmetic, AggSum) {
  std::vector<float> arg{1., 3., 2.};
  EXPECT_EQ(BatchAggSum<float>(arg), 6.);
}

}  // namespace
}  // namespace arolla
