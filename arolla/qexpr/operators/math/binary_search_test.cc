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
#include "arolla/qexpr/operators/math/binary_search.h"

#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"

namespace arolla {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

TEST(BinarySearch, VerifyHaystackFullBitmap) {
  Buffer<float> values(CreateBuffer(std::vector<float>{1., 2., 3.}));
  Buffer<bitmap::Word> bitmask(CreateBuffer(std::vector<bitmap::Word>{7}));
  DenseArray<float> array{std::move(values), std::move(bitmask)};

  EXPECT_THAT(SearchSortedOp::VerifyHaystack(array), IsOk());
}

TEST(BinarySearch, VerifyHaystackEmptyBitmap) {
  Buffer<float> values(CreateBuffer(std::vector<float>{1., 2., 3.}));
  Buffer<bitmap::Word> bitmask(CreateBuffer(std::vector<bitmap::Word>{}));
  DenseArray<float> array{std::move(values), std::move(bitmask)};

  EXPECT_THAT(SearchSortedOp::VerifyHaystack(array), IsOk());
}

TEST(BinarySearch, VerifyHaystackRaisesNotFullBitmap) {
  Buffer<float> values(CreateBuffer(std::vector<float>{1., 2., 3.}));
  Buffer<bitmap::Word> bitmask(CreateBuffer(std::vector<bitmap::Word>{5}));
  DenseArray<float> array{std::move(values), std::move(bitmask)};

  EXPECT_THAT(
      SearchSortedOp::VerifyHaystack(array),
      StatusIs(absl::StatusCode::kUnimplemented,
               "math.searchsorted operator supports only full haystacks"));
}

}  // namespace
}  // namespace arolla
