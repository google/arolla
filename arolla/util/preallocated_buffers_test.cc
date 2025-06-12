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
#include "arolla/util/preallocated_buffers.h"

#include <cstddef>
#include <cstdint>

#include "gtest/gtest.h"
#include "absl/types/span.h"

namespace arolla {
namespace {

template <typename T>
class ZeroInitializedBufferTest : public ::testing::Test {
 public:
  typedef T value_type;
};
using Types = testing::Types<char, int, float, int64_t, double, uint64_t>;
TYPED_TEST_SUITE(ZeroInitializedBufferTest, Types);

TYPED_TEST(ZeroInitializedBufferTest, TestAccess) {
  using T = typename TestFixture::value_type;
  static_assert(alignof(T) <= kZeroInitializedBufferAlignment);
  size_t size = kZeroInitializedBufferSize / sizeof(T);
  absl::Span<const T> data(static_cast<const T*>(GetZeroInitializedBuffer()),
                           size);
  for (const T& v : data) {
    ASSERT_EQ(v, 0);
  }
}

}  // namespace
}  // namespace arolla
