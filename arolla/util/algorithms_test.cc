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
#include "arolla/util/algorithms.h"

#include <array>
#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::Eq;

TEST(Algorithms, ExponentialLowerBound) {
  std::vector<int> v{2, 4, 5, 6};
  auto i1 = exp_lower_bound(v.begin(), v.end(), 4);
  EXPECT_THAT(i1, Eq(v.begin() + 1));
}

TEST(Algorithms, InplaceLogicalAndWithOffsets) {
  const std::array<uint32_t, 3> a = {0xf0ff0000, 0xff0fffff, 0x0000fff0};
  int a_bit_offset = 16;
  const std::array<uint32_t, 2> b = {0x87654321, 0x0fedcba9};
  int b_bit_offset = 0;
  const std::array<uint32_t, 3> c = {0x43210000, 0xcba98765, 0x00000fed};
  int c_bit_offset = 16;

  // dest_bit_offset > src_bit_offset
  auto a_copy = a;
  InplaceLogicalAndWithOffsets(64, b.data(), b_bit_offset, a_copy.data(),
                               a_bit_offset);
  EXPECT_THAT(a_copy, ElementsAre(0x40210000, 0xcb098765, 0x00000fe0));

  // dest_bit_offset < src_bit_offset
  auto b_copy = b;
  InplaceLogicalAndWithOffsets(64, a.data(), a_bit_offset, b_copy.data(),
                               b_bit_offset);
  EXPECT_THAT(b_copy, ElementsAre(0x87654021, 0x0fe0cb09));

  // dest_bit_offset == src_bit_offset
  auto c_copy = c;
  InplaceLogicalAndWithOffsets(64, a.data(), a_bit_offset, c_copy.data(),
                               c_bit_offset);
  EXPECT_THAT(a_copy, ElementsAre(0x40210000, 0xcb098765, 0x00000fe0));
}

TEST(Algorithms, CopyBits) {
  // Src buffer, offset=16
  const std::array<uint32_t, 3> src = {0x3210dead, 0xba987654, 0xbeeffedc};

  // Pattern for destination buffer.
  const std::array<uint32_t, 3> empty = {0x5a5a5a5a, 0x5a5a5a5a, 0x5a5a5a5a};

  // Copy data with same offset in destination.
  auto dest1 = empty;
  CopyBits(64, src.data(), 16, dest1.data(), 16);
  EXPECT_THAT(dest1, ElementsAre(0x32105a5a, 0xba987654, 0x5a5afedc));

  // Copy data into destination with lower offset
  auto dest2 = empty;
  CopyBits(64, src.data(), 16, dest2.data(), 8);
  EXPECT_THAT(dest2, ElementsAre(0x5432105a, 0xdcba9876, 0x5a5a5afe));

  // Copy data into destination with higher offset
  auto dest3 = empty;
  CopyBits(64, src.data(), 16, dest3.data(), 24);
  EXPECT_THAT(dest3, ElementsAre(0x105a5a5a, 0x98765432, 0x5afedcba));

  // Copy small range into a single word
  uint32_t dest4 = 0xffffffff;
  CopyBits(16, src.data(), 16, &dest4, 8);
  EXPECT_THAT(dest4, Eq(0xff3210ff));

  // Copy small range which crosses output word boundary
  uint32_t src5 = 0xdcba;
  std::array<uint32_t, 2> dest5 = {0xffffffff, 0xffffffff};
  CopyBits(16, &src5, 0, dest5.data(), 24);
  EXPECT_THAT(dest5, ElementsAre(0xbaffffff, 0xffffffdc));
}

}  // namespace
}  // namespace arolla
