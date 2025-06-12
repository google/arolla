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
#include "arolla/util/bits.h"

#include <array>
#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace arolla {
namespace {

using ::testing::Eq;

TEST(Bits, CountLeadingZeros_UInt32) {
  EXPECT_EQ(31, CountLeadingZeros(static_cast<uint32_t>(1)));
  EXPECT_EQ(15, CountLeadingZeros(static_cast<uint32_t>(1) << 16));
  EXPECT_EQ(0, CountLeadingZeros(static_cast<uint32_t>(1) << 31));
}

TEST(Bits, CountLeadingZeros_UInt64) {
  EXPECT_EQ(63, CountLeadingZeros(static_cast<uint64_t>(1)));
  EXPECT_EQ(31, CountLeadingZeros(static_cast<uint64_t>(1) << 32));
  EXPECT_EQ(0, CountLeadingZeros(static_cast<uint64_t>(1) << 63));
}

TEST(Bits, BitScanReverse) {
  EXPECT_EQ(BitScanReverse(1U), 0);
  EXPECT_EQ(BitScanReverse(2U), 1);
  EXPECT_EQ(BitScanReverse(3141U), 11);
}

TEST(Bits, FindLSBSetNonZero) {
  EXPECT_THAT(FindLSBSetNonZero<uint32_t>(0x80000000), Eq(31));
  EXPECT_THAT(FindLSBSetNonZero<uint32_t>(0x80000001), Eq(0));
}

TEST(Bits, GetBit) {
  std::array<uint32_t, 3> bitmap = {0x00000001, 0x0000ffff, 0x55555555};
  EXPECT_TRUE(GetBit(bitmap.data(), 0));
  EXPECT_TRUE(GetBit(bitmap.data(), 32));
  EXPECT_TRUE(GetBit(bitmap.data(), 64));
  EXPECT_FALSE(GetBit(bitmap.data(), 31));
  EXPECT_FALSE(GetBit(bitmap.data(), 63));
  EXPECT_FALSE(GetBit(bitmap.data(), 95));
}

TEST(Bits, SetBit) {
  std::array<uint32_t, 3> bitmap = {0x00000001, 0x0000ffff, 0x55555555};
  SetBit(bitmap.data(), 31);
  EXPECT_THAT(bitmap[0], Eq(0x80000001));
  SetBit(bitmap.data(), 63);
  EXPECT_THAT(bitmap[1], Eq(0x8000ffff));
  SetBit(bitmap.data(), 95);
  EXPECT_THAT(bitmap[2], Eq(0xd5555555));
}

TEST(Bits, SetBitsInRange) {
  std::array<uint32_t, 5> bitmap = {0x00000000, 0x00000000, 0x00000000,
                                    0x00000000, 0x00000000};
  // Sets first bit in a word.
  SetBitsInRange(bitmap.data(), 0, 1);

  // Sets 8 bits inside a word.
  SetBitsInRange(bitmap.data(), 8, 16);

  // Sets last bit in a word.
  SetBitsInRange(bitmap.data(), 31, 32);

  // Empty range is no-op.
  SetBitsInRange(bitmap.data(), 32, 32);

  // Sets 32 bits crossing word boundary.
  SetBitsInRange(bitmap.data(), 48, 80);

  // Sets 32 bits on word boundary.
  SetBitsInRange(bitmap.data(), 96, 128);

  // Verify results.
  EXPECT_EQ(bitmap[0], 0x8000ff01);
  EXPECT_EQ(bitmap[1], 0xffff0000);
  EXPECT_EQ(bitmap[2], 0x0000ffff);
  EXPECT_EQ(bitmap[3], 0xffffffff);
  EXPECT_EQ(bitmap[4], 0x00000000);
}

TEST(Bits, CountOnesInRange) {
  std::array<uint32_t, 4> bitmap = {0x55555555, 0x55555555, 0x55555555,
                                    0x55555555};
  EXPECT_THAT(GetOnesCountInRange(bitmap.data(), 0, 128), Eq(64));
  EXPECT_THAT(GetOnesCountInRange(bitmap.data(), 40, 80), Eq(20));
}

TEST(Bits, FindNextSetBitInVector) {
  std::array<uint32_t, 3> bitmap = {
      0x00000000,   // bits  0-31
      0x00ff00ff,   // bits 32-63
      0x55550001};  // bits 64-80 (high order bits ignored)
  EXPECT_THAT(FindNextSetBitInVector(bitmap.data(), 0, 80), Eq(32));
  EXPECT_THAT(FindNextSetBitInVector(bitmap.data(), 32, 80), Eq(32));
  EXPECT_THAT(FindNextSetBitInVector(bitmap.data(), 40, 80), Eq(48));
  EXPECT_THAT(FindNextSetBitInVector(bitmap.data(), 56, 80), Eq(64));
  EXPECT_THAT(FindNextSetBitInVector(bitmap.data(), 65, 80), Eq(80));
}

}  // namespace
}  // namespace arolla
