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
#include "arolla/dense_array/bitmap.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <utility>

#include "absl/log/check.h"
#include "arolla/util/bits.h"

namespace arolla::bitmap {

bool AreAllBitsSet(const Word* bitmap, int64_t bitCount) {
  while (bitCount >= kWordBitCount) {
    if (*bitmap != kFullWord) return false;
    bitmap++;
    bitCount -= kWordBitCount;
  }
  if (bitCount > 0) {
    auto mask = kFullWord >> (kWordBitCount - bitCount);
    return (*bitmap & mask) == mask;
  }
  return true;
}

int64_t CountBits(const Bitmap& bitmap, int64_t offset, int64_t size) {
  DCHECK_GE(size, 0);
  const int64_t begin = std::max<int64_t>(
      0, std::min<int64_t>(bitmap.size() * kWordBitCount, offset));
  const int64_t end = std::max<int64_t>(
      begin, std::min<int64_t>(bitmap.size() * kWordBitCount, offset + size));
  return size - (end - begin) +
         GetOnesCountInRange(bitmap.span().data(), begin, end);
}

void AlmostFullBuilder::CreateFullBitmap() {
  Bitmap::Builder bldr(BitmapSize(bit_count_), factory_);
  auto span = bldr.GetMutableSpan();
  bitmap_ = span.begin();
  std::memset(bitmap_, 0xff, span.size() * sizeof(Word));
  int64_t last_bits = bit_count_ & (kWordBitCount - 1);
  if (last_bits != 0) {
    // clear all unused bits
    span.back() &= ((Word{1} << last_bits) - 1);
  }
  bitmap_buffer_ = std::move(bldr).Build();
}

}  // namespace arolla::bitmap
