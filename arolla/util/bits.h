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
#ifndef AROLLA_UTIL_BITS_H_
#define AROLLA_UTIL_BITS_H_

#include <bitset>
#include <climits>
#include <cstddef>

#include "absl/log/check.h"

namespace arolla {

// Counts leading zero-bits in n. Undefined behaviour for n=0.
template <typename T>
constexpr int CountLeadingZeros(T n);

template <>
constexpr int CountLeadingZeros(unsigned int n) {
  return __builtin_clz(n);
}

template <>
constexpr int CountLeadingZeros(unsigned long n) {  // NOLINT
  return __builtin_clzl(n);
}

template <>
constexpr int CountLeadingZeros(unsigned long long n) {  // NOLINT
  return __builtin_clzll(n);
}

// Returns index of the most significant set bit in n.
// Undefined behaviour for n=0.
template <typename T>
constexpr int BitScanReverse(T n) {
  return 8 * sizeof(n) - 1 - CountLeadingZeros<T>(n);
}

template <typename Word>
inline int FindLSBSetNonZero(Word n);

// Here we use int/long istead of uint32_t/uint64_t because __builtin_ctz* work
// exactly with unsigned int / unsigned long / unsigned long long.
template <>
inline int FindLSBSetNonZero(unsigned int n) {
  return __builtin_ctz(n);
}

template <>
inline int FindLSBSetNonZero(unsigned long n) {  // NOLINT
  return __builtin_ctzl(n);
}

template <>
inline int FindLSBSetNonZero(unsigned long long n) {  // NOLINT
  return __builtin_ctzll(n);
}

template <typename Word>
class Bits {
 public:
  static constexpr unsigned Log2(unsigned n, unsigned p = 0) {
    return (n <= 1) ? p : Log2(n / 2, p + 1);
  }
  static constexpr size_t kIntBits = CHAR_BIT * sizeof(Word);
  static constexpr int kLogIntBits = Log2(kIntBits, 0);

  static bool GetBit(const Word* map, size_t index) {
    return std::bitset<kIntBits>(map[index / kIntBits])[index & (kIntBits - 1)];
  }

  static void SetBit(Word* map, size_t index) {
    map[index / kIntBits] |= (Word{1} << (index & (kIntBits - 1)));
  }

  // Sets to '1' all of the bits in `bitmap` in range [start, end).
  static void SetBitsInRange(Word* bitmap, size_t start, size_t end) {
    DCHECK_LE(start, end);
    if (start == end) {
      return;
    }
    size_t start_word = start / kIntBits;
    size_t end_word = (end - 1) / kIntBits;  // Word containing the last bit.
    Word* p = bitmap + start_word;
    Word ones = ~Word{0};
    Word start_mask = ones << (start & (kIntBits - 1));
    Word end_mask = ones >> ((end_word + 1) * kIntBits - end);
    if (end_word == start_word) {
      *p = *p | (start_mask & end_mask);
    } else {
      *p = *p | start_mask;
      for (++p; p < bitmap + end_word; ++p) {
        *p = ones;
      }
      *p = *p | end_mask;
    }
  }

  static size_t CountOnes(Word word) {
    return std::bitset<kIntBits>(word).count();
  }

  static size_t GetOnesCountInRange(const Word* bitmap, size_t start,
                                    size_t end) {
    DCHECK_LE(start, end);
    if (start == end) {
      return 0;
    }
    size_t start_word = start / kIntBits;
    size_t end_word = (end - 1) / kIntBits;  // Word containing the last bit.
    const Word* p = bitmap + start_word;
    Word c = (*p & (~Word{0} << (start & (kIntBits - 1))));
    Word endmask = (~Word{0} >> ((end_word + 1) * kIntBits - end));
    if (end_word == start_word) {
      return CountOnes(c & endmask);
    }
    size_t sum = CountOnes(c);
    for (++p; p < bitmap + end_word; ++p) {
      sum += CountOnes(*p);
    }
    return sum + CountOnes(*p & endmask);
  }

  // Adapted from util/bitmap/bitmap.h
  static size_t FindNextSetBitInVector(const Word* words, size_t bit_index,
                                       size_t limit) {
    if (bit_index >= limit) return limit;
    // From now on limit != 0, since if it was we would have returned false.
    size_t int_index = bit_index >> kLogIntBits;
    Word one_word = words[int_index];

    // Simple optimization where we can immediately return true if the first
    // bit is set.  This helps for cases where many bits are set, and doesn't
    // hurt too much if not.
    const size_t first_bit_offset = bit_index & (kIntBits - 1);
    if (one_word & (Word{1} << first_bit_offset)) return bit_index;

    // First word is special - we need to mask off leading bits
    one_word &= (~Word{0} << first_bit_offset);

    // Loop through all but the last word.  Note that 'limit' is one
    // past the last bit we want to check, and we don't want to read
    // past the end of "words".  E.g. if size_ == kIntBits only words[0] is
    // valid, so we want to avoid reading words[1] when limit == kIntBits.
    const size_t last_int_index = (limit - 1) >> kLogIntBits;
    while (int_index < last_int_index) {
      if (one_word != Word{0}) {
        return (int_index << kLogIntBits) + FindLSBSetNonZero(one_word);
      }
      one_word = words[++int_index];
    }

    // Last word is special - we may need to mask off trailing bits.  Note
    // that 'limit' is one past the last bit we want to check, and if limit is
    // a multiple of kIntBits we want to check all bits in this word.
    one_word &= ~((~Word{0} - 1) << ((limit - 1) & (kIntBits - 1)));
    if (one_word != 0) {
      return (int_index << kLogIntBits) + FindLSBSetNonZero(one_word);
    }
    return limit;
  }
};

template <typename Word>
inline bool GetBit(const Word* map, size_t index) {
  return Bits<Word>::GetBit(map, index);
}

template <typename Word>
inline void SetBit(Word* map, size_t index) {
  Bits<Word>::SetBit(map, index);
}

template <typename Word>
inline void SetBitsInRange(Word* bitmap, size_t start, size_t end) {
  Bits<Word>::SetBitsInRange(bitmap, start, end);
}

template <typename Word>
inline size_t CountOnes(Word word) {
  return Bits<Word>::CountOnes(word);
}

template <typename Word>
inline size_t GetOnesCountInRange(const Word* bitmap, size_t start,
                                  size_t end) {
  return Bits<Word>::GetOnesCountInRange(bitmap, start, end);
}

template <typename Word>
inline size_t FindNextSetBitInVector(const Word* words, size_t bit_index,
                                     size_t bit_limit) {
  return Bits<Word>::FindNextSetBitInVector(words, bit_index, bit_limit);
}

}  // namespace arolla

#endif  // AROLLA_UTIL_BITS_H_
