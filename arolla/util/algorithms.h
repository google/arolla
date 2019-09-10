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
#ifndef AROLLA_UTIL_ALGORITHMS_H_
#define AROLLA_UTIL_ALGORITHMS_H_

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <limits>
#include <type_traits>

#include "absl/log/check.h"

namespace arolla {

// Returns an iterator pointing to the first element in the range [first, last)
// that is not less than (i.e. greater or equal to) value, or last if no such
// element is found.
//
// Function is a drop-in replacement for std::lower_bound. This implementation
// is tuned for faster location of values near the front of the sequence.
template <class FwdIter, class T, class Compare>
inline FwdIter exp_lower_bound(FwdIter first, FwdIter last, const T& val,
                               Compare comp) {
  typename std::iterator_traits<FwdIter>::difference_type count, step;
  count = std::distance(first, last);
  step = 1;
  FwdIter it = first;
  while (step < count) {
    if (comp(*it, val)) {
      // Continue exponential search on tail
      first = ++it;
      count -= step;
      std::advance(it, step);
      step *= 2;
    } else {
      // Perform binary search on head
      last = it;
      break;
    }
  }

  // After narrowing range with exponential search, finish with binary search.
  return std::lower_bound(first, last, val, comp);
}

template <class FwdIter, class T>
inline FwdIter exp_lower_bound(FwdIter first, FwdIter last, const T& val) {
  using T1 = typename std::iterator_traits<FwdIter>::value_type;
  return exp_lower_bound(first, last, val,
                         [](const T1& a, const T& b) { return a < b; });
}

template <typename Word>
inline void LogicalAnd(const Word* src1, const Word* src2, Word* result,
                       size_t word_count) {
  const size_t e = (word_count / 8) * 8;
  for (size_t i = 0; i < e; i += 8) {
    result[i] = src1[i] & src2[i];
    result[i + 1] = src1[i + 1] & src2[i + 1];
    result[i + 2] = src1[i + 2] & src2[i + 2];
    result[i + 3] = src1[i + 3] & src2[i + 3];
    result[i + 4] = src1[i + 4] & src2[i + 4];
    result[i + 5] = src1[i + 5] & src2[i + 5];
    result[i + 6] = src1[i + 6] & src2[i + 6];
    result[i + 7] = src1[i + 7] & src2[i + 7];
  }
  switch (word_count - e) {
    case 7:
      result[e + 6] = src1[e + 6] & src2[e + 6];
      [[fallthrough]];
    case 6:
      result[e + 5] = src1[e + 5] & src2[e + 5];
      [[fallthrough]];
    case 5:
      result[e + 4] = src1[e + 4] & src2[e + 4];
      [[fallthrough]];
    case 4:
      result[e + 3] = src1[e + 3] & src2[e + 3];
      [[fallthrough]];
    case 3:
      result[e + 2] = src1[e + 2] & src2[e + 2];
      [[fallthrough]];
    case 2:
      result[e + 1] = src1[e + 1] & src2[e + 1];
      [[fallthrough]];
    case 1:
      result[e] = src1[e] & src2[e];
  }
}

template <typename Word>
inline void InPlaceLogicalAnd(const Word* src, Word* dest, size_t word_count) {
  const size_t e = (word_count / 8) * 8;
  for (size_t i = 0; i < e; i += 8) {
    dest[i] &= src[i];
    dest[i + 1] &= src[i + 1];
    dest[i + 2] &= src[i + 2];
    dest[i + 3] &= src[i + 3];
    dest[i + 4] &= src[i + 4];
    dest[i + 5] &= src[i + 5];
    dest[i + 6] &= src[i + 6];
    dest[i + 7] &= src[i + 7];
  }
  switch (word_count - e) {
    case 7:
      dest[e + 6] &= src[e + 6];
      [[fallthrough]];
    case 6:
      dest[e + 5] &= src[e + 5];
      [[fallthrough]];
    case 5:
      dest[e + 4] &= src[e + 4];
      [[fallthrough]];
    case 4:
      dest[e + 3] &= src[e + 3];
      [[fallthrough]];
    case 3:
      dest[e + 2] &= src[e + 2];
      [[fallthrough]];
    case 2:
      dest[e + 1] &= src[e + 1];
      [[fallthrough]];
    case 1:
      dest[e] &= src[e];
  }
}

// Perform a bitwise "lhs[lhs_skip:lhs_skip+n] &= rhs[rhs_skip:rhs_skip+n],
// where "skips" and "n" are expressed in number of bits. lhs_skip and rhs_skip
// must be less than the number of bits per Word. Output is written
// in whole Word values, so padding bits in these words are not preserved.
template <typename Word>
void InplaceLogicalAndWithOffsets(
    size_t bitmaps_size,  // lengths of bitmaps (in bits) excluding skips
    const Word* rhs,      // rhs bitmap
    int rhs_skip,         // number of bits to skip in rhs
    Word* lhs,            // lhs bitmap
    int lhs_skip)         // number of bits to skip in lhs
{
  static_assert(std::is_unsigned<Word>::value);
  static constexpr int bits_per_word = std::numeric_limits<Word>::digits;
  DCHECK_LT(lhs_skip, bits_per_word);
  DCHECK_GE(lhs_skip, 0);
  DCHECK_LT(rhs_skip, bits_per_word);
  DCHECK_GE(rhs_skip, 0);
  size_t rhs_words =
      (bitmaps_size + rhs_skip + bits_per_word - 1) / bits_per_word;
  size_t lhs_words =
      (bitmaps_size + lhs_skip + bits_per_word - 1) / bits_per_word;
  if (lhs_skip == rhs_skip) {
    // use unrolled version...
    InPlaceLogicalAnd(rhs, lhs, rhs_words);
  } else if (lhs_skip < rhs_skip) {
    int a = rhs_skip - lhs_skip;
    int b = bits_per_word - a;
    size_t i = 0;
    for (; i < rhs_words - 1; ++i) {
      lhs[i] &= (rhs[i] >> a) | (rhs[i + 1] << b);
    }
    if (i < lhs_words) {
      lhs[i] &= (rhs[i] >> a);
    }
  } else {  // lhs_skip > rhs_skip
    int a = lhs_skip - rhs_skip;
    int b = bits_per_word - a;
    lhs[0] &= rhs[0] << a;
    size_t i;
    for (i = 1; i < rhs_words; ++i) {
      lhs[i] &= (rhs[i - 1] >> b) | (rhs[i] << a);
    }
    if (i < lhs_words) {
      lhs[i] &= rhs[i - 1] >> b;
    }
  }
}

// Perform a bitwise "lhs[lhs_skip:lhs_skip+n] = rhs[rhs_skip:rhs_skip+n],
// where "skips" and "n" are expressed in number of bits. lhs_skip and rhs_skip
// must be less than the number of bits per Word. Destination bits outside of
// this range are preserved.
template <typename Word>
void CopyBits(size_t bitmaps_size, const Word* rhs, int rhs_skip, Word* lhs,
              int lhs_skip) {
  static_assert(std::is_unsigned<Word>::value);
  static constexpr int bits_per_word = std::numeric_limits<Word>::digits;
  DCHECK_LT(lhs_skip, bits_per_word);
  DCHECK_GE(lhs_skip, 0);
  DCHECK_LT(rhs_skip, bits_per_word);
  DCHECK_GE(rhs_skip, 0);
  if (bitmaps_size == 0) {
    return;
  }
  size_t rhs_words =
      (bitmaps_size + rhs_skip + bits_per_word - 1) / bits_per_word;
  size_t lhs_words =
      (bitmaps_size + lhs_skip + bits_per_word - 1) / bits_per_word;

  // Number of unused bits at the tail of the last words.
  int lhs_tail = lhs_words * bits_per_word - (bitmaps_size + lhs_skip);
  DCHECK_LT(lhs_tail, bits_per_word);

  if (lhs_skip != 0) {
    Word rhs_val;
    if (lhs_skip == rhs_skip) {
      rhs_val = *rhs;
    } else if (lhs_skip < rhs_skip) {
      // May need to read from 2 words of input to fill first word of
      // output.
      int a = rhs_skip - lhs_skip;
      rhs_val = rhs[0] >> a;
      if (rhs_words > 1) {
        rhs_val |= rhs[1] << (bits_per_word - a);
      }
    } else {
      // If rhs skip is smaller, then first word of input contains bits
      // for first word of output.
      rhs_val = rhs[0] << (lhs_skip - rhs_skip);
    }

    if (lhs_words == 1) {
      // Output mask. 0's represent values to preserve in original.
      Word output_mask = (~Word{0} << lhs_skip) & (~Word{0} >> lhs_tail);
      *lhs = (*lhs & ~output_mask) | (rhs_val & output_mask);
      return;
    } else {
      Word output_mask = (~Word{0} << lhs_skip);
      *lhs = (*lhs & ~output_mask) | (rhs_val & output_mask);
      if (lhs_skip > rhs_skip) {
        rhs_skip += bits_per_word - lhs_skip;
      } else {
        ++rhs;
        --rhs_words;
        rhs_skip -= lhs_skip;
      }
      lhs_skip = 0;
      ++lhs;
      --lhs_words;
    }
  }

  DCHECK_EQ(lhs_skip, 0);

  // Copy all full words (no masking required) into lhs.
  size_t full_lhs_words = (lhs_tail == 0) ? lhs_words : lhs_words - 1;
  if (full_lhs_words > 0) {
    if (rhs_skip == 0) {
      for (size_t i = 0; i < full_lhs_words; ++i) {
        lhs[i] = rhs[i];
      }
    } else {
      size_t i = 0;
      for (; i < std::min(rhs_words - 1, full_lhs_words); ++i) {
        lhs[i] =
            (rhs[i] >> rhs_skip) | (rhs[i + 1] << (bits_per_word - rhs_skip));
      }
      if (i < full_lhs_words) {
        lhs[i] = (rhs[i] >> rhs_skip);
      }
    }
    lhs += full_lhs_words;
    lhs_words -= full_lhs_words;
    rhs += full_lhs_words;
    rhs_words -= full_lhs_words;
  }

  // Write final partial word if any.
  if (lhs_tail) {
    Word rhs_val = rhs[0] >> rhs_skip;
    if (rhs_words == 2) {
      rhs_val |= rhs[1] << (bits_per_word - rhs_skip);
    }
    Word output_mask = (~Word{0} >> lhs_tail);
    *lhs = (*lhs & ~output_mask) | (rhs_val & output_mask);
  }
}

template <typename Integer>
Integer RoundUp(Integer value, Integer divisor) {
  return (value + divisor - 1) / divisor * divisor;
}

}  // namespace arolla

#endif  // AROLLA_UTIL_ALGORITHMS_H_
