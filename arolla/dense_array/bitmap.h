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
#ifndef AROLLA_DENSE_ARRAY_BITMAP_H_
#define AROLLA_DENSE_ARRAY_BITMAP_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>

#include "absl//base/attributes.h"
#include "absl//base/optimization.h"
#include "absl//log/check.h"
#include "absl//types/span.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/preallocated_buffers.h"

// bitmap::Bitmap is an alias to Buffer<uint32_t>. It is an immutable object
// that represents present/missing state of values in DenseArray. Each bit of
// the bitmap corresponds to some item. 1 means present, 0 means missing.
// Empty bitmap means that all values are present.
// This namespace contains utilities to work with Bitmap.
namespace arolla::bitmap {

using Word = uint32_t;
constexpr int kWordBitCount = sizeof(Word) * 8;
constexpr Word kFullWord = ~Word(0);

using Bitmap = Buffer<Word>;

inline int64_t BitmapSize(int64_t bit_count) {
  return (bit_count + kWordBitCount - 1) / kWordBitCount;
}

inline Word GetWord(const Bitmap& bitmap, int64_t index) {
  if (bitmap.size() <= index) {
    return kFullWord;
  } else {
    return bitmap[index];
  }
}

inline Word GetWordWithOffset(const Bitmap& bitmap, int64_t index, int offset) {
  DCHECK_LT(offset, kWordBitCount);
  if (bitmap.size() <= index) return kFullWord;
  Word mask = bitmap[index] >> offset;
  if (offset == 0 || index + 1 == bitmap.size()) {
    return mask;
  } else {
    return mask | (bitmap[index + 1] << (kWordBitCount - offset));
  }
}

// Checks if all `bitCount` bits in the bitmap are ones.
bool AreAllBitsSet(const Word* bitmap, int64_t bitCount);

// Checks if all `bitCount` bits in the bitmap are zeros.
inline bool AreAllBitsUnset(const Word* bitmap, int64_t bitCount) {
  for (size_t i = 0; i < static_cast<size_t>(bitCount) / kWordBitCount; ++i) {
    if (*(bitmap++) != 0) return false;
  }
  size_t suffixBitCount = static_cast<size_t>(bitCount) % kWordBitCount;
  return suffixBitCount == 0 ||
         ((*bitmap >> suffixBitCount) << suffixBitCount) == *bitmap;
}

ABSL_ATTRIBUTE_ALWAYS_INLINE
inline void Intersect(const Bitmap& a, const Bitmap& b,
                      absl::Span<Word> result) {
  DCHECK_EQ(a.size(), b.size());
  DCHECK_EQ(a.size(), result.size());
  Word* res = result.begin();
  const Word* ra = a.begin();
  const Word* rb = b.begin();
  for (int64_t i = 0; i < a.size(); ++i) {
    res[i] = ra[i] & rb[i];
  }
}

// Intersect two bitmaps.
// Resulted bitmap will have bit offset = std::min(bit_offset_a, bit_offset_b).
ABSL_ATTRIBUTE_ALWAYS_INLINE
inline void Intersect(const Bitmap& a, const Bitmap& b, int bit_offset_a,
                      int bit_offset_b, absl::Span<Word> result) {
  DCHECK_EQ(std::min(a.size(), b.size()), result.size());
  if (bit_offset_a == bit_offset_b) {
    Intersect(a, b, result);
  } else {
    Word* res = result.begin();
    const Word* first = a.begin();
    const Word* second = b.begin();
    int64_t size1 = a.size();
    int64_t size2 = b.size();
    if (bit_offset_a > bit_offset_b) {
      first = b.begin();
      second = a.begin();
      size1 = b.size();
      size2 = a.size();
    }
    const int offset = std::abs(bit_offset_b - bit_offset_a);
    for (int64_t i = 0; i < std::min(size1, size2 - 1); ++i) {
      Word second_shifted =
          (second[i] >> offset) | (second[i + 1] << (kWordBitCount - offset));
      res[i] = first[i] & second_shifted;
    }
    if (size2 > 0 && size2 - 1 < size1) {
      res[size2 - 1] = first[size2 - 1] & (second[size2 - 1] >> offset);
    }
  }
}

inline bool GetBit(Word word, int bit) { return word & (Word(1) << bit); }

inline bool GetBit(const Word* bitmap, int64_t bit_index) {
  Word word = bitmap[bit_index / kWordBitCount];
  int bit = bit_index & (kWordBitCount - 1);
  return GetBit(word, bit);
}

inline bool GetBit(const Bitmap& bitmap, int64_t bit_index) {
  DCHECK_GE(bit_index, 0);
  DCHECK(bitmap.empty() || bit_index / kWordBitCount < bitmap.size());
  if (bitmap.empty()) return true;
  Word word = bitmap[bit_index / kWordBitCount];
  int bit = bit_index & (kWordBitCount - 1);
  return GetBit(word, bit);
}

inline void SetBit(Word* bitmap, int64_t bit_index) {
  bitmap[static_cast<size_t>(bit_index) / kWordBitCount] |=
      Word{1} << (bit_index & (kWordBitCount - 1));
}

inline void UnsetBit(Word* bitmap, int64_t bit_index) {
  bitmap[static_cast<size_t>(bit_index) / kWordBitCount] &=
      ~(Word{1} << (bit_index & (kWordBitCount - 1)));
}

// Iterates over bits (from low to high) of bitmap word.
// It calls fn(int bit_id, bool value) for every of the first `count` bits.
template <class Fn>
void IterateWord(Word word, Fn&& fn, int count = kWordBitCount) {
  DCHECK_LE(count, kWordBitCount);
  for (int i = 0; i < count; ++i) {
    fn(i, GetBit(word, i));
  }
}

// Low-level function to iterate over given range of bits in a bitmap.
// For better performance iterations are split into groups of 32 elements.
// `init_group_fn(offset)` should initialize a group starting from `offset` and
// return a function `fn(i, bool v)` where `v` is a value of a bit and `i` can
// be in range [0, 31] (corresponds to indices from `offset` to `offset+31`).
// In some cases the ability to control group initialization allows to write
// more performance efficient code. It is recommended for `init_group_fn` to
// copy all small objects to the local variables (iterators, counters and etc)
// to make it clear for compiler that they are not shared across different
// groups.
template <class Fn>
void IterateByGroups(const Word* bitmap, int64_t first_bit, int64_t count,
                     Fn&& init_group_fn) {
  bitmap += static_cast<size_t>(first_bit) / kWordBitCount;
  int64_t bit_offset = first_bit & (kWordBitCount - 1);
  int64_t group_offset = 0;
  if (bit_offset > 0 && count > 0) {
    int first_word_size = std::min(count, kWordBitCount - bit_offset);
    bitmap::IterateWord(*(bitmap++) >> bit_offset, init_group_fn(group_offset),
                        first_word_size);
    group_offset = first_word_size;
  }
  for (; group_offset <= count - kWordBitCount; group_offset += kWordBitCount) {
    bitmap::IterateWord(*(bitmap++), init_group_fn(group_offset));
  }
  if (group_offset != count) {
    bitmap::IterateWord(*bitmap, init_group_fn(group_offset),
                        count - group_offset);
  }
}

// Iterates over given range of bits in Bitmap.
// fn(bool) will be called `count` times.
template <class Fn>
void Iterate(const Bitmap& bitmap, int64_t first_bit, int64_t count, Fn&& fn) {
  if (bitmap.empty()) {
    for (int64_t i = 0; i < count; ++i) fn(true);
  } else {
    DCHECK_GE(bitmap.size(), BitmapSize(first_bit + count));
    auto wrapped_fn = [&fn](int, bool present) { fn(present); };
    IterateByGroups(bitmap.begin(), first_bit, count,
                    [&](int64_t) { return wrapped_fn; });
  }
}

// Counts the set bits in range [offset, offset+size).
int64_t CountBits(const Bitmap& bitmap, int64_t offset, int64_t size);

// An alias for generic buffer builder.
// It works with words rather than with bits.
using RawBuilder = Buffer<Word>::Builder;

// Returns bitmap of the given size with all zero bits.
inline Bitmap CreateEmptyBitmap(
    int64_t bit_count, RawBufferFactory* buf_factory = GetHeapBufferFactory()) {
  if (bit_count <= kZeroInitializedBufferSize * 8) {
    return Buffer<Word>(
        nullptr, absl::Span<const Word>(
                     static_cast<const Word*>(GetZeroInitializedBuffer()),
                     BitmapSize(bit_count)));
  }
  int64_t bitmap_size = BitmapSize(bit_count);
  RawBuilder bldr(bitmap_size, buf_factory);
  std::memset(bldr.GetMutableSpan().data(), 0, bitmap_size * sizeof(Word));
  return std::move(bldr).Build();
}

// Builder for Bitmap. Optimized for almost full case. All bits are initialized
// to ones (present), so only zeros (missing) should be set. Missed ids can
// be added in any order. If there are no missing ids, then Build() returns an
// empty buffer.
class AlmostFullBuilder {
 public:
  explicit AlmostFullBuilder(
      int64_t bit_count, RawBufferFactory* buf_factory = GetHeapBufferFactory())
      : bit_count_(bit_count), factory_(buf_factory) {}

  void AddMissed(int64_t id) {
    DCHECK_GE(id, 0);
    DCHECK_LT(id, bit_count_);
    if (ABSL_PREDICT_FALSE(!bitmap_)) {
      CreateFullBitmap();
    }
    UnsetBit(bitmap_, id);
  }

  Bitmap Build() && { return std::move(bitmap_buffer_); }
  Bitmap Build(int64_t size) && {
    return std::move(bitmap_buffer_).Slice(0, BitmapSize(size));
  }

 private:
  // Creates bitmap filled with 1s.
  void CreateFullBitmap();

  int64_t bit_count_;
  RawBufferFactory* factory_;
  Word* bitmap_ = nullptr;
  Bitmap bitmap_buffer_;
};

// Wrapper around Buffer<uint32_t>::Builder that simplifies building a Bitmap.
class Builder {
 public:
  explicit Builder(int64_t bit_count,
                   RawBufferFactory* buf_factory = GetHeapBufferFactory())
      : bldr_(BitmapSize(bit_count), buf_factory) {}

  // Low-level function that adds `count` bit to the Bitmap.
  // For better performance processing split into groups of 32 elements (similar
  // to `IterateByGroups`). `init_group_fn(offset)` should initialize a group
  // starting from `offset` and return a generator function `fn(i) -> bool`
  // where `i` can be in range [0, 31] (corresponds to indices from `offset`
  // to `offset+31`). In some cases the ability to control group initialization
  // allows to write more performance efficient code. It is recommended for
  // `init_group_fn` to copy all small objects to the local variables
  // (iterators, counters and etc) to make it clear for compiler that they are
  // not shared across different groups.
  template <typename Fn>
  void AddByGroups(int64_t count, Fn&& init_group_fn) {
    DCHECK_LE(current_bit_ + count,
              bldr_.GetMutableSpan().size() * kWordBitCount);

    int bit_offset = current_bit_ & (kWordBitCount - 1);
    int64_t offset = 0;

    if (bit_offset == 0) {
      Word* data =
          bldr_.GetMutableSpan().begin() + (current_bit_ / kWordBitCount);
      for (; offset + kWordBitCount <= count; offset += kWordBitCount) {
        *(data++) = Group(kWordBitCount, init_group_fn(offset));
      }
      if (offset < count) {
        *data = Group(count - offset, init_group_fn(offset));
      }
    } else {
      absl::Span<Word> data = bldr_.GetMutableSpan();
      auto add_word_fn = [&](Word w) {
        size_t word_id = (current_bit_ + offset) / kWordBitCount;
        data[word_id] |= w << bit_offset;
        if (word_id + 1 < data.size()) {
          data[word_id + 1] = w >> (kWordBitCount - bit_offset);
        }
      };
      for (; offset + kWordBitCount <= count; offset += kWordBitCount) {
        add_word_fn(Group(kWordBitCount, init_group_fn(offset)));
      }
      if (offset < count) {
        add_word_fn(Group(count - offset, init_group_fn(offset)));
      }
    }
    current_bit_ += count;
  }

  // Adds `std::size(c)` elements to the Bitmap.
  // For each `value` in the container `fn(value) -> bool` is called and boolean
  // result is used to generate bits.
  // Callback may have side effects.
  template <typename Container, typename Fn>
  void AddForEach(const Container& c, Fn&& fn) {
    AddByGroups(std::size(c), [&](int64_t offset) {
      auto g = std::begin(c) + offset;
      return [&fn, g](int i) { return fn(g[i]); };
    });
  }
  template <typename Iterator, typename Fn>
  void AddForEach(Iterator from, Iterator to, Fn&& fn) {
    AddByGroups(to - from, [&](int64_t offset) {
      auto g = from + offset;
      return [&fn, g](int i) { return fn(g[i]); };
    });
  }

  Bitmap Build() && {
    if (all_present_) return Bitmap();
    return std::move(bldr_).Build();
  }

 private:
  template <typename Fn>
  Word Group(int count, Fn&& fn) {
    Word res = 0;
    for (int i = 0; i < count; ++i) {
      if (fn(i)) {
        res |= (1 << i);
      } else {
        all_present_ = false;
      }
    }
    return res;
  }

  Bitmap::Builder bldr_;
  uint64_t current_bit_ = 0;
  bool all_present_ = true;
};

}  // namespace arolla::bitmap

#endif  // AROLLA_DENSE_ARRAY_BITMAP_H_
