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
#ifndef AROLLA_DENSE_ARRAY_DENSE_ARRAY_H_
#define AROLLA_DENSE_ARRAY_DENSE_ARRAY_H_

#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/api.h"
#include "arolla/util/bits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/iterator.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"

namespace arolla {

// Array that supports missing values. Analogue of std::vector<OptionalValue<T>>
// It is implemented on top of Buffer<T>, so values are immutable.
// DenseArray consists of `values`, `bitmap` and `bitmap_bit_offset`.
// Both `values` and `bitmap` can be empty.
// DenseArray with empty `values` considered empty.
// DenseArray with empty `bitmap` considered all present.
// bitmap_bit_offset should always be in range [0, 31).
// `bitmap` must be either empty or have at least
//       (size()+bitmap_bit_offset+31)/32 elements.
// The first value in `values` corresponds with the LSB starting at
// `bitmap_bit_offset`.
template <typename T>
struct AROLLA_API DenseArray {
  using base_type = T;
  using value_type = OptionalValue<view_type_t<T>>;
  using size_type = int64_t;
  using const_iterator = ConstArrayIterator<DenseArray<T>>;
  using difference_type = int64_t;
  using offset_type = int64_t;

  Buffer<T> values;
  Buffer<bitmap::Word> bitmap;  // Presence bitmap. Empty means all present.
  int bitmap_bit_offset = 0;  // Offset of the first element bit in the bitmap.

  int64_t size() const { return values.size(); }
  bool empty() const { return values.empty(); }

  bool IsFull() const { return PresentCount() == size(); }

  int64_t PresentCount() const {
    return bitmap::CountBits(bitmap, bitmap_bit_offset, size());
  }

  bool IsAllMissing() const { return PresentCount() == 0; }
  bool IsAllPresent() const {
    return bitmap.empty() || PresentCount() == size();
  }

  // Tests whether the value corresponding to the given offset is present.
  bool present(int64_t offset) const {
    DCHECK_GE(offset, 0);
    DCHECK_LT(offset, size());
    DCHECK(CheckBitmapMatchesValues());
    return bitmap::GetBit(bitmap, offset + bitmap_bit_offset);
  }

  // Returns value by offset.
  const OptionalValue<view_type_t<T>> operator[](  // NOLINT
      int64_t offset) const {
    if (present(offset)) {
      return values[offset];
    } else {
      return std::nullopt;
    }
  }

  bool is_owned() const { return values.is_owner() && bitmap.is_owner(); }

  // It is cheap if underlaying buffers are already owned and requires full copy
  // otherwise.
  DenseArray MakeOwned(
      RawBufferFactory* buf_factory = GetHeapBufferFactory()) const {
    return {values.DeepCopy(buf_factory), bitmap.DeepCopy(buf_factory),
            bitmap_bit_offset};
  }

  // Unowned DenseArray is a bit cheaper to copy because internal shared
  // pointers are set to nullptr.
  DenseArray MakeUnowned() const {
    return {values.ShallowCopy(), bitmap.ShallowCopy(), bitmap_bit_offset};
  }

  DenseArray Slice(int64_t start_id, int64_t row_count) const {
    DCHECK_GE(start_id, 0);
    DCHECK_GE(row_count, 0);
    DCHECK_LE(start_id + row_count, size());
    DenseArray res;
    res.values = values.Slice(start_id, row_count);
    if (!bitmap.empty()) {
      res.bitmap_bit_offset =
          (start_id + bitmap_bit_offset) & (bitmap::kWordBitCount - 1);
      int64_t b_start = (start_id + bitmap_bit_offset) / bitmap::kWordBitCount;
      int64_t b_size = bitmap::BitmapSize(res.bitmap_bit_offset + row_count);
      res.bitmap = bitmap.Slice(b_start, b_size);
    }
    return res;
  }

  // Returns an equivalent DenseArray with bitmap_bit_offset = 0.
  DenseArray ForceNoBitmapBitOffset(
      RawBufferFactory* factory = GetHeapBufferFactory()) && {
    if (bitmap_bit_offset > 0) {
      int64_t bitmap_size = bitmap::BitmapSize(size());
      bitmap::Bitmap::Builder bldr(bitmap_size, factory);
      for (int64_t i = 0; i < bitmap_size; ++i) {
        bldr.Set(i, bitmap::GetWordWithOffset(bitmap, i, bitmap_bit_offset));
      }
      bitmap = std::move(bldr).Build();
      bitmap_bit_offset = 0;
    }
    return std::move(*this);
  }

  DenseArray ForceNoBitmapBitOffset(
      RawBufferFactory* factory = GetHeapBufferFactory()) const& {
    DenseArray copy = *this;
    return std::move(copy).ForceNoBitmapBitOffset(factory);
  }

  DenseArray<Unit> ToMask() const {
    return {VoidBuffer(size()), bitmap, bitmap_bit_offset};
  }

  // Iterates through all elements (including missing) in order. Callback `fn`
  // should have 3 arguments: int64_t id, bool presence, view_type_t<T> value.
  template <typename Fn>
  void ForEach(Fn&& fn) const {
    DCHECK(CheckBitmapMatchesValues());
    if (bitmap.empty()) {
      auto iter = values.begin();
      for (int64_t id = 0; id < size(); ++id) fn(id, true, *(iter++));
    } else {
      bitmap::IterateByGroups(
          bitmap.begin(), bitmap_bit_offset, size(), [&](int64_t offset) {
            auto values_group = values.begin() + offset;
            return [&fn, values_group, offset](int i, bool present) {
              fn(offset + i, present, values_group[i]);
            };
          });
    }
  }

  // Iterates through all present elements in order. Callback `fn` should
  // have 2 arguments: int64_t id, view_type_t<T> value.
  template <typename Fn>
  void ForEachPresent(Fn&& fn) const {
    ForEach([&](int64_t id, bool presence, view_type_t<T> value) {
      if (presence) {
        fn(id, value);
      }
    });
  }

  // Low-level version of `ForEach`. Iterations are split into groups of 32
  // elements. `init_group_fn(offset)` should initialize a group with given
  // offset and return a processing function `fn(id, presence, value)`. Here
  // `id` is an index in the DenseArray, not in the group.
  // In some cases the ability to control group initialization allows to write
  // more performance efficient code. It is recommended for `init_group_fn` to
  // copy all small objects to the local variables (iterators, counters and etc)
  // to make it clear for compiler that they are not shared across different
  // groups.
  template <typename Fn>
  void ForEachByGroups(Fn init_group_fn) const {
    DCHECK(CheckBitmapMatchesValues());
    if (bitmap.empty()) {
      auto fn = init_group_fn(0);
      auto iter = values.begin();
      for (int64_t id = 0; id < size(); ++id) fn(id, true, *(iter++));
    } else {
      bitmap::IterateByGroups(bitmap.begin(), bitmap_bit_offset, size(),
                              [&](int64_t offset) {
                                auto values_group = values.begin() + offset;
                                auto fn = init_group_fn(offset);
                                return [=](int i, bool present) mutable {
                                  fn(offset + i, present, values_group[i]);
                                };
                              });
    }
  }

  const_iterator begin() const { return const_iterator{this, 0}; }
  const_iterator end() const { return const_iterator{this, size()}; }

  // Intended for tests and DCHECKs. For valid DenseArray should always be true.
  bool CheckBitmapMatchesValues() const {
    return bitmap.empty() ||
           bitmap.size() ==
               bitmap::BitmapSize(values.size() + bitmap_bit_offset);
  }
};

// Returns true iff lhs and rhs represent the same data.
template <typename T>
bool ArraysAreEquivalent(const DenseArray<T>& lhs, const DenseArray<T>& rhs) {
  if (lhs.bitmap.empty() && rhs.bitmap.empty()) {
    return lhs.values == rhs.values;
  }
  if (lhs.size() != rhs.size()) return false;
  for (int64_t i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) return false;
  }
  return true;
}

// This helper allows to get DenseArray type from optional types and references.
// For example AsDenseArray<OptionalValue<int>&> is just DenseArray<int>.
template <class T>
using AsDenseArray = DenseArray<strip_optional_t<std::decay_t<T>>>;

struct AROLLA_API DenseArrayShape {
  int64_t size;

  bool operator==(const DenseArrayShape& other) const {
    return size == other.size;
  }
};

// Random access builder for DenseArray. In general case it is not the fastest
// way to create DenseArray. For better performance consider constructing bitmap
// directly (see bitmap::Builder, bitmap::AlmostFullBuilder).
template <typename T>
class DenseArrayBuilder {
 public:
  using base_type = T;

  explicit DenseArrayBuilder(int64_t max_size,
                             RawBufferFactory* factory = GetHeapBufferFactory())
      : values_bldr_(max_size, factory),
        bitmap_bldr_(bitmap::BitmapSize(max_size), factory) {
    bitmap_ = bitmap_bldr_.GetMutableSpan().begin();
    std::memset(bitmap_, 0,
                bitmap_bldr_.GetMutableSpan().size() * sizeof(bitmap::Word));
  }

  // Sets a value with given index. All values that are not set are missing.
  // Given value can be optional. If the given value is missing, then `Set` has
  // no effect.
  template <typename ValueT>
  void Set(int64_t id, const ValueT& v) {
    if constexpr (std::is_same_v<ValueT, std::nullopt_t>) {
      return;
    } else if constexpr (is_optional_v<ValueT>) {
      if (!v.present) return;
      values_bldr_.Set(id, v.value);
    } else if constexpr (meta::is_wrapped_with_v<std::optional, ValueT>) {
      if (!v) return;
      if constexpr (!std::is_same_v<T, Unit>) {
        values_bldr_.Set(id, *v);
      }
    } else {
      values_bldr_.Set(id, v);
    }
    SetBit(bitmap_, id);
  }

  template <typename ValueT>
  void SetNConst(int64_t id, int64_t count, const ValueT& v) {
    if constexpr (std::is_same_v<ValueT, std::nullopt_t>) {
      return;
    } else if constexpr (is_optional_v<ValueT>) {
      if (!v.present) return;
      values_bldr_.SetNConst(id, count, v.value);
    } else if constexpr (meta::is_wrapped_with_v<std::optional, ValueT>) {
      if (!v) return;
      if constexpr (!std::is_same_v<T, Unit>) {
        values_bldr_.SetNConst(id, count, *v);
      }
    } else {
      values_bldr_.SetNConst(id, count, v);
    }
    SetBitsInRange(bitmap_, id, id + count);
  }

  // The same as `Set`, but ids must be added in ascending order.
  // Needed for compatibility with not-random-access builders.
  template <typename ValueT>
  void Add(int64_t id, const ValueT& v) {
    Set(id, v);
  }

  DenseArray<T> Build() && {
    return {std::move(values_bldr_).Build(), std::move(bitmap_bldr_).Build()};
  }

  // Shrinks internal buffers and builds a DenseArray of given `size`.
  // The argument can not be greater than the size of the builder.
  DenseArray<T> Build(int64_t size) && {
    return {std::move(values_bldr_).Build(size),
            std::move(bitmap_bldr_).Build(bitmap::BitmapSize(size))};
  }

 private:
  typename Buffer<T>::Builder values_bldr_;
  bitmap::Bitmap::Builder bitmap_bldr_;
  bitmap::Word* bitmap_;
};

template <typename T>
DenseArray<T> CreateDenseArray(
    absl::Span<const OptionalValue<T>> data,
    RawBufferFactory* factory = GetHeapBufferFactory()) {
  typename Buffer<T>::Builder values_builder(data.size(), factory);
  auto values_inserter = values_builder.GetInserter();
  bitmap::Builder bitmap_builder(data.size(), factory);
  bitmap_builder.AddForEach(data, [&](OptionalValue<T> v) {
    values_inserter.Add(v.value);
    return v.present;
  });
  return {std::move(values_builder).Build(), std::move(bitmap_builder).Build()};
}

// Build DenseArray<DestType> from a range of iterators over container of
// std::optional<SourceType> efficiently, and converting each element from
// SourceType to view_type_t<DestType>.
template <typename DestType, class InputIt>
DenseArray<DestType> CreateDenseArray(
    InputIt start, InputIt end,
    RawBufferFactory* factory = GetHeapBufferFactory()) {
  static_assert(
      std::is_same_v<typename InputIt::value_type,
                     std::optional<typename InputIt::value_type::value_type>>,
      "InputIt should be iterator to container of std::optional of some "
      "type.");
  static_assert(
      std::is_same_v<DestType, Unit> ||
          std::is_constructible_v<DestType,
                                  typename InputIt::value_type::value_type>,
      "Source type is not convertible to destination type.");
  auto size = std::distance(start, end);
  bitmap::Builder bitmap_builder(size, factory);
  typename Buffer<DestType>::Builder values_builder(size, factory);
  bitmap_builder.AddForEach(
      start, end,
      [inserter = values_builder.GetInserter()](const auto& v) mutable {
        if (v.has_value()) {
          if constexpr (std::is_same_v<DestType, Unit>) {
            inserter.Add(kUnit);
          } else {
            inserter.Add(static_cast<view_type_t<DestType>>(*v));
          }
        } else {
          inserter.SkipN(1);
        }
        return v.has_value();
      });
  return DenseArray<DestType>{std::move(values_builder).Build(),
                              std::move(bitmap_builder).Build()};
}

template <typename T, typename InputIt>
DenseArray<T> CreateFullDenseArray(
    InputIt start, InputIt end,
    RawBufferFactory* factory = GetHeapBufferFactory()) {
  return {Buffer<T>::Create(start, end, factory), Buffer<bitmap::Word>()};
}

template <typename T>
DenseArray<T> CreateFullDenseArray(
    std::initializer_list<T> data,
    RawBufferFactory* factory = GetHeapBufferFactory()) {
  return CreateFullDenseArray<T>(data.begin(), data.end(), factory);
}

template <typename T>
DenseArray<T> CreateFullDenseArray(
    const std::vector<T>& data,
    RawBufferFactory* factory = GetHeapBufferFactory()) {
  return CreateFullDenseArray<T>(data.begin(), data.end(), factory);
}

template <typename T>
DenseArray<T> CreateFullDenseArray(std::vector<T>&& data,
                                   RawBufferFactory* = GetHeapBufferFactory()) {
  return {Buffer<T>::Create(std::move(data)), Buffer<bitmap::Word>()};
}

template <typename T>
DenseArray<T> CreateConstDenseArray(
    int64_t size, view_type_t<T> value,
    RawBufferFactory* buf_factory = GetHeapBufferFactory()) {
  typename Buffer<T>::Builder values_builder(size, buf_factory);
  values_builder.SetNConst(0, size, value);
  return DenseArray<T>{std::move(values_builder).Build()};
}

template <typename T>
DenseArray<T> CreateEmptyDenseArray(
    int64_t size, RawBufferFactory* buf_factory = GetHeapBufferFactory()) {
  return {Buffer<T>::CreateUninitialized(size, buf_factory),
          bitmap::CreateEmptyBitmap(size, buf_factory)};
}

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(DenseArrayShape);

template <typename T>
struct FingerprintHasherTraits<DenseArray<T>> {
  void operator()(FingerprintHasher* hasher, const DenseArray<T>& arg) const {
    hasher->Combine(arg.size());
    for (int64_t i = 0; i < arg.size(); ++i) {
      hasher->Combine(arg[i]);
    }
  }
};

// Types that can be unowned should overload ArenaTraits. Should be used
// in ModelExecutor to make the result owned even if was created using
// UnsafeArenaBufferFactory.
template <typename T>
struct ArenaTraits<DenseArray<T>> {
  static DenseArray<T> MakeOwned(DenseArray<T>&& v,
                                 RawBufferFactory* buf_factory) {
    return v.MakeOwned(buf_factory);
  }
};

}  // namespace arolla

#endif  // AROLLA_DENSE_ARRAY_DENSE_ARRAY_H_
