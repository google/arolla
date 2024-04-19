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
#ifndef AROLLA_ARRAY_ARRAY_H_
#define AROLLA_ARRAY_ARRAY_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/array/id_filter.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/api.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/iterator.h"
#include "arolla/util/repr.h"
#include "arolla/util/view_types.h"

namespace arolla {

// ::arolla::Array is an immutable array with support of missing values.
// Implemented on top of DenseArray. It efficiently represents very sparse data
// and constants, but has bigger fixed overhead than DenseArray.
//
// Array contains IdFilter and DenseArray. If some index present in IdFilter,
// then the corresponding value is stored in DenseArray with the same offset as
// in IdFilter. Otherwise the value for the index is missing_id_value (that can
// be either nullopt or some default value).
template <class T>
class AROLLA_API Array {
 public:
  // Constant array
  explicit Array(int64_t size = 0, OptionalValue<T> value = std::nullopt)
      : size_(size),
        id_filter_(IdFilter::kEmpty),
        missing_id_value_(std::move(value)) {
    DCHECK_GE(size_, 0);
  }

  // From DenseArray
  explicit Array(DenseArray<T> data)
      : size_(data.size()),
        id_filter_(IdFilter::kFull),
        dense_data_(std::move(data)) {
    DCHECK(dense_data_.CheckBitmapMatchesValues());
  }

  // From Buffer
  explicit Array(Buffer<T> data)
      : size_(data.size()),
        id_filter_(IdFilter::kFull),
        dense_data_(DenseArray<T>{std::move(data)}) {}

  // This constructor directly assigns all fields of the Array.
  // It is not a factory function with StatusOr output for performance reasons.
  // Should be used carefully.
  //
  // Requirements to the arguments are:
  //   size >= 0
  //   if `ids` is kEmpty, then data.size() == 0
  //   if `ids` is kFull, then data.size() == size
  //   if `ids` is kPartial, then data.size() == ids.ids().size()
  explicit Array(int64_t size, IdFilter ids, DenseArray<T> data,
                 OptionalValue<T> missing_id_value = std::nullopt)
      : size_(size),
        id_filter_(std::move(ids)),
        dense_data_(std::move(data)),
        missing_id_value_(std::move(missing_id_value)) {
    DCHECK_GE(size_, 0);
    DCHECK(dense_data_.CheckBitmapMatchesValues());
    switch (id_filter_.type()) {
      case IdFilter::kEmpty:
        DCHECK(dense_data_.empty());
        break;
      case IdFilter::kPartial:
        DCHECK_LT(id_filter_.ids().size(), size_);
        DCHECK_EQ(id_filter_.ids().size(), dense_data_.size());
        DCHECK_LT(id_filter_.ids().back() - id_filter_.ids_offset(), size_);
        break;
      case IdFilter::kFull:
        DCHECK_EQ(dense_data_.size(), size_);
        // missing_id_value makes no sense if IdFilter is kFull
        missing_id_value_ = std::nullopt;
        break;
    }
  }

  int64_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  const IdFilter& id_filter() const { return id_filter_; }
  const DenseArray<T>& dense_data() const { return dense_data_; }
  const OptionalValue<T>& missing_id_value() const { return missing_id_value_; }

  const OptionalValue<view_type_t<T>> operator[](  // NOLINT
      int64_t index) const {
    DCHECK_GE(index, 0);
    DCHECK_LT(index, size_);
    OptionalValue<int64_t> offset = id_filter_.IdToOffset(index);
    if (offset.present) {
      return dense_data_[offset.value];
    } else {
      return missing_id_value_;
    }
  }

  // Functions `Is*Form` give information about the format rather than about
  // actual sparsity and constancy.
  // For example this is a valid representation of const array [5.0, 5.0, 5.0]:
  //   {size = 3,
  //    id_filter = {type = kPartial, ids = {0, 2}, ids_offset = 0},
  //    dense_data = {5.0, 5.0},
  //    missing_id_value = {true, 5.0}
  //   }
  // But it is not a canonical const representation, so `IsConstForm()` here
  // returns 'false'.

  // Constant "missing_id_value() (may be std::nullopt, so "all missing" is
  // a specific case of the const form).
  bool IsConstForm() const { return id_filter_.type() == IdFilter::kEmpty; }

  // Only dense_data.values and probably dense_data.bitmap are used.
  bool IsDenseForm() const { return id_filter_.type() == IdFilter::kFull; }

  // Both dense_data and id_filter().ids are used.
  bool IsSparseForm() const { return id_filter_.type() == IdFilter::kPartial; }

  // Canonical representation of "all values are missing".
  bool IsAllMissingForm() const {
    return IsConstForm() && !missing_id_value_.present;
  }

  // Only dense_data.values is used.
  bool IsFullForm() const {
    return IsDenseForm() && dense_data_.bitmap.empty();
  }

  // True if missing_id_value_.value is used at least for one item.
  bool HasMissingIdValue() const {
    return !IsDenseForm() && missing_id_value_.present && size_ > 0;
  }

  // Changes IdFilter to the given one. For ids from the new IdFilter,
  // the resulted block will contain the same values as the current block.
  // All other values will be equal to `missing_id_value`.
  // An example:
  // IdFilter filter(8, CreateBuffer<int64_t>({1, 4, 5, 6}));
  // Array<int> block(CreateDenseArray<int>({1, 2, 3, 4, nullopt, 6, 7, 8}));
  // Array<int> new_block = block.WithIds(filter, 0);
  //     new_block.id_filter() == filter
  //     Elements are: 0, 2, 0, 0, nullopt, 6, 7, 0
  Array WithIds(const IdFilter& ids, const OptionalValue<T>& missing_id_value,
                RawBufferFactory* buf_factory = GetHeapBufferFactory()) const;

  Array ToDenseForm(
      RawBufferFactory* buf_factory = GetHeapBufferFactory()) const {
    return WithIds(IdFilter::kFull, std::nullopt, buf_factory);
  }

  // Converts Array to sparse form with given missing_id_value. If
  // missing_id_value is nullopt, then the resulted Array will have no bitmap.
  // Note: if input Array is in const form, then conversion to sparse makes
  // no sense. Output will be either the same (if provided missing_id_value is
  // the same as before), or will be converted to full form.
  Array ToSparseForm(
      OptionalValue<T> missing_id_value = std::nullopt,
      RawBufferFactory* buf_factory = GetHeapBufferFactory()) const;

  bool is_owned() const {
    return dense_data_.is_owned() && (id_filter_.type() != IdFilter::kPartial ||
                                      id_filter_.ids().is_owner());
  }

  // It is cheap if underlaying buffers are already owned and requires full copy
  // otherwise.
  Array MakeOwned(
      RawBufferFactory* buf_factory = GetHeapBufferFactory()) const {
    IdFilter ids = id_filter_.type() == IdFilter::kPartial
                       ? IdFilter(size_, id_filter_.ids().DeepCopy(buf_factory),
                                  id_filter_.ids_offset())
                       : id_filter_;
    return Array(size_, std::move(ids), dense_data_.MakeOwned(buf_factory),
                 missing_id_value_);
  }

  Array Slice(int64_t start_id, int64_t row_count) const {
    DCHECK_GE(start_id, 0);
    DCHECK_GE(row_count, 0);
    DCHECK_LE(start_id + row_count, size_);
    if (id_filter_.type() == IdFilter::kEmpty) {
      return Array(row_count, missing_id_value_);
    }
    IdFilter filter = IdFilter::kFull;
    int64_t start_offset = start_id;
    int64_t dense_count = row_count;
    if (id_filter_.type() == IdFilter::kPartial) {
      int64_t new_ids_offset = id_filter_.ids_offset() + start_id;
      const Buffer<int64_t>& ids = id_filter_.ids();
      auto iter_start =
          std::lower_bound(ids.begin(), ids.end(), new_ids_offset);
      auto iter_end =
          std::lower_bound(ids.begin(), ids.end(), new_ids_offset + row_count);
      start_offset = std::distance(ids.begin(), iter_start);
      dense_count = std::distance(iter_start, iter_end);
      filter = IdFilter(row_count, ids.Slice(start_offset, dense_count),
                        new_ids_offset);
    }
    return Array(row_count, std::move(filter),
                 dense_data_.Slice(start_offset, dense_count),
                 missing_id_value_);
  }

  int64_t PresentCount() const {
    int64_t res = bitmap::CountBits(
        dense_data_.bitmap, dense_data_.bitmap_bit_offset, dense_data_.size());
    if (HasMissingIdValue()) res += size_ - dense_data_.size();
    return res;
  }

  using base_type = T;

  // Needed to support testing::ElementsAre matcher.
  // Not recommended to use iterators outside of test code, because it is slow.
  using value_type = OptionalValue<view_type_t<T>>;
  using size_type = int64_t;
  using const_iterator = ConstArrayIterator<Array<T>>;
  using difference_type = int64_t;
  using offset_type = int64_t;
  const_iterator begin() const { return const_iterator{this, 0}; }
  const_iterator end() const { return const_iterator{this, size()}; }

  // Iterates through all elements including missing. Callback `fn` should
  // have 3 arguments: int64_t id, bool present, view_type_t<T> value (the same
  // as DenseArray::ForEach).
  // Callback `repeated_fn` is optional. If present, it can be called instead of
  // a serie of `fn` when ids are sequential and values are equal.
  // `repeated_fn` has arguments first_id, count, present, value.
  template <typename Fn, typename RepeatedFn>
  void ForEach(Fn&& fn, RepeatedFn&& repeated_fn) const {
    if (IsConstForm()) {
      repeated_fn(0, size_, missing_id_value_.present, missing_id_value_.value);
      return;
    }
    if (IsDenseForm()) {
      dense_data_.ForEach(fn);
      return;
    }
    int64_t id = 0;
    dense_data_.ForEach([&](int64_t offset, bool present, view_type_t<T> v) {
      int64_t new_id = id_filter_.IdsOffsetToId(offset);
      if (id < new_id)
        repeated_fn(id, new_id - id, missing_id_value_.present,
                    missing_id_value_.value);
      fn(id_filter_.IdsOffsetToId(offset), present, v);
      id = new_id + 1;
    });
    if (id < size_) {
      repeated_fn(id, size_ - id, missing_id_value_.present,
                  missing_id_value_.value);
    }
  }

  template <typename Fn>
  void ForEach(Fn&& fn) const {
    ForEach(fn, [&](int64_t first_id, int64_t count, bool present,
                    view_type_t<T> value) {
      for (int64_t i = 0; i < count; ++i) {
        fn(first_id + i, present, value);
      }
    });
  }

  // Iterates through all present elements. Callback `fn` should
  // have 2 arguments: int64_t id, view_type_t<T> value.
  // Callback `repeated_fn` is optional. If present, it can be called instead of
  // a serie of `fn` when ids are sequential and values are equal.
  // `repeated_fn` has arguments first_id, count, value.
  template <typename Fn, typename RepeatedFn>
  void ForEachPresent(Fn&& fn, RepeatedFn&& repeated_fn) const {
    if (IsAllMissingForm()) return;
    if (IsConstForm()) {
      repeated_fn(0, size_, missing_id_value_.value);
      return;
    }
    if (IsDenseForm()) {
      dense_data_.ForEach([&fn](int64_t id, bool present, view_type_t<T> v) {
        if (present) fn(id, v);
      });
      return;
    }
    if (HasMissingIdValue()) {
      int64_t id = 0;
      dense_data_.ForEach([&](int64_t offset, bool present, view_type_t<T> v) {
        int64_t new_id = id_filter_.IdsOffsetToId(offset);
        if (id < new_id) repeated_fn(id, new_id - id, missing_id_value_.value);
        if (present) fn(id_filter_.IdsOffsetToId(offset), v);
        id = new_id + 1;
      });
      if (id < size_) {
        repeated_fn(id, size_ - id, missing_id_value_.value);
      }
    } else {
      dense_data_.ForEach(
          [this, &fn](int64_t offset, bool present, view_type_t<T> v) {
            if (present) fn(id_filter_.IdsOffsetToId(offset), v);
          });
    }
  }

  template <typename Fn>
  void ForEachPresent(Fn&& fn) const {
    ForEachPresent(fn,
                   [&](int64_t first_id, int64_t count, view_type_t<T> value) {
                     for (int64_t i = 0; i < count; ++i) {
                       fn(first_id + i, value);
                     }
                   });
  }

 private:
  // Used in WithIds
  DenseArray<T> WithIdsFromSparse(const IdFilter& ids,
                                  RawBufferFactory* buf_factory) const;
  DenseArray<T> WithIdsDenseToSparse(const IdFilter& ids,
                                     RawBufferFactory* buf_factory) const;

  // Used in ToSparseForm
  Array<T> ToSparseFormWithChangedMissedIdValue(
      OptionalValue<T> missing_id_value, RawBufferFactory* buf_factory) const;
  Array<T> ToSparseFormWithMissedIdValue(const T& missing_id_value,
                                         RawBufferFactory* buf_factory) const;
  Array<T> ToSparseFormWithoutMissedIdValue(
      RawBufferFactory* buf_factory) const;

  int64_t size_;
  IdFilter id_filter_;
  DenseArray<T> dense_data_;
  OptionalValue<T> missing_id_value_;
};

// Returns true iff lhs and rhs represent the same data.
template <typename T>
bool ArraysAreEquivalent(const Array<T>& lhs, const Array<T>& rhs) {
  if (lhs.size() != rhs.size()) return false;
  if (lhs.IsDenseForm() && rhs.IsDenseForm()) {
    return ArraysAreEquivalent(lhs.dense_data(), rhs.dense_data());
  }
  IdFilter id_union = IdFilter::UpperBoundMerge(
      lhs.size(), GetHeapBufferFactory(), lhs.id_filter(), rhs.id_filter());
  auto lhs_transformed = lhs.WithIds(id_union, lhs.missing_id_value());
  auto rhs_transformed = rhs.WithIds(id_union, rhs.missing_id_value());
  return lhs_transformed.missing_id_value() ==
             rhs_transformed.missing_id_value() &&
         ArraysAreEquivalent(lhs_transformed.dense_data(),
                             rhs_transformed.dense_data());
}

template <class T>
Array<T> CreateArray(absl::Span<const OptionalValue<T>> data) {
  return Array<T>(CreateDenseArray<T>(data));
}

// Creates Array from lists of ids and values. It chooses dense or sparse
// representation automatically. ValueT should be one of T, OptionalValue<T>,
// std::optional<T>, or corresponding view types.
template <class T, class ValueT = T>
Array<T> CreateArray(int64_t size, absl::Span<const int64_t> ids,
                     absl::Span<const ValueT> values) {
  DCHECK_EQ(ids.size(), values.size());
  DCHECK_LE(values.size(), size);
  if (values.size() > size * IdFilter::kDenseSparsityLimit) {
    DenseArrayBuilder<T> bldr(size);
    for (int64_t i = 0; i < values.size(); ++i) {
      bldr.Set(ids[i], values[i]);
    }
    return Array<T>(std::move(bldr).Build());
  } else {
    Buffer<int64_t>::Builder ids_bldr(ids.size());
    DenseArrayBuilder<T> values_bldr(values.size());
    for (int64_t i = 0; i < values.size(); ++i) {
      ids_bldr.Set(i, ids[i]);
      values_bldr.Set(i, values[i]);
    }
    return Array<T>(size, IdFilter(size, std::move(ids_bldr).Build()),
                    std::move(values_bldr).Build());
  }
}

// This helper allows to get Array type from optional types and references.
// For example AsArray<OptionalValue<int>&> is just Array<int>.
template <class T>
using AsArray = Array<strip_optional_t<std::decay_t<T>>>;

struct AROLLA_API ArrayShape {
  int64_t size;

  bool operator==(const ArrayShape& other) const { return size == other.size; }
};

AROLLA_DECLARE_REPR(ArrayShape);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(ArrayShape);

template <class T>
Array<T> Array<T>::WithIds(const IdFilter& ids,
                           const OptionalValue<T>& missing_id_value,
                           RawBufferFactory* buf_factory) const {
  if (ids.type() == IdFilter::kEmpty || size_ == 0) {
    return Array(size_, missing_id_value);
  }
  if (id_filter_.IsSame(ids)) {
    return Array(size_, ids, dense_data_, missing_id_value);
  }

  DenseArray<T> new_data;
  switch (id_filter_.type()) {
    case IdFilter::kEmpty: {
      int64_t data_size =
          ids.type() == IdFilter::kPartial ? ids.ids().size() : size_;
      if (missing_id_value_.present) {
        new_data = CreateConstDenseArray<T>(data_size, missing_id_value_.value,
                                            buf_factory);
      } else {
        new_data = CreateEmptyDenseArray<T>(data_size, buf_factory);
      }
    } break;
    case IdFilter::kPartial: {
      new_data = WithIdsFromSparse(ids, buf_factory);
    } break;
    case IdFilter::kFull: {
      new_data = WithIdsDenseToSparse(ids, buf_factory);
    } break;
  }
  return Array(size_, ids, std::move(new_data), missing_id_value);
}

// Used in WithIds. Prepares new dense_data in the case if old IdFilter is
// kPartial and the new one is either kPartial or kFull.
template <class T>
ABSL_ATTRIBUTE_NOINLINE DenseArray<T> Array<T>::WithIdsFromSparse(
    const IdFilter& ids, RawBufferFactory* buf_factory) const {
  DCHECK_EQ(id_filter_.type(), IdFilter::kPartial);
  DCHECK_NE(ids.type(), IdFilter::kEmpty);

  size_t data_size =
      ids.type() == IdFilter::kPartial ? ids.ids().size() : size_;
  typename Buffer<T>::ReshuffleBuilder values_bldr(
      data_size, dense_data_.values, missing_id_value_, buf_factory);
  bitmap::RawBuilder bitmap_bldr(bitmap::BitmapSize(data_size), buf_factory);
  auto bitmap = bitmap_bldr.GetMutableSpan();
  std::memset(bitmap.begin(), missing_id_value_.present ? 0xff : 0,
              bitmap.size() * sizeof(bitmap::Word));

  if (ids.type() == IdFilter::kPartial) {
    IdFilter::IntersectPartial_ForEach(
        id_filter_, ids,
        [&](int64_t /*id*/, int64_t old_offset, int64_t new_offset) {
          if (dense_data_.present(old_offset)) {
            values_bldr.CopyValue(new_offset, old_offset);
            bitmap::SetBit(bitmap.begin(), new_offset);
          } else {
            bitmap::UnsetBit(bitmap.begin(), new_offset);
          }
        });
  } else if (missing_id_value_.present) {
    DCHECK_EQ(ids.type(), IdFilter::kFull);
    // Bitmap is already set to all present.
    // Values are already set to missing_id_value_.value().
    dense_data_.ForEach([&](int64_t offset, bool present, view_type_t<T>) {
      int64_t new_offset = id_filter_.IdsOffsetToId(offset);
      if (present) {
        values_bldr.CopyValue(new_offset, offset);
      } else {
        bitmap::UnsetBit(bitmap.begin(), new_offset);
      }
    });
  } else {
    DCHECK_EQ(ids.type(), IdFilter::kFull);
    // Bitmap is already set to all missing.
    dense_data_.ForEach([&](int64_t offset, bool present, view_type_t<T>) {
      int64_t new_offset = id_filter_.IdsOffsetToId(offset);
      if (present) {
        values_bldr.CopyValue(new_offset, offset);
        bitmap::SetBit(bitmap.begin(), new_offset);
      }
    });
  }

  if (bitmap::AreAllBitsSet(bitmap.begin(), data_size)) {
    return {std::move(values_bldr).Build()};
  } else {
    return {std::move(values_bldr).Build(), std::move(bitmap_bldr).Build()};
  }
}

// Used in WithIds. Prepares new dense_data in the case if old IdFilter is
// kFull and the new one is kPartial.
template <class T>
ABSL_ATTRIBUTE_NOINLINE DenseArray<T> Array<T>::WithIdsDenseToSparse(
    const IdFilter& ids, RawBufferFactory* buf_factory) const {
  DCHECK_EQ(id_filter_.type(), IdFilter::kFull);
  DCHECK_EQ(ids.type(), IdFilter::kPartial);

  typename Buffer<T>::ReshuffleBuilder values_bldr(
      ids.ids().size(), dense_data_.values, /*default_value=*/std::nullopt,
      buf_factory);
  int64_t index = 0;
  if (dense_data_.bitmap.empty()) {
    for (IdFilter::IdWithOffset id : ids.ids()) {
      values_bldr.CopyValue(index++, id - ids.ids_offset());
    }
    return {std::move(values_bldr).Build()};
  } else {
    bitmap::Builder bitmap_bldr(ids.ids().size(), buf_factory);
    bitmap_bldr.AddForEach(ids.ids(),
                           [&](IdFilter::IdWithOffset id_with_offset) {
                             int64_t id = id_with_offset - ids.ids_offset();
                             values_bldr.CopyValue(index++, id);
                             return dense_data_.present(id);
                           });
    return {std::move(values_bldr).Build(), std::move(bitmap_bldr).Build()};
  }
}

template <class T>
Array<T> Array<T>::ToSparseForm(OptionalValue<T> missing_id_value,
                                RawBufferFactory* buf_factory) const {
  if (!IsDenseForm() && missing_id_value != missing_id_value_) {
    return ToSparseFormWithChangedMissedIdValue(missing_id_value, buf_factory);
  } else if (missing_id_value.present) {
    return ToSparseFormWithMissedIdValue(missing_id_value.value, buf_factory);
  } else {
    return ToSparseFormWithoutMissedIdValue(buf_factory);
  }
}

template <class T>
Array<T> Array<T>::ToSparseFormWithChangedMissedIdValue(
    OptionalValue<T> missing_id_value, RawBufferFactory* buf_factory) const {
  DCHECK(!IsDenseForm() && missing_id_value != missing_id_value_);
  Buffer<IdFilter::IdWithOffset>::Builder bldr(size_, buf_factory);
  auto inserter = bldr.GetInserter();

  // missing_id_value_ (old) != missing_id_value (new), so all ids that were
  // missing (have value = missing_id_value_), should be present after
  // conversion.
  int64_t next_id = 0;
  dense_data_.ForEach([&](int64_t offset, bool presence, view_type_t<T> value) {
    int64_t id = id_filter_.IdsOffsetToId(offset);
    while (next_id < id) inserter.Add(next_id++);
    // Condition is equivalent to (dense_data_[offset] != missing_id_value)
    if (presence != missing_id_value.present ||
        (presence && missing_id_value.value != value)) {
      inserter.Add(id);
    }
    next_id = id + 1;
  });
  while (next_id < size_) inserter.Add(next_id++);

  return WithIds(IdFilter(size_, std::move(bldr).Build(inserter)),
                 missing_id_value, buf_factory);
}

template <class T>
Array<T> Array<T>::ToSparseFormWithMissedIdValue(
    const T& missing_id_value, RawBufferFactory* buf_factory) const {
  // missing_id_value_ either was not used at all (dense form) or was not
  // changed.
  DCHECK(IsDenseForm() || missing_id_value_ == missing_id_value);

  // new id_filter should contain only ids where value != missing_id_value
  Buffer<IdFilter::IdWithOffset>::Builder ids_bldr(dense_data_.size(),
                                                   buf_factory);
  auto ids_inserter = ids_bldr.GetInserter();

  if (IsDenseForm() && !dense_data_.bitmap.empty()) {
    dense_data_.ForEach(
        [&](int64_t offset, bool presence, view_type_t<T> value) {
          if (!presence || value != missing_id_value) {
            ids_inserter.Add(offset);
          }
        });
    return WithIds(IdFilter(size_, std::move(ids_bldr).Build(ids_inserter)),
                   missing_id_value, buf_factory);
  }

  typename Buffer<T>::ReshuffleBuilder values_bldr(
      dense_data_.size(), dense_data_.values, std::nullopt, buf_factory);
  bitmap::Bitmap bitmap;
  int64_t new_offset = 0;

  if (dense_data_.bitmap.empty()) {
    // Copy all values != missing_id_value consequently to the new array.
    // Fill ids_inserter with corresponding ids.
    if (IsDenseForm()) {
      for (int64_t offset = 0; offset < dense_data_.size(); ++offset) {
        if (dense_data_.values[offset] != missing_id_value) {
          ids_inserter.Add(offset);
          values_bldr.CopyValue(new_offset++, offset);
        }
      }
    } else {
      for (int64_t offset = 0; offset < dense_data_.size(); ++offset) {
        if (dense_data_.values[offset] != missing_id_value) {
          ids_inserter.Add(id_filter_.IdsOffsetToId(offset));
          values_bldr.CopyValue(new_offset++, offset);
        }
      }
    }
  } else {
    // Copy all values != missing_id_value (including missing) consequently to
    // the new array. Construct a new bitmap for the resulting array.
    bitmap::AlmostFullBuilder bitmap_bldr(dense_data_.size(), buf_factory);
    dense_data_.ForEach(
        [&](int64_t offset, bool presence, view_type_t<T> value) {
          if (presence && value == missing_id_value) return;
          ids_inserter.Add(id_filter_.IdsOffsetToId(offset));
          if (presence) {
            values_bldr.CopyValue(new_offset++, offset);
          } else {
            bitmap_bldr.AddMissed(new_offset++);
          }
        });
    bitmap = std::move(bitmap_bldr).Build(new_offset);
  }

  IdFilter id_filter(size_, std::move(ids_bldr).Build(ids_inserter));
  Buffer<T> values = std::move(values_bldr).Build(new_offset);
  return Array(size_, std::move(id_filter),
               {std::move(values), std::move(bitmap)}, missing_id_value);
}

template <class T>
Array<T> Array<T>::ToSparseFormWithoutMissedIdValue(
    RawBufferFactory* buf_factory) const {
  // missing_id_value is not used both before and after conversion.
  // We need only to filter out missing values.
  DCHECK(!HasMissingIdValue());

  // No missing values, nothing to do.
  if (dense_data_.bitmap.empty()) return *this;

  Buffer<IdFilter::IdWithOffset>::Builder ids_bldr(dense_data_.size(),
                                                   buf_factory);
  typename Buffer<T>::ReshuffleBuilder values_bldr(
      dense_data_.size(), dense_data_.values, std::nullopt, buf_factory);
  auto ids_inserter = ids_bldr.GetInserter();
  int64_t new_offset = 0;

  if (IsDenseForm()) {
    dense_data_.ForEach([&](int64_t offset, bool presence, view_type_t<T>) {
      if (presence) {
        ids_inserter.Add(offset);
        values_bldr.CopyValue(new_offset++, offset);
      }
    });
  } else {
    dense_data_.ForEach([&](int64_t offset, bool presence, view_type_t<T>) {
      if (presence) {
        ids_inserter.Add(id_filter_.IdsOffsetToId(offset));
        values_bldr.CopyValue(new_offset++, offset);
      }
    });
  }
  IdFilter id_filter(size_, std::move(ids_bldr).Build(ids_inserter));
  Buffer<T> values = std::move(values_bldr).Build(new_offset);
  return Array(size_, std::move(id_filter), {std::move(values)});
}

// Types that can be unowned should overload ArenaTraits. Should be used
// in ModelExecutor to make the result owned even if was created using
// UnsafeArenaBufferFactory.
template <typename T>
struct ArenaTraits<Array<T>> {
  static Array<T> MakeOwned(Array<T>&& v, RawBufferFactory* buf_factory) {
    return v.MakeOwned(buf_factory);
  }
};

template <typename T>
struct FingerprintHasherTraits<Array<T>> {
  void operator()(FingerprintHasher* hasher, const Array<T>& arg) const {
    hasher->Combine(arg.size(), arg.dense_data(), arg.missing_id_value(),
                    arg.id_filter());
  }
};

// Sparse builder for Array. It holds DenseArrayBuilder for dense data and
// Buffer<int64_t>::Builder for ids.
template <typename T>
class SparseArrayBuilder {
 public:
  explicit SparseArrayBuilder(
      int64_t size, int64_t max_present_count,
      RawBufferFactory* buf_factory = GetHeapBufferFactory())
      : size_(size),
        dense_builder_(max_present_count, buf_factory),
        ids_builder_(max_present_count, buf_factory) {}

  // Adds `id` and the corresponding value.
  // Ids must be added in ascending order.
  template <typename V>
  void Add(int64_t id, const V& v) {
    dense_builder_.Set(offset_, v);
    AddId(id);
  }

  // Adds `id`. Ids must be added in ascending order.
  // The corresponding value can be set using `SetByOffset`.
  void AddId(int64_t id) {
    DCHECK(id >= 0 && id < size_);
    DCHECK(offset_ == 0 || ids_builder_.GetMutableSpan()[offset_ - 1] < id);
    ids_builder_.Set(offset_++, id);
  }

  // Returns the first not yet used offset. It increases every time when
  // either `Add` or `AddId` is called.
  int64_t NextOffset() const { return offset_; }

  // Sets value for previously added id.
  template <typename V>
  void SetByOffset(int64_t offset, const V& v) {
    dense_builder_.Set(offset, v);
  }

  Array<T> Build() && {
    return Array<T>(size_,
                    IdFilter(size_, std::move(ids_builder_).Build(offset_)),
                    std::move(dense_builder_).Build(offset_));
  }

 private:
  int64_t size_;
  int64_t offset_ = 0;
  DenseArrayBuilder<T> dense_builder_;
  Buffer<int64_t>::Builder ids_builder_;
};

}  // namespace arolla

#endif  // AROLLA_ARRAY_ARRAY_H_
