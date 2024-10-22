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
#ifndef AROLLA_ARRAY_ARRAY_UTIL_H_
#define AROLLA_ARRAY_ARRAY_UTIL_H_

#include <cstdint>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/id_filter.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/unit.h"

namespace arolla {

// Converts Array<T> to Array<Unit>. Each present value transforms to kUnit.
template <typename T>
Array<Unit> ToArrayMask(const Array<T>& array) {
  return Array<Unit>(array.size(), array.id_filter(),
                     array.dense_data().ToMask(),
                     OptionalUnit(array.missing_id_value().present));
}

// Converts to a DenseArray. The returned DenseArray might not own its data if
// the original Array did not. Use DenseArray::MakeOwned to ensure that it
// doesn't refer to a memory that can become invalid in future.
template <typename T>
DenseArray<T> ToDenseArray(const Array<T>& array) {
  return array.ToDenseForm().dense_data().ForceNoBitmapBitOffset();
}

// Converts a full Array to a Buffer. The returned Buffer might not own its
// data if the original Array did not. Use Buffer::DeepCopy to ensure that
// it doesn't refer to a memory that can become invalid in future.
template <typename T>
absl::StatusOr<Buffer<T>> ToBuffer(const Array<T>& array) {
  DenseArray<T> dense_array = array.ToDenseForm().dense_data();
  if (dense_array.IsFull()) {
    return dense_array.values;
  } else {
    return absl::InvalidArgumentError(
        "Array with missing values can not be converted to a Buffer");
  }
}

// Returns first max_count (or all, if it is less than max_count) present ids.
std::vector<int64_t> ArrayFirstPresentIds(const Array<Unit>& array,
                                          int64_t max_count);

// Returns last max_count (or all, if it is less than max_count) present ids.
// Ids are returned in reversed (descending) order.
std::vector<int64_t> ArrayLastPresentIds(const Array<Unit>& array,
                                         int64_t max_count);

namespace array_foreach_in_subset_impl {

template <class T, class IdT, class Fn>
ABSL_ATTRIBUTE_NOINLINE void SparseArrayForEachInSubset(
    const Array<T>& a, absl::Span<const IdT> subset, int64_t subset_ids_offset,
    Fn&& fn) {
  const Buffer<T>& values = a.dense_data().values;
  const IdFilter& f = a.id_filter();
  auto array_it = f.ids().begin();
  auto subset_it = subset.begin();
  while (subset_it != subset.end() && array_it != f.ids().end()) {
    int64_t id_in_subset = *subset_it - subset_ids_offset;
    int64_t id_in_array = *array_it - f.ids_offset();
    if (id_in_array == id_in_subset) {
      int64_t offset = array_it - f.ids().begin();
      fn(id_in_subset, a.dense_data().present(offset), values[offset]);
      array_it++;
      subset_it++;
    } else if (id_in_array < id_in_subset) {
      array_it++;
    } else {
      fn(id_in_subset, a.missing_id_value().present,
         a.missing_id_value().value);
      subset_it++;
    }
  }
  for (; subset_it != subset.end(); ++subset_it) {
    int64_t id_in_subset = *subset_it - subset_ids_offset;
    fn(id_in_subset, a.missing_id_value().present, a.missing_id_value().value);
  }
}

}  // namespace array_foreach_in_subset_impl

// Iterate over subset of an array.
// IdT must be convertible to int64_t.
// ids in `subset` must be in ascending order
// in range [subset_ids_offset, subset_ids_offset + a.size()). Callback `fn` has
// arguments `id`, `is_present`, `value` (the same as in DenseArray::ForEach).
// Equivalent code:
//   for (IdT id_with_offset : subset) {
//     int64_t id = id_with_offset - subset_ids_offset;
//     fn(id, a[id].present, a[id].value);
//   }
//
template <class T, class IdT, class Fn>
void ArrayForEachInSubset(const Array<T>& a, absl::Span<const IdT> subset,
                          int64_t subset_ids_offset, Fn&& fn) {
  if (a.IsConstForm()) {
    for (IdT s : subset) {
      fn(s - subset_ids_offset, a.missing_id_value().present,
         a.missing_id_value().value);
    }
  } else if (a.IsFullForm()) {
    const Buffer<T>& values = a.dense_data().values;
    for (IdT s : subset) {
      DCHECK(0 <= s - subset_ids_offset &&
             s - subset_ids_offset < values.size());
      // For performance reasons we don't validate the indices (the caller
      // should guarantee that inputs are valid), but we apply std::clamp
      // to avoid crashes.
      int64_t id =
          std::clamp<int64_t>(s - subset_ids_offset, 0, values.size() - 1);
      fn(id, true, values[id]);
    }
  } else if (a.IsDenseForm()) {
    const Buffer<T>& values = a.dense_data().values;
    // Bitmap is not empty because a.IsDenseForm() && !a.IsFullForm()
    DCHECK_GE(
        a.dense_data().bitmap.size(),
        bitmap::BitmapSize(values.size() + a.dense_data().bitmap_bit_offset));
    const bitmap::Word* presence = a.dense_data().bitmap.begin();
    for (IdT s : subset) {
      DCHECK(0 <= s - subset_ids_offset &&
             s - subset_ids_offset < values.size());
      int64_t id =
          std::clamp<int64_t>(s - subset_ids_offset, 0, values.size() - 1);
      fn(id, bitmap::GetBit(presence, id + a.dense_data().bitmap_bit_offset),
         values[id]);
    }
  } else {
    array_foreach_in_subset_impl::SparseArrayForEachInSubset(
        a, subset, subset_ids_offset, std::forward<Fn>(fn));
  }
}

}  // namespace arolla

#endif  // AROLLA_ARRAY_ARRAY_UTIL_H_
