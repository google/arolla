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
#ifndef AROLLA_DENSE_ARRAY_TESTING_UTIL_H_
#define AROLLA_DENSE_ARRAY_TESTING_UTIL_H_

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl//random/random.h"
#include "absl//strings/str_cat.h"
#include "absl//strings/string_view.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/meta.h"
#include "arolla/util/view_types.h"

namespace arolla::testing {

template <typename T>
std::vector<std::optional<T>> ToVectorOptional(const DenseArray<T>& ar) {
  std::vector<std::optional<T>> res(ar.size());
  ar.ForEach([&](int64_t id, bool present, view_type_t<T> v) {
    if (present) res[id] = T(v);
  });
  return res;
}

template <typename T>
DenseArray<T> CreateDenseArrayFromIdValues(
    int64_t size, std::initializer_list<std::tuple<int64_t, T>> data) {
  DenseArrayBuilder<T> builder(size);
  for (const auto& [id, v] : data) builder.Set(id, v);
  return std::move(builder).Build();
}

template <class T>
AsDenseArray<T> RandomDenseArray(int64_t size, bool full, int bit_offset,
                                 absl::BitGen& gen) {
  using ValueT = strip_optional_t<T>;
  DenseArray<ValueT> res;

  typename Buffer<ValueT>::Builder values_builder(size);
  for (int64_t row_id = 0; row_id < size; ++row_id) {
    if constexpr (std::is_same_v<view_type_t<T>, absl::string_view>) {
      values_builder.Set(row_id, absl::StrCat(absl::Uniform<float>(gen, 0, 1)));
    } else {
      values_builder.Set(row_id, absl::Uniform<ValueT>(gen, 0, 1));
    }
  }
  res.values = std::move(values_builder).Build(size);

  if (!full) {
    int64_t presence_size = bitmap::BitmapSize(size + bit_offset);
    bitmap::RawBuilder builder(presence_size);
    for (int64_t i = 0; i < presence_size; ++i) {
      builder.Set(i, absl::Uniform<bitmap::Word>(gen, 0, bitmap::kFullWord));
    }
    res.bitmap = std::move(builder).Build(presence_size);
    res.bitmap_bit_offset = bit_offset;
  }
  return res;
}

// Generate a set of random DenseArrays of given types. For example:
//
// auto [a, b] =
//     RandomDenseArrays(meta::type_list<int32_t, float>(), 100, false, 0);
//
// generates two random DenseArrays of size 100 with value types int32_t and
// float, respectively.
template <class... Ts, size_t... Is>
std::tuple<AsDenseArray<Ts>...> RandomDenseArrays(meta::type_list<Ts...>,
                                                  int64_t size, bool full,
                                                  bool bit_offset,
                                                  std::index_sequence<Is...>) {
  absl::BitGen gen;
  return {RandomDenseArray<Ts>(size, full, bit_offset ? Is + 1 : 0, gen)...};
}
template <class... Ts>
std::tuple<AsDenseArray<Ts>...> RandomDenseArrays(meta::type_list<Ts...> types,
                                                  int64_t size, bool full,
                                                  bool bit_offset) {
  return RandomDenseArrays(types, size, full, bit_offset,
                           std::index_sequence_for<Ts...>());
}

// Returns DenseArray with unowned buffers that points to the original array.
// Becomes invalid if the original array is removed.
template <typename T>
DenseArray<T> AsUnownedDenseArray(const DenseArray<T>& ar) {
  return {ar.values.ShallowCopy(), ar.bitmap.ShallowCopy(),
          ar.bitmap_bit_offset};
}

template <class... Ts, size_t... Is>
std::tuple<DenseArray<Ts>...> AsUnownedDenseArrays(
    const std::tuple<DenseArray<Ts>...>& arrays, std::index_sequence<Is...>) {
  return {AsUnownedDenseArray<Ts>(std::get<Is>(arrays))...};
}
template <class... Ts>
std::tuple<DenseArray<Ts>...> AsUnownedDenseArrays(
    const std::tuple<DenseArray<Ts>...>& arrays) {
  return AsUnownedDenseArrays(arrays, std::index_sequence_for<Ts...>());
}

}  // namespace arolla::testing

#endif  // AROLLA_DENSE_ARRAY_TESTING_UTIL_H_
