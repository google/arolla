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
#ifndef AROLLA_UTIL_BINARY_SEARCH_H_
#define AROLLA_UTIL_BINARY_SEARCH_H_

#include <cstddef>
#include <cstdint>
#include <optional>

#include "absl/base/attributes.h"
#include "absl/types/span.h"

namespace arolla {

// Left sided binary search within a sorted array.
// It is a better performance version of std::lower_bound().
size_t LowerBound(float value, absl::Span<const float> array);

// Left sided binary search within a sorted array.
// It is a better performance version of std::lower_bound().
size_t LowerBound(double value, absl::Span<const double> array);

// Left sided binary search within a sorted array.
// It is a better performance version of std::lower_bound().
size_t LowerBound(int32_t value, absl::Span<const int32_t> array);

// Left sided binary search within a sorted array.
// It is a better performance version of std::lower_bound().
size_t LowerBound(int64_t value, absl::Span<const int64_t> array);

// Right sided binary search within a sorted array.
// It is a better performance version of std::upper_bound().
size_t UpperBound(float value, absl::Span<const float> array);

// Right sided binary search within a sorted array.
// It is a better performance version of std::upper_bound().
size_t UpperBound(double value, absl::Span<const double> array);

// Right sided binary search within a sorted array.
// It is a better performance version of std::upper_bound().
size_t UpperBound(int32_t value, absl::Span<const int32_t> array);

// Right sided binary search within a sorted array.
// It is a better performance version of std::upper_bound().
size_t UpperBound(int64_t value, absl::Span<const int64_t> array);

// Implementation of lower bound using exponential search
// (see https://en.wikipedia.org/wiki/Exponential_search).
// Optimized for the case when the lower bound is more likely to be found close
// to `begin` rather than at the end of the array.
template <typename T, typename Iter>
Iter GallopingLowerBound(Iter begin, Iter end, const T& value);

}  // namespace arolla

// IMPLEMENTATION DETAILS

namespace arolla::binary_search_details {

// TODO: Fix perforamnce degradation for large inputs.
//
// Array size after which std::upper_bound() outperformas our custom
// implementation.
constexpr size_t kSupremacySizeThreshold = 1'000'000;

template <typename T>
size_t LowerBound(T value, absl::Span<const T> array);

template <typename T>
size_t UpperBound(T value, absl::Span<const T> array);

template <typename T, typename Predicate>
inline ABSL_ATTRIBUTE_ALWAYS_INLINE std::optional<size_t> SmallLinearSearch(
    absl::Span<const T> array, Predicate predicate) {
  if (array.size() <= 2) {
    if (array.empty() || predicate(array[0])) {
      return 0;
    } else if (array.size() == 1 || predicate(array[1])) {
      return 1;
    }
    return 2;
  }
  return std::nullopt;
}

size_t UpperBoundImpl(float value, absl::Span<const float> array);
size_t UpperBoundImpl(double value, absl::Span<const double> array);
size_t UpperBoundImpl(int32_t value, absl::Span<const int32_t> array);
size_t UpperBoundImpl(int64_t value, absl::Span<const int64_t> array);
size_t LowerBoundImpl(float value, absl::Span<const float> array);
size_t LowerBoundImpl(double value, absl::Span<const double> array);
size_t LowerBoundImpl(int32_t value, absl::Span<const int32_t> array);
size_t LowerBoundImpl(int64_t value, absl::Span<const int64_t> array);

template <typename T>
inline ABSL_ATTRIBUTE_ALWAYS_INLINE size_t
LowerBound(T value, absl::Span<const T> array) {
  if (auto result =
          SmallLinearSearch(array, [value](T arg) { return !(arg < value); })) {
    return *result;
  }
  return LowerBoundImpl(value, array);
}

template <typename T>
inline ABSL_ATTRIBUTE_ALWAYS_INLINE size_t
UpperBound(T value, absl::Span<const T> array) {
  if (auto result =
          SmallLinearSearch(array, [value](T arg) { return value < arg; })) {
    return *result;
  }
  return UpperBoundImpl(value, array);
}

}  // namespace arolla::binary_search_details

namespace arolla {

inline size_t LowerBound(float value, absl::Span<const float> array) {
  return binary_search_details::LowerBound<float>(value, array);
}

inline size_t LowerBound(double value, absl::Span<const double> array) {
  return binary_search_details::LowerBound<double>(value, array);
}

inline size_t LowerBound(int32_t value, absl::Span<const int32_t> array) {
  return binary_search_details::LowerBound<int32_t>(value, array);
}

inline size_t LowerBound(int64_t value, absl::Span<const int64_t> array) {
  return binary_search_details::LowerBound<int64_t>(value, array);
}

inline size_t UpperBound(float value, absl::Span<const float> array) {
  return binary_search_details::UpperBound<float>(value, array);
}

inline size_t UpperBound(double value, absl::Span<const double> array) {
  return binary_search_details::UpperBound<double>(value, array);
}

inline size_t UpperBound(int32_t value, absl::Span<const int32_t> array) {
  return binary_search_details::UpperBound<int32_t>(value, array);
}

inline size_t UpperBound(int64_t value, absl::Span<const int64_t> array) {
  return binary_search_details::UpperBound<int64_t>(value, array);
}

template <typename T, typename Iter>
Iter GallopingLowerBound(Iter begin, Iter end, const T& value) {
  size_t i = 0;
  size_t size = end - begin;
  if (begin >= end || !(*begin < value)) {
    // We use std::min(begin, end) for performance reasons.
    // Changing it to `return begin` causes performance drop (not clear why):
    // name                                    old cpu/op   new cpu/op   delta
    // BM_GallopingLowerBound_Float32/1/1000   42.7ns ± 0%  44.7ns ± 1%   +4.76%
    // BM_GallopingLowerBound_Float32/4/1000   31.7ns ± 1%  33.4ns ± 1%   +5.45%
    // BM_GallopingLowerBound_Float32/16/1000  21.7ns ± 0%  23.7ns ± 0%   +9.35%
    // BM_GallopingLowerBound_Float32/64/1000  14.3ns ± 1%  16.3ns ± 0%  +13.59%
    // BM_GallopingLowerBound_Float32/256/1000 9.43ns ± 0% 10.76ns ± 0%  +14.01%
    // BM_GallopingLowerBound_Float32/512/1000 4.30ns ± 0%  4.80ns ± 0%  +11.71%
    return std::min<Iter>(begin, end);
  }
  // Scan forward, doubling step size after each step, until the next step
  // would hit or exceed value.
  size_t d = 1;
  while (i + d < size && begin[i + d] < value) {
    i += d;
    d <<= 1;
  }
  // Halve step size repeatedly and step forward any time we won't hit or
  // exceed value.
  while (d > 1) {
    d >>= 1;
    if (i + d < size && begin[i + d] < value) {
      i += d;
    }
  }
  return begin + i + 1;
}

}  // namespace arolla

#endif  // AROLLA_UTIL_BINARY_SEARCH_H_
