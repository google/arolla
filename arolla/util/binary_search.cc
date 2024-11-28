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
#include "arolla/util/binary_search.h"

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>

#include "absl//types/span.h"
#include "arolla/util/bits.h"
#include "arolla/util/switch_index.h"

namespace arolla::binary_search_details {
namespace {

// Fast binary search algorithm for an array of predefined size.
//
// kArraySize must be of a form 2**k - 1.
//
// Returns index of the first value where the given predicate became True.
//
// The perforamnce gain comes from the usage of conditional move instruction
// instead of conditional jumps (supported by many modern CPUs, including ARMs)
// and loop unrolling (because the size of the array is known at the compile
// time).
//
// The implementation is based on the ideas from:
//
//   https://arxiv.org/abs/1506.08620
// and
//   https://arxiv.org/abs/1509.05053
//
template <size_t kArraySize, typename T, class Predicate>
size_t FastBinarySearchT(const T* const array, Predicate predicate) {
  static_assert((kArraySize & (kArraySize + 1)) == 0);
  size_t offset = 0;
  for (size_t k = kArraySize; k > 0;) {
    k >>= 1;
    // We use ternary operator because clang++ translates it to the conditional
    // move instruction more willingly.
    offset = (!predicate(array[offset + k]) ? offset + k + 1 : offset);
  }
  return offset;
}

// Dispatching function for the FastBinarySearchT.
//
// Returns index of the first value where the given predicate became True.
template <typename T, typename Predicate>
size_t BinarySearchT(absl::Span<const T> array, Predicate predicate) {
  assert(!array.empty());
  const int log2_size = BitScanReverse(array.size());

  // Our goal is to reduce the binary search for an arbitrary array to
  // the binary search for an array of size = 2**k - 1, that can be handled by
  // FastBinarySearch().

  // Find such `size` that: size = 2**k - 1 and array.size() <= 2 * size + 1.
  // The value belongs either to the prefix of length `size`, or to the suffix
  // of length `size` (such prefix and suffix may ovelap).
  return switch_index<8 * sizeof(size_t)>(
      log2_size, [array, predicate](auto constexpr_log2_size) {
        constexpr size_t size =
            (1ULL << static_cast<int>(constexpr_log2_size)) - 1;
        size_t offset = 0;

#if !defined(__clang__) && defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
        offset = (!predicate(array[size]) ? array.size() - size : offset);
#if !defined(__clang__) && defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

        return offset +
               FastBinarySearchT<size>(array.begin() + offset, predicate);
      });
}

}  // namespace

size_t LowerBoundImpl(float value, absl::Span<const float> array) {
  return BinarySearchT(array, [value](auto arg) { return !(arg < value); });
}

size_t LowerBoundImpl(double value, absl::Span<const double> array) {
  return BinarySearchT(array, [value](auto arg) { return !(arg < value); });
}

size_t LowerBoundImpl(int32_t value, absl::Span<const int32_t> array) {
  return BinarySearchT(array, [value](auto arg) { return arg >= value; });
}

size_t LowerBoundImpl(int64_t value, absl::Span<const int64_t> array) {
  return BinarySearchT(array, [value](auto arg) { return arg >= value; });
}

// UpperBound algorithm can be implemented as a binary search either with
// condition NotLessThan or with condition GreaterEqual.
//
// There is a difference between these conditions when the value is NaN.
//
// Usually, it's more natural to use NotLessThan condition, because it doesn't
// require special handling for NaNs. But we found that FastBinarySearchT<T>
// with GreaterEqual works slightly faster. So we explicitly handle the NaNs.

size_t UpperBoundImpl(float value, absl::Span<const float> array) {
  if (std::isnan(value)) {
    return array.size();
  }
  return BinarySearchT(array, [value](auto arg) { return !(arg <= value); });
}

size_t UpperBoundImpl(double value, absl::Span<const double> array) {
  if (std::isnan(value)) {
    return array.size();
  }
  return BinarySearchT(array, [value](auto arg) { return !(arg <= value); });
}

size_t UpperBoundImpl(int32_t value, absl::Span<const int32_t> array) {
  return BinarySearchT(array, [value](auto arg) { return arg > value; });
}

size_t UpperBoundImpl(int64_t value, absl::Span<const int64_t> array) {
  return BinarySearchT(array, [value](auto arg) { return arg > value; });
}

}  // namespace arolla::binary_search_details
