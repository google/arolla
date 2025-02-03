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
#ifndef AROLLA_UTIL_SWITCH_INDEX_H_
#define AROLLA_UTIL_SWITCH_INDEX_H_

#include <cassert>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"

namespace arolla {

#define CASE_1(k) \
  case (k):       \
    return std::forward<Callback>(callback)(std::integral_constant<int, (k)>())
#define CASE_4(k) \
  CASE_1(k);      \
  CASE_1(k + 1);  \
  CASE_1(k + 2);  \
  CASE_1(k + 3)
#define CASE_16(k) \
  CASE_4(k);       \
  CASE_4(k + 4);   \
  CASE_4(k + 8);   \
  CASE_4(k + 12)

// Evaluates to callback(std::integral_constant<int, N>()) where N == n.
template <typename Callback>
auto switch_index_32(int n, Callback&& callback) {
  assert(0 <= n && n < 32);
  switch (n) {
    CASE_16(0);
    CASE_16(16);
  }
  return std::forward<Callback>(callback)(std::integral_constant<int, 31>());
}

// Evaluates to callback(std::integral_constant<int, N>()) where N == n.
template <typename Callback>
auto switch_index_64(int n, Callback&& callback) {
  assert(0 <= n && n < 64);
  switch (n) {
    CASE_16(0);
    CASE_16(16);
    CASE_16(32);
    CASE_16(48);
  }
  return std::forward<Callback>(callback)(std::integral_constant<int, 63>());
}

// Evaluates to callback(std::integral_constant<int, N>()) where N == n.
template <int N, typename Callback>
inline ABSL_ATTRIBUTE_ALWAYS_INLINE auto switch_index(int n,
                                                      Callback&& callback) {
  static_assert(N == 32 || N == 64);
  if constexpr (N == 32) {
    return switch_index_32(n, std::forward<Callback>(callback));
  } else {
    return switch_index_64(n, std::forward<Callback>(callback));
  }
}

#undef CASE_16
#undef CASE_4
#undef CASE_1

}  // namespace arolla

#endif  // AROLLA_UTIL_SWITCH_INDEX_H_
