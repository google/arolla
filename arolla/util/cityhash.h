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
// Proxy API for CityHash: https://github.com/google/cityhash
//
// To prevent types like `uint64` and `uint128` from polluting the global
// namespace, we wrap the CityHash implementation within
// the `third_party_cityhash` namespace and expose only the essential API here.

#ifndef AROLLA_UTIL_CITYHASH_H_
#define AROLLA_UTIL_CITYHASH_H_

#include <cstddef>
#include <cstdint>
#include <utility>

#include "cityhash/city.h"

namespace arolla {

using CityHashUInt128 = std::pair<uint64_t, uint64_t>;

// Hash function for a byte array. For convenience, a 64-bit seed is also
// hashed into the result.
inline uint64_t CityHash64WithSeed(const void *data, size_t size,
                                   uint64_t seed) {
  return ::third_party_cityhash::CityHash64WithSeed(
      static_cast<const char *>(data), size, seed);
}

// Hash function for a byte array. For convenience, a 128-bit seed is also
// hashed into the result.
inline CityHashUInt128 CityHash128WithSeed(const void *data, size_t size,
                                           CityHashUInt128 seed) {
  return ::third_party_cityhash::CityHash128WithSeed(
      static_cast<const char *>(data), size, seed);
}

}  // namespace arolla

#endif  // AROLLA_UTIL_CITYHASH_H_
