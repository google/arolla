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
#ifndef AROLLA_OPERATORS_RANDOM_H_
#define AROLLA_OPERATORS_RANDOM_H_

#include <cstdint>

#include "absl/strings/string_view.h"
#include "cityhash/city.h"

namespace arolla {

// random.cityhash operator returns a 63-bit integer hash value which is
// stable for the same value and seed.
struct CityHashOp {
  // TODO: switch to reuse fingerprint hasher.
  int64_t operator()(absl::string_view str, const int64_t seed) const {
    return cityhash::CityHash64WithSeed(str.data(), str.size(),
                                                    seed) &
           0x7FFFFFFFFFFFFFFF;
  }
  template <typename StringT>
  int64_t operator()(const StringT& str, const int64_t seed) const {
    return this->operator()(absl::string_view(str), seed);
  }
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_RANDOM_H_
