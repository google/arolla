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
#ifndef AROLLA_OPERATORS_ARRAY_RANDOM_H_
#define AROLLA_OPERATORS_ARRAY_RANDOM_H_

#include <cstdint>
#include <random>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"

namespace arolla {

// array.randint_with_shape generates a pseudo-random sequence.
class RandIntWithArrayShape {
 public:
  absl::StatusOr<Array<int64_t>> operator()(const ArrayShape& shape,
                                            int64_t low, int64_t high,
                                            int64_t seed) const {
    int64_t size = shape.size;
    if (size < 0) {
      return absl::InvalidArgumentError(
          absl::StrFormat("size=%d is negative", size));
    }
    if (low >= high) {
      return absl::InvalidArgumentError(
          absl::StrFormat("low=%d must be less than high=%d", low, high));
    }
    std::seed_seq seed_seq({int64_t{1}, shape.size, low, high, seed});
    std::mt19937_64 generator(seed_seq);
    std::uniform_int_distribution<int64_t> dist(low, high - 1);
    std::vector<int64_t> buffer(size);
    for (auto& x : buffer) {
      x = dist(generator);
    }
    return Array<int64_t>(
        DenseArray<int64_t>{Buffer<int64_t>::Create(std::move(buffer))});
  }
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_ARRAY_RANDOM_H_
