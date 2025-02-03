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
// IWYU pragma: always_keep, the file defines ConcatResultArrayBuilderHelper<T> specialization.

#ifndef AROLLA_JAGGED_SHAPE_DENSE_ARRAY_UTIL_CONCAT_H_
#define AROLLA_JAGGED_SHAPE_DENSE_ARRAY_UTIL_CONCAT_H_

#include <cstdint>

#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/jagged_shape/util/concat.h"

namespace arolla {
namespace jagged_shape_internal {

template <typename T>
struct ConcatResultArrayBuilderHelper<DenseArray<T>> {
  DenseArrayBuilder<T> operator()(
      absl::Span<const DenseArray<T>> arrays) const {
    int64_t result_size = 0;
    for (const auto& array : arrays) {
      result_size += array.size();
    }
    return DenseArrayBuilder<T>(result_size);
  }
};

}  // namespace jagged_shape_internal
}  // namespace arolla

#endif  // AROLLA_JAGGED_SHAPE_DENSE_ARRAY_UTIL_CONCAT_H_
