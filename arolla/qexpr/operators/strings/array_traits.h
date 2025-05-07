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
#ifndef AROLLA_QEXPR_OPERATORS_STRINGS_ARRAY_TRAITS_H_
#define AROLLA_QEXPR_OPERATORS_STRINGS_ARRAY_TRAITS_H_

#include <vector>

#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"

namespace arolla::internal {

template <template <typename T> class ArrayType>
struct ArrayTraits {};

template <>
struct ArrayTraits<DenseArray> {
  template <typename T>
  static DenseArray<T> CreateFromVector(
      const std::vector<OptionalValue<T>>& v) {
    return CreateDenseArray(absl::MakeConstSpan(v));
  }

  template <typename T>
  static DenseArray<T> CreateFromBuffer(Buffer<T> buffer) {
    return {std::move(buffer)};
  }

  using edge_type = DenseArrayEdge;
};

template <>
struct ArrayTraits<Array> {
  template <typename T>
  static Array<T> CreateFromVector(const std::vector<OptionalValue<T>>& v) {
    return CreateArray(absl::MakeConstSpan(v));
  }

  template <typename T>
  static Array<T> CreateFromBuffer(Buffer<T> buffer) {
    return Array<T>(std::move(buffer));
  }

  using edge_type = ArrayEdge;
};

}  // namespace arolla::internal

#endif  // AROLLA_QEXPR_OPERATORS_STRINGS_ARRAY_TRAITS_H_
