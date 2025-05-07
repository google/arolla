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
#include "arolla/qexpr/operators/strings/array_traits.h"

#include <cstdint>
#include <optional>
#include <type_traits>
#include <vector>

#include "gtest/gtest.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"

namespace arolla {
namespace {

TEST(ArrayTraitsTest, TraitsOfDenseArray) {
  using TraitsOfDenseArray = internal::ArrayTraits<DenseArray>;

  std::vector<OptionalValue<int32_t>> values = {
      OptionalValue<int32_t>(1), std::nullopt, OptionalValue<int32_t>(3)};

  DenseArray<int32_t> dense_array_from_vector =
      TraitsOfDenseArray::CreateFromVector<int32_t>(values);
  EXPECT_TRUE(ArraysAreEquivalent(
      dense_array_from_vector,
      CreateDenseArray(absl::Span<const OptionalValue<int32_t>>(values))));

  Buffer<int32_t> buffer = CreateBuffer({1, 3});

  DenseArray<int32_t> dense_array_from_buffer =
      TraitsOfDenseArray::CreateFromBuffer<int32_t>(buffer);
  EXPECT_TRUE(ArraysAreEquivalent(dense_array_from_buffer,
                                  DenseArray<int32_t>(buffer)));

  static_assert(
      std::is_same<TraitsOfDenseArray::edge_type, DenseArrayEdge>::value);
}

TEST(ArrayTraitsTest, TraitsOfArray) {
  using TraitsOfArray = internal::ArrayTraits<Array>;

  std::vector<OptionalValue<int32_t>> values = {
      OptionalValue<int32_t>(1), std::nullopt, OptionalValue<int32_t>(3)};

  Array<int32_t> array_from_vector =
      TraitsOfArray::CreateFromVector<int32_t>(values);
  EXPECT_TRUE(ArraysAreEquivalent(
      array_from_vector,
      CreateArray(absl::Span<const OptionalValue<int32_t>>(values))));

  Buffer<int32_t> buffer = CreateBuffer({1, 3});

  Array<int32_t> array_from_buffer =
      TraitsOfArray::CreateFromBuffer<int32_t>(buffer);
  EXPECT_TRUE(ArraysAreEquivalent(array_from_buffer, Array<int32_t>(buffer)));

  static_assert(
      std::is_same<internal::ArrayTraits<Array>::edge_type, ArrayEdge>::value);
}

}  // namespace
}  // namespace arolla
