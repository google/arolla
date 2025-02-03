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
#ifndef AROLLA_QEXPR_OPERATORS_STRINGS_SPLIT_H_
#define AROLLA_QEXPR_OPERATORS_STRINGS_SPLIT_H_

#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/qtype/types.h"  // IWYU pragma: keep
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"  // IWYU pragma: keep
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"  // IWYU pragma: keep
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace split_operators_impl {

template <template <typename T> class ArrayType>
struct ArrayTraits {};

template <>
struct ArrayTraits<DenseArray> {
  template <typename T>
  static DenseArray<T> CreateFromVector(
      const std::vector<OptionalValue<T>>& v) {
    return CreateDenseArray(absl::Span<const OptionalValue<T>>(v));
  }

  template <typename T>
  static DenseArray<T> CreateFromBuffer(const Buffer<T>& buffer) {
    return {buffer};
  }

  using edge_type = DenseArrayEdge;
};

template <>
struct ArrayTraits<Array> {
  template <typename T>
  static Array<T> CreateFromVector(const std::vector<OptionalValue<T>>& v) {
    return CreateArray(absl::Span<const OptionalValue<T>>(v));
  }

  template <typename T>
  static Array<T> CreateFromBuffer(const Buffer<T>& buffer) {
    return Array<T>(buffer);
  }

  using edge_type = ArrayEdge;
};

}  // namespace split_operators_impl
// strings.split splits the string with the given separator, and returns the
// array of split substrings and the edge that maps it to the original array in
// a tuple.
struct SplitOp {
  template <template <typename T> class ArrayType, typename StringType>
  absl::StatusOr<std::tuple<
      ArrayType<StringType>,
      typename split_operators_impl::ArrayTraits<ArrayType>::edge_type>>
  operator()(const ArrayType<StringType>& array,
             const OptionalValue<StringType>& separator) const {
    std::vector<int64_t> splits_per_string{0};
    splits_per_string.reserve(array.size());
    std::vector<OptionalValue<StringType>> splits;
    splits.reserve(array.size());
    array.ForEach([&](int64_t id, bool present, absl::string_view value) {
      if (!present) {
        splits_per_string.push_back(splits_per_string.back());
      } else {
        std::vector<absl::string_view> new_splits;
        if (separator.present) {
          new_splits =
              absl::StrSplit(value, absl::string_view(separator.value));
        } else {
          new_splits = absl::StrSplit(value, absl::ByAnyChar(" \t\v\f\r\n"),
                                      absl::SkipEmpty());
        }
        splits_per_string.push_back(splits_per_string.back() +
                                    new_splits.size());
        for (auto it = new_splits.begin(); it != new_splits.end(); ++it) {
          splits.emplace_back(*it);
        }
      }
    });
    ASSIGN_OR_RETURN(
        auto edge,
        split_operators_impl::ArrayTraits<ArrayType>::edge_type::
            FromSplitPoints(
                split_operators_impl::ArrayTraits<ArrayType>::
                    template CreateFromBuffer<int64_t>(Buffer<int64_t>::Create(
                        std::move(splits_per_string)))));
    return std::make_tuple(
        split_operators_impl::ArrayTraits<ArrayType>::template CreateFromVector<
            StringType>(splits),
        edge);
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_STRINGS_SPLIT_H_
