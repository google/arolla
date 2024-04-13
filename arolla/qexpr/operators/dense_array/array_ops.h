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
#ifndef AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_DENSE_ARRAY_OPS_H_
#define AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_DENSE_ARRAY_OPS_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// array.at operator.
// Returns the value stored by index or missed if out of range.
struct DenseArrayAtOp {
  template <typename T>
  OptionalValue<T> operator()(EvaluationContext* ctx, const DenseArray<T>& arr,
                              int64_t id) const {
    if (ABSL_PREDICT_FALSE(id < 0 || id >= arr.size())) {
      ReportIndexOutOfRangeError(ctx, id, arr.size());
      return std::nullopt;
    }
    return {arr.present(id), T{arr.values[id]}};
  }

  template <typename T>
  OptionalValue<T> operator()(EvaluationContext* ctx, const DenseArray<T>& arr,
                              OptionalValue<int64_t> id) const {
    return id.present ? (*this)(ctx, arr, id.value) : std::nullopt;
  }

  // If last argument is a list of ids, it returns values for all specified ids.
  // I.e. at(arr, list) -> [at(arr, i) for i in list]
  template <typename T>
  DenseArray<T> operator()(EvaluationContext* ctx, const DenseArray<T>& arr,
                           const DenseArray<int64_t>& ids) const {
    auto fn = [ctx, &arr](int64_t id) -> OptionalValue<view_type_t<T>> {
      if (ABSL_PREDICT_FALSE(id < 0 || id >= arr.size())) {
        ReportIndexOutOfRangeError(ctx, id, arr.size());
        return std::nullopt;
      }
      if (!arr.present(id)) {
        return std::nullopt;
      }
      return arr.values[id];
    };
    auto op = CreateDenseOp<DenseOpFlags::kNoBitmapOffset, decltype(fn), T>(
        fn, &ctx->buffer_factory());
    return op(ids);
  }

 private:
  // The function must be in *.cc to guarantee that it is not inlined for
  // performance reasons.
  static void ReportIndexOutOfRangeError(EvaluationContext* ctx, int64_t index,
                                         int64_t size);
};

// array.slice operator.
struct DenseArraySliceOp {
  template <typename T>
  absl::StatusOr<DenseArray<T>> operator()(EvaluationContext* ctx,
                                           const DenseArray<T>& array,
                                           int64_t offset, int64_t size) const {
    if (offset < 0 || offset > array.size()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected `offset` in [0, %d], but got %d", array.size(), offset));
    }
    if (size < -1 || size > array.size() - offset) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected `size` in [0, %d], but got %d",
                          array.size() - offset, size));
    }
    if (size == -1) {
      size = array.size() - offset;
    }
    // Note: we use ForceNoBimapBitOffset because for performance reasons
    // `lift_to_dense_array` has NoBitmapOffset=true.
    return array.Slice(offset, size)
        .ForceNoBitmapBitOffset(&ctx->buffer_factory());
  }
};

// array.concat operator.
struct DenseArrayConcatOp {
  template <typename T>
  DenseArray<T> operator()(EvaluationContext* ctx, const DenseArray<T>& arr1,
                           const DenseArray<T>& arr2) const {
    typename Buffer<T>::Builder values_bldr(arr1.size() + arr2.size(),
                                            &ctx->buffer_factory());
    auto values_inserter = values_bldr.GetInserter();
    for (const auto& v : arr1.values) values_inserter.Add(v);
    for (const auto& v : arr2.values) values_inserter.Add(v);
    if (arr1.bitmap.empty() && arr2.bitmap.empty()) {
      return {std::move(values_bldr).Build()};
    }
    size_t bitmap_size = bitmap::BitmapSize(arr1.size() + arr2.size());
    bitmap::Bitmap::Builder bitmap_bldr(bitmap_size, &ctx->buffer_factory());
    absl::Span<bitmap::Word> bitmap = bitmap_bldr.GetMutableSpan();
    std::fill(bitmap.begin(), bitmap.end(), bitmap::kFullWord);
    if (!arr1.bitmap.empty()) {
      CopyBits<bitmap::Word>(arr1.size(), arr1.bitmap.begin(),
                             arr1.bitmap_bit_offset, bitmap.begin(), 0);
    }
    if (!arr2.bitmap.empty()) {
      size_t offset = arr1.size();
      CopyBits<bitmap::Word>(arr2.size(), arr2.bitmap.begin(),
                             arr2.bitmap_bit_offset,
                             bitmap.begin() + offset / bitmap::kWordBitCount,
                             offset % bitmap::kWordBitCount);
    }
    return {std::move(values_bldr).Build(), std::move(bitmap_bldr).Build()};
  }
};

// array._present_indices returns indices of all non-missing elements.
class DenseArrayPresentIndicesOp {
 public:
  DenseArray<int64_t> operator()(EvaluationContext* ctx,
                                 const DenseArray<Unit>& input) const {
    const int64_t count =
        bitmap::CountBits(input.bitmap, input.bitmap_bit_offset, input.size());
    Buffer<int64_t>::Builder buffer_builder(count, &ctx->buffer_factory());
    auto inserter = buffer_builder.GetInserter();
    input.ForEach([&](int64_t index, bool present, const auto& /*value*/) {
      if (present) {
        inserter.Add(index);
      }
    });
    return DenseArray<int64_t>{std::move(buffer_builder).Build(count)};
  }
};

// array.present_values returns all non-missing elements.
class DenseArrayPresentValuesOp {
 public:
  template <typename T>
  DenseArray<T> operator()(EvaluationContext* ctx,
                           const DenseArray<T>& input) const {
    const int64_t count =
        bitmap::CountBits(input.bitmap, input.bitmap_bit_offset, input.size());
    typename Buffer<T>::Builder buffer_builder(count, &ctx->buffer_factory());
    auto inserter = buffer_builder.GetInserter();
    input.ForEach([&](int64_t /*index*/, bool present, const auto& value) {
      if (present) {
        inserter.Add(value);
      }
    });
    return DenseArray<T>{std::move(buffer_builder).Build(count)};
  }
};

// array.from_indices_and_values returns an array constructed from the given
// indices and values.
class DenseArrayFromIndicesAndValues {
 public:
  template <typename T>
  DenseArray<T> operator()(EvaluationContext* ctx,
                           const DenseArray<int64_t>& indices,
                           const DenseArray<T>& values, int64_t size) const {
    if (!ValidateInputs(ctx, indices, values.size(), size)) {
      return DenseArray<T>{Buffer<T>(), Buffer<bitmap::Word>()};
    }
    DenseArrayBuilder<T> builder(size, &ctx->buffer_factory());
    const int64_t n = indices.size();
    for (int64_t i = 0; i < n; ++i) {
      if (values.present(i)) {
        builder.Set(indices.values[i], values.values[i]);
      }
    }
    return std::move(builder).Build();
  }

 private:
  // Returns true, if the inputs pass validation.
  static bool ValidateInputs(EvaluationContext* ctx,
                             const DenseArray<int64_t>& indices,
                             int64_t values_size, int64_t size);
};

// array.unique returns an array containing unique non-missing elements.
struct DenseArrayUniqueOp {
  template <typename T>
  DenseArray<T> operator()(EvaluationContext* ctx,
                           const DenseArray<T>& input) const {
    typename Buffer<T>::Builder bldr(input.size(), &ctx->buffer_factory());
    auto inserter = bldr.GetInserter();
    absl::flat_hash_set<view_type_t<T>> unique_values;
    input.ForEachPresent([&](int64_t /*index*/, const auto& value) {
      if (auto [it, inserted] = unique_values.insert(value); inserted) {
        inserter.Add(value);
      }
    });
    return DenseArray<T>{std::move(bldr).Build(unique_values.size())};
  }
};

// array.select selects elements in the first argument if the filter mask is
// present and filters out missing items.
struct DenseArraySelectOp {
  template <typename T>
  absl::StatusOr<DenseArray<T>> operator()(
      EvaluationContext* ctx, const DenseArray<T>& input,
      const DenseArray<Unit>& filter) const {
    if (ABSL_PREDICT_FALSE(input.size() != filter.size())) {
      return SizeMismatchError({input.size(), filter.size()});
    }
    if (filter.bitmap.empty()) {
      return input;
    }
    const int64_t count = filter.PresentCount();
    if (count == 0) {
      return DenseArray<T>();
    }

    DenseArrayBuilder<T> builder(count);

    int64_t offset = 0;
    using view_type = arolla::view_type_t<T>;
    auto fn = [&](int64_t id, Unit mask, OptionalValue<view_type> value) {
      builder.Set(offset++, value);
    };

    RETURN_IF_ERROR(DenseArraysForEachPresent(fn, filter, input));
    return std::move(builder).Build();
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_DENSE_ARRAY_OPS_H_
