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
#ifndef AROLLA_QEXPR_OPERATORS_MATH_BINARY_SEARCH_H_
#define AROLLA_QEXPR_OPERATORS_MATH_BINARY_SEARCH_H_

#include <cstdint>
#include <optional>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/array/array.h"
#include "arolla/array/pointwise_op.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/util/binary_search.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// math._searchsorted_impl(haystack, needle, right) operator.
struct SearchSortedOp {
  template <typename T>
  static int64_t SearchFull(const DenseArray<T>& haystack, T needle,
                            OptionalValue<bool> right) {
    return (right.present && right.value)
               ? UpperBound(needle, haystack.values.span())
               : LowerBound(needle, haystack.values.span());
  }

  template <typename T>
  static absl::Status VerifyHaystack(const DenseArray<T>& haystack) {
    if (!haystack.IsFull()) {
      return absl::UnimplementedError(
          "math.searchsorted operator supports only full haystacks");
    }
    return absl::OkStatus();
  }

  template <typename T>
  absl::StatusOr<int64_t> operator()(const DenseArray<T>& haystack, T needle,
                                     OptionalValue<bool> right) const {
    RETURN_IF_ERROR(VerifyHaystack(haystack));
    return SearchFull(haystack, needle, right);
  }

  template <typename T>
  absl::StatusOr<OptionalValue<int64_t>> operator()(
      const DenseArray<T>& haystack, const OptionalValue<T>& needle,
      OptionalValue<bool> right) const {
    RETURN_IF_ERROR(VerifyHaystack(haystack));
    if (needle.present) {
      return SearchFull(haystack, needle.value, right);
    } else {
      return std::nullopt;
    }
  }

  template <typename T>
  absl::StatusOr<DenseArray<int64_t>> operator()(
      EvaluationContext* ctx, const DenseArray<T>& haystack,
      const DenseArray<T>& needle, OptionalValue<bool> right) const {
    RETURN_IF_ERROR(VerifyHaystack(haystack));
    auto op = CreateDenseOp<DenseOpFlags::kNoBitmapOffset>(
        [&](view_type_t<T> needle) -> int64_t {
          return SearchFull(haystack, needle, right);
        },
        &ctx->buffer_factory());
    return op(needle);
  }

  template <typename T>
  absl::StatusOr<Array<int64_t>> operator()(EvaluationContext* ctx,
                                            const DenseArray<T>& haystack,
                                            const Array<T>& needle,
                                            OptionalValue<bool> right) const {
    RETURN_IF_ERROR(VerifyHaystack(haystack));
    auto op = CreateArrayOp<DenseOpFlags::kNoBitmapOffset>(
        [&](view_type_t<T> needle) -> int64_t {
          return SearchFull(haystack, needle, right);
        },
        &ctx->buffer_factory());
    return op(needle);
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_MATH_BINARY_SEARCH_H_
