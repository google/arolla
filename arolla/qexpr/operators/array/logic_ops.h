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
#ifndef AROLLA_QEXPR_OPERATORS_ARRAY_LOGIC_OPS_H_
#define AROLLA_QEXPR_OPERATORS_ARRAY_LOGIC_OPS_H_

#include <cstdint>

#include "absl/base/optimization.h"
#include "absl/status/statusor.h"
#include "arolla/array/array.h"
#include "arolla/array/pointwise_op.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/util/meta.h"
#include "arolla/util/view_types.h"

namespace arolla {

// core.presence_or operator returns the first argument if it is present and the
// second argument otherwise.
struct ArrayPresenceOrOp {
  template <typename T>
  absl::StatusOr<Array<T>> operator()(EvaluationContext* ctx,
                                      const Array<T>& lhs,
                                      const Array<T>& rhs) const {
    if (ABSL_PREDICT_FALSE(lhs.size() != rhs.size())) {
      return SizeMismatchError({lhs.size(), rhs.size()});
    }
    if (lhs.IsAllMissingForm()) {
      return rhs;
    } else if (lhs.IsConstForm() || lhs.IsFullForm() ||
               rhs.IsAllMissingForm()) {
      return lhs;
    }
    if (rhs.IsConstForm()) {
      return (*this)(ctx, lhs, rhs.missing_id_value().value);
    }
    auto fn = [&](OptionalValue<view_type_t<T>> a,
                  OptionalValue<view_type_t<T>> b) {
      // NOTE: It is optimized for a specific compiler version.
      // Consider to replace with `return {a.present ? a : b}` later.
      return OptionalValue<view_type_t<T>>{a.present || b.present,
                                           a.present ? a.value : b.value};
    };
    auto dense_fn = CreateDenseOp<DenseOpFlags::kRunOnMissing, decltype(fn), T>(
        fn, &ctx->buffer_factory());
    auto qblock_op =
        ArrayPointwiseOp<T, decltype(dense_fn), decltype(fn),
                         meta::type_list<OptionalValue<T>, OptionalValue<T>>>(
            std::move(dense_fn), std::move(fn), &ctx->buffer_factory());
    return qblock_op(lhs, rhs);
  }

  template <typename T>
  Array<T> operator()(EvaluationContext* ctx, const Array<T>& lhs,
                      const T& rhs) const {
    T missing_id_value = lhs.missing_id_value().AsOptional().value_or(rhs);
    const DenseArray<T>& lhs_dense = lhs.dense_data();
    if (lhs_dense.bitmap.empty()) {
      return Array<T>(lhs.size(), lhs.id_filter(), lhs_dense,
                      std::move(missing_id_value));
    } else {
      view_type_t<T> default_value(rhs);
      typename Buffer<T>::Builder bldr(lhs_dense.size(),
                                       &ctx->buffer_factory());
      lhs_dense.ForEachByGroups([&](int64_t offset) {
        auto inserter = bldr.GetInserter(offset);
        return [=](int64_t i, bool present, view_type_t<T> value) mutable {
          inserter.Add(present ? value : default_value);
        };
      });
      return Array<T>(lhs.size(), lhs.id_filter(),
                      DenseArray<T>{std::move(bldr).Build()},
                      std::move(missing_id_value));
    }
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_ARRAY_LOGIC_OPS_H_
