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
#ifndef AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_FACTORY_OPS_H_
#define AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_FACTORY_OPS_H_

#include <cstdint>
#include <numeric>
#include <utility>

#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_format.h"
#include "absl//types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/unit.h"

namespace arolla {

// core._array_shape_of operator returns shape of the provided array.
struct DenseArrayShapeOfOp {
  DenseArrayShape operator()(const DenseArray<Unit>& array) const {
    return {array.size()};
  }
};

// array.array_shape_size operator. Accepts a shape of dense array and returns
// its size.
struct DenseArrayShapeSizeOp {
  int64_t operator()(DenseArrayShape shape) const { return shape.size; }
};

// array.resize_array_shape operator returns shape with a different size.
struct DenseArrayResizeShapeOp {
  absl::StatusOr<DenseArrayShape> operator()(DenseArrayShape,
                                             int64_t size) const {
    if (size < 0) {
      return absl::InvalidArgumentError(absl::StrFormat("bad size: %d", size));
    }
    return DenseArrayShape{size};
  }
};

// core._const_array_with_shape operator creates a DenseArray filled with
// the given value of given size.
struct DenseArrayConstWithShapeOp {
  template <typename T>
  DenseArray<T> operator()(EvaluationContext* ctx, const DenseArrayShape& shape,
                           const T& fill_value) const {
    return CreateConstDenseArray<T>(shape.size, fill_value,
                                    &ctx->buffer_factory());
  }

  template <typename T>
  DenseArray<T> operator()(EvaluationContext* ctx, const DenseArrayShape& shape,
                           const OptionalValue<T>& fill_value) const {
    if (fill_value.present) {
      return CreateConstDenseArray<T>(shape.size, fill_value.value,
                                      &ctx->buffer_factory());
    } else {
      return CreateEmptyDenseArray<T>(shape.size, &ctx->buffer_factory());
    }
  }
};

// Implementation of array._iota operator.
struct DenseArrayIotaOp {
  DenseArray<int64_t> operator()(EvaluationContext* ctx,
                                 const DenseArrayShape& shape) const {
    Buffer<int64_t>::Builder bldr(shape.size, &ctx->buffer_factory());
    std::iota(bldr.GetMutableSpan().begin(), bldr.GetMutableSpan().end(),
              int64_t{0});
    return {std::move(bldr).Build()};
  }
};

// array.make_dense_array operator
class MakeDenseArrayOperatorFamily : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type) const final;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_FACTORY_OPS_H_
