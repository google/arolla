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
#ifndef AROLLA_QEXPR_OPERATORS_ARRAY_FACTORY_OPS_H_
#define AROLLA_QEXPR_OPERATORS_ARRAY_FACTORY_OPS_H_

#include <cstdint>
#include <numeric>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/util/unit.h"

namespace arolla {

// core._array_shape_of operator returns shape of the provided array.
struct ArrayShapeOfOp {
  ArrayShape operator()(const Array<Unit>& array) const {
    return {array.size()};
  }
};

// array.array_shape_size operator. Accepts a shape of dense array and returns
// its size.
struct ArrayShapeSizeOp {
  int64_t operator()(ArrayShape shape) const { return shape.size; }
};

// array.resize_array_shape operator returns shape with a different size.
struct ArrayResizeShapeOp {
  absl::StatusOr<ArrayShape> operator()(ArrayShape, int64_t size) const {
    if (size < 0) {
      return absl::InvalidArgumentError(absl::StrFormat("bad size: %d", size));
    }
    return ArrayShape{size};
  }
};

// core._const_array_with_shape operator creates a Array filled with
// the given value of given size.
struct ArrayConstWithShapeOp {
  template <typename T>
  Array<T> operator()(const ArrayShape& shape, const T& fill_value) const {
    return Array<T>(shape.size, fill_value);
  }

  template <typename T>
  Array<T> operator()(const ArrayShape& shape,
                      const OptionalValue<T>& fill_value) const {
    return Array<T>(shape.size, fill_value);
  }
};

// array._as_dense_array operator creates a DenseArray from a Array.
struct ArrayAsDenseArrayOp {
  template <typename T>
  DenseArray<T> operator()(EvaluationContext* ctx,
                           const Array<T>& array) const {
    return array.ToDenseForm(&ctx->buffer_factory())
        .dense_data()
        // Note: we use ForceNoBimapBitOffset because for performance reasons
        // `lift_to_dense_array` has NoBitmapOffset=true.
        .ForceNoBitmapBitOffset(&ctx->buffer_factory());
  }
};

// array._as_qblock operator creates a Array from a DenseArray.
struct ArrayFromDenseArrayOp {
  template <typename T>
  Array<T> operator()(const DenseArray<T>& array) const {
    return Array<T>(array);
  }
};

// Implementation of array._iota operator.
// Shape argument is used by Expr to determine output type.
struct ArrayIotaOp {
  Array<int64_t> operator()(EvaluationContext* ctx,
                            const ArrayShape& shape) const {
    Buffer<int64_t>::Builder bldr(shape.size, &ctx->buffer_factory());
    std::iota(bldr.GetMutableSpan().begin(), bldr.GetMutableSpan().end(),
              int64_t{0});
    return Array<int64_t>(DenseArray<int64_t>{std::move(bldr).Build()});
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_ARRAY_FACTORY_OPS_H_
