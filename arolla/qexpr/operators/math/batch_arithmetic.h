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
#ifndef AROLLA_QEXPR_OPERATORS_MATH_BATCH_ARITHMETIC_H_
#define AROLLA_QEXPR_OPERATORS_MATH_BATCH_ARITHMETIC_H_

#include <type_traits>

#include "absl/log/check.h"
#include "absl/types/span.h"
#include "Eigen/Core"

// Implements basic operations +,-,* for spans using eigen3.
// Usage examples:
//   BatchAdd<float>()(mutable_span_res, span1, span2);
//   BatchSub<int64_t>()(mutable_span_res, span1, span2);
//   BatchProd<double>()(mutable_span_res, span1, span2);
//   float sum = BatchAggSum<float>(span);

namespace arolla {

namespace batch_arithmetic_internal {

template <typename T>
using DynamicEigenVector = Eigen::Array<T, 1, Eigen::Dynamic, Eigen::RowMajor>;

template <typename T>
using DynamicEigenVectorView = Eigen::Map<const DynamicEigenVector<T>>;

template <typename T>
using DynamicMutableEigenVectorView = Eigen::Map<DynamicEigenVector<T>>;

template <typename T, class OP, typename InternalT = T>
struct BinaryEigenOperation {
  void operator()(absl::Span<T> result, absl::Span<const T> a,
                  absl::Span<const T> b) const {
    auto size = a.size();
    DCHECK_EQ(size, b.size());
    DCHECK_EQ(size, result.size());
    static_assert(
        sizeof(T) == sizeof(InternalT) &&
            std::is_floating_point_v<T> == std::is_floating_point_v<InternalT>,
        "Incorrect InternalT");
    const auto* a_data = reinterpret_cast<const InternalT*>(a.data());
    const auto* b_data = reinterpret_cast<const InternalT*>(b.data());
    auto* result_data = reinterpret_cast<InternalT*>(result.data());
    DynamicEigenVectorView<InternalT> eigen_a(a_data, size);
    DynamicEigenVectorView<InternalT> eigen_b(b_data, size);
    DynamicMutableEigenVectorView<InternalT> eigen_result(result_data, size);
    OP::Apply(eigen_a, eigen_b, &eigen_result);
  }
};

struct ProdOp {
  template <typename T, typename RT>
  static void Apply(const T& a, const T& b, RT* c) {
    *c = a * b;
  }
};

struct AddOp {
  template <typename T, typename RT>
  static void Apply(const T& a, const T& b, RT* c) {
    *c = a + b;
  }
};

struct SubOp {
  template <typename T, typename RT>
  static void Apply(const T& a, const T& b, RT* c) {
    *c = a - b;
  }
};

template <typename T>
static auto MakeUnsignedIfIntegralFn() {
  if constexpr (std::is_integral_v<T>) {
    return std::make_unsigned_t<T>();
  } else {
    return T();
  }
}
// We apply unsigned operations for signed integral types in order to prevent
// "UndefinedBehaviorSanitizer: signed-integer-overflow" for missed
// (uninitialized) values. Used by Prod, Add, and Sub.
template <typename T>
using UnsignedIfIntegral = decltype(MakeUnsignedIfIntegralFn<T>());

}  // namespace batch_arithmetic_internal

template <typename T>
using BatchAdd = batch_arithmetic_internal::BinaryEigenOperation<
    T, batch_arithmetic_internal::AddOp,
    batch_arithmetic_internal::UnsignedIfIntegral<T>>;

template <typename T>
using BatchSub = batch_arithmetic_internal::BinaryEigenOperation<
    T, batch_arithmetic_internal::SubOp,
    batch_arithmetic_internal::UnsignedIfIntegral<T>>;

template <typename T>
using BatchProd = batch_arithmetic_internal::BinaryEigenOperation<
    T, batch_arithmetic_internal::ProdOp,
    batch_arithmetic_internal::UnsignedIfIntegral<T>>;

template <typename T>
T BatchAggSum(absl::Span<const T> data) {
  batch_arithmetic_internal::DynamicEigenVectorView<T> e_data(data.data(),
                                                              data.size());
  return e_data.sum();
}

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_MATH_BATCH_ARITHMETIC_H_
