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
#ifndef AROLLA_ARRAY_POINTWISE_OP_H_
#define AROLLA_ARRAY_POINTWISE_OP_H_

#include <cstdint>
#include <optional>
#include <utility>

#include "absl/status/statusor.h"
#include "arolla/array/array.h"
#include "arolla/array/id_filter.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/meta.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Generic implementation of a pointwise operation on Arrays on top of
// a pointwise operation on DenseArrays.

template <class ResT, class DenseArrayOp, class PointwiseFn, class ArgList>
class ArrayPointwiseOp;

template <class ResT, class DenseArrayOp, class PointwiseFn, class... Args>
class ArrayPointwiseOp<ResT, DenseArrayOp, PointwiseFn,
                       meta::type_list<Args...>> {
 public:
  explicit ArrayPointwiseOp(DenseArrayOp dense_op, PointwiseFn pointwise_fn,
                            RawBufferFactory* buf_factory)
      : dense_op_(std::move(dense_op)),
        pointwise_fn_(std::move(pointwise_fn)),
        buf_factory_(std::move(buf_factory)) {}

  template <class... As>
  absl::StatusOr<Array<ResT>> operator()(const As&... args) const {
    ASSIGN_OR_RETURN(int64_t size, GetCommonSize(args...));

    if ((((!is_optional_v<Args> && args.IsAllMissingForm())) || ...)) {
      return Array<ResT>(size, std::nullopt);
    }

    IdFilter id_filter(IdFilter::kEmpty);
    DenseArray<ResT> data;

    if (IsSameIdFilter(args...)) {
      id_filter = First(args...).id_filter();
      if (id_filter.type() != IdFilter::kEmpty) {
        ASSIGN_OR_RETURN(data, ApplyDenseOp(args...));
      }
    } else {
      if ((CanBeIntersected<Args>(args) || ...)) {
        IdFilter full(IdFilter::kFull);
        id_filter = IdFilter::UpperBoundIntersect(
            (CanBeIntersected<Args>(args) ? args.id_filter() : full)...);
      } else {
        id_filter =
            IdFilter::UpperBoundMerge(size, buf_factory_, args.id_filter()...);
      }
      ASSIGN_OR_RETURN(data, ApplyDenseOp(args.WithIds(
                                 id_filter, args.missing_id_value())...));
    }

    auto missing_id_value = pointwise_fn_(args.missing_id_value()...);
    RETURN_IF_ERROR(GetStatusOrOk(missing_id_value));
    return Array<ResT>(
        size, std::move(id_filter), std::move(data),
        OptionalValue<ResT>(UnStatus(std::move(missing_id_value))));
  }

 private:
  template <class... As>
  absl::StatusOr<DenseArray<ResT>> ApplyDenseOp(const As&... args) const {
    return dense_op_(args.dense_data()...);
  }

  template <class Arg, class A>
  static bool CanBeIntersected(const A& arg) {
    return !is_optional_v<Arg> && !arg.HasMissingIdValue();
  }

  template <class A, class... As>
  static absl::StatusOr<int64_t> GetCommonSize(const A& a, const As&... as) {
    if (!((a.size() == as.size()) && ...)) {
      return SizeMismatchError({a.size(), as.size()...});
    } else {
      return a.size();
    }
  }

  template <class A, class... As>
  static bool IsSameIdFilter(const A& a, const As&... as) {
    return ((a.id_filter().IsSame(as.id_filter()) && ...));
  }

  template <class A, class... As>
  static const A& First(const A& a, const As&...) {
    return a;
  }

  DenseArrayOp dense_op_;
  PointwiseFn pointwise_fn_;
  RawBufferFactory* buf_factory_;
};

// Creates an operation on Arrays from a pointwise functor. Argument types
// will be deduced from the function `Fn::operator()` (so it can't be overloaded
// or templated). It is equivalent to CreateDenseOp from dense_array/ops.
// See examples in dense_array/ops/dense_ops.h.
//
// Side effect in `fn` is discouraged. There is no guarantee that `fn` is called
// once per every present item or that it's called in a particular order.
// Each call of the returned op is independent (unless `fn` has mutable internal
// state).
//
// `flags` is BITWISE OR of DenseOpFlags.
template <int flags, class Fn,
          class ResT = dense_ops_internal::result_base_t<Fn>>
auto CreateArrayOp(Fn fn,
                   RawBufferFactory* buf_factory = GetHeapBufferFactory()) {
  auto dense_op = CreateDenseOp<flags | DenseOpFlags::kNoSizeValidation,
                                decltype(fn), ResT>(fn, buf_factory);
  auto optional_fn = WrapFnToAcceptOptionalArgs(fn);
  using Op = ArrayPointwiseOp<ResT, decltype(dense_op), decltype(optional_fn),
                              typename meta::function_traits<Fn>::arg_types>;
  return Op(dense_op, optional_fn, buf_factory);
}

template <class Fn, class ResT = dense_ops_internal::result_base_t<Fn>>
auto CreateArrayOp(Fn fn,
                   RawBufferFactory* buf_factory = GetHeapBufferFactory()) {
  return CreateArrayOp<0, decltype(fn), ResT>(fn, buf_factory);
}

}  // namespace arolla

#endif  // AROLLA_ARRAY_POINTWISE_OP_H_
