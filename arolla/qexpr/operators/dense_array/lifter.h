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
#ifndef AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_LIFTER_H_
#define AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_LIFTER_H_

#include <type_traits>

#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/lifting.h"
#include "arolla/util/meta.h"
#include "arolla/util/view_types.h"

namespace arolla {

// OperatorTraits for an operator on DenseArray lifted from scalar
// SimpleOperator. Fn is a pointwise functor.
// If the operator is cheaper than a single conditional jump, it is recommended
// to add "using run_on_missing = std::true_type;" to the functor definition.
// Limitations on Fn:
// 1) If operator has an argument of type `DenseArray<T>`, then the
//   corresponding argument of Fn should have type `view_type_t<T>` or
//   OptionalValue<view_type_t<T>>. I.e. if the operator works with
//   DenseArray<Bytes> or DenseArray<Text>, then Fn should accept
//   absl::string_view or OptionalValue<absl::string_view>.
// 2) To get an operator with return type DenseArray<T>, the output type of Fn
//   should be one of:
//    - T
//    - OptionalValue<T>
//    - absl::StatusOr<T>
//    - absl::StatusOr<OptionalValue<T>>
template <class Fn, class ArgsList, bool NoBitmapOffset = false>
class DenseArrayLifter;

template <class, class = void>
struct IsRunOnMissingOp : std::false_type {};
template <class T>
struct IsRunOnMissingOp<T, std::void_t<typename T::run_on_missing>>
    : std::true_type {};

template <class Fn, bool NoBitmapOffset, class... Args>
class DenseArrayLifter<Fn, meta::type_list<Args...>, NoBitmapOffset> {
 private:
  template <class T>
  using LiftedFunctorViewType = ::arolla::LiftedType<view_type_t, T>;

  static_assert(
      std::is_invocable_v<const Fn&, LiftedFunctorViewType<Args>...>,
      "Can not construct DenseArray operator from given function. "
      "Verify that these conditions are met: "
      "0) `operator()` is marked as const "
      "1) If operator has an argument of type `DenseArray<T>`, then the "
      "corresponding argument of Fn should have type `view_type_t<T>` or "
      "OptionalValue<view_type_t<T>>. I.e. if the operator works with "
      "DenseArray<Bytes> or DenseArray<Text>, then Fn should accept "
      "absl::string_view or OptionalValue<absl::string_view>. "
      "2) To get an operator with return type DenseArray<T>, the output type "
      "of Fn should be one of:"
      "   - T"
      "   - OptionalValue<T>"
      "   - absl::StatusOr<T>"
      "   - absl::StatusOr<OptionalValue<T>>");

  template <class T>
  using LiftedType = ::arolla::LiftedType<AsDenseArray, T>;

  using OutputValueT = strip_optional_t<meta::strip_template_t<
      absl::StatusOr,
      decltype(Fn()(
          std::declval<meta::strip_template_t<DoNotLiftTag, Args>>()...))>>;

 public:
  // Create an operation that captures all arguments marked with `DoNotLiftTag`
  // and accept other arguments as `DenseArray` in the same order.
  // Note that `args` for lifted arguments are ignored and can be anything.
  template <int ExtraFlags, class... Ts>
  auto CreateDenseOpWithCapturedScalars(EvaluationContext* ctx,
                                        const Ts&... args) const {
    using Tools = LiftingTools<Args...>;
    auto strict_fn = Tools::template CreateFnWithDontLiftCaptured<view_type_t>(
        Fn(), args...);
    constexpr int flags =
        ExtraFlags | (NoBitmapOffset ? DenseOpFlags::kNoBitmapOffset : 0) |
        (IsRunOnMissingOp<Fn>::value ? DenseOpFlags::kRunOnMissing : 0);
    return CreateDenseOp<flags, decltype(strict_fn), OutputValueT>(
        strict_fn, &ctx->buffer_factory());
  }

  auto operator()(EvaluationContext* ctx,
                  const LiftedType<Args>&... args) const {
    using Tools = LiftingTools<Args...>;
    using ResT = absl::StatusOr<DenseArray<OutputValueT>>;
    auto op = CreateDenseOpWithCapturedScalars<0>(ctx, args...);
    return ResT(Tools::CallOnLiftedArgs(op, args...));
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_LIFTER_H_
