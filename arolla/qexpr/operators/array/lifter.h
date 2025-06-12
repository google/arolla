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
#ifndef AROLLA_QEXPR_OPERATORS_ARRAY_LIFTER_H_
#define AROLLA_QEXPR_OPERATORS_ARRAY_LIFTER_H_

#include <type_traits>

#include "absl/status/statusor.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/group_op.h"
#include "arolla/array/pointwise_op.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/aggregation_ops_interface.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/lift_to_optional_operator.h"
#include "arolla/qexpr/lifting.h"
#include "arolla/qexpr/operators/dense_array/lifter.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Functor for an operator on Arrays. It allows to create a Array
// operator from a functor that implements DenseArray QExpr operator.
//
// DenseArrayOp - an operation on DenseArrays.
// PointwiseFn - corresponding scalar operation with optional arguments
//               (needed to process missing id values).
// Usage example:
//
//
// struct AddFn { float operator()(float a, float b) const { return a + b; } };
// struct EvalMyCurveFn {
//    float operator()(const MyCurve& curve, float x) const {
//        return curve.Eval(x);
//    }
// };
//
// using OpAdd = ArrayPointwiseLifter<AddFn, meta::type_list<float, float>>;
// using OpEvalMyCurve = ArrayPointwiseLifter<
//     EvalMyCurveFn,
//     meta::type_list<arolla::DoNotLiftTag<MyCurve>, float>>;
template <class Fn, class ArgsList>
class ArrayPointwiseLifter;

template <class Fn, class... Args>
class ArrayPointwiseLifter<Fn, meta::type_list<Args...>> {
 private:
  template <class T>
  using LiftedType = ::arolla::LiftedType<AsArray, T>;

  using ResT = strip_optional_t<meta::strip_template_t<
      absl::StatusOr,
      decltype(Fn()(
          std::declval<meta::strip_template_t<DoNotLiftTag, Args>>()...))>>;

 public:
  absl::StatusOr<Array<ResT>> operator()(
      EvaluationContext* ctx, const LiftedType<Args>&... args) const {
    auto dense_op = DenseArrayLifter<Fn, meta::type_list<Args...>>()
                        .template CreateDenseOpWithCapturedScalars<
                            DenseOpFlags::kNoSizeValidation>(ctx, args...);
    auto pointwise_op = OptionalLiftedOperator<Fn, meta::type_list<Args...>>()
                            .CreateOptionalOpWithCapturedScalars(args...);
    using Tools = LiftingTools<Args...>;
    using Op =
        ArrayPointwiseOp<ResT, decltype(dense_op), decltype(pointwise_op),
                         typename Tools::LiftableArgs>;
    return Tools::CallOnLiftedArgs(
        Op(dense_op, pointwise_op, &ctx->buffer_factory()), args...);
  }
};

// Functor for an operator on Arrays. It allows to create a Array
// operator from a functor that implements DenseArray QExpr operator.
// Prefer `ArrayPointwiseLifter` in case of standard
//
// DenseArrayOp - an operation on DenseArrays.
// PointwiseFn - corresponding scalar operation with optional arguments
//               (needed to process missing id values).
// Usage example:
//
// struct AddDenseArrayFn {
//   DenseArray<float> operator()(const DenseArray<float>& a,
//                                const DenseArray<float>& b) const { ... }
// };
//
// struct AddOptionalFn {
//  OptionalValue<float> operator()(
//    OptionalValue<float> a, OptionalValue<float> b) const {
//      if (a.present && b.present) {
//        return a.value + b.value;
//      }
//      return OptionalValue<float>();
//    }
// };
//
// using OpAdd = ArrayPointwiseLifterOnDenseOp<
//      AddDenseArrayFn, AddOptionalFn, meta::type_list<float, float>>;
template <class DenseArrayOp, class PointwiseFn, class LiftableArgs>
class ArrayPointwiseLifterOnDenseOp;

template <class DenseArrayOp, class PointwiseFn, class... LiftableArgs>
class ArrayPointwiseLifterOnDenseOp<DenseArrayOp, PointwiseFn,
                                    meta::type_list<LiftableArgs...>> {
 private:
  using DenseOpResT = decltype(DenseArrayOp()(
      nullptr, std::declval<AsDenseArray<LiftableArgs>>()...));
  using ResT =
      meta::strip_template_t<DenseArray, strip_statusor_t<DenseOpResT>>;

 public:
  absl::StatusOr<Array<ResT>> operator()(
      EvaluationContext* ctx, const AsArray<LiftableArgs>&... args) const {
    auto dense_op = [ctx](const AsDenseArray<LiftableArgs>&... ds) {
      return DenseArrayOp()(ctx, ds...);
    };
    auto pointwise_op = [](const wrap_with_optional_t<LiftableArgs>&... args) {
      return PointwiseFn()(args...);
    };
    using Op =
        ArrayPointwiseOp<ResT, decltype(dense_op), decltype(pointwise_op),
                         meta::type_list<LiftableArgs...>>;
    return Op(dense_op, pointwise_op, &ctx->buffer_factory())(args...);
  }
};

// Template for a group_op array operator with a specified accumulator.
template <typename Accumulator, typename GroupTypes, typename DetailTypes>
class ArrayGroupLifter;

template <typename Accumulator, typename... GroupTs, typename... DetailTs>
class ArrayGroupLifter<Accumulator, meta::type_list<GroupTs...>,
                       meta::type_list<DetailTs...>> {
 private:
  template <typename Edge, typename T>
  using GArg = std::conditional_t<std::is_same_v<Edge, ArrayGroupScalarEdge>, T,
                                  AsArray<T>>;

 public:
  template <typename Edge, typename... Ts>
  auto operator()(EvaluationContext* ctx, const GArg<Edge, GroupTs>&... g_args,
                  const AsArray<DetailTs>&... d_args, const Edge& edge,
                  const Ts&... init_args) const
      -> decltype(std::declval<ArrayGroupOp<Accumulator>&>().Apply(edge,
                                                                   g_args...,
                                                                   d_args...)) {
    auto accumulator =
        CreateAccumulator<Accumulator>(ctx->options(), init_args...);
    ArrayGroupOp<Accumulator> agg(&ctx->buffer_factory(),
                                  std::move(accumulator));
    return agg.Apply(edge, g_args..., d_args...);
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_ARRAY_LIFTER_H_
