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
#ifndef AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_AGG_OPS_H_
#define AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_AGG_OPS_H_

#include <type_traits>

#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qexpr/aggregation_ops_interface.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/util/meta.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Template for a group_op dense array operator with a specified accumulator.
template <typename Accumulator, typename GroupTypes, typename DetailTypes>
class DenseArrayGroupLifter;

template <typename Accumulator, typename... GroupTs, typename... DetailTs>
class DenseArrayGroupLifter<Accumulator, meta::type_list<GroupTs...>,
                            meta::type_list<DetailTs...>> {
 private:
  template <typename Edge, typename T>
  using GArg =
      std::conditional_t<std::is_same_v<Edge, DenseArrayGroupScalarEdge>, T,
                         AsDenseArray<T>>;

 public:
  template <typename Edge, typename... Ts>
  auto operator()(EvaluationContext* ctx, const GArg<Edge, GroupTs>&... g_args,
                  const AsDenseArray<DetailTs>&... d_args, const Edge& edge,
                  const Ts&... init_args) const
      -> decltype(std::declval<DenseGroupOps<Accumulator>&>().Apply(
          edge, g_args..., d_args...)) {
    auto accumulator = CreateAccumulator<Accumulator>(init_args...);
    DenseGroupOps<Accumulator> agg(&ctx->buffer_factory(),
                                   std::move(accumulator));

    return agg.Apply(edge, g_args..., d_args...);
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_DENSE_ARRAY_AGG_OPS_H_
