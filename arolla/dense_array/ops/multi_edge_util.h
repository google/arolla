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
#ifndef AROLLA_DENSE_ARRAY_OPS_MULTI_EDGE_UTIL_H_
#define AROLLA_DENSE_ARRAY_OPS_MULTI_EDGE_UTIL_H_

#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/util.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Utilities for complicated group operations on DenseArrays (e.g. operations
// that uses several edges) that can not be implemented via Accumulators
// framework.
struct DenseArrayMultiEdgeUtil {
  template <class T>
  using Array = DenseArray<T>;

  template <class T>
  using AsArray = AsDenseArray<T>;

  using Edge = DenseArrayEdge;

  // Iterates through (states, args...) and applies
  // `fn(State&, view_type_t<ParentTs>...)` to each valid row.
  // ParentTs can be either OptionalValue<T> (optional argument) or T (required
  // argument). A row is valid if all required argument are present.
  // States and all input arrays must have the same size.
  template <class State, class Fn, class... ParentTs>
  static absl::Status ApplyParentArgs(Fn fn, absl::Span<State> states,
                                      meta::type_list<ParentTs...>,
                                      const AsDenseArray<ParentTs>&... args) {
    using ParentUtil =
        dense_ops_internal::DenseOpsUtil<meta::type_list<ParentTs...>>;
    if (((args.size() != states.size()) || ... || false)) {
      return SizeMismatchError(
          {static_cast<int64_t>(states.size()), args.size()...});
    }
    auto fn_with_state = [&](int64_t id, bool valid,
                             view_type_t<ParentTs>... v) {
      if (valid) fn(states[id], v...);
    };
    ParentUtil::IterateFromZero(fn_with_state, states.size(), args...);
    return absl::OkStatus();
  }

  // Applies `fn(State&, int64_t child_id, view_type_t<ChildTs>...)` to each
  // valid row. ChildTs can be either OptionalValue<T> (optional argument) or
  // T (required argument). A row is valid if all required argument are present.
  // `states` are in group index space (i.e. states.size() ==
  // edge.parent_size()) `args` are in child index space (i.e. args.size() ==
  // edge.child_size()). So each valid row of `args...` is used only once. Each
  // state is used for all child rows in the corresponding group.
  template <class State, class Fn, class... ChildTs>
  static absl::Status ApplyChildArgs(Fn fn, absl::Span<State> states,
                                     const DenseArrayEdge& edge,
                                     meta::type_list<ChildTs...>,
                                     const AsDenseArray<ChildTs>&... args) {
    if (states.size() != edge.parent_size()) {
      return SizeMismatchError(
          {static_cast<int64_t>(states.size()), edge.parent_size()});
    }
    if (((args.size() != edge.child_size()) || ... || false)) {
      return SizeMismatchError({edge.child_size(), args.size()...});
    }
    switch (edge.edge_type()) {
      case DenseArrayEdge::SPLIT_POINTS: {
        using ChildUtil =
            dense_ops_internal::DenseOpsUtil<meta::type_list<ChildTs...>>;
        absl::Span<const int64_t> splits = edge.edge_values().values.span();
        for (int64_t parent_id = 0; parent_id < edge.parent_size();
             ++parent_id) {
          State& state = states[parent_id];
          auto fn_with_state = [&](int64_t child_id, bool valid,
                                   view_type_t<ChildTs>... v) {
            if (valid) fn(state, child_id, v...);
          };
          ChildUtil::Iterate(fn_with_state, splits[parent_id],
                             splits[parent_id + 1], args...);
        }
        return absl::OkStatus();
      }
      case DenseArrayEdge::MAPPING: {
        auto fn_with_state = [&](int64_t child_id, bool valid,
                                 int64_t parent_id, view_type_t<ChildTs>... v) {
          if (valid) fn(states[parent_id], child_id, v...);
        };
        const DenseArray<int64_t>& mapping = edge.edge_values();
        using MappingAndChildUtil = dense_ops_internal::DenseOpsUtil<
            meta::type_list<int64_t, ChildTs...>>;
        MappingAndChildUtil::IterateFromZero(fn_with_state, edge.child_size(),
                                             mapping, args...);
        return absl::OkStatus();
      }
      default:
        return absl::InvalidArgumentError("unsupported edge type");
    }
  }

  // Similar to `ApplyChildArgs`, but also produces an output DenseArray<ResT>
  // with the same index space as `args...`.
  // `fn` should return either `view_type_t<ResT>` or
  // `view_type_t<OptionalValue<ResT>>`
  template <class ResT, class State, class Fn, class... ChildTs>
  static absl::StatusOr<DenseArray<ResT>> ProduceResult(
      RawBufferFactory* buf_factory, Fn fn, absl::Span<State> states,
      const DenseArrayEdge& edge, meta::type_list<ChildTs...> types,
      const AsDenseArray<ChildTs>&... args) {
    DenseArrayBuilder<ResT> builder(edge.child_size(), buf_factory);
    auto process_result_fn = [&](State& state, int64_t child_id,
                                 view_type_t<ChildTs>... v) {
      builder.Set(child_id, fn(state, child_id, v...));
    };
    RETURN_IF_ERROR(
        ApplyChildArgs(process_result_fn, states, edge, types, args...));
    return std::move(builder).Build();
  }
};

}  // namespace arolla

#endif  // AROLLA_DENSE_ARRAY_OPS_MULTI_EDGE_UTIL_H_
