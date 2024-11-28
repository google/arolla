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
#ifndef AROLLA_ARRAY_MULTI_EDGE_UTIL_H_
#define AROLLA_ARRAY_MULTI_EDGE_UTIL_H_

#include <cstdint>

#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/id_filter.h"
#include "arolla/array/ops_util.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "arolla/util/view_types.h"

namespace arolla {

// Utilities for complicated group operations on Arrays (e.g. operations
// that uses several edges) that can not be implemented via Accumulators
// framework.
struct ArrayMultiEdgeUtil {
  template <class T>
  using Array = Array<T>;

  template <class T>
  using AsArray = AsArray<T>;

  using Edge = ArrayEdge;

  // Iterates through (states, args...) and applies
  // `fn(State&, view_type_t<ParentTs>...)` to each valid row.
  // ParentTs can be either OptionalValue<T> (optional argument) or T (required
  // argument). A row is valid if all required argument are present.
  // States and all input arrays must have the same size.
  template <class State, class Fn, class... ParentTs>
  static absl::Status ApplyParentArgs(Fn fn, absl::Span<State> states,
                                      meta::type_list<ParentTs...>,
                                      const AsArray<ParentTs>&... args) {
    if (((args.size() != states.size()) || ... || false)) {
      return SizeMismatchError(
          {static_cast<int64_t>(states.size()), args.size()...});
    }
    using ParentUtil =
        array_ops_internal::ArrayOpsUtil</*ConvertToDense=*/true,
                                         meta::type_list<ParentTs...>>;
    ParentUtil util(states.size(), args...);
    util.IterateSimple(
        [&](int64_t id, view_type_t<ParentTs>... v) { fn(states[id], v...); });
    return absl::OkStatus();
  }

  // Applies `fn(State&, int64_t child_id, view_type_t<ChildTs>...)` to each
  // valid row. ChildTs can be either OptionalValue<T> (optional argument) or
  // T (required argument). A row is valid if all required argument are present.
  // `states` are in group index space (i.e. states.size() ==
  // edge.parent_size()) `args` are in child index space (i.e. args.size() ==
  // edge.child_size()). So each valid row of `args...` is used only once, but
  // one state can be used for several rows.
  template <class State, class Fn, class... ChildTs>
  static absl::Status ApplyChildArgs(Fn fn, absl::Span<State> states,
                                     const ArrayEdge& edge,
                                     meta::type_list<ChildTs...>,
                                     const AsArray<ChildTs>&... args) {
    if (states.size() != edge.parent_size()) {
      return SizeMismatchError(
          {static_cast<int64_t>(states.size()), edge.parent_size()});
    }
    if (((args.size() != edge.child_size()) || ... || false)) {
      return SizeMismatchError({edge.child_size(), args.size()...});
    }
    switch (edge.edge_type()) {
      case ArrayEdge::SPLIT_POINTS: {
        using ChildUtil =
            array_ops_internal::ArrayOpsUtil</*ConvertToDense=*/false,
                                             meta::type_list<ChildTs...>>;
        ChildUtil util(edge.child_size(), args...);
        DCHECK(edge.edge_values().IsFullForm());
        const auto& splits = edge.edge_values().dense_data().values.span();
        for (int64_t parent_id = 0; parent_id < edge.parent_size();
             ++parent_id) {
          State& state = states[parent_id];
          util.Iterate(splits[parent_id], splits[parent_id + 1],
                       [&](int64_t child_id, view_type_t<ChildTs>... v) {
                         fn(state, child_id, v...);
                       });
        }
        return absl::OkStatus();
      }
      case ArrayEdge::MAPPING: {
        const auto& mapping = edge.edge_values();
        using MappingAndChildUtil = array_ops_internal::ArrayOpsUtil<
            /*ConvertToDense=*/false, meta::type_list<int64_t, ChildTs...>>;
        MappingAndChildUtil util(edge.child_size(), mapping, args...);
        util.IterateSimple([&](int64_t child_id, int64_t parent_id,
                               view_type_t<ChildTs>... v) {
          fn(states[parent_id], child_id, v...);
        });
        return absl::OkStatus();
      }
      default:
        return absl::InvalidArgumentError("unsupported edge type");
    }
  }

  // Similar to `ApplyChildArgs`, but also produces an output Array<ResT>
  // with the same index space as `args...`.
  // `fn` should return either `view_type_t<ResT>` or
  // `view_type_t<OptionalValue<ResT>>`
  template <class ResT, class State, class Fn, class... ChildTs>
  static absl::StatusOr<Array<ResT>> ProduceResult(
      RawBufferFactory* buf_factory, Fn fn, absl::Span<State> states,
      const ArrayEdge& edge, meta::type_list<ChildTs...> types,
      const AsArray<ChildTs>&... args) {
    if (states.size() != edge.parent_size()) {
      return SizeMismatchError(
          {static_cast<int64_t>(states.size()), edge.parent_size()});
    }
    if (((args.size() != edge.child_size()) || ... || false)) {
      return SizeMismatchError({edge.child_size(), args.size()...});
    }
    switch (edge.edge_type()) {
      case ArrayEdge::SPLIT_POINTS: {
        using ChildUtil =
            array_ops_internal::ArrayOpsUtil</*ConvertToDense=*/false,
                                             meta::type_list<ChildTs...>>;
        ChildUtil util(edge.child_size(), args...);
        DCHECK(edge.edge_values().IsFullForm());
        const auto& splits = edge.edge_values().dense_data().values.span();
        auto process_fn = [&](auto& bldr) {
          for (int64_t parent_id = 0; parent_id < edge.parent_size();
               ++parent_id) {
            State& state = states[parent_id];
            util.Iterate(splits[parent_id], splits[parent_id + 1],
                         [&](int64_t child_id, view_type_t<ChildTs>... v) {
                           bldr.Add(child_id, fn(state, child_id, v...));
                         });
          }
        };
        if (util.PresentCountUpperEstimate() <
            IdFilter::kDenseSparsityLimit * util.size()) {
          SparseArrayBuilder<ResT> bldr(
              util.size(), util.PresentCountUpperEstimate(), buf_factory);
          process_fn(bldr);
          return std::move(bldr).Build();
        } else {
          DenseArrayBuilder<ResT> bldr(util.size(), buf_factory);
          process_fn(bldr);
          return Array<ResT>(std::move(bldr).Build());
        }
      }
      case ArrayEdge::MAPPING: {
        const auto& mapping = edge.edge_values();
        using MappingAndChildUtil = array_ops_internal::ArrayOpsUtil<
            /*ConvertToDense=*/false, meta::type_list<int64_t, ChildTs...>>;
        MappingAndChildUtil util(edge.child_size(), mapping, args...);
        auto process_fn = [&](auto& bldr) {
          util.IterateSimple([&](int64_t child_id, int64_t parent_id,
                                 view_type_t<ChildTs>... v) {
            bldr.Add(child_id, fn(states[parent_id], child_id, v...));
          });
        };
        if (util.PresentCountUpperEstimate() <
            IdFilter::kDenseSparsityLimit * util.size()) {
          SparseArrayBuilder<ResT> bldr(
              util.size(), util.PresentCountUpperEstimate(), buf_factory);
          process_fn(bldr);
          return std::move(bldr).Build();
        } else {
          DenseArrayBuilder<ResT> bldr(util.size(), buf_factory);
          process_fn(bldr);
          return Array<ResT>(std::move(bldr).Build());
        }
      }
      default:
        return absl::InvalidArgumentError("unsupported edge type");
    }
  }
};

}  // namespace arolla

#endif  // AROLLA_ARRAY_MULTI_EDGE_UTIL_H_
