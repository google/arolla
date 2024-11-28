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
#ifndef AROLLA_DENSE_ARRAY_OPS_DENSE_GROUP_OPS_H_
#define AROLLA_DENSE_ARRAY_OPS_DENSE_GROUP_OPS_H_

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/util.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/meta.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace dense_ops_internal {

template <class Accumulator, class ParentTypes, class ChildTypes,
          bool ForwardId = false>
class DenseGroupOpsImpl;

template <class Accumulator, class... ParentTs, class... ChildTs,
          bool ForwardId>
class DenseGroupOpsImpl<Accumulator, meta::type_list<ParentTs...>,
                        meta::type_list<ChildTs...>, ForwardId> {
  using ParentUtil = DenseOpsUtil<meta::type_list<ParentTs...>>;
  using ChildUtil = DenseOpsUtil<meta::type_list<ChildTs...>>;
  using ResT = strip_optional_t<typename Accumulator::result_type>;

  static constexpr bool kIsAggregator = Accumulator::IsAggregator();
  static constexpr bool kIsPartial = Accumulator::IsPartial();
  static constexpr bool kIsFull = Accumulator::IsFull();

 public:
  // DenseGroupOps constructor.
  //
  // Arguments:
  //  `buffer_factory` is the factory used to generate results.
  //  `empty_accumulator` is an Accumulator instance used as a prototype for
  //      creating new accumulators. Note that a given accumulator may be used
  //      for multiple groups within a single operation.
  explicit DenseGroupOpsImpl(RawBufferFactory* buffer_factory,
                             Accumulator empty_accumulator = Accumulator())
      : buffer_factory_(buffer_factory),
        empty_accumulator_(std::move(empty_accumulator)) {}

  // Applies this group operator.
  //
  // Arguments:
  // `edge`       the edge defining the mapping between the parent and child
  //              index types.
  // `values...`  the Containers corresponding the this group operator's parent
  //              and child features, in the order defined within the
  //              Accumulator.
  absl::StatusOr<DenseArray<ResT>> Apply(
      const DenseArrayEdge& edge, const AsDenseArray<ParentTs>&... p_args,
      const AsDenseArray<ChildTs>&... c_args) const {
    if (((p_args.size() != edge.parent_size()) || ... || false)) {
      return SizeMismatchError({edge.parent_size(), p_args.size()...});
    }
    if (((c_args.size() != edge.child_size()) || ... || false)) {
      return SizeMismatchError({edge.child_size(), c_args.size()...});
    }
    switch (edge.edge_type()) {
      case DenseArrayEdge::SPLIT_POINTS: {
        const auto& split_points = edge.edge_values();
        return ApplyWithSplitPoints(edge.parent_size(), edge.child_size(),
                                    split_points, p_args..., c_args...);
      }
      case DenseArrayEdge::MAPPING: {
        const auto& mapping = edge.edge_values();
        return ApplyWithMapping(edge.parent_size(), edge.child_size(), mapping,
                                p_args..., c_args...);
      }
      default:
        return absl::InvalidArgumentError("unsupported edge type");
    }
  }

  absl::StatusOr<std::conditional_t<
      kIsAggregator, typename Accumulator::result_type, DenseArray<ResT>>>
  Apply(const DenseArrayGroupScalarEdge& edge, view_type_t<ParentTs>... p_args,
        const AsDenseArray<ChildTs>&... c_args) const {
    if (((c_args.size() != edge.child_size()) || ... || false)) {
      return SizeMismatchError({edge.child_size(), c_args.size()...});
    }
    Accumulator accumulator = empty_accumulator_;
    accumulator.Reset(p_args...);

    if constexpr (kIsAggregator) {
      auto fn = [&](int64_t child_id, bool child_row_valid,
                    view_type_t<ChildTs>... args) {
        if (child_row_valid) {
          Add(accumulator, child_id, args...);
        }
      };
      ChildUtil::Iterate(fn, 0, edge.child_size(), c_args...);
      auto res = accumulator.GetResult();
      RETURN_IF_ERROR(accumulator.GetStatus());
      return typename Accumulator::result_type(std::move(res));
    } else {
      DenseArrayBuilder<ResT> builder(edge.child_size(), buffer_factory_);
      std::vector<int64_t> processed_rows;
      auto fn = [&](int64_t child_id, bool child_row_valid,
                    view_type_t<ChildTs>... args) {
        if (child_row_valid) {
          Add(accumulator, child_id, args...);
          if constexpr (kIsPartial) {
            builder.Set(child_id, accumulator.GetResult());
          } else if constexpr (kIsFull) {
            // push back the child row id for post-processing
            processed_rows.push_back(child_id);
          }
        }
      };
      ChildUtil::Iterate(fn, 0, edge.child_size(), c_args...);
      if constexpr (kIsFull) {
        accumulator.FinalizeFullGroup();
        for (int64_t row_id : processed_rows) {
          builder.Set(row_id, accumulator.GetResult());
        }
      }
      RETURN_IF_ERROR(accumulator.GetStatus());
      return std::move(builder).Build();
    }
  }

 private:
  // Applies this group operator using a mapping from the child index to the
  // parent index. This mapping is a child index feature having RowId values of
  // the corresponding group.
  absl::StatusOr<DenseArray<ResT>> ApplyWithMapping(
      int64_t parent_row_count, int64_t child_row_count,
      const DenseArray<int64_t>& mapping,
      const AsDenseArray<ParentTs>&... p_values,
      const AsDenseArray<ChildTs>&... c_values) const {
    DCHECK_EQ(child_row_count, mapping.size());
    using MappingAndChildUtil =
        DenseOpsUtil<meta::type_list<int64_t, ChildTs...>>;

    // One accumulator slot per group.
    std::vector<Accumulator> accumulators(parent_row_count, empty_accumulator_);

    // Create one accumulator per row in group id space which has the required
    // group values.
    std::vector<bool> valid_groups(parent_row_count, false);
    {
      auto fn = [&](int64_t group, bool valid, view_type_t<ParentTs>... args) {
        if (valid) accumulators[group].Reset(args...);
        valid_groups[group] = valid;
      };
      ParentUtil::IterateFromZero(fn, parent_row_count, p_values...);
    }

    // Bitmap of rows which were processed. Only used by full accumulators.
    std::vector<bool> processed_child_rows;
    if constexpr (kIsFull) {
      processed_child_rows.resize(child_row_count, false);
    }

    // Builder for result
    const int64_t result_row_count =
        kIsAggregator ? parent_row_count : child_row_count;
    DenseArrayBuilder<ResT> builder(result_row_count, buffer_factory_);

    auto process_child_row_fn = [&](int64_t child_id, bool valid,
                                    int64_t parent_id,
                                    view_type_t<ChildTs>... args) {
      // parent_id is coming from `mapping`.
      if (!valid || !valid_groups[parent_id]) return;
      auto& accumulator = accumulators[parent_id];
      Add(accumulator, child_id, args...);
      if constexpr (kIsFull) {
        // mark the child row id for post-processing
        processed_child_rows[child_id] = true;
      }
      // Partial outputs only where input rows are present.
      if constexpr (kIsPartial) {
        builder.Set(child_id, accumulator.GetResult());
      }
    };
    MappingAndChildUtil::Iterate(process_child_row_fn, 0, child_row_count,
                                 mapping, c_values...);

    // full accumulator output.
    if constexpr (kIsFull) {
      int64_t parent_id = 0;
      for (bool valid : valid_groups) {
        if (valid) accumulators[parent_id].FinalizeFullGroup();
        parent_id++;
      }
      for (int64_t child_id = 0; child_id < processed_child_rows.size();
           ++child_id) {
        if (processed_child_rows[child_id]) {
          int64_t parent_id = mapping.values[child_id];
          DCHECK(valid_groups[parent_id])
              << "Child rows from invalid groups shouldn't be processed";
          builder.Set(child_id, accumulators[parent_id].GetResult());
        }
      }
    }

    for (int64_t parent_id = 0; parent_id < parent_row_count; ++parent_id) {
      if (valid_groups[parent_id]) {
        if constexpr (kIsAggregator) {
          // emit single result per accumulator
          builder.Set(parent_id, accumulators[parent_id].GetResult());
        }
        RETURN_IF_ERROR(accumulators[parent_id].GetStatus());
      }
    }

    return std::move(builder).Build();
  }

  // Applies this group operator using a `splits` mapping from parent to child
  // row ids. `splits` is a DenseArray having a row_count which is one greater
  // than the parent index. It defines a mapping wherein parent id P corresponds
  // to child ids [splits[P], splits[P+1]).
  // splits[parent_row_count] should be equal to child_row_count.
  template <size_t... GIs>
  absl::StatusOr<DenseArray<ResT>> ApplyWithSplitPoints(
      int64_t parent_row_count, int64_t child_row_count,
      const DenseArray<int64_t>& splits,
      const AsDenseArray<ParentTs>&... p_values,
      const AsDenseArray<ChildTs>&... c_values) const {
    if (splits.size() != parent_row_count + 1) {
      return absl::InvalidArgumentError(
          "splits row count is not compatible with parent row count");
    }

    const int64_t result_row_count =
        kIsAggregator ? parent_row_count : child_row_count;
    DenseArrayBuilder<ResT> builder(result_row_count, buffer_factory_);
    std::vector<int64_t> processed_rows;
    Accumulator accumulator = empty_accumulator_;

    ParentUtil::IterateFromZero(
        [&](int64_t parent_id, bool parent_valid,
            view_type_t<ParentTs>... args) {
          if (parent_valid) {
            accumulator.Reset(args...);
            ProcessSingleGroupWithSplitPoints(parent_id, splits, c_values...,
                                              processed_rows, accumulator,
                                              builder);
          }
        },
        parent_row_count, p_values...);
    RETURN_IF_ERROR(accumulator.GetStatus());
    return std::move(builder).Build();
  }

  void ProcessSingleGroupWithSplitPoints(
      int64_t parent_id, const DenseArray<int64_t>& splits,
      const AsDenseArray<ChildTs>&... c_values,
      std::vector<int64_t>& processed_rows, Accumulator& accumulator,
      DenseArrayBuilder<ResT>& builder) const {
    DCHECK(splits.present(parent_id));
    DCHECK(splits.present(parent_id + 1));
    int64_t child_from = splits.values[parent_id];
    int64_t child_to = splits.values[parent_id + 1];

    auto fn = [&](int64_t child_id, bool child_row_valid,
                  view_type_t<ChildTs>... args) {
      if (child_row_valid) {
        Add(accumulator, child_id, args...);
        if constexpr (kIsPartial) {
          builder.Set(child_id, accumulator.GetResult());
        } else if constexpr (kIsFull) {
          // push back the child row id for post-processing
          processed_rows.push_back(child_id);
        }
      }
    };
    ChildUtil::Iterate(fn, child_from, child_to, c_values...);

    if constexpr (kIsAggregator) {
      builder.Set(parent_id, accumulator.GetResult());
    } else if constexpr (kIsFull) {
      accumulator.FinalizeFullGroup();
      for (int64_t row_id : processed_rows) {
        builder.Set(row_id, accumulator.GetResult());
      }
      processed_rows.clear();
    }
  }

  void Add(Accumulator& accumulator, int64_t child_id,
           view_type_t<ChildTs>... args) const {
    if constexpr (ForwardId) {
      accumulator.Add(child_id, args...);
    } else {
      (void)child_id;  // not used
      accumulator.Add(args...);
    }
  }

  RawBufferFactory* buffer_factory_;
  const Accumulator empty_accumulator_;
};

}  // namespace dense_ops_internal

// Allows to apply a given accumulator on a set of dense arrays
// (see qexpr/aggregation_ops_interface.h).
// Usage example:
//     DenseGroupOps<SomeAccumulator> op(GetHeapBufferFactory());
//     ASSIGN_OR_RETURN(auto res,
//                      op.Apply(edge, parent_values..., child_values...));
template <class Accumulator>
using DenseGroupOps =
    dense_ops_internal::DenseGroupOpsImpl<Accumulator,
                                          typename Accumulator::parent_types,
                                          typename Accumulator::child_types>;

// Similar to DenseGroupOps, but passes child_id to the first child argument.
template <class Accumulator>
using DenseGroupOpsWithId = dense_ops_internal::DenseGroupOpsImpl<
    Accumulator, typename Accumulator::parent_types,
    meta::tail_t<typename Accumulator::child_types>, /*ForwardId=*/true>;

}  // namespace arolla

#endif  // AROLLA_DENSE_ARRAY_OPS_DENSE_GROUP_OPS_H_
