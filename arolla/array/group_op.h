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
#ifndef AROLLA_ARRAY_GROUP_OP_H_
#define AROLLA_ARRAY_GROUP_OP_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/id_filter.h"
#include "arolla/array/ops_util.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/binary_search.h"
#include "arolla/util/meta.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace array_ops_internal {

template <class Accumulator, class ParentTypes, class ChildTypes,
          bool ForwardId = false, bool UseDenseGroupOps = true>
class ArrayGroupOpImpl;

template <class Accumulator, class... ParentTs, class... ChildTs,
          bool ForwardId, bool UseDenseGroupOps>
class ArrayGroupOpImpl<Accumulator, meta::type_list<ParentTs...>,
                       meta::type_list<ChildTs...>, ForwardId,
                       UseDenseGroupOps> {
  // First template argument of ArrayOpsUtil is ConvertToDense.
  // When true, it converts all arguments to the dense form.
  // It is performance critical that ParentUtil has ConvertToDense=true,
  // otherwise the code can not be properly inlined (in this case
  // BM_WeightedAggSum/1 in array/benhcmarks.cc becomes twice slower).
  using ParentUtil =
      ArrayOpsUtil</*ConvertToDense=*/true, meta::type_list<ParentTs...>>;
  using ChildUtil =
      ArrayOpsUtil</*ConvertToDense=*/false, meta::type_list<ChildTs...>>;
  using MappingAndChildUtil =
      ArrayOpsUtil</*ConvertToDense=*/false,
                   meta::type_list<int64_t, ChildTs...>>;
  using ResT = strip_optional_t<typename Accumulator::result_type>;
  using DenseGroupOp = dense_ops_internal::DenseGroupOpsImpl<
      Accumulator, meta::type_list<ParentTs...>, meta::type_list<ChildTs...>,
      ForwardId>;

  static constexpr bool kIsAggregator = Accumulator::IsAggregator();
  static constexpr bool kIsPartial = Accumulator::IsPartial();
  static constexpr bool kIsFull = Accumulator::IsFull();

  // We use a special optimized implementation if there are no parent features
  // and most of the groups are empty. Value X means that optimization is used
  // if present_count(child_index_type) < X * total_count(parent_index_type).
  static constexpr double kNonEmptyGroupProbLimit = 0.25;

 public:
  // ArrayGroupOp constructor.
  //
  // Arguments:
  //  `buffer_factory` is the factory used to generate results.
  //  `empty_accumulator` is an Accumulator instance used as a prototype for
  //      creating new accumulators. Note that a given accumulator may be used
  //      for multiple groups within a single operation.
  explicit ArrayGroupOpImpl(RawBufferFactory* buffer_factory,
                            Accumulator empty_accumulator = Accumulator())
      : buffer_factory_(buffer_factory),
        empty_accumulator_(std::move(empty_accumulator)) {}

  // Applies this group operator.
  //
  // Arguments:
  // `edge`       the edge defining the mapping between the parent and child
  //              index types.
  // `values...`  the Containers corresponding to this group operator's parent
  //              and child features, in the order defined within the
  //              Accumulator.
  absl::StatusOr<Array<ResT>> Apply(const ArrayEdge& edge,
                                    const AsArray<ParentTs>&... p_args,
                                    const AsArray<ChildTs>&... c_args) const {
    // UseDenseGroupOps is false only in tests (in order to have good test
    // coverage of other branches).
    if constexpr (UseDenseGroupOps) {
      if (edge.edge_values().IsDenseForm() &&
          (p_args.IsDenseForm() && ... && true) &&
          (c_args.IsDenseForm() && ... && true)) {
        auto op = [this](const auto&... args) ABSL_ATTRIBUTE_NOINLINE {
          return DenseGroupOp(buffer_factory_, empty_accumulator_)
              .Apply(args...);
        };
        ASSIGN_OR_RETURN(DenseArray<ResT> res,
                         op(edge.ToDenseArrayEdge(), p_args.dense_data()...,
                            c_args.dense_data()...));
        return Array<ResT>(res);
      }
    }

    if (((p_args.size() != edge.parent_size()) || ... || false)) {
      return SizeMismatchError({edge.parent_size(), p_args.size()...});
    }
    if (((c_args.size() != edge.child_size()) || ... || false)) {
      return SizeMismatchError({edge.child_size(), c_args.size()...});
    }

    switch (edge.edge_type()) {
      case ArrayEdge::SPLIT_POINTS: {
        const Buffer<int64_t>& splits = edge.edge_values().dense_data().values;
        ChildUtil child_util(edge.child_size(), c_args..., buffer_factory_);
        if constexpr (kIsAggregator) {
          if constexpr (sizeof...(ParentTs) == 0) {
            if (child_util.PresentCountUpperEstimate() <
                kNonEmptyGroupProbLimit * edge.parent_size()) {
              return ApplyAggregatorWithSplitPointsOnVerySparseData(
                  edge.parent_size(), child_util, splits.span());
            }
          }
          ParentUtil parent_util(edge.parent_size(), p_args...,
                                 buffer_factory_);
          return ApplyAggregatorWithSplitPoints(parent_util, child_util,
                                                splits);
        } else {
          ParentUtil parent_util(edge.parent_size(), p_args...,
                                 buffer_factory_);
          if (child_util.PresentCountUpperEstimate() >
              edge.child_size() * IdFilter::kDenseSparsityLimit) {
            return ApplyDenseWithSplitPoints(parent_util, child_util, splits);
          } else {
            return ApplySparseWithSplitPoints(parent_util, child_util, splits);
          }
        }
      }
      case ArrayEdge::MAPPING: {
        MappingAndChildUtil mapchild_util(edge.child_size(), edge.edge_values(),
                                          c_args..., buffer_factory_);
        if constexpr (kIsAggregator && sizeof...(ParentTs) == 0) {
          if (mapchild_util.PresentCountUpperEstimate() <
              kNonEmptyGroupProbLimit * edge.parent_size()) {
            return ApplyAggregatorWithMappingOnVerySparseData(
                edge.parent_size(), mapchild_util);
          }
        }
        ParentUtil parent_util(edge.parent_size(), p_args..., buffer_factory_);
        return ApplyWithMapping(parent_util, mapchild_util);
      }
      default:
        return absl::InvalidArgumentError("unsupported edge type");
    }
  }

  // Applies this group operator with mapping to scalar. The difference from
  // the previous implementation is that there is only one group and `p_args`
  // are scalars rather than Arrays. If the accumulator is an aggregator, then
  // the result is also scalar.
  absl::StatusOr<
      std::conditional_t<Accumulator::IsAggregator(),
                         typename Accumulator::result_type, Array<ResT>>>
  Apply(const ArrayGroupScalarEdge& edge, view_type_t<ParentTs>... p_args,
        const AsArray<ChildTs>&... c_args) const {
    // UseDenseGroupOps is false only in tests (in order to have good test
    // coverage of other branches).
    if constexpr (UseDenseGroupOps) {
      if ((c_args.IsDenseForm() && ... && true)) {
        auto op = [this](const auto&... args) ABSL_ATTRIBUTE_NOINLINE {
          return DenseGroupOp(buffer_factory_, empty_accumulator_)
              .Apply(args...);
        };
        ASSIGN_OR_RETURN(auto res, op(edge.ToDenseArrayGroupScalarEdge(),
                                      p_args..., c_args.dense_data()...));
        if constexpr (Accumulator::IsAggregator()) {
          return res;
        } else {
          return Array<ResT>(res);
        }
      }
    }

    if (((c_args.size() != edge.child_size()) || ... || false)) {
      return SizeMismatchError({edge.child_size(), c_args.size()...});
    }

    ChildUtil util(edge.child_size(), c_args..., buffer_factory_);
    Accumulator accumulator = empty_accumulator_;
    accumulator.Reset(p_args...);

    if constexpr (kIsAggregator) {
      AggregateSingleGroup(accumulator, util, 0, edge.child_size());
      auto res = accumulator.GetResult();
      RETURN_IF_ERROR(accumulator.GetStatus());
      return typename Accumulator::result_type(std::move(res));
    } else {
      const int64_t max_present_count = util.PresentCountUpperEstimate();
      if (kIsPartial && max_present_count >
                            edge.child_size() * IdFilter::kDenseSparsityLimit) {
        DenseArrayBuilder<ResT> builder(edge.child_size(), buffer_factory_);
        auto fn = [&](int64_t child_id, view_type_t<ChildTs>... args) {
          Add(accumulator, child_id, args...);
          builder.Set(child_id, accumulator.GetResult());
        };
        util.Iterate(0, edge.child_size(), fn);
        RETURN_IF_ERROR(accumulator.GetStatus());
        return Array<ResT>(std::move(builder).Build());
      }

      SparseArrayBuilder<ResT> builder(edge.child_size(), max_present_count,
                                       buffer_factory_);
      auto fn = [&](int64_t child_id, view_type_t<ChildTs>... args) {
        Add(accumulator, child_id, args...);
        if constexpr (kIsPartial) {
          builder.SetByOffset(builder.NextOffset(), accumulator.GetResult());
        }
        builder.AddId(child_id);
      };
      util.Iterate(0, edge.child_size(), fn);
      if constexpr (kIsFull) {
        accumulator.FinalizeFullGroup();
        for (int64_t offset = 0; offset < builder.NextOffset(); ++offset) {
          builder.SetByOffset(offset, accumulator.GetResult());
        }
      }
      RETURN_IF_ERROR(accumulator.GetStatus());
      return std::move(builder).Build();
    }
  }

 private:
  absl::StatusOr<Array<ResT>> ApplyWithMapping(
      ParentUtil& parent_util, MappingAndChildUtil& mapchild_util) const {
    // One accumulator slot per parent.
    std::vector<Accumulator> accumulators(parent_util.size(),
                                          empty_accumulator_);

    // Create one accumulator per row in parent id space which has the required
    // parent values.
    std::vector<bool> valid_parents(parent_util.size(), false);
    parent_util.IterateSimple(
        [&](int64_t parent, view_type_t<ParentTs>... args) {
          accumulators[parent].Reset(args...);
          valid_parents[parent] = true;
        });

    const int64_t child_row_count = mapchild_util.size();
    const int64_t max_present_count = mapchild_util.PresentCountUpperEstimate();
    if (kIsAggregator ||
        (kIsPartial &&
         max_present_count > child_row_count * IdFilter::kDenseSparsityLimit)) {
      return ApplyAggregatorOrDensePartialWithMapping(
          parent_util, mapchild_util, accumulators, valid_parents);
    }

    DCHECK(kIsFull || kIsPartial);
    SparseArrayBuilder<ResT> builder(child_row_count, max_present_count,
                                     buffer_factory_);
    std::vector<int64_t> parent_ids;
    if constexpr (kIsFull) {
      parent_ids.reserve(max_present_count);
    }
    auto fn = [&](int64_t child_id, int64_t parent_id,
                  view_type_t<ChildTs>... args) {
      if (!valid_parents[parent_id]) return;
      auto& accumulator = accumulators[parent_id];
      Add(accumulator, child_id, args...);
      if constexpr (kIsPartial) {
        builder.SetByOffset(builder.NextOffset(), accumulator.GetResult());
      } else {
        DCHECK(kIsFull);
        parent_ids.push_back(parent_id);
      }
      builder.AddId(child_id);
    };
    mapchild_util.IterateSimple(fn);

    // full accumulator output.
    if constexpr (kIsFull) {
      int64_t parent_id = 0;
      for (bool valid : valid_parents) {
        if (valid) accumulators[parent_id].FinalizeFullGroup();
        parent_id++;
      }
      for (int64_t offset = 0; offset < builder.NextOffset(); ++offset) {
        builder.SetByOffset(offset,
                            accumulators[parent_ids[offset]].GetResult());
      }
    }

    int64_t parent_id = 0;
    for (bool valid : valid_parents) {
      if (valid) {
        RETURN_IF_ERROR(accumulators[parent_id].GetStatus());
      }
      parent_id++;
    }

    return std::move(builder).Build();
  }

  // This algorithm is for aggregators that don't use parent args.
  // Optimized for the case when most of groups are empty.
  //   * Iterate over children items and update parent accumulators.
  //     Accumulators are created lazily and stored in a map.
  //   * We don't create accumulators for empty groups: since p_args are not
  //     used, all empty groups produce the same result.
  //   * Get result for all non empty groups and create a sparse output array.
  //     We call `GetResult` on an empty accumulator and use it as
  //     `missing_id_value` - value for empty groups.
  absl::StatusOr<Array<ResT>> ApplyAggregatorWithMappingOnVerySparseData(
      size_t parent_size, MappingAndChildUtil& mapchild_util) const {
    static_assert(kIsAggregator);
    static_assert(sizeof...(ParentTs) == 0);

    absl::flat_hash_map<int64_t, Accumulator> accumulators;
    mapchild_util.IterateSimple(
        [&](int64_t child_id, int64_t parent_id, view_type_t<ChildTs>... args) {
          auto it = accumulators.find(parent_id);
          if (it == accumulators.end()) {
            it = accumulators.emplace(parent_id, empty_accumulator_).first;
            it->second.Reset();
          }
          Add(it->second, child_id, args...);
        });

    std::vector<std::pair<
        int64_t, decltype(Accumulator(empty_accumulator_).GetResult())>>
        results;
    results.reserve(accumulators.size());
    for (auto& [parent_id, accumulator] : accumulators) {
      results.emplace_back(parent_id, accumulator.GetResult());
      RETURN_IF_ERROR(accumulator.GetStatus());
    }
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    Buffer<int64_t>::Builder ids_bldr(accumulators.size(), buffer_factory_);
    DenseArrayBuilder<ResT> dense_builder(accumulators.size(), buffer_factory_);
    int64_t offset = 0;
    for (auto& [parent_id, value] : results) {
      ids_bldr.Set(offset, parent_id);
      dense_builder.Set(offset, value);
      offset++;
    }

    OptionalValue<ResT> missing_id_value;
    if (accumulators.size() < parent_size) {
      Accumulator acc(empty_accumulator_);
      acc.Reset();
      missing_id_value = OptionalValue<ResT>(acc.GetResult());
      RETURN_IF_ERROR(acc.GetStatus());
    }

    IdFilter id_filter(parent_size, std::move(ids_bldr).Build());
    return Array<ResT>(parent_size, std::move(id_filter),
                       std::move(dense_builder).Build(), missing_id_value);
  }

  absl::StatusOr<Array<ResT>> ApplyAggregatorOrDensePartialWithMapping(
      ParentUtil& parent_util, MappingAndChildUtil& mapchild_util,
      std::vector<Accumulator>& accumulators,
      std::vector<bool>& valid_parents) const {
    DCHECK(kIsAggregator || kIsPartial);
    DenseArrayBuilder<ResT> builder(
        kIsAggregator ? parent_util.size() : mapchild_util.size(),
        buffer_factory_);
    auto fn = [&](int64_t child_id, int64_t parent_id,
                  view_type_t<ChildTs>... args) {
      auto& accumulator = accumulators[parent_id];
      if constexpr (kIsAggregator) {
        // We add values even if valid_parents[parent_id] == false in order
        // to reduce the number of conditional jumps.
        Add(accumulator, child_id, args...);
      } else {
        if (valid_parents[parent_id]) {
          Add(accumulator, child_id, args...);
          builder.Set(child_id, accumulator.GetResult());
        }
      }
    };
    mapchild_util.IterateSimple(fn);

    int64_t parent_id = 0;
    for (bool valid : valid_parents) {
      if (valid) {
        if constexpr (kIsAggregator) {
          builder.Set(parent_id, accumulators[parent_id].GetResult());
        }
        RETURN_IF_ERROR(accumulators[parent_id].GetStatus());
      }
      parent_id++;
    }

    return Array<ResT>(std::move(builder).Build());
  }

  absl::StatusOr<Array<ResT>> ApplyAggregatorWithSplitPoints(
      ParentUtil& parent_util, ChildUtil& child_util,
      const Buffer<int64_t>& splits) const {
    static_assert(kIsAggregator);
    DCHECK_EQ(splits.size(), parent_util.size() + 1);
    Accumulator accumulator = empty_accumulator_;
    DenseArrayBuilder<ResT> builder(parent_util.size(), buffer_factory_);
    auto process_group = [&](int64_t parent_id, view_type_t<ParentTs>... args) {
      accumulator.Reset(args...);
      int64_t child_from = splits[parent_id];
      int64_t child_to = splits[parent_id + 1];
      AggregateSingleGroup(accumulator, child_util, child_from, child_to);
      builder.Set(parent_id, accumulator.GetResult());
    };
    parent_util.IterateSimple(process_group);
    RETURN_IF_ERROR(accumulator.GetStatus());
    return Array<ResT>(std::move(builder).Build());
  }

  void AggregateSingleGroup(Accumulator& accumulator, ChildUtil& child_util,
                            int64_t child_from, int64_t child_to) const {
    static_assert(kIsAggregator);
    auto fn = [&](int64_t child_id, view_type_t<ChildTs>... args) {
      Add(accumulator, child_id, args...);
    };
    auto repeated_fn = [&](int64_t first_child_id, int64_t count,
                           view_type_t<ChildTs>... args) {
      AddN(accumulator, first_child_id, count, args...);
    };
    child_util.Iterate(child_from, child_to, fn, empty_missing_fn, repeated_fn);
  }

  // This algorithm is for aggregators that don't use parent args.
  // Optimized for the case when most of groups are empty.
  //   * Iterate over children items and update the accumulator. Every time we
  //     cross a split point, we store parent_id and the accumulator result to
  //     the output buffers and reset the accumulator.
  //   * Empty groups are ignored: since p_args are not used, all empty groups
  //     produce the same result.
  //   * And the end we construct a sparse Array from ids and results of
  //     non-empty groups. We call `GetResult` on an empty accumulator and use
  //     it as `missing_id_value` - value for empty groups.
  absl::StatusOr<Array<ResT>> ApplyAggregatorWithSplitPointsOnVerySparseData(
      int64_t parent_size, ChildUtil& child_util,
      absl::Span<const int64_t> splits) const {
    static_assert(kIsAggregator);
    static_assert(sizeof...(ParentTs) == 0);
    DCHECK_EQ(splits.size(), parent_size + 1);

    const int64_t max_res_dense_count =
        std::min(parent_size, child_util.PresentCountUpperEstimate());
    Buffer<int64_t>::Builder ids_bldr(max_res_dense_count, buffer_factory_);
    DenseArrayBuilder<ResT> dense_builder(max_res_dense_count, buffer_factory_);
    int64_t res_offset = 0;

    int64_t next_parent_id = 0;
    Accumulator accumulator = empty_accumulator_;
    accumulator.Reset();
    absl::Status status = absl::OkStatus();

    // Adds id and result of the previous group to builders AND resets
    // the accumulator.
    auto add_previous_to_results = [&]() {
      if (next_parent_id > 0 && status.ok()) {
        ids_bldr.Set(res_offset, next_parent_id - 1);
        dense_builder.Set(res_offset, accumulator.GetResult());
        status = accumulator.GetStatus();
        accumulator.Reset();
        res_offset++;
      }
    };

    child_util.IterateSimple([&](int64_t child_id,
                                 view_type_t<ChildTs>... args) {
      if (child_id >= splits[next_parent_id]) {
        add_previous_to_results();
        next_parent_id = GallopingLowerBound(splits.begin() + next_parent_id,
                                             splits.end(), child_id + 1) -
                         splits.begin();
      }
      Add(accumulator, child_id, args...);
    });
    add_previous_to_results();
    RETURN_IF_ERROR(status);

    OptionalValue<ResT> missing_id_value;
    if (res_offset < parent_size) {
      missing_id_value = OptionalValue<ResT>(accumulator.GetResult());
      RETURN_IF_ERROR(accumulator.GetStatus());
    }

    IdFilter id_filter(parent_size, std::move(ids_bldr).Build(res_offset));
    return Array<ResT>(parent_size, std::move(id_filter),
                       std::move(dense_builder).Build(res_offset),
                       missing_id_value);
  }

  // Applies partial or full accumulator with split points. Returns a Array
  // in dense form.
  absl::StatusOr<Array<ResT>> ApplyDenseWithSplitPoints(
      ParentUtil& parent_util, ChildUtil& child_util,
      const Buffer<int64_t>& splits) const {
    static_assert(kIsPartial || kIsFull);
    DenseArrayBuilder<ResT> builder(child_util.size(), buffer_factory_);
    std::vector<int64_t> processed_rows;
    Accumulator accumulator = empty_accumulator_;

    auto fn = [&](int64_t child_id, view_type_t<ChildTs>... args) {
      Add(accumulator, child_id, args...);
      if constexpr (kIsPartial) {
        builder.Set(child_id, accumulator.GetResult());
      } else {  // kIsFull: push back the child row id for post-processing
        processed_rows.push_back(child_id);
      }
    };

    auto process_group = [&](int64_t parent_id, view_type_t<ParentTs>... args) {
      accumulator.Reset(args...);
      int64_t child_from = splits[parent_id];
      int64_t child_to = splits[parent_id + 1];
      child_util.Iterate(child_from, child_to, fn);
      if constexpr (kIsFull) {
        accumulator.FinalizeFullGroup();
        for (int64_t row_id : processed_rows) {
          builder.Set(row_id, accumulator.GetResult());
        }
        processed_rows.clear();
      }
    };
    parent_util.IterateSimple(process_group);
    RETURN_IF_ERROR(accumulator.GetStatus());
    return Array<ResT>(std::move(builder).Build());
  }

  // Similar to ApplyDenseWithSplitPoints, but returns a sparse Array.
  // Should be used if `child_util.PresentCountUpperEstimate()` is much
  // lesser than `child_util.size()`.
  absl::StatusOr<Array<ResT>> ApplySparseWithSplitPoints(
      ParentUtil& parent_util, ChildUtil& child_util,
      const Buffer<int64_t>& splits) const {
    static_assert(kIsPartial || kIsFull);
    SparseArrayBuilder<ResT> builder(child_util.size(),
                                     child_util.PresentCountUpperEstimate(),
                                     buffer_factory_);
    Accumulator accumulator = empty_accumulator_;

    auto fn = [&](int64_t child_id, view_type_t<ChildTs>... args) {
      Add(accumulator, child_id, args...);
      if constexpr (kIsPartial) {
        builder.SetByOffset(builder.NextOffset(), accumulator.GetResult());
      }
      builder.AddId(child_id);
    };

    auto process_group = [&](int64_t parent_id, view_type_t<ParentTs>... args) {
      accumulator.Reset(args...);
      int64_t child_from = splits[parent_id];
      int64_t child_to = splits[parent_id + 1];
      int64_t offset = builder.NextOffset();
      child_util.Iterate(child_from, child_to, fn);
      if constexpr (kIsFull) {
        accumulator.FinalizeFullGroup();
        while (offset < builder.NextOffset()) {
          builder.SetByOffset(offset++, accumulator.GetResult());
        }
      }
    };
    parent_util.IterateSimple(process_group);
    RETURN_IF_ERROR(accumulator.GetStatus());
    return std::move(builder).Build();
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

  void AddN(Accumulator& accumulator, int64_t first_child_id, int64_t N,
            view_type_t<ChildTs>... args) const {
    if constexpr (ForwardId) {
      for (int64_t i = 0; i < N; ++i) {
        accumulator.Add(first_child_id + i, args...);
      }
    } else {
      (void)first_child_id;  // not used
      accumulator.AddN(N, args...);
    }
  }

  RawBufferFactory* buffer_factory_;
  const Accumulator empty_accumulator_;
};

}  // namespace array_ops_internal

// Allows to apply a given accumulator on a set of arrays
// (see qexpr/aggregation_ops_interface.h).
// Usage example:
//     ArrayGroupOp<SomeAccumulator> op(GetHeapBufferFactory());
//     ASSIGN_OR_RETURN(auto res,
//                      op.Apply(edge, parent_values..., child_values...));
template <class Accumulator>
using ArrayGroupOp =
    array_ops_internal::ArrayGroupOpImpl<Accumulator,
                                         typename Accumulator::parent_types,
                                         typename Accumulator::child_types>;

// Similar to ArrayGroupOp, but passes child_id to the first child argument.
template <class Accumulator>
using ArrayGroupOpWithId = array_ops_internal::ArrayGroupOpImpl<
    Accumulator, typename Accumulator::parent_types,
    meta::tail_t<typename Accumulator::child_types>, /*ForwardId=*/true>;

}  // namespace arolla

#endif  // AROLLA_ARRAY_GROUP_OP_H_
