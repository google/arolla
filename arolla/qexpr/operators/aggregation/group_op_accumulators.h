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
// Implementations of accumulators to be used in implementations of
// aggregational operators irrespective of container types.
#ifndef AROLLA_QEXPR_OPERATORS_AGGREGATION_GROUP_OP_ACCUMULATORS_H_
#define AROLLA_QEXPR_OPERATORS_AGGREGATION_GROUP_OP_ACCUMULATORS_H_

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/aggregation_ops_interface.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/math/arithmetic.h"
#include "arolla/util/meta.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// SimpleAggCount Accumulator definition.
template <AccumulatorType TYPE>
struct SimpleCountAccumulator
    : Accumulator<TYPE, int64_t, meta::type_list<>, meta::type_list<Unit>> {
  void Reset() final { accumulator = 0; }
  void Add(Unit) final { accumulator += 1; }
  void AddN(int64_t n, Unit) final { accumulator += n; }
  int64_t GetResult() final { return accumulator; }

  int64_t accumulator{0};
};

using SimpleCountAggregator =
    SimpleCountAccumulator<AccumulatorType::kAggregator>;
using CountPartialAccumulator =
    SimpleCountAccumulator<AccumulatorType::kPartial>;

// AnyAccumulator applies core.agg_any.
template <AccumulatorType TYPE>
struct AnyAccumulator : Accumulator<TYPE, OptionalValue<Unit>,
                                    meta::type_list<>, meta::type_list<Unit>> {
  void Reset() final { accumulator = false; };
  void Add(Unit value) final { accumulator = true; }
  void AddN(int64_t, Unit value) final { accumulator = true; }
  OptionalValue<Unit> GetResult() final { return {accumulator, Unit{}}; }
  bool accumulator;
};

using AnyAggregator = AnyAccumulator<AccumulatorType::kAggregator>;

// AllAccumulator applies core.agg_all.
template <AccumulatorType TYPE>
struct AllAccumulator
    : Accumulator<TYPE, OptionalValue<Unit>, meta::type_list<>,
                  meta::type_list<OptionalUnit>> {
  void Reset() final { accumulator = true; };
  void Add(OptionalUnit value) final {
    accumulator = accumulator && value.present;
  }
  void AddN(int64_t, OptionalUnit value) final {
    accumulator = accumulator && value.present;
  }
  OptionalValue<Unit> GetResult() final { return {accumulator, Unit{}}; }
  bool accumulator;
};

using AllAggregator = AllAccumulator<AccumulatorType::kAggregator>;

// LogicalAllAccumulator applies LogicalAnd(3-valued And).
// All present true -> true
// All present true and at least one missing -> missing
// At least one present false -> false
template <AccumulatorType TYPE>
struct LogicalAllAccumulator
    : Accumulator<TYPE, OptionalValue<bool>, meta::type_list<>,
                  meta::type_list<OptionalValue<bool>>> {
  void Reset() final { has_false = has_missing = false; };
  void Add(OptionalValue<bool> v) final {
    has_false = has_false || (v.present && !v.value);
    has_missing = has_missing || !v.present;
  }
  void AddN(int64_t, OptionalValue<bool> v) final { Add(v); }
  OptionalValue<bool> GetResult() final {
    return {has_false || !has_missing, !has_false};
  }
  bool has_false;
  bool has_missing;
};

using LogicalAllAggregator =
    LogicalAllAccumulator<AccumulatorType::kAggregator>;

// LogicalOrAccumulator applies LogicalOr(3-valued Or).
// All present false -> false
// All present false and at least one missing -> missing
// At least one present true -> true
template <AccumulatorType TYPE>
struct LogicalAnyAccumulator
    : Accumulator<TYPE, OptionalValue<bool>, meta::type_list<>,
                  meta::type_list<OptionalValue<bool>>> {
  void Reset() final { has_true = has_missing = false; };
  void Add(OptionalValue<bool> v) final {
    has_true = has_true || (v.present && v.value);
    has_missing = has_missing || !v.present;
  }
  void AddN(int64_t, OptionalValue<bool> v) final { Add(v); }
  OptionalValue<bool> GetResult() final {
    return {has_true || !has_missing, has_true};
  }
  bool has_true;
  bool has_missing;
};

using LogicalAnyAggregator =
    LogicalAnyAccumulator<AccumulatorType::kAggregator>;

// SameTypeAsValue defines the result type to be the same as the value type.
template <typename T>
struct SameTypeAsValue {
  using type = T;
};

// WideAccumulator defines the accumulator type when summing a certain type.
// It is double in the case floating-point aggregations for more precision.
template <typename T, class Enable = void>
struct WideAccumulator {};

template <typename T>
struct WideAccumulator<
    T, typename std::enable_if<std::is_integral<T>::value>::type> {
  using type = T;
};

template <typename T>
struct WideAccumulator<
    T, typename std::enable_if<std::is_floating_point<T>::value>::type> {
  using type = double;
};

template <typename ValueT, AccumulatorType AccumulatorType>
struct SumAccumulator
    : Accumulator<AccumulatorType, OptionalValue<ValueT>, meta::type_list<>,
                  meta::type_list<ValueT>> {
  using AccumulatorT = typename WideAccumulator<ValueT>::type;

  SumAccumulator() = default;
  explicit SumAccumulator(const OptionalValue<ValueT>& initial)
      : initial(initial) {
    if (!this->initial.present) this->initial.value = 0;
  }

  void Reset() final {
    accumulator = {initial.present, static_cast<AccumulatorT>(initial.value)};
  };
  void Add(ValueT value) final {
    accumulator = AddOp()(accumulator.value, static_cast<AccumulatorT>(value));
  }
  void AddN(int64_t n, ValueT value) final {
    accumulator = AddOp()(accumulator.value,
                          MultiplyOp()(static_cast<AccumulatorT>(value),
                                       static_cast<AccumulatorT>(n)));
  }
  OptionalValue<ValueT> GetResult() final {
    return {accumulator.present, static_cast<ValueT>(accumulator.value)};
  }

  OptionalValue<ValueT> initial;
  OptionalValue<AccumulatorT> accumulator;
};

template <typename ValueT>
using SumAggregator = SumAccumulator<ValueT, AccumulatorType::kAggregator>;
template <typename ValueT>
using SumPartialAccumulator = SumAccumulator<ValueT, AccumulatorType::kPartial>;

// TODO: Consider implementing this using the iterative mean
// algorithm to avoid over- and underflows.
template <typename ValueT, AccumulatorType AccumulatorType>
struct MeanAccumulator
    : Accumulator<AccumulatorType, OptionalValue<ValueT>, meta::type_list<>,
                  meta::type_list<ValueT>> {
  using SumAccumulatorT = typename WideAccumulator<ValueT>::type;

  MeanAccumulator() = default;

  void Reset() final {
    accumulator_sum = 0;
    accumulator_count = 0;
  }

  void Add(ValueT value) final {
    accumulator_sum =
        AddOp()(accumulator_sum, static_cast<SumAccumulatorT>(value));
    accumulator_count += 1;
  }
  void AddN(int64_t n, ValueT value) final {
    accumulator_sum = AddOp()(accumulator_sum,
                              MultiplyOp()(static_cast<SumAccumulatorT>(value),
                                           static_cast<SumAccumulatorT>(n)));
    accumulator_count += n;
  }
  OptionalValue<ValueT> GetResult() final {
    if (accumulator_count == 0) {
      return std::nullopt;
    }
    // NOTE: Casting before division in order to exactly match previous
    // behavior.
    return DivideOp()(static_cast<ValueT>(accumulator_sum),
                      static_cast<ValueT>(accumulator_count));
  }

  int64_t accumulator_count;
  SumAccumulatorT accumulator_sum;
};

template <typename ValueT>
using MeanAggregator = MeanAccumulator<ValueT, AccumulatorType::kAggregator>;

// TODO: renew this class comment.
template <typename ValueT, AccumulatorType AccumulatorType, typename FunctorT,
          template <typename...> class ResultTypeTraits,
          template <typename...> class AccumulatorTypeTraits,
          bool IgnoreRepeating>
struct FunctorAccumulator
    : Accumulator<AccumulatorType,
                  OptionalValue<typename ResultTypeTraits<ValueT>::type>,
                  meta::type_list<>, meta::type_list<ValueT>> {
  using ResultT = typename ResultTypeTraits<ValueT>::type;
  using AccumulatorT = typename AccumulatorTypeTraits<ValueT>::type;

  FunctorAccumulator() = default;
  explicit FunctorAccumulator(const OptionalValue<ResultT>& initial)
      : initial(initial) {}

  void Reset() final {
    accumulator = {initial.present, static_cast<AccumulatorT>(initial.value)};
  };
  void Add(ValueT value) final {
    if (accumulator.present) {
      // The cast here is necessary as arithmetic functors take both
      // arguments of the same type.
      accumulator =
          FunctorT()(accumulator.value, static_cast<AccumulatorT>(value));
    } else {
      accumulator = value;
    }
  }
  void AddN(int64_t n, ValueT value) final {
    if constexpr (IgnoreRepeating) {
      Add(value);
    } else {
      for (int64_t i = 0; i < n; ++i) Add(value);
    }
  }
  OptionalValue<ResultT> GetResult() final {
    return {accumulator.present, static_cast<ResultT>(accumulator.value)};
  }

  OptionalValue<ResultT> initial;
  OptionalValue<AccumulatorT> accumulator;
};

template <typename ValueT>
using ProdAggregator =
    FunctorAccumulator<ValueT, AccumulatorType::kAggregator, MultiplyOp,
                       SameTypeAsValue, WideAccumulator, false>;
template <typename ValueT>
using MinAggregator =
    FunctorAccumulator<ValueT, AccumulatorType::kAggregator, MinOp,
                       SameTypeAsValue, SameTypeAsValue, true>;
template <typename ValueT>
using MinPartialAccumulator =
    FunctorAccumulator<ValueT, AccumulatorType::kPartial, MinOp,
                       SameTypeAsValue, SameTypeAsValue, true>;
template <typename ValueT>
using MaxAggregator =
    FunctorAccumulator<ValueT, AccumulatorType::kAggregator, MaxOp,
                       SameTypeAsValue, SameTypeAsValue, true>;
template <typename ValueT>
using MaxPartialAccumulator =
    FunctorAccumulator<ValueT, AccumulatorType::kPartial, MaxOp,
                       SameTypeAsValue, SameTypeAsValue, true>;

template <typename T>
struct InverseCdfAccumulator
    : Accumulator<AccumulatorType::kAggregator, OptionalValue<T>,
                  meta::type_list<>, meta::type_list<T>> {
  explicit InverseCdfAccumulator(float cdf) : cdf(cdf) {}
  void Reset() final { values.clear(); };

  void Add(view_type_t<T> v) final { values.push_back(v); }

  OptionalValue<view_type_t<T>> GetResult() final {
    if (values.empty()) {
      return std::nullopt;
    }
    // When cdf == 1/N, we return the 0th element instead of the 1st by
    // taking ceil(cdf * size) - 1. E.g. for p = [a, b, c, d], we map
    // [.0, .25] -> a; (.25, .5] -> b; (.5, .75] -> c, (.75, 1.] -> d.
    int64_t offset = static_cast<int64_t>(ceil(cdf * values.size()) - 1);
    // The minimum element has CDF of 1/N; the maximum element has CDF
    // of 1. If CDF is outside of this range, return the minimum or
    // maximum.
    offset = std::clamp<int64_t>(offset, 0, values.size() - 1);
    std::nth_element(values.begin(), values.begin() + offset, values.end());
    return values[offset];
  }

  // A buffer to gather all values before sorting.
  std::vector<view_type_t<T>> values;
  // CDF values to use for each result.
  float cdf;
};

template <typename T>
class CollapseAccumulator
    : public Accumulator<AccumulatorType::kAggregator, OptionalValue<T>,
                         meta::type_list<>, meta::type_list<T>> {
 public:
  void Reset() final { present_ = false; }

  void Add(view_type_t<T> value) final {
    if constexpr (std::numeric_limits<T>::has_quiet_NaN) {
      if (!present_) {
        value_ = value;
        present_ = true;
        all_equal_ = true;
        is_nan_ = std::isnan(value);
      } else {
        all_equal_ = all_equal_ && ((is_nan_ && std::isnan(value)) ||
                                    (!is_nan_ && value == value_));
      }
    } else {
      if (!present_) {
        value_ = value;
        present_ = true;
        all_equal_ = true;
      } else {
        all_equal_ = all_equal_ && (value == value_);
      }
    }
  }

  void AddN(int64_t, view_type_t<T> value) final { return Add(value); }

  OptionalValue<view_type_t<T>> GetResult() final {
    if (present_ && all_equal_) {
      return value_;
    }
    return std::nullopt;
  }

 private:
  view_type_t<T> value_;
  bool present_ = false;
  bool all_equal_ = false;
  bool is_nan_ = false;
};

// InverseMappingAccumulator treats the input as a permutation and emits the
// inverse permutation.
class InverseMappingAccumulator
    : public Accumulator<AccumulatorType::kFull, OptionalValue<int64_t>,
                         meta::type_list<>,
                         meta::type_list<OptionalValue<int64_t>>> {
 public:
  explicit InverseMappingAccumulator()
      : return_id_(0), status_(absl::OkStatus()) {}

  void Reset() final { permutation_.clear(); };

  void Add(OptionalValue<int64_t> id) final { permutation_.push_back(id); }

  void FinalizeFullGroup() final {
    inv_permutation_.assign(permutation_.size(), std::nullopt);
    for (int i = 0; i < permutation_.size(); ++i) {
      if (!permutation_[i].present) continue;
      int64_t value = permutation_[i].value;
      if (ABSL_PREDICT_FALSE(value < 0 || value >= permutation_.size())) {
        status_ = absl::InvalidArgumentError(absl::StrFormat(
            "unable to compute array.inverse_mapping: invalid permutation, "
            "element %d is not a valid element of a permutation of size %d",
            value, permutation_.size()));
        break;
      }
      if (ABSL_PREDICT_FALSE(inv_permutation_[value].present)) {
        status_ = absl::InvalidArgumentError(absl::StrFormat(
            "unable to compute array.inverse_mapping: invalid permutation, "
            "element %d appears twice in the permutation",
            value));
        break;
      }
      inv_permutation_[value] = i;
    }
    return_id_ = 0;
  }

  OptionalValue<int64_t> GetResult() final {
    return inv_permutation_[return_id_++];
  }

  absl::Status GetStatus() final { return status_; }

  int64_t return_id_;
  std::vector<OptionalValue<int64_t>> permutation_, inv_permutation_;
  absl::Status status_;
};

template <typename StringType>
struct StringJoinAggregator
    : Accumulator<AccumulatorType::kAggregator, OptionalValue<StringType>,
                  meta::type_list<>, meta::type_list<StringType>> {
  static_assert(std::is_same_v<view_type_t<StringType>, absl::string_view>);

  explicit StringJoinAggregator(absl::string_view sep) : sep(sep) {}

  void Reset() final {
    present = false;
    accumulator.clear();
  }

  void Add(absl::string_view value) final {
    if (!present) {
      // first value
      absl::StrAppend(&accumulator, value);
      present = true;
    } else {
      // subsequent values
      absl::StrAppend(&accumulator, sep, value);
    }
  }

  OptionalValue<absl::string_view> GetResult() final {
    // Must return reference to member field, not a local. Result should
    // be valid until this object is next mutated or destroyed.
    return {present, absl::string_view{accumulator}};
  }

  // separator string
  std::string sep;

  // true if at least one, possibly empty, value was added
  bool present = false;
  std::string accumulator;
};

// Implementation of edge.group_by. Produces mapping.
template <typename T>
class GroupByAccumulator final
    : public Accumulator<AccumulatorType::kPartial, int64_t, meta::type_list<>,
                         meta::type_list<T>> {
 public:
  // group_counter is supposed to be shared across several instances of
  // GroupByAccumulator that process different input groups. Needed to avoid
  // group_id collisions in the output mapping.
  explicit GroupByAccumulator(int64_t* group_counter)
      : group_counter_(group_counter), status_(absl::OkStatus()) {}
  void Reset() final {
    // `clear` is not used in order to reuse memory.
    unique_values_index_.erase(unique_values_index_.begin(),
                               unique_values_index_.end());
  }
  void Add(view_type_t<T> v) final {
    if constexpr (std::is_floating_point_v<T>) {
      if (std::isnan(v)) {
        status_ = absl::InvalidArgumentError(
            "unable to compute edge.group_by, NaN key is not allowed");
        return;
      }
    }
    auto [iter, inserted] =
        unique_values_index_.try_emplace(v, *group_counter_);
    if (inserted) (*group_counter_)++;
    next_result_ = iter->second;
  }
  int64_t GetResult() final { return next_result_; }

  absl::Status GetStatus() final { return status_; }

 private:
  absl::flat_hash_map<view_type_t<T>, int64_t> unique_values_index_;
  int64_t* group_counter_;
  int64_t next_result_;
  absl::Status status_;
};

// array._take_over is a variant of array.take with a single edge.
template <typename T>
class ArrayTakeOverAccumulator
    : public Accumulator<
          AccumulatorType::kFull, OptionalValue<T>, meta::type_list<>,
          meta::type_list<OptionalValue<T>, OptionalValue<int64_t>>> {
 public:
  explicit ArrayTakeOverAccumulator()
      : return_id_(0), status_(absl::OkStatus()) {}

  // This could potentially be made faster if split size was passed to Reset.
  // We could reserve the space in vectors.
  void Reset() final {
    values_.clear();
    offsets_.clear();
    return_id_ = 0;
  };

  void Add(OptionalValue<view_type_t<T>> v,
           OptionalValue<int64_t> offset) final {
    values_.push_back(v);
    offsets_.push_back(offset);
  }

  void FinalizeFullGroup() final {
    for (auto& offset : offsets_) {
      if (ABSL_PREDICT_FALSE(
              offset.present &&
              (offset.value < 0 || offset.value >= offsets_.size()))) {
        status_ = absl::InvalidArgumentError(
            absl::StrFormat("invalid offsets: %d is not a valid offset of an "
                            "array of size %d",
                            offset.value, offsets_.size()));
        offset.value = values_.size();  // offset of fake element
      }
      if (!offset.present) {
        offset.value = values_.size();  // offset of fake element
      }
    }
    // add missing fake element to be used with missing offsets
    values_.push_back(std::nullopt);
  }

  OptionalValue<view_type_t<T>> GetResult() final {
    return values_[offsets_[return_id_++].value];
  }

  absl::Status GetStatus() final { return status_; }

 private:
  int64_t return_id_;
  std::vector<OptionalValue<view_type_t<T>>> values_;
  std::vector<OptionalValue<int64_t>> offsets_;
  absl::Status status_;
};

// array._take_over_over is a variant of array.take with two edges.
template <typename MultiEdgeUtil>
struct ArrayTakeOverOverOp {
  template <typename T>
  using Array = typename MultiEdgeUtil::template Array<T>;
  using Edge = typename MultiEdgeUtil::Edge;

  template <typename T>
  absl::StatusOr<Array<T>> operator()(EvaluationContext* ctx,
                                      const Array<T>& values,
                                      const Array<int64_t>& offsets,
                                      const Edge& values_edge,
                                      const Edge& offsets_edge) const {
    using OptT = OptionalValue<T>;
    using ValuesPerGroup = std::vector<view_type_t<OptT>>;
    std::vector<ValuesPerGroup> groups(values_edge.parent_size());
    absl::Span<ValuesPerGroup> groups_span(groups.data(), groups.size());
    std::optional<int64_t> incorrect_offset_id;

    auto add_values_fn = [](ValuesPerGroup& values_per_group, int64_t,
                            view_type_t<OptT> v) {
      values_per_group.push_back(v);
    };
    RETURN_IF_ERROR(
        MultiEdgeUtil::ApplyChildArgs(add_values_fn, groups_span, values_edge,
                                      meta::type_list<OptT>{}, values));

    auto result_fn = [&](const ValuesPerGroup& values_per_group,
                         int64_t child_id,
                         int64_t offset) -> view_type_t<OptT> {
      if (ABSL_PREDICT_FALSE(offset < 0 || offset >= values_per_group.size())) {
        incorrect_offset_id = child_id;
        return std::nullopt;
      }
      return values_per_group[offset];
    };
    auto res = MultiEdgeUtil::template ProduceResult<T>(
        &ctx->buffer_factory(), result_fn, groups_span, offsets_edge,
        meta::type_list<int64_t>{}, offsets);
    if (incorrect_offset_id) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "invalid offset %d at child_id=%d",
          offsets[*incorrect_offset_id].value, *incorrect_offset_id));
    }
    return res;
  }
};

template <typename ValueT>
class WeightedAverageAccumulator
    : public Accumulator<AccumulatorType::kAggregator, ValueT,
                         meta::type_list<>, meta::type_list<ValueT, ValueT>> {
  using AccumulatorT = typename WideAccumulator<ValueT>::type;

 public:
  void Reset() final {
    weighted_value_sum_ = 0;
    weight_sum_ = 0;
  };

  void Add(ValueT value, ValueT weight) final {
    weighted_value_sum_ +=
        static_cast<AccumulatorT>(value) * static_cast<AccumulatorT>(weight);
    weight_sum_ += static_cast<AccumulatorT>(weight);
  }

  void AddN(int64_t n, ValueT value, ValueT weight) final {
    weighted_value_sum_ += static_cast<AccumulatorT>(value) *
                           static_cast<AccumulatorT>(weight) * n;
    weight_sum_ += static_cast<AccumulatorT>(weight) * n;
  }

  ValueT GetResult() final {
    return static_cast<ValueT>(weighted_value_sum_ / weight_sum_);
  }

  AccumulatorT weighted_value_sum_, weight_sum_;
};

template <typename T>
struct CDFTypeTraits {
  using return_type = float;
};

// CDF for argument of type double has return type of double. This is a
// remnant of Arolla v1.
// TODO: Revisit this decision.
template <>
struct CDFTypeTraits<double> {
  using return_type = double;
};

// WeightedCDFAccumulator implements math._weighted_cdf. It accepts a stream of
// values and weights, and, for each
// value, outputs a percentile, i.e. the weighted percentage of values in the
// stream that are smaller than or equal to it.
template <typename T, typename W>
class WeightedCDFAccumulator
    : public Accumulator<AccumulatorType::kFull,
                         typename CDFTypeTraits<T>::return_type,
                         meta::type_list<>, meta::type_list<T, W>> {
  using ReturnType = typename CDFTypeTraits<T>::return_type;

 public:
  explicit WeightedCDFAccumulator() : return_id_(0) {}

  void Reset() final {
    values_.clear();
    return_id_ = 0;
  };

  void Add(T value, W weight) final {
    values_.push_back({value, values_.size(), weight});
  }

  void FinalizeFullGroup() final {
    cdf_.resize(values_.size());
    std::sort(values_.begin(), values_.end());

    // Use double, not float, or unit summation will saturate at 16777216.0f.
    double running_weight = 0;
    for (auto it = values_.begin(); it != values_.end(); ++it) {
      running_weight += std::get<2>(*it);
      cdf_[std::get<1>(*it)] = running_weight;
    }

    // Divide by total weight.
    for (int i = 0; i < cdf_.size(); ++i) {
      cdf_[i] = cdf_[i] / running_weight;
    }

    // Following for loop assumes at least one element.
    if (values_.size() == 0) {
      return;
    }

    // Handle equal values.
    for (auto it = std::next(values_.rbegin()); it != values_.rend(); ++it) {
      if (std::get<0>(*it) == std::get<0>(*std::prev(it))) {
        cdf_[std::get<1>(*it)] = cdf_[std::get<1>(*std::prev(it))];
      }
    }
  }

  ReturnType GetResult() final { return cdf_[return_id_++]; }

 private:
  int64_t return_id_;
  std::vector<std::tuple<T, int64_t, W>> values_;
  std::vector<ReturnType> cdf_;
};

template <typename T, typename TieBreaker>
class OrdinalRankAccumulator
    : public Accumulator<AccumulatorType::kFull, int64_t, meta::type_list<>,
                         meta::type_list<T, TieBreaker>> {
 public:
  OrdinalRankAccumulator() : return_id_(0), descending_(false) {}
  explicit OrdinalRankAccumulator(bool descending)
      : return_id_(0), descending_(descending) {}

  void Reset() final {
    elems_.clear();
    return_id_ = 0;
  };

  void Add(view_type_t<T> value, view_type_t<TieBreaker> tie_breaker) final {
    elems_.push_back({value, tie_breaker, static_cast<int64_t>(elems_.size())});
  }

  void FinalizeFullGroup() final {
    ranks_.resize(elems_.size());

    // Avoid sorting a range that contains NaNs.
    auto sort_end = elems_.end();
    if constexpr (std::numeric_limits<T>::has_quiet_NaN) {
      sort_end = std::stable_partition(
          elems_.begin(), sort_end,
          [](const Element& elem) { return !std::isnan(elem.value); });
    }

    if (descending_) {
      std::sort(elems_.begin(), sort_end, DescendingComparator());
    } else {
      std::sort(elems_.begin(), sort_end, AscendingComparator());
    }

    int64_t current_rank = 0;
    for (auto it = elems_.begin(); it != elems_.end(); ++it) {
      ranks_[it->position] = current_rank++;
    }
  }

  int64_t GetResult() final { return ranks_[return_id_++]; }

 private:
  struct Element {
    view_type_t<T> value;
    view_type_t<TieBreaker> tie_breaker;
    int64_t position;
  };

  struct AscendingComparator {
    bool operator()(const Element& a, const Element& b) const {
      return std::tie(a.value, a.tie_breaker, a.position) <
             std::tie(b.value, b.tie_breaker, b.position);
    }
  };

  struct DescendingComparator {
    // Compares the `value` in descending order while `tie_breaker` and
    // `position` in ascending order.
    // - Preserving ascending position guarantees that the ranks are assigned
    //   to ties in the stable-sorting order regardless of `descending`.
    // - Preserving ascending tie_breaker makes it easier to understand the
    //   ordering when multiple features are chained on tie_breaker.
    //
    //   Example:
    //     // Order by (x DESC, y ASC, z DESC).
    //     ordinal_rank(
    //         x, descending=True,
    //         tie_breaker=ordinal_rank(
    //             y, descending=False,
    //             tie_breaker=ordinal_rank(z, descending=True)))
    //
    bool operator()(const Element& a, const Element& b) const {
      return std::tie(a.value, b.tie_breaker, b.position) >
             std::tie(b.value, a.tie_breaker, a.position);
    }
  };

  int64_t return_id_;
  bool descending_;
  std::vector<Element> elems_;
  std::vector<int64_t> ranks_;
};

template <typename T>
class DenseRankAccumulator
    : public Accumulator<AccumulatorType::kFull, int64_t, meta::type_list<>,
                         meta::type_list<T>> {
 public:
  DenseRankAccumulator() : return_id_(0), descending_(false) {}
  explicit DenseRankAccumulator(bool descending)
      : return_id_(0), descending_(descending) {}

  void Reset() final {
    values_.clear();
    return_id_ = 0;
  };

  void Add(view_type_t<T> v) final {
    values_.push_back({v, static_cast<int64_t>(values_.size())});
  }

  void FinalizeFullGroup() final {
    ranks_.resize(values_.size());

    auto sort_end = values_.end();
    if constexpr (std::numeric_limits<T>::has_quiet_NaN) {
      // To avoid undefined behavior of sorting a range that contains NaNs,
      // we put NaNs to the end regardless of the sorting order. NaNs are
      // always assigned least significant ranks.
      sort_end = std::partition(
          values_.begin(), values_.end(),
          [](const auto& value) { return !std::isnan(value.first); });
    }
    if (descending_) {
      std::sort(values_.begin(), sort_end, std::greater<>());
    } else {
      std::sort(values_.begin(), sort_end);
    }
    if (!values_.empty()) {
      int64_t current_rank = 0;
      auto it = values_.begin();
      ranks_[it->second] = current_rank;
      for (auto prev_it = it++; it != values_.end(); prev_it = it++) {
        if (it->first != prev_it->first) {
          ++current_rank;
        }
        ranks_[it->second] = current_rank;
      }
    }
  }

  int64_t GetResult() final { return ranks_[return_id_++]; }

 private:
  int64_t return_id_;
  bool descending_;
  std::vector<std::pair<view_type_t<T>, int64_t>> values_;
  std::vector<int64_t> ranks_;
};

template <typename T>
class MedianAggregator
    : public Accumulator<AccumulatorType::kAggregator, OptionalValue<T>,
                         meta::type_list<>, meta::type_list<T>> {
 public:
  void Reset() final { values_.clear(); };

  void Add(view_type_t<T> v) final { values_.push_back(v); }

  OptionalValue<view_type_t<T>> GetResult() final {
    if (values_.empty()) {
      return std::nullopt;
    }
    if constexpr (std::numeric_limits<T>::has_quiet_NaN) {
      for (const auto& x : values_) {
        if (std::isnan(x)) {
          return x;
        }
      }
    }
    int64_t offset = (values_.size() - 1) / 2;
    std::nth_element(values_.begin(), values_.begin() + offset, values_.end());
    return values_[offset];
  }

 private:
  // A buffer to gather all values before sorting.
  std::vector<view_type_t<T>> values_;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_AGGREGATION_GROUP_OP_ACCUMULATORS_H_
