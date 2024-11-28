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
#ifndef AROLLA_QEXPR_OPERATORS_EXPERIMENTAL_DENSE_ARRAY_TIMESERIES_H_
#define AROLLA_QEXPR_OPERATORS_EXPERIMENTAL_DENSE_ARRAY_TIMESERIES_H_

#include <cstdint>
#include <deque>
#include <optional>

#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/aggregation_ops_interface.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/util/meta.h"
#include "arolla/util/view_types.h"

namespace arolla {
namespace moving_average_operator_impl {

template <typename ScalarT>
class MovingAverageAccumulator final
    : public Accumulator<AccumulatorType::kPartial, OptionalValue<ScalarT>,
                         meta::type_list<>,
                         meta::type_list<OptionalValue<ScalarT>>> {
 public:
  explicit MovingAverageAccumulator(int window_size)
      : window_size_(window_size) {}
  void Reset() final {
    current_window_.clear();
    window_sum_ = 0;
  }

  void Add(OptionalValue<ScalarT> tail_value) final {
    if (tail_value.present) {
      current_window_.push_front(tail_value.value);
      window_sum_ += tail_value.value;
    } else {
      Reset();  // reset queue on missing time-series value
    }
  }

  OptionalValue<ScalarT> GetResult() final {
    if (current_window_.size() == window_size_) {
      auto result = window_sum_ / window_size_;
      window_sum_ -= current_window_.back();
      current_window_.pop_back();
      return result;
    } else {
      return std::nullopt;
    }
  }

 private:
  std::deque<ScalarT> current_window_;
  int window_size_;
  double window_sum_ = 0;
};

}  // namespace moving_average_operator_impl

// Moving average operator.
//
// Takes in the (time-series) values and returns the trailing window moving
// average for the specified window size.
struct AggMovingAverageOp {
  template <typename ScalarT>
  absl::StatusOr<DenseArray<ScalarT>> operator()(
      EvaluationContext* ctx,
      const DenseArray<ScalarT>& series,  // detail array
      const int64_t window_size, const DenseArrayEdge& edge) const {
    using MovingAvgAcc =
        moving_average_operator_impl::MovingAverageAccumulator<ScalarT>;
    DenseGroupOps<MovingAvgAcc> agg(&ctx->buffer_factory(),
                                    MovingAvgAcc(window_size));
    return agg.Apply(edge, series);
  }
};

// Exponential weighted average operator.
//
// Takes in the (time-series) values and returns the exponential weighted moving
// average. The implementation follows the behaviour in
// pd.DataFrame.ewm(alpha, ignore_mising).mean()
// https://pandas.pydata.org/docs/dev/reference/api/pandas.DataFrame.ewm.html
struct ExponentialWeightedMovingAverageOp {
  template <typename ScalarT>
  DenseArray<ScalarT> AdjustedEWMA(const DenseArray<ScalarT>& series,
                                   double alpha,
                                   bool ignore_missing = false) const {
    DenseArrayBuilder<ScalarT> builder(series.size());
    int64_t previous_non_missing_id = -1;
    double previous_non_missing_value = 0;
    double current_ewma_numerator = 0;
    double current_ewma_denominator = 0;

    series.ForEach([&](int64_t current_row_id, bool present,
                       view_type_t<ScalarT> tail_value) {
      if (!present) return;
      if (previous_non_missing_id >= 0) {
        // Fill in the skipped entries with the previous non-missing value.
        for (int64_t i = previous_non_missing_id + 1; i < current_row_id; i++) {
          builder.Set(i, previous_non_missing_value);
          if (!ignore_missing) {
            current_ewma_numerator *= (1.0 - alpha);
            current_ewma_denominator *= (1.0 - alpha);
          }
        }
      }

      current_ewma_numerator =
          tail_value + (1. - alpha) * current_ewma_numerator;
      current_ewma_denominator = 1. + (1. - alpha) * current_ewma_denominator;
      previous_non_missing_value =
          current_ewma_numerator / current_ewma_denominator;
      builder.Set(current_row_id, previous_non_missing_value);

      previous_non_missing_id = current_row_id;
    });

    return std::move(builder).Build();
  }

  template <typename ScalarT>
  DenseArray<ScalarT> UnadjustedEWMA(const DenseArray<ScalarT>& series,
                                     double alpha,
                                     bool ignore_missing = false) const {
    DenseArrayBuilder<ScalarT> builder(series.size());
    int64_t previous_non_missing_id = -1;
    double previous_non_missing_value = 0;

    series.ForEach([&](int64_t current_row_id, bool present,
                       view_type_t<ScalarT> tail_value) {
      if (!present) return;
      double previous_weight = (1. - alpha);

      if (previous_non_missing_id >= 0) {
        // Fill in the skipped entries with the previous non-missing value.
        for (int64_t i = previous_non_missing_id + 1; i < current_row_id; i++) {
          builder.Set(i, previous_non_missing_value);
          if (!ignore_missing) {
            previous_weight *= (1. - alpha);
          }
        }
      } else {
        previous_non_missing_value = tail_value;
      }

      previous_non_missing_value =
          (alpha * tail_value + previous_weight * previous_non_missing_value) /
          (alpha + previous_weight);
      builder.Set(current_row_id, previous_non_missing_value);

      previous_non_missing_id = current_row_id;
    });

    return std::move(builder).Build();
  }

  template <typename ScalarT>
  absl::StatusOr<DenseArray<ScalarT>> operator()(
      const DenseArray<ScalarT>& series, double alpha, bool adjust = true,
      bool ignore_missing = false) const {
    if (alpha <= 0 || alpha > 1) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat("alpha must be in range (0, 1], got %f", alpha));
    }

    if (adjust) {
      return AdjustedEWMA(series, alpha, ignore_missing);
    } else {
      return UnadjustedEWMA(series, alpha, ignore_missing);
    }
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_EXPERIMENTAL_DENSE_ARRAY_TIMESERIES_H_
