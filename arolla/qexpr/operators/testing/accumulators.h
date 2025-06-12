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
// Accumulators used in testing group operations.

#ifndef AROLLA_QEXPR_OPERATORS_TESTING_ACCUMULATORS_H_
#define AROLLA_QEXPR_OPERATORS_TESTING_ACCUMULATORS_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>
#include <string>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/aggregation_ops_interface.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"

namespace arolla::testing {

// Simple sum aggregation.
template <typename T>
class AggSumAccumulator final
    : public Accumulator<AccumulatorType::kAggregator, OptionalValue<T>,
                         meta::type_list<>, meta::type_list<T>> {
 public:
  void Reset() final { accumulator_ = {false, 0}; }
  void Add(T value) final { accumulator_ = accumulator_.value + value; }
  OptionalValue<T> GetResult() final { return accumulator_; }

 private:
  OptionalValue<T> accumulator_;
};

// A simple accumulator with default value for empty groups.
template <typename T>
class AggCountAccumulator final
    : public Accumulator<AccumulatorType::kAggregator, int64_t,
                         meta::type_list<>, meta::type_list<T>> {
 public:
  void Reset() final { count_ = 0; }
  void Add(T value) final { count_++; }
  int64_t GetResult() final { return count_; }

 private:
  int64_t count_ = 12345;  // to test that Reset() is used before first Add.
};

// Average with a status.
class AverageAccumulator final
    : public Accumulator<AccumulatorType::kAggregator, float, meta::type_list<>,
                         meta::type_list<float>> {
 public:
  void Reset() final { accumulator_ = count_ = 0; }
  void Add(float value) final {
    accumulator_ += value;
    count_++;
  }
  float GetResult() final {
    if (count_ > 0) {
      return accumulator_ / count_;
    } else {
      error_ = true;
      return 0;
    }
  }
  absl::Status GetStatus() final {
    if (error_) {
      return absl::InvalidArgumentError("empty group");
    } else {
      return absl::OkStatus();
    }
  }

 private:
  float accumulator_ = 0;
  int64_t count_ = 0;
  bool error_ = false;
};

// Fake accumulator that just saves all inputs to given vector.
class CollectIdsAccumulator final
    : public Accumulator<AccumulatorType::kAggregator, float, meta::type_list<>,
                         meta::type_list<int64_t>> {
 public:
  explicit CollectIdsAccumulator(std::vector<int64_t>* ids)
      : detail_ids_(ids) {}
  void Reset() final {}
  void Add(int64_t detail_id) final { detail_ids_->push_back(detail_id); }
  float GetResult() final { return 0; }

 private:
  std::vector<int64_t>* detail_ids_;
};

// Full accumulator implementing AggRank.
template <typename T>
class RankValuesAccumulator final
    : public Accumulator<AccumulatorType::kFull, OptionalValue<int64_t>,
                         meta::type_list<>, meta::type_list<T>> {
 public:
  void Reset() final {
    values_.clear();
    offsets_.clear();
    processed_ = false;
  }
  void Add(T value) final { values_.push_back(value); }

  void FinalizeFullGroup() {
    offsets_.resize(values_.size());
    std::iota(offsets_.begin(), offsets_.end(), 0);
    // Avoid sorting a range that contains NaNs.
    auto valid_values_end = values_.end();
    if constexpr (std::numeric_limits<float>::has_quiet_NaN) {
      valid_values_end = std::stable_partition(
          values_.begin(), valid_values_end,
          [](const T value) { return !std::isnan(value); });
    }

    const size_t sort_offset = values_.end() - valid_values_end;
    std::sort(offsets_.begin(), offsets_.end() - sort_offset,
              [this](int64_t i, int64_t j) { return values_[i] > values_[j]; });
    result_iter_ = offsets_.begin();
    processed_ = true;
  }

  OptionalValue<int64_t> GetResult() final {
    CHECK(processed_);
    if (result_iter_ != offsets_.end()) {
      return *(result_iter_++);
    }
    return {};
  }

 private:
  bool processed_ = false;
  std::vector<T> values_;
  std::vector<int64_t> offsets_;
  std::vector<int64_t>::const_iterator result_iter_;
};

// Compute ax+by+cz, with (a,b,c) coming from group rows and (x, y, z)
// coming from detail rows.
class WeightedSumAccumulator final
    : public Accumulator<AccumulatorType::kPartial, float,
                         meta::type_list<float, float, float>,
                         meta::type_list<float, float, float>> {
 public:
  void Reset(float a, float b, float c) final {
    a_ = a;
    b_ = b;
    c_ = c;
    result_ = 0.0f;
  }

  void Add(float x, float y, float z) final {
    result_ = (a_ * x) + (b_ * y) + (c_ * z);
  }

  float GetResult() final { return result_; }

 private:
  float a_;
  float b_;
  float c_;
  float result_;
};

// Aggregates Text values and optional comments.
class AggTextAccumulator final
    : public Accumulator<AccumulatorType::kAggregator, Text,
                         meta::type_list<OptionalValue<Text>>,
                         meta::type_list<Text, OptionalValue<Text>>> {
 public:
  void Reset(OptionalValue<absl::string_view> prefix) final {
    res_.clear();
    if (prefix.present) res_.append(prefix.value);
  }

  void Add(absl::string_view value,
           OptionalValue<absl::string_view> comment) final {
    if (comment.present) {
      absl::StrAppendFormat(&res_, "%s (%s)\n", value, comment.value);
    } else {
      absl::StrAppendFormat(&res_, "%s\n", value);
    }
  }

  absl::string_view GetResult() final { return res_; }

 private:
  std::string res_;
};

}  // namespace arolla::testing

#endif  // AROLLA_QEXPR_OPERATORS_TESTING_ACCUMULATORS_H_
