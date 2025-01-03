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
#ifndef AROLLA_DECISION_FOREST_INTERVAL_SPLIT_CONDITION_H_
#define AROLLA_DECISION_FOREST_INTERVAL_SPLIT_CONDITION_H_

#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "absl//hash/hash.h"
#include "absl//strings/str_format.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/cityhash.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

class IntervalSplitCondition final
    : public SingleInputSplitCondition<OptionalValue<float>> {
 public:
  IntervalSplitCondition() = default;
  IntervalSplitCondition(int input_id, float left, float right)
      : SingleInputSplitCondition<OptionalValue<float>>(input_id),
        left_(left),
        right_(right) {}

  bool EvaluateCondition(const OptionalValue<float>& value) const final {
    return value.present && left_ <= value.value && value.value <= right_;
  }

  std::string ToString() const override {
    return absl::StrFormat("#%d in range [%.6f %.6f]", input_id(), left_,
                           right_);
  }

  float left() const { return left_; }
  float right() const { return right_; }

  size_t StableHash() const final {
    std::array<size_t, 2> values = {std::hash<float>()(left_),
                                    std::hash<float>()(right_)};
    return CityHash64WithSeed(values.data(), sizeof(values), input_id());
  }

 private:
  void AbslHashValueImpl(absl::HashState state) const override {
    absl::HashState::combine(std::move(state), input_id(), left_, right_);
  }

  void CombineToFingerprintHasher(FingerprintHasher* hasher) const override {
    hasher->Combine(input_id(), left_, right_);
  }

  bool Equals(const SplitCondition& other) const override {
    auto as_interval =
        fast_dynamic_downcast_final<const IntervalSplitCondition*>(&other);
    if (as_interval == nullptr) return false;
    return input_id() == as_interval->input_id() &&
           left_ == as_interval->left_ && right_ == as_interval->right_;
  }
  std::shared_ptr<SplitCondition> WithNewInputId(int input_id) const override {
    return std::make_shared<IntervalSplitCondition>(input_id, left_, right_);
  }

  float left_, right_;
};

auto inline IntervalSplit(int input_id, float left, float right) {
  return std::make_shared<IntervalSplitCondition>(input_id, left, right);
}

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_INTERVAL_SPLIT_CONDITION_H_
