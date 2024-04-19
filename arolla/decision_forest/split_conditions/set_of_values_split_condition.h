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
#ifndef AROLLA_DECISION_FOREST_SET_OF_VALUES_SPLIT_CONDITION_H_
#define AROLLA_DECISION_FOREST_SET_OF_VALUES_SPLIT_CONDITION_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

template <class T>
class SetOfValuesSplitCondition final
    : public SingleInputSplitCondition<OptionalValue<T>> {
 public:
  SetOfValuesSplitCondition() = default;
  SetOfValuesSplitCondition(int input_id, absl::flat_hash_set<T> values,
                            bool result_if_missed = false)
      : SingleInputSplitCondition<OptionalValue<T>>(input_id),
        values_(std::move(values)),
        result_if_missed_(result_if_missed) {}

  bool EvaluateCondition(const OptionalValue<T>& value) const final {
    return (value.present && values_.contains(value.value)) ||
           (!value.present && result_if_missed_);
  }

  std::string ToString() const override {
    std::string res = absl::StrFormat("#%d in set [", this->input_id());
    absl::StrAppend(&res,
                    absl::StrJoin(ValuesAsVector(), ", ", ValueFormatter()));
    absl::StrAppend(&res, "]");
    if (result_if_missed_) {
      absl::StrAppend(&res, " or missed");
    }
    return res;
  }

  const absl::flat_hash_set<T>& values() const { return values_; }

  const bool GetDefaultResultForMissedInput() const {
    return result_if_missed_;
  }

  // Convert to vector for deterministic order.
  std::vector<T> ValuesAsVector() const {
    std::vector<T> vec(values_.begin(), values_.end());
    absl::c_sort(vec);
    return vec;
  }

 private:
  struct ValueFormatter {
    void operator()(std::string* out, const T& v) const;
  };

  void AbslHashValueImpl(absl::HashState state) const override {
    absl::HashState::combine(std::move(state), this->input_id(),
                             result_if_missed_, ValuesAsVector(),
                             this->GetInputQType()->name());
  }

  void CombineToFingerprintHasher(FingerprintHasher* hasher) const override {
    hasher->Combine(this->input_id(), result_if_missed_, this->GetInputQType())
        .CombineSpan(ValuesAsVector());
  }

  bool Equals(const SplitCondition& other) const override {
    auto as_set_of_values =
        fast_dynamic_downcast_final<const SetOfValuesSplitCondition<T>*>(
            &other);
    if (as_set_of_values == nullptr) return false;
    return this->input_id() == as_set_of_values->input_id() &&
           values_ == as_set_of_values->values_ &&
           result_if_missed_ == as_set_of_values->result_if_missed_;
  }

  std::shared_ptr<SplitCondition> WithNewInputId(int input_id) const override {
    return std::make_shared<SetOfValuesSplitCondition<T>>(input_id, values_,
                                                          result_if_missed_);
  }

  absl::flat_hash_set<T> values_;
  bool result_if_missed_;
};

template <class T>
void SetOfValuesSplitCondition<T>::ValueFormatter::operator()(
    std::string* out, const T& v) const {
  absl::StrAppend(out, v);
}

template <>
inline void SetOfValuesSplitCondition<Bytes>::ValueFormatter::operator()(
    std::string* out, const Bytes& v) const {
  absl::StrAppend(out, "b'", absl::string_view(v), "'");
}

template <typename T>
std::shared_ptr<SetOfValuesSplitCondition<T>> SetOfValuesSplit(
    int input_id, absl::flat_hash_set<T> set, bool result_if_missed) {
  return std::make_shared<SetOfValuesSplitCondition<T>>(
      input_id, std::move(set), result_if_missed);
}

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_SET_OF_VALUES_SPLIT_CONDITION_H_
