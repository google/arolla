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
#ifndef AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_BOUND_SPLIT_CONDITIONS_H_
#define AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_BOUND_SPLIT_CONDITIONS_H_

#include <memory>
#include <variant>

#include "absl//container/flat_hash_set.h"
#include "absl//container/inlined_vector.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//types/span.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// For every SplitCondition we should declare a bound condition with a function
// bool operator()(const ConstFramePtr ctx)

// BoundCondition should be default constructible because during forest
// compilation we create arrays at first and then initialize them
// in non-sequential order.

struct IntervalBoundCondition {
  FrameLayout::Slot<OptionalValue<float>> input_slot =
      // should be default constructible
      FrameLayout::Slot<OptionalValue<float>>::UnsafeUninitializedSlot();
  float left, right;
  bool operator()(const ConstFramePtr ctx) const {
    OptionalValue<float> v = ctx.Get(input_slot);
    return v.present && left <= v.value && v.value <= right;
  }

  using cond_type = IntervalSplitCondition;
  static absl::StatusOr<IntervalBoundCondition> Create(
      const std::shared_ptr<const cond_type>& cond,
      absl::Span<const TypedSlot> input_slots) {
    ASSIGN_OR_RETURN(
        auto input_slot,
        input_slots[cond->input_id()].ToSlot<OptionalValue<float>>());
    return IntervalBoundCondition{input_slot, cond->left(), cond->right()};
  }
};

template <class T>
struct SetOfValuesBoundCondition {
  FrameLayout::Slot<OptionalValue<T>> input_slot =
      // should be default constructible
      FrameLayout::Slot<OptionalValue<T>>::UnsafeUninitializedSlot();
  absl::flat_hash_set<T> values;
  bool result_if_missed;
  bool operator()(const ConstFramePtr ctx) const {
    const OptionalValue<T>& v = ctx.Get(input_slot);
    return (v.present && values.contains(v.value)) ||
           (!v.present && result_if_missed);
  }

  using cond_type = SetOfValuesSplitCondition<T>;
  static absl::StatusOr<SetOfValuesBoundCondition> Create(
      const std::shared_ptr<const cond_type>& cond,
      absl::Span<const TypedSlot> input_slots) {
    ASSIGN_OR_RETURN(
        auto input_slot,
        input_slots[cond->input_id()].template ToSlot<OptionalValue<T>>());
    return SetOfValuesBoundCondition{input_slot, cond->values(),
                                     cond->GetDefaultResultForMissedInput()};
  }
};

// It is a wrapper on top of unbound SplitCondition. Consists of a pointer and
// a list of input slots. It's slow, but provides universal support for
// all split conditions.
struct VirtualBoundCondition {
  std::shared_ptr<const SplitCondition> condition;
  absl::InlinedVector<TypedSlot, 1> inputs;
  bool operator()(const ConstFramePtr ctx) const {
    return condition->EvaluateCondition(ctx, inputs);
  }

  using cond_type = SplitCondition;
  static absl::StatusOr<VirtualBoundCondition> Create(
      const std::shared_ptr<const cond_type>& cond,
      absl::Span<const TypedSlot> input_slots) {
    VirtualBoundCondition res;
    res.condition = cond;
    res.inputs.insert(res.inputs.end(), input_slots.begin(), input_slots.end());
    return res;
  }
};

// Bound condition that is std::variant of several others bound conditions.
// VariantBoundCondition::Create accepts SplitCondition, tries to cast it to
// one of implementations, and converts it to the corresponding bound condition.
template <typename... Args>
class VariantBoundCondition {
 public:
  bool operator()(const ConstFramePtr ctx) const {
    return std::visit(Visitor{ctx}, bound_condition_);
  }

  static absl::StatusOr<VariantBoundCondition<Args...>> Create(
      const std::shared_ptr<const SplitCondition>& condition,
      absl::Span<const TypedSlot> input_slots) {
    VariantBoundCondition<Args...> res;
    bool initialized = false;

    for (auto status :
         {res.TryInit<Args>(condition, input_slots, &initialized)...}) {
      if (!status.ok()) return status;
    }

    if (initialized) {
      return res;
    } else {
      return absl::InvalidArgumentError("unsupported SplitCondition");
    }
  }

 private:
  template <class T>
  absl::Status TryInit(const std::shared_ptr<const SplitCondition>& cond,
                       absl::Span<const TypedSlot> input_slots,
                       bool* initialized) {
    if (*initialized) return absl::OkStatus();
    if (auto casted_cond =
            std::dynamic_pointer_cast<const typename T::cond_type>(cond)) {
      ASSIGN_OR_RETURN(bound_condition_, T::Create(casted_cond, input_slots));
      *initialized = true;
    }
    return absl::OkStatus();
  }

  struct Visitor {
    const ConstFramePtr context;
    explicit Visitor(const ConstFramePtr ctx) : context(ctx) {}

    template <class T>
    bool operator()(const T& condition) {
      return condition(context);
    }
  };

  std::variant<Args...> bound_condition_;
};

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_BOUND_SPLIT_CONDITIONS_H_
