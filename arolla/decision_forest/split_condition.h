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
#ifndef AROLLA_DECISION_TREES_SPLIT_CONDITION_H_
#define AROLLA_DECISION_TREES_SPLIT_CONDITION_H_

#include <memory>
#include <string>

#include "absl//container/flat_hash_map.h"
#include "absl//container/inlined_vector.h"
#include "absl//hash/hash.h"
#include "absl//log/check.h"
#include "absl//types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

// Base class for all split conditions.
class SplitCondition {
 public:
  virtual ~SplitCondition() {}

  virtual std::string ToString() const = 0;

  struct InputSignature {
    int id;
    QTypePtr type;
  };
  using InputSignatures = absl::InlinedVector<InputSignature, 2>;

  // Returns id/QType pair for each used input.
  virtual InputSignatures GetInputSignatures() const = 0;

  // Return a copy of the condition with remapped input ids. If mapping doesn't
  // contain the input id, then the original one remains.
  virtual std::shared_ptr<SplitCondition> RemapInputs(
      const absl::flat_hash_map<int, int>& mapping) const = 0;

  virtual bool EvaluateCondition(const ConstFramePtr ctx,
                                 absl::Span<const TypedSlot> inputs) const = 0;

  template <typename H>
  friend H AbslHashValue(H state, const SplitCondition& value) {
    value.AbslHashValueImpl(absl::HashState::Create(&state));
    return std::move(state);
  }

  bool operator==(const SplitCondition& other) const {
    return this == &other || Equals(other);
  }
  bool operator!=(const SplitCondition& other) const {
    return !(*this == other);
  }

  virtual void CombineToFingerprintHasher(FingerprintHasher* hasher) const = 0;

 private:
  virtual void AbslHashValueImpl(absl::HashState state) const = 0;
  virtual bool Equals(const SplitCondition& other) const = 0;
};

template <class T>
class SingleInputSplitCondition : public SplitCondition {
 public:
  SingleInputSplitCondition() = default;
  explicit SingleInputSplitCondition(int input_id) : input_id_(input_id) {}

  bool EvaluateCondition(const ConstFramePtr ctx,
                         absl::Span<const TypedSlot> inputs) const final {
    DCHECK_EQ(inputs[input_id_].GetType(), GetInputQType());
    // Types should be already checked by DecisionForest::ValidateInputSlots.
    auto slot = FrameLayout::Slot<T>::UnsafeSlotFromOffset(
        inputs[input_id_].byte_offset());
    return EvaluateCondition(ctx.Get(slot));
  }

  virtual bool EvaluateCondition(const T& value) const = 0;

  std::shared_ptr<SplitCondition> RemapInputs(
      const absl::flat_hash_map<int, int>& mapping) const final {
    int new_input_id =
        mapping.contains(input_id_) ? mapping.at(input_id_) : input_id_;
    return WithNewInputId(new_input_id);
  }

  static QTypePtr GetInputQType() { return GetQType<T>(); }
  int input_id() const { return input_id_; }

  InputSignatures GetInputSignatures() const final {
    return {{input_id_, GetInputQType()}};
  }

 private:
  virtual std::shared_ptr<SplitCondition> WithNewInputId(
      int input_id) const = 0;

  int input_id_;
};

}  // namespace arolla

#endif  // AROLLA_DECISION_TREES_SPLIT_CONDITION_H_
