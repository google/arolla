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
#include "arolla/objects/operators/qexpr_operators.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/objects/object_qtype.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"

namespace arolla {
namespace {

class MakeObjectOperator : public QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator([named_tuple_slot = input_slots[0],
                              prototype_slot = input_slots[1],
                              output_slot = output_slot.UnsafeToSlot<Object>()](
                                 EvaluationContext*, FramePtr frame) -> void {
      Object::Attributes attributes;
      attributes.reserve(named_tuple_slot.SubSlotCount());
      absl::Span<const std::string> field_names =
          GetFieldNames(named_tuple_slot.GetType());
      for (int64_t index = 0; index < named_tuple_slot.SubSlotCount();
           ++index) {
        auto field_slot = named_tuple_slot.SubSlot(index);
        auto tv = TypedValue::FromSlot(field_slot, frame);
        attributes.insert({field_names[index], std::move(tv)});
      }
      std::optional<Object> prototype;
      if (prototype_slot.GetType() != GetUnspecifiedQType()) {
        prototype = frame.Get(prototype_slot.UnsafeToSlot<Object>());
      }
      frame.Set(output_slot,
                Object(std::move(attributes), std::move(prototype)));
    });
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> MakeObjectOperatorFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (!IsNamedTupleQType(input_types[0])) {
    return absl::InvalidArgumentError(
        "requires the first argument to be NamedTuple");
  }
  if (input_types[1] != GetUnspecifiedQType() &&
      input_types[1] != GetQType<Object>()) {
    return absl::InvalidArgumentError(
        "requires the second argument to be unspecified or an Object");
  }
  return std::make_shared<MakeObjectOperator>(input_types, output_type);
}

}  // namespace arolla
