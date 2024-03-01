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
#ifndef AROLLA_IO_INPLACE_LOADER_H_
#define AROLLA_IO_INPLACE_LOADER_H_

#include <cstddef>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {

// Helper for reading/writing data already stored inside FrameLayout.
// Original data stored in the FrameLayout must either be a buffer or
// satisfy StandardLayoutType (e.g., POD).
// https://en.cppreference.com/w/cpp/named_req/StandardLayoutType
// InplaceSlotBuilder helps to create .
// Example:
// struct MyInput {
//   int a;
//   double b;
// };
//
// FrameLayout::Builder layout_builder;
// auto struct_slot = layout_builder.AddSlot<MyInput>();
//
// InplaceSlotBuilder<MyInput> builder;
// CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, a, "a"));
// CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, b, "super_b"));
//
// auto input_slots = builder.OutputSlots(struct_slot);
template <class Struct>
class InplaceSlotBuilder final {
  static_assert(
      std::is_standard_layout<Struct>::value,
      "Data must be standard layout to be used with InplaceSlotBuilder.");

 public:
  using value_type = Struct;

  // Returns named TypedSlots pointing inside of another slot of FrameLayout.
  absl::flat_hash_map<std::string, TypedSlot> OutputSlots(
      FrameLayout::Slot<Struct> slot) const {
    absl::flat_hash_map<std::string, TypedSlot> slots;
    slots.reserve(zero_based_slots_.size());
    for (const auto& name_slot : zero_based_slots_) {
      slots.insert({name_slot.first,
                    TypedSlot::UnsafeFromOffset(
                        name_slot.second.GetType(),
                        name_slot.second.byte_offset() + slot.byte_offset())});
    }
    return slots;
  }

  // Adding named field with provided type and offset.
  // Returns FailedPreconditionError on duplicated name.
  absl::Status AddUnsafeField(const std::string& name, QTypePtr type,
                              size_t field_offset) {
    if (!zero_based_slots_
             .insert({name, TypedSlot::UnsafeFromOffset(type, field_offset)})
             .second) {
      return absl::FailedPreconditionError(absl::StrCat(
          "InplaceLoaderBuilder: duplicated slot name: '", name, "'"));
    }
    return absl::OkStatus();
  }

 private:
  absl::flat_hash_map<std::string, TypedSlot> zero_based_slots_;
};

// Adds a struct field to the InplaceSlotBuilder.
// Name is string representation of the field.
// Offset is computed using offsetof standard C macro.
// Type is determinated automatically by the field type.
// Args:
//   builder: InplaceSlotBuilder to add field
//   field: member-designator as in offsetof macro (e.g., field name).
//     Nested fields with member-designator like a.b.c works well, but it is
//     not clear to be guaranteed by C++ standard.
//   name: name of the field in resulted OutputSlots map.
// Returns:
//   Non OkStatus in case of field name duplication.
// See comments for InplaceSlotBuilder for usage examples.
#define AROLLA_ADD_INPLACE_SLOT_FIELD(builder, field, name)                 \
  builder.AddUnsafeField(                                                   \
      name,                                                                 \
      ::arolla::GetQType<                                                   \
          decltype(std::declval<decltype(builder)::value_type>().field)>(), \
      offsetof(decltype(builder)::value_type, field))

}  // namespace arolla

#endif  // AROLLA_IO_INPLACE_LOADER_H_
