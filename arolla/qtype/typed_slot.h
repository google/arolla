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
#ifndef AROLLA_QTYPE_TYPED_SLOT_H_
#define AROLLA_QTYPE_TYPED_SLOT_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <tuple>
#include <typeinfo>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// A TypedSlot is a polymorphic wrapper around Slot<T> for value types which
// have a corresponding QTypeTraits class. It's only purpose is to allow
// Slots to be treated polymorphically, for example in the AddSlot().
class TypedSlot {
 public:
  template <typename T>
  using Slot = FrameLayout::Slot<T>;

  TypedSlot(const TypedSlot&) = default;
  TypedSlot& operator=(const TypedSlot&) = default;

  static TypedSlot UnsafeFromOffset(QTypePtr type, size_t byte_offset) {
    return TypedSlot(type, byte_offset);
  }

  // Create a TypedSlot from a Slot<T> given an explicit QType.
  template <typename T>
  static TypedSlot FromSlot(Slot<T> slot, QTypePtr type) {
    DCHECK(type->type_info() == typeid(T)) << "Type mismatch";
    return TypedSlot(type, slot.byte_offset());
  }

  // Create a TypedSlot from a Slot<T> where type can be inferred from T.
  template <typename T>
  static TypedSlot FromSlot(Slot<T> slot) {
    return TypedSlot(GetQType<T>(), slot.byte_offset());
  }

  // Convert a TypedSlot into a Slot<T>. There is a check for matching
  // type_info, but it is caller responsibility to ensure that the logical QType
  // also matches.
  template <class T>
  absl::StatusOr<Slot<T>> ToSlot() const {
    RETURN_IF_ERROR(VerifyType(typeid(T)));
    return Slot<T>::UnsafeSlotFromOffset(byte_offset_);
  }

  // Convert a TypedSlot into a Slot<T>. The caller must guarantee that
  // C++ type for GetType() matches T.
  template <class T>
  Slot<T> UnsafeToSlot() const {
    DCHECK(GetType()->type_info() == typeid(T));
    return Slot<T>::UnsafeSlotFromOffset(byte_offset_);
  }

  // Returns the number of sub-slots of this TypedSlot.
  int64_t SubSlotCount() const { return type_->type_fields().size(); }

  // Return a sub-slot with given index (should from 0 to SubSlotCount()-1).
  TypedSlot SubSlot(int64_t index) const {
    DCHECK_GE(index, 0);
    DCHECK_LT(index, SubSlotCount());
    const auto& field = type_->type_fields()[index];
    return TypedSlot(field.GetType(), byte_offset_ + field.byte_offset());
  }

  QTypePtr GetType() const { return type_; }

  size_t byte_offset() const { return byte_offset_; }

  // Copies data to the destination TypedSlot, which must be same type.
  // The `destination_alloc` can be same to `alloc` if we are copying within the
  // same memory layout.
  void CopyTo(ConstFramePtr source_frame, TypedSlot destination_slot,
              FramePtr destination_frame) const {
    DCHECK_EQ(type_, destination_slot.type_) << "Type mismatch";
    source_frame.DCheckFieldType(byte_offset_, type_->type_info());
    destination_frame.DCheckFieldType(destination_slot.byte_offset_,
                                      destination_slot.type_->type_info());
    type_->UnsafeCopy(
        source_frame.GetRawPointer(byte_offset_),
        destination_frame.GetRawPointer(destination_slot.byte_offset_));
  }

  // Resets value referenced by TypedSlot to its initial state.
  void Reset(FramePtr frame) const {
    frame.DCheckFieldType(byte_offset_, type_->type_info());
    const auto& layout = type_->type_layout();
    void* ptr = frame.GetRawPointer(byte_offset_);
    layout.DestroyAlloc(ptr);
    layout.InitializeAlignedAlloc(ptr);
  }

  friend bool operator==(const TypedSlot& a, const TypedSlot& b) {
    return a.type_ == b.type_ && a.byte_offset_ == b.byte_offset_;
  }
  friend bool operator!=(const TypedSlot& a, const TypedSlot& b) {
    return !(a == b);
  }
  template <typename H>
  friend H AbslHashValue(H h, const TypedSlot& a) {
    return H::combine(std::move(h), a.type_, a.byte_offset_);
  }

  friend std::ostream& operator<<(std::ostream& out, const TypedSlot& ts) {
    return out << "TypedSlot<" << ts.GetType()->name() << ">@"
               << ts.byte_offset_;
  }

  // Converts a sequence of typed slots into a tuple of slots of given types.
  // Returns an error if slot types don't match template arguments.
  template <typename... Ts>
  static absl::StatusOr<std::tuple<FrameLayout::Slot<Ts>...>> ToSlots(
      absl::Span<const TypedSlot> slots) {
    if (slots.size() != sizeof...(Ts)) {
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat("wrong number of slots: expected %d, got %d",
                          sizeof...(Ts), slots.size()));
    }
    return ToSlotsImpl<std::tuple<FrameLayout::Slot<Ts>...>>(
        slots, std::index_sequence_for<Ts...>{});
  }

 private:
  TypedSlot(const QType* type, size_t byte_offset)
      : type_(type), byte_offset_(byte_offset) {}

  absl::Status VerifyType(const std::type_info& tpe) const;

  template <typename ResultTuple, std::size_t... is>
  static absl::StatusOr<ResultTuple> ToSlotsImpl(
      absl::Span<const TypedSlot> slots, std::index_sequence<is...>) {
    (void)slots;  // Suppress warning for potentially unused variable.
    return LiftStatusUp(slots[is]
                            .ToSlot<typename std::tuple_element_t<
                                is, ResultTuple>::value_type>()...);
  }

  QTypePtr type_;
  size_t byte_offset_;
};

// Converts a sequence of `Slot<T>` into an array of `TypedSlot`s where all
// the types can be inferred from T.
template <typename... Slots>
std::array<TypedSlot, sizeof...(Slots)> ToTypedSlots(Slots... slots) {
  return std::array<TypedSlot, sizeof...(Slots)>{TypedSlot::FromSlot(slots)...};
}

// Extract types from given TypedSlots.
std::vector<QTypePtr> SlotsToTypes(absl::Span<const TypedSlot> slots);
absl::flat_hash_map<std::string, QTypePtr> SlotsToTypes(
    const absl::flat_hash_map<std::string, TypedSlot>& slots);

// Adds a slot of the requested type to the memory layout.
inline TypedSlot AddSlot(QTypePtr type, FrameLayout::Builder* layout_builder) {
  return TypedSlot::UnsafeFromOffset(
      type, layout_builder->AddSubFrame(type->type_layout()).byte_offset());
}

// Adds slots of the requested types to the memory layout.
std::vector<TypedSlot> AddSlots(absl::Span<const QTypePtr> types,
                                FrameLayout::Builder* layout_builder);

// Adds named slots of the requested types to the memory layout.
std::vector<std::pair<std::string, TypedSlot>> AddNamedSlots(
    absl::Span<const std::pair<std::string, QTypePtr>> types,
    FrameLayout::Builder* layout_builder);

// Adds named slots of the requested types to the memory layout.
absl::flat_hash_map<std::string, TypedSlot> AddSlotsMap(
    const absl::flat_hash_map<std::string, QTypePtr>& types,
    FrameLayout::Builder* layout_builder);

// Register additional slots to the memory layout to pass runtime type checks.
// Non-trivial fields registered this way are expected to be initialized and
// destroyed by their containing object.
// Caller is responsible for correctness of the provided slot.
absl::Status RegisterUnsafeSlots(absl::Span<const TypedSlot> slots,
                                 FrameLayout::Builder* layout_builder);

// Register additional slots to the memory layout to pass runtime type checks.
// Non-trivial fields registered this way are expected to be initialized and
// destroyed by their containing object.
// Caller is responsible for correctness of the provided slot.
absl::Status RegisterUnsafeSlotsMap(
    const absl::flat_hash_map<std::string, TypedSlot>& slots,
    FrameLayout::Builder* layout_builder);

// For each types_in_order element we find corresponding TypedSlot.
// If not found nullopt will be set. Error returned on type mismatch.
absl::StatusOr<std::vector<std::optional<TypedSlot>>>
MaybeFindSlotsAndVerifyTypes(
    absl::Span<const std::pair<std::string, QTypePtr>> types_in_order,
    const absl::flat_hash_map<std::string, TypedSlot>& slots);

// For each types_in_order element we find corresponding TypedSlot.
// If not found or type mismatch, then error will be returned.
absl::StatusOr<std::vector<TypedSlot>> FindSlotsAndVerifyTypes(
    absl::Span<const std::pair<std::string, QTypePtr>> types_in_order,
    const absl::flat_hash_map<std::string, TypedSlot>& slots);

// Verify that for every QType corresponding TypeSlot has correct type.
// Returns error if TypedSlot has incorrect type.
// If verify_missed_slots is true (default) check that there is a slot for each
// type.
// If verify_unwanted_slots is true (default),
// check that there is no additional not expected slots.
absl::Status VerifySlotTypes(
    const absl::flat_hash_map<std::string, QTypePtr>& types,
    const absl::flat_hash_map<std::string, TypedSlot>& slots,
    bool verify_unwanted_slots = true, bool verify_missed_slots = true);

}  // namespace arolla

#endif  // AROLLA_QTYPE_TYPED_SLOT_H_
