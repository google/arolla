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
#ifndef AROLLA_QTYPE_OPTIONAL_QTYPE_H_
#define AROLLA_QTYPE_OPTIONAL_QTYPE_H_

// IWYU pragma: always_keep, the file defines QTypeTraits<T> specializations.

#include "absl/base/no_destructor.h"  // IWYU pragma: keep, used in macro specialization.
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"  // IWYU pragma: keep, used in macro specialization.
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/meta.h"  // IWYU pragma: keep, used in macro specialization.

namespace arolla {

template <typename T>
QTypePtr GetOptionalQType() {
  return GetQType<OptionalValue<T>>();
}

// Returns true if `qtype` is an OptionalValue<> type.
bool IsOptionalQType(const QType* /*nullable*/ qtype);

// Returns the optional QType corresponding to this QType, or NotFoundError
// if there is no such QType. Returns `qtype` if `qtype` is an Optional QType.
// NOTE: The function relies on global variables initialization and so may
// return errors if called before all the globals are initialized.
absl::StatusOr<QTypePtr> ToOptionalQType(QTypePtr qtype);

// Returns the non-optional QType corresponding to this QType. Returns `qtype`
// if `qtype` is already a non-optional QType.
//
// Returns `nullptr` only for `nullptr` input.
const QType* /*nullable*/ DecayOptionalQType(const QType* /*nullable*/ qtype);

// Extract the presence slot from a TypedSlot containing an OptionalValue.
// Returns an error if the given slot does not correspond to an
// OptionalValue<T>.
absl::StatusOr<FrameLayout::Slot<bool>> GetPresenceSubslotFromOptional(
    TypedSlot slot);

// Extract the presence slot from a TypedSlot containing an OptionalValue.
// The argument must be OptionalValue qtype.
inline FrameLayout::Slot<bool> UnsafePresenceSubslotFromOptional(
    TypedSlot slot) {
  DCHECK(IsOptionalQType(slot.GetType()));
  return slot.SubSlot(0).UnsafeToSlot<bool>();
}

// Extract the presence slot from an OptionalValue<T> slot. Prefer when slot
// type is known at compile time.
template <typename T>
FrameLayout::Slot<bool> GetPresenceSubslotFromOptional(
    FrameLayout::Slot<OptionalValue<T>> slot) {
  return slot.template GetSubslot<0>();
}

// Utility function to extract the value slot from a TypedSlot containing
// an OptionalValue. Returns an error if the given slot does not correspond
// to an OptionalValue<T>.
//
// Note that since the layout of OptionalValue<Unit> does not contain a value
// field, this function will also return an error for that QType.
absl::StatusOr<TypedSlot> GetValueSubslotFromOptional(TypedSlot slot);

// Utility function to extract the presence slot from a TypedSlot containing
// an OptionalValue. The argument must be OptionalValue<T> qtype where T is not
// arolla::Unit.
inline TypedSlot UnsafeValueSubslotFromOptional(TypedSlot slot) {
  DCHECK(IsOptionalQType(slot.GetType()));
  DCHECK_EQ(slot.SubSlotCount(), 2);
  return slot.SubSlot(1);
}

// Returns true if the given TypedRef to optional is present. The argument must
// point to OptionalValue.
inline bool UnsafeIsPresent(TypedRef optional) {
  DCHECK(IsOptionalQType(optional.GetType()));
  DCHECK_GE(optional.GetFieldCount(), 1);
  return optional.GetField(0).UnsafeAs<bool>();
}

// Extract the value slot from an OptionalValue<T> slot. Prefer when slot type
// is known at compile time.
template <typename T>
FrameLayout::Slot<T> GetValueSubslotFromOptional(
    FrameLayout::Slot<OptionalValue<T>> slot) {
  return slot.template GetSubslot<1>();
}

// Constructs missing optional value of the given optional_qtype. Returns an
// error if the given type is not optional.
absl::StatusOr<TypedValue> CreateMissingValue(QTypePtr optional_qtype);

// Registers the given optional_qtype as an optional QType.
//
// An optional QType should have two fields, for presence (bool) and value (of
// the type returned by optional_qtype->value_qtype()), unless it is
// OPTIONAL_UNIT where there is only presence.)
void RegisterOptionalQType(QTypePtr optional_qtype);

// Template for declaring QTypeTraits for optional types. An optional type
// must be declared following the declaration of the corresponding non-optional
// type. This macro also registers the association between the optional and
// corresponding non-optional types.
#define AROLLA_DECLARE_OPTIONAL_QTYPE(NAME, /*BASE_TYPE*/...) \
  AROLLA_DECLARE_QTYPE(OptionalValue<__VA_ARGS__>)

#define AROLLA_DEFINE_OPTIONAL_QTYPE(NAME, /*BASE_TYPE*/...)           \
  QTypePtr QTypeTraits<OptionalValue<__VA_ARGS__>>::type() {           \
    static const absl::NoDestructor<SimpleQType> result(               \
        meta::type<OptionalValue<__VA_ARGS__>>(), ("OPTIONAL_" #NAME), \
        GetQType<__VA_ARGS__>());                                      \
    return result.get();                                               \
  }                                                                    \
  static const int optional_##NAME##_registered =                      \
      (RegisterOptionalQType(GetOptionalQType<__VA_ARGS__>()), 1);

}  // namespace arolla

#endif  // AROLLA_QTYPE_OPTIONAL_QTYPE_H_
