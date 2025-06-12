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
#ifndef AROLLA_QTYPE_TYPED_REF_H_
#define AROLLA_QTYPE_TYPED_REF_H_

#include <cstdint>
#include <functional>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Reference to an immutable value of a given QType.
class TypedRef {
 public:
  // Creates a reference to `value`.
  template <typename T>
  static TypedRef FromValue(const T& value) {
    return TypedRef(GetQType<T>(), &value);
  }

  // Creates a reference to `value`. Returns an error if `typeid(value)`
  // does not match `type`.
  template <typename T>
  static absl::StatusOr<TypedRef> FromValue(const T& value, QTypePtr type) {
    RETURN_IF_ERROR(VerifyQTypeTypeInfo(type, typeid(T)));
    return TypedRef(type, &value);
  }

  // Creates a reference to a value in a `slot` within the allocation
  // referenced by `ptr`.
  static TypedRef FromSlot(TypedSlot slot, ConstFramePtr ptr);

  // Creates a reference for a qtype and a pointer to a value.
  // This method is unsafe and not recommended for direct usage.
  static TypedRef UnsafeFromRawPointer(QTypePtr type, const void* value_ptr) {
    // Check that the provided pointer is not null; otherwise, the reference
    // isn't going to be valid. There is an exemption for types with no size.
    // For such a type, the value pointer is never dereferenced, and it might be
    // null.
    DCHECK(value_ptr || type->type_layout().AllocSize() == 0);
    return TypedRef(type, value_ptr);
  }

  // Copyable.
  TypedRef(const TypedRef&) = default;
  TypedRef& operator=(const TypedRef&) = default;

  QTypePtr GetType() const { return type_; }

  const void* GetRawPointer() const { return value_ptr_; }

  // Returns field count. Equivalent to GetType()->type_field().size().
  int64_t GetFieldCount() const { return type_->type_fields().size(); }

  // Returns reference to a field with given index.
  TypedRef GetField(int64_t index) const {
    DCHECK_GE(index, 0);
    DCHECK_LT(index, GetFieldCount());
    const auto& field = type_->type_fields()[index];
    return TypedRef::UnsafeFromRawPointer(
        field.GetType(),
        static_cast<const char*>(value_ptr_) + field.byte_offset());
  }

  // Copies the value to `slot` within `frame`.
  absl::Status CopyToSlot(TypedSlot slot, FramePtr frame) const;

  // Casts the pointer to the given type. Returns an error if type does
  // not match the given type `T`.
  template <typename T>
  absl::StatusOr<std::reference_wrapper<const T>> As() const {
    static_assert(
        std::is_same_v<T, std::decay_t<T>>,
        "TypedRef::As does not support casting to reference types. "
        "Make sure the destination type T is equal to std::decay_t<T>");
    // This case is applicable when several QTypes correspond to one C++ type
    // and so QTypeTraits for this type is not specialized.
    RETURN_IF_ERROR(VerifyQTypeTypeInfo(type_, typeid(T)));
    return std::cref(*static_cast<const T*>(value_ptr_));
  }

  // Casts the pointer to the given type T. It's safe to use this method only if
  // you have just checked that value's qtype is GetQType<T>().
  template <typename T>
  const T& UnsafeAs() const {
    static_assert(
        std::is_same_v<T, std::decay_t<T>>,
        "TypedRef::As does not support casting to reference types. "
        "Make sure the destination type T is equal to std::decay_t<T>");
    DCHECK_OK(VerifyQTypeTypeInfo(type_, typeid(T)));
    return *static_cast<const T*>(value_ptr_);
  }

  // Returns the "official" string representation of the value.
  std::string Repr() const { return std::move(GenReprToken().str); }

  // Returns the "official" string representation of the value.
  ReprToken GenReprToken() const { return type_->UnsafeReprToken(value_ptr_); }

  // Returns a qvalue-specialisation-key, that helps to choose a specialised
  // wrapper for the value.
  absl::string_view PyQValueSpecializationKey() const {
    return type_->UnsafePyQValueSpecializationKey(value_ptr_);
  }

 private:
  TypedRef(QTypePtr type, const void* value_ptr)
      : type_(type), value_ptr_(value_ptr) {}

  QTypePtr type_;
  const void* value_ptr_;
};

}  // namespace arolla

#endif  // AROLLA_QTYPE_TYPED_REF_H_
