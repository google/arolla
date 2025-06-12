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
#ifndef AROLLA_QTYPE_TYPED_VALUE_H_
#define AROLLA_QTYPE_TYPED_VALUE_H_

#include <cstdint>
#include <functional>
#include <new>  // IWYU pragma: keep
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/call_once.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/refcount.h"
#include "arolla/util/repr.h"

namespace arolla {

// Container for a single immutable value of a given QType. Allows values
// to be read from and written to TypedSlots generically.
class TypedValue {
 public:
  // Creates a TypedValue containing `value`. Requires that `value`'s
  // QType can be inferred from `T`.
  template <typename T>
  static TypedValue FromValue(T&& value);

  // Creates a TypedValue containing `value`. Requires that `value`'s
  // QType can be inferred from `T`.
  template <typename T>
  static TypedValue FromValue(const T& value);

  // Creates a TypedValue containing `value`. Returns an error if `value`
  // does not match `qtype`.
  template <typename T>
  static absl::StatusOr<TypedValue> FromValueWithQType(T&& value,
                                                       QTypePtr qtype);

  // Creates a TypedValue containing `value`. Returns an error if `value`
  // does not match `qtype`.
  template <typename T>
  static absl::StatusOr<TypedValue> FromValueWithQType(const T& value,
                                                       QTypePtr qtype);

  // Returns default constructed value of the given type.
  // NOTE: The function is marked "unsafe" because the default-constructed
  // object can violate some implicitly assumed properties of the QType. For
  // example, our code generally assumes that the pointer types like OperatorPtr
  // or QTypePtr are not null, but the function fills them with nullptr.
  static TypedValue UnsafeFromTypeDefaultConstructed(QTypePtr type);

  // Creates a TypedValue from a value in the provided `slot` within `frame`.
  static TypedValue FromSlot(TypedSlot slot, ConstFramePtr frame);

  // Constructs a TypedValue from the fields' values. Most users can use
  // MakeTuple(), defined in tuple_qtype.h, as a more convenient mechanism for
  // creating compound TypedValues instead of these methods.
  static absl::StatusOr<TypedValue> FromFields(
      QTypePtr compound_type, absl::Span<const TypedRef> fields);
  static absl::StatusOr<TypedValue> FromFields(
      QTypePtr compound_type, absl::Span<const TypedValue> fields);

  // Creates a TypedValue from the given `value_ref`.
  explicit TypedValue(TypedRef value_ref);
  ~TypedValue() noexcept;

  // Movable.
  TypedValue(TypedValue&& rhs) noexcept;
  TypedValue& operator=(TypedValue&& rhs) noexcept;

  // Copyable.
  TypedValue(const TypedValue& rhs) noexcept;
  TypedValue& operator=(const TypedValue& rhs) noexcept;

  // Returns the type of the stored value.
  QTypePtr GetType() const;

  // Returns a pointer to the value stored inside of the instance.
  const void* GetRawPointer() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns a typed reference to the value stored within this object.
  TypedRef AsRef() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns fingerprint of the stored value.
  const Fingerprint& GetFingerprint() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns the number of fields in the type.
  int64_t GetFieldCount() const;

  // Returns references to the values from the corresponding to the
  // QType::SubSlot(i)
  TypedRef GetField(int64_t index) const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Copy of this value into the provided `slot` within `frame`.
  absl::Status CopyToSlot(TypedSlot slot, FramePtr frame) const;

  // Returns value as given type. Returns an error if type does not match
  // the given type `T` exactly.
  template <typename T>
  absl::StatusOr<std::reference_wrapper<const T>> As() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Casts the pointer to the given type T. It's safe to use this method only if
  // you have just checked that value's qtype is GetQType<T>().
  template <typename T>
  const T& UnsafeAs() const ABSL_ATTRIBUTE_LIFETIME_BOUND;

  // Returns the "official" string representation of the value.
  std::string Repr() const;

  // Returns the "official" string representation of the value.
  ReprToken GenReprToken() const;

  // Returns a qvalue-specialisation-key, that helps to choose a specialised
  // wrapper for the value.
  absl::string_view PyQValueSpecializationKey() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

 private:
  struct Impl {
    Refcount refcount;
    absl::once_flag fingerprint_once;
    QTypePtr qtype;
    void* data;
    Fingerprint fingerprint;
  };

  explicit TypedValue(Impl* impl) noexcept : impl_(impl) {}

  // Returns a instance with uninitialized data.
  static Impl* AllocRawImpl(QTypePtr qtype);

  // Returns a instance with initialized data.
  static Impl* AllocImpl(QTypePtr qtype, const void* value);

  Impl* impl_;
};

//
// Implementations.
//

template <typename T>
TypedValue TypedValue::FromValue(T&& value) {
  using V = std::decay_t<T>;
  static const QTypePtr qtype = GetQType<V>();
  if constexpr (std::is_copy_constructible_v<V> ||
                std::is_move_constructible_v<V>) {
    // We assume that the copy/move constructor leads to the same state as
    // qtype->UnsafeCopy().
    auto* raw_impl = AllocRawImpl(qtype);
    new (raw_impl->data) V(std::forward<T>(value));
    return TypedValue(raw_impl);
  } else {
    return TypedValue(AllocImpl(qtype, &value));
  }
}

template <typename T>
TypedValue TypedValue::FromValue(const T& value) {
  static const QTypePtr qtype = GetQType<T>();
  if constexpr (std::is_copy_constructible_v<T>) {
    // We assume that the copy constructor leads to the same state as
    // qtype->UnsafeCopy().
    auto* raw_impl = AllocRawImpl(qtype);
    new (raw_impl->data) T(value);
    return TypedValue(raw_impl);
  } else {
    return TypedValue(AllocImpl(qtype, &value));
  }
}

template <typename T>
absl::StatusOr<TypedValue> TypedValue::FromValueWithQType(T&& value,
                                                          QTypePtr qtype) {
  using V = std::decay_t<T>;
  if (auto status = VerifyQTypeTypeInfo(qtype, typeid(V)); !status.ok()) {
    return status;
  }
  if constexpr (std::is_copy_constructible_v<V> ||
                std::is_move_constructible_v<V>) {
    // We assume that the copy/move constructor leads to the same state as
    // qtype->UnsafeCopy().
    auto* raw_impl = AllocRawImpl(qtype);
    new (raw_impl->data) V(std::forward<T>(value));
    return TypedValue(raw_impl);
  } else {
    return TypedValue(AllocImpl(qtype, &value));
  }
}

template <typename T>
absl::StatusOr<TypedValue> TypedValue::FromValueWithQType(const T& value,
                                                          QTypePtr qtype) {
  if (auto status = VerifyQTypeTypeInfo(qtype, typeid(T)); !status.ok()) {
    return status;
  }
  if constexpr (std::is_copy_constructible_v<T>) {
    // We assume that the copy constructor leads to the same state as
    // qtype->UnsafeCopy().
    auto* raw_impl = AllocRawImpl(qtype);
    new (raw_impl->data) T(value);
    return TypedValue(raw_impl);
  } else {
    return TypedValue(AllocImpl(qtype, &value));
  }
}

inline TypedValue TypedValue::FromSlot(TypedSlot slot, ConstFramePtr frame) {
  return TypedValue(TypedRef::FromSlot(slot, frame));
}

inline TypedValue::TypedValue(TypedRef value_ref)
    : impl_(AllocImpl(value_ref.GetType(), value_ref.GetRawPointer())) {}

inline TypedValue::~TypedValue() noexcept {
  // TODO: Investigate the performance implications of using
  // `decrement()` here.
  if (impl_ != nullptr && !impl_->refcount.skewed_decrement()) {
    impl_->qtype->type_layout().DestroyAlloc(impl_->data);
    impl_->~Impl();
    ::operator delete(impl_);
  }
}

inline TypedValue::TypedValue(TypedValue&& rhs) noexcept : impl_(rhs.impl_) {
  rhs.impl_ = nullptr;
}

inline TypedValue& TypedValue::operator=(TypedValue&& rhs) noexcept {
  using std::swap;  // go/using-std-swap
  swap(impl_, rhs.impl_);
  return *this;
}

inline TypedValue::TypedValue(const TypedValue& rhs) noexcept
    : impl_(rhs.impl_) {
  if (impl_ != nullptr) {
    impl_->refcount.increment();
  }
}

inline TypedValue& TypedValue::operator=(const TypedValue& rhs) noexcept {
  if (impl_ != rhs.impl_) {
    *this = TypedValue(rhs);
  }
  return *this;
}

inline QTypePtr TypedValue::GetType() const { return impl_->qtype; }

inline const void* TypedValue::GetRawPointer() const { return impl_->data; }

inline TypedRef TypedValue::AsRef() const {
  return TypedRef::UnsafeFromRawPointer(impl_->qtype, impl_->data);
}

inline int64_t TypedValue::GetFieldCount() const {
  return impl_->qtype->type_fields().size();
}

inline TypedRef TypedValue::GetField(int64_t index) const {
  return AsRef().GetField(index);
}

inline absl::Status TypedValue::CopyToSlot(TypedSlot slot,
                                           FramePtr frame) const {
  return AsRef().CopyToSlot(slot, frame);
}

template <typename T>
absl::StatusOr<std::reference_wrapper<const T>> TypedValue::As() const {
  return AsRef().As<T>();
}

template <typename T>
const T& TypedValue::UnsafeAs() const {
  return AsRef().UnsafeAs<T>();
}

inline std::string TypedValue::Repr() const {
  return std::move(AsRef().GenReprToken().str);
}

inline ReprToken TypedValue::GenReprToken() const {
  return AsRef().GenReprToken();
}

inline absl::string_view TypedValue::PyQValueSpecializationKey() const {
  return AsRef().PyQValueSpecializationKey();
}

}  // namespace arolla

#endif  // AROLLA_QTYPE_TYPED_VALUE_H_
