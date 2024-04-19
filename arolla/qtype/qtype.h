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
#ifndef AROLLA_QTYPE_QTYPE_H_
#define AROLLA_QTYPE_QTYPE_H_

#include <ostream>
#include <string>
#include <typeinfo>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/api.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

class AROLLA_API QType;

// Default pointer type for QType.
//
// NOTE: QType instances are immutable singletons. For that reason
// raw pointers are safe to use.
using QTypePtr = const QType*;

class TypedSlot;

// Base class for defining Arolla types. Every distinct Arolla type has
// a single instance of this class.
//
// Instances of this class may be created dynamically, but after a new QType
// gets in use, it must not be destroyed.
class QType {
 public:
  virtual ~QType();

  // Returns a descriptive name for this QType.
  //
  // One should not rely on the names uniqueness, e.g. don't compare QTypes by
  // name.
  absl::string_view name() const { return name_; }

  // Returns type_info of corresponding C++ type (may also return dummy
  // type_info for QTypes that don't have a corresponding C++ type).
  const std::type_info& type_info() const { return type_info_; }

  // Returns the underlying frame layout for objects of the type.
  const FrameLayout& type_layout() const { return type_layout_; }

  // Returns sub-slots corresponding to the type's fields (within type_layout)
  // or an empty list if this type has no fields.
  const std::vector<TypedSlot>& type_fields() const { return type_fields_; }

  // Returns qtype of values for container types, nullptr otherwise.
  const QType* /*nullable*/ value_qtype() const { return value_qtype_; }

  // Returns the "official" string representation of an object.
  //
  // It is used e.g. for printing TypedValue objects that contain this QType.
  // The default implementation just prints the QType name + memory address of
  // the object.
  //
  // NOTE: `source` must point to a value compatible with the given qtype;
  // otherwise the behaviour is undefined.
  virtual ReprToken UnsafeReprToken(const void* source) const;

  // Copy value from source to destination. Both source and destination
  // must be allocated and initialized with type corresponding to the QType.
  //
  // NOTE: `source` must point to a value compatible with the given qtype;
  // otherwise the behaviour is undefined.
  virtual void UnsafeCopy(const void* source, void* destination) const = 0;

  // Combine source value to the hasher state.
  //
  // NOTE: `source` must point to a value compatible with the given qtype;
  // otherwise the behaviour is undefined.
  virtual void UnsafeCombineToFingerprintHasher(
      const void* source, FingerprintHasher* hasher) const = 0;

  // Returns a specialization key for the QType, or an empty string if no
  // specialization is supported.
  //
  // The key may be unique for a QType, but it may also be shared for a group of
  // QTypes, e.g. common for all the Tuple QTypes. The convention is to use
  // fully qualified C++ class names to avoid collisions.
  //
  // The key is used for example to select QValue specialization in Python (see
  // the rl.abc.register_qvalue_specialization and
  // arolla::python::WrapAsPyQValue docs).
  //
  // NOTE: GetQTypeQType()->UnsafePyQValueSpecializationKey(qt) returns
  // qt->qtype_specialization_key().
  absl::string_view qtype_specialization_key() const {
    return qtype_specialization_key_;
  }

  // Returns a specialization key for the given value, or an empty string if no
  // specialization is supported.
  //
  // The key is used for example to select QValue specialization in Python (see
  // the rl.abc.register_qvalue_specialization and
  // arolla::python::WrapAsPyQValue docs).
  //
  // NOTE: usually qtype_specialization_key provides sufficient flexibility for
  // customisation, so it is not recommended to override this method.
  //
  // NOTE: `source` must point to a value compatible with the given qtype;
  // otherwise the behaviour is undefined.
  virtual absl::string_view UnsafePyQValueSpecializationKey(
      const void* source) const;

  // Prevent object slicing.
  QType(const QType&) = delete;
  QType& operator=(const QType&) = delete;

 protected:
  struct ConstructorArgs {
    std::string name;                    // required
    const std::type_info& type_info;     // required
    FrameLayout type_layout;             // required
    std::vector<TypedSlot> type_fields;  // empty for types without fields
    const QType* value_qtype = nullptr;  // nullptr for non-containers
    std::string
        qtype_specialization_key;  // qvalue specialization key for *qtype*
  };

  explicit QType(ConstructorArgs args);

  QType(std::string name, const std::type_info& type_info,
        FrameLayout type_layout);

 private:
  std::string name_;
  const std::type_info& type_info_;
  FrameLayout type_layout_;
  std::vector<TypedSlot> type_fields_;
  // For container types, represents the type stored inside container.
  const QType* /*nullable*/ value_qtype_;
  std::string qtype_specialization_key_;
};

AROLLA_DECLARE_REPR(QTypePtr);

std::ostream& operator<<(std::ostream& stream, const QType& type);
std::ostream& operator<<(std::ostream& stream, QTypePtr type);

template <>
struct FingerprintHasherTraits<QTypePtr> {
  void operator()(FingerprintHasher* hasher, QTypePtr qtype) const;
};

// Returns `QTYPE` qtype.
QTypePtr GetQTypeQType();

// Returns `NOTHING` qtype.
//
// `NOTHING` is an uninhabited type. In other words, it's a type with no values.
QTypePtr GetNothingQType();

// Join type names separated by ",".
std::string JoinTypeNames(absl::Span<const QTypePtr> types);

// Formats a vector of QTypes for error messages.
std::string FormatTypeVector(absl::Span<const QTypePtr> types);

// Returns error if `tpe` doesn't correspond to the qtype.
absl::Status VerifyQTypeTypeInfo(QTypePtr expected_qtype,
                                 const std::type_info& actual_type_info);

}  // namespace arolla

#endif  // AROLLA_QTYPE_QTYPE_H_
