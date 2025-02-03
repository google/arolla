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
#ifndef AROLLA_QTYPE_SHAPE_QTYPE_H_
#define AROLLA_QTYPE_SHAPE_QTYPE_H_

#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/api.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"

namespace arolla {

// Base class for all "shape" QTypes. "Shape" type contains enough information
// to create an object of this kind populated with the given value. Objects with
// equal "shapes" can safely participate in pointwise operations.
class AROLLA_API ShapeQType : public SimpleQType {
 public:
  // Returns a QType for this kind of object with the specified value type.
  virtual absl::StatusOr<QTypePtr> WithValueQType(
      QTypePtr value_qtype) const = 0;

  // Returns type that represents presence.
  virtual QTypePtr presence_qtype() const = 0;

 protected:
  template <typename T>
  ShapeQType(meta::type<T> type, std::string type_name)
      : SimpleQType(type, std::move(type_name)) {}
};

inline bool IsShapeQType(const QType* /*nullable*/ qtype) {
  return dynamic_cast<const ShapeQType*>(qtype) != nullptr;
}

inline absl::StatusOr<const ShapeQType*> ToShapeQType(QTypePtr qtype) {
  const ShapeQType* shape_qtype = dynamic_cast<const ShapeQType*>(qtype);
  if (!shape_qtype) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a shape, got %s", qtype->name()));
  }
  return shape_qtype;
}

// Struct to represent shape of non-optional scalars (essentially a monostate).
struct AROLLA_API ScalarShape {};

inline bool operator==(ScalarShape, ScalarShape) { return true; }
inline bool operator!=(ScalarShape, ScalarShape) { return false; }

// Struct to represent shape of optional scalars (essentially a monostate).
struct AROLLA_API OptionalScalarShape {};

inline bool operator==(OptionalScalarShape, OptionalScalarShape) {
  return true;
}
inline bool operator!=(OptionalScalarShape, OptionalScalarShape) {
  return false;
}

AROLLA_DECLARE_QTYPE(ScalarShape);
AROLLA_DECLARE_REPR(ScalarShape);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(ScalarShape);

AROLLA_DECLARE_QTYPE(OptionalScalarShape);
AROLLA_DECLARE_REPR(OptionalScalarShape);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(OptionalScalarShape);

}  // namespace arolla

#endif  // AROLLA_QTYPE_SHAPE_QTYPE_H_
