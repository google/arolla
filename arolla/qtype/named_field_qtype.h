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
#ifndef AROLLA_QTYPE_NAMED_FIELD_QTYPE_H_
#define AROLLA_QTYPE_NAMED_FIELD_QTYPE_H_

#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Interface for QTypes with named subfields.
class NamedFieldQTypeInterface {
 public:
  virtual ~NamedFieldQTypeInterface() = default;

  // Returns list of the field names.
  // Length of returned span must be equal to `QType::type_fields()`.
  virtual absl::Span<const std::string> GetFieldNames() const = 0;

  // Returns field index by the given name or `nullopt` if name is not
  // present.
  virtual std::optional<int64_t> GetFieldIndexByName(
      absl::string_view field_name) const = 0;
};

// Returns field index by name.
// Returns nullopt if qtype  doesn't implement `NamedFieldQTypeInterface`
// or `field_name` doesn't exist.
std::optional<int64_t> GetFieldIndexByName(const QType* /*nullable*/ qtype,
                                           absl::string_view field_name);

// Returns a reference to the named field. Returns an error if the field
// does not exist.
absl::StatusOr<TypedRef> GetFieldByName(TypedRef qvalue,
                                        absl::string_view field_name);

// Returns the named field as the given type. Returns an error if the field
// does not exist or if type does not match the given type `T` exactly.
template <typename T>
absl::StatusOr<std::reference_wrapper<const T>> GetFieldByNameAs(
    TypedRef qvalue, absl::string_view field_name) {
  ASSIGN_OR_RETURN(auto field, GetFieldByName(qvalue, field_name));
  ASSIGN_OR_RETURN(auto value, field.As<T>(),
                   _ << "while accessing field \"" << field_name << "\"");
  return value;
}

// Returns field names in the qtype.
// Returns empty list if `qtype` doesn't implement NamedFieldQTypeInterface.
absl::Span<const std::string> GetFieldNames(const QType* /*nullable*/ qtype);

// Returns field type by name.
// Returns nullptr if qtype doesn't implementing `NamedFieldQTypeInterface`
// or `field_name` doesn't exist.
const QType* /*nullable*/ GetFieldQTypeByName(const QType* /*nullable*/ qtype,
                                              absl::string_view field_name);

}  // namespace arolla

#endif  // AROLLA_QTYPE_NAMED_FIELD_QTYPE_H_
