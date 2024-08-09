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

  // Returns a list of field names. The names in the list must align with
  // `QType::type_fields()`.
  virtual absl::Span<const std::string> GetFieldNames() const = 0;

  // Returns the index of the field with the given name, or `nullopt` if no such
  // field exists.
  virtual std::optional<int64_t> GetFieldIndexByName(
      absl::string_view field_name) const = 0;
};

// Returns a list of field names; the names in the list are aligned with
// `qtype->type_fields()`. If `qtype` doesn't implement
// `NamedFieldQTypeInterface`, returns an empty list.
absl::Span<const std::string> GetFieldNames(const QType* /*nullable*/ qtype);

// Returns the index of the field with the given name.
// Returns `nullopt` if `qtype` doesn't implement `NamedFieldQTypeInterface`
// or if no such field exists.
std::optional<int64_t> GetFieldIndexByName(const QType* /*nullable*/ qtype,
                                           absl::string_view field_name);

// Returns a reference to the field with the given name.
// Returns an error if `qvalue` doesn't support `NamedFieldQTypeInterface`
// or if no such field exists.
absl::StatusOr<TypedRef> GetFieldByName(TypedRef qvalue,
                                        absl::string_view field_name);

// Returns a reference to the field value with the given name.
// Returns an error if `qvalue` doesn't support `NamedFieldQTypeInterface`,
// if no such field exists, or if the C++ type of the field is not `T`.
template <typename T>
absl::StatusOr<std::reference_wrapper<const T>> GetFieldByNameAs(
    TypedRef qvalue, absl::string_view field_name) {
  ASSIGN_OR_RETURN(auto field, GetFieldByName(qvalue, field_name));
  ASSIGN_OR_RETURN(auto value, field.As<T>(),
                   _ << "while accessing field \"" << field_name << "\"");
  return value;
}

// Returns the type of the field with the given name.
// Returns `nullptr` if `qtype` doesn't implement `NamedFieldQTypeInterface`
// or if no such field exists.
const QType* /*nullable*/ GetFieldQTypeByName(const QType* /*nullable*/ qtype,
                                              absl::string_view field_name);

}  // namespace arolla

#endif  // AROLLA_QTYPE_NAMED_FIELD_QTYPE_H_
