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
#ifndef AROLLA_QTYPE_TUPLE_QTYPE_H_
#define AROLLA_QTYPE_TUPLE_QTYPE_H_

#include <initializer_list>
#include <string>
#include <type_traits>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {

// Returns true if `qtype` is a tuple qtype.
bool IsTupleQType(const QType* /*nullable*/ qtype);

// Returns the tuple qtype corresponding to the provided field qtypes.
QTypePtr MakeTupleQType(absl::Span<const QTypePtr> field_qtypes);

// Returns a TypedValue containing a tuple of the given fields.
TypedValue MakeTuple(absl::Span<const TypedRef> fields);

TypedValue MakeTuple(absl::Span<const TypedValue> fields);

// TODO: Refactor these functions to not separate names and fields
// into different spans if possible.
absl::StatusOr<TypedValue> MakeNamedTuple(
    absl::Span<const std::string> field_names,
    absl::Span<const TypedRef> fields);

absl::StatusOr<TypedValue> MakeNamedTuple(
    absl::Span<const std::string> field_names,
    absl::Span<const TypedValue> fields);

// Convenience MakeTuple wrapper which accepts any combination of TypedValues,
// TypedRefs, or simple raw values for which the QType can be statically
// determined.
template <typename... Ts>
TypedValue MakeTupleFromFields(const Ts&... fields) {
  auto typed_fields = std::initializer_list<TypedRef>{[](const auto& field) {
    using FieldType = std::decay_t<decltype(field)>;
    if constexpr (std::is_same_v<FieldType, TypedRef>) {
      return field;
    } else if constexpr (std::is_same_v<FieldType, TypedValue>) {
      return field.AsRef();
    } else {
      return TypedRef::FromValue(field);
    }
  }(fields)...};
  return MakeTuple(absl::Span<const TypedRef>(typed_fields));
}

// Returns true if `qtype` is a named tuple qtype.
bool IsNamedTupleQType(const QType* /*nullable*/ qtype);

// Returns the named tuple qtype corresponding
// to the provided tuple qtypes and field names.
absl::StatusOr<QTypePtr> MakeNamedTupleQType(
    absl::Span<const std::string> field_names, QTypePtr tuple_qtype);

}  // namespace arolla

#endif  // AROLLA_QTYPE_TUPLE_QTYPE_H_
