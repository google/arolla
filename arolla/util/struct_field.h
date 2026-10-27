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
#ifndef AROLLA_UTIL_STRUCT_FIELD_H_
#define AROLLA_UTIL_STRUCT_FIELD_H_

#include <array>
#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"

// Utilities for defining reflection for the struct types.

namespace arolla {

// Struct field meta information.
// If kIsSkipped is true:
// 1. Field will not be included into the QType.
// 2. It will be treated as padding by Arolla library.
template <typename T, bool kIsSkipped = false>
struct StructField {
  static constexpr bool kIsIncludedToArollaQType = !kIsSkipped;

  static_assert(!kIsIncludedToArollaQType || !std::is_array_v<T>,
                "array field types are not supported");
  using field_type = T;
  size_t field_offset;  // offset in bytes from the beginning of the structure
  absl::string_view field_name;
};

// Returns pointer to the field by given pointer to the struct.
template <class FieldType>
const FieldType* UnsafeGetStructFieldPtr(const StructField<FieldType>& field,
                                         const void* value) {
  return reinterpret_cast<const FieldType*>(static_cast<const char*>(value) +
                                            field.field_offset);
}

namespace struct_field_impl {

// Default traits for struct reflection definition.
//
// Motivation: Allows non-struct C++ types (scalars, opaque types, etc.) that do
// not define `T::ArollaStructFields()` to participate uniformly in
// `GetStructFields<T>()`, `StructFieldCount<T>()`, and `HasStructFields<T>()`
// by returning an empty field tuple at compile time.
template <typename T, typename Enabled = void>
struct StructFieldTraits {
  // ArollaStructFields defines a set of fields in the struct.
  // *All* the struct fields *must* be listed in order as
  // std::tuple<StructField<T>>.
  static constexpr auto ArollaStructFields() { return std::tuple(); }
};

// Specialization for types with a static constexpr `ArollaStructFields` method.
//
// Motivation: Forwards `StructFieldTraits<T>::ArollaStructFields()` to
// `T::ArollaStructFields()` when a struct defines its own reflection metadata
// inline (instead of specializing `StructFieldTraits<T>` externally).
template <typename T>
struct StructFieldTraits<
    T, std::enable_if_t<std::is_invocable_v<decltype(T::ArollaStructFields)>>> {
  static constexpr auto ArollaStructFields() { return T::ArollaStructFields(); }
};

// Validates at compile time that `fields` (the raw tuple returned by
// `StructFieldTraits<T>::ArollaStructFields()`, including skipped fields)
// covers the complete memory layout of `T` in strictly increasing offset order
// without unlisted fields. Returns an empty string_view if valid, or a
// diagnostic error message otherwise.
//
// Motivation: Ensures that whenever a C++ struct is modified (fields added,
// reordered, or resized), forgetting to update `ArollaStructFields()` fails
// immediately. Returning a `constexpr absl::string_view` instead of
// `absl::Status` allows the same validation logic to be evaluated in a
// compile-time `static_assert` inside `GetStructFields<T>()` (eliminating
// runtime `once` checks and `.bss` guard variables) while also backing
// `VerifyArollaStructFields` for unit tests.
template <class T, class FieldTuple, size_t... Is>
constexpr absl::string_view GetArollaStructFieldsError(
    [[maybe_unused]] const FieldTuple& fields, std::index_sequence<Is...>) {
  if constexpr (sizeof...(Is) != 0) {
    auto offsets =
        std::array<size_t, sizeof...(Is)>{std::get<Is>(fields).field_offset...};
    auto alignments = std::array<size_t, sizeof...(Is)>{
        alignof(typename std::tuple_element_t<Is, FieldTuple>::field_type)...};
    auto sizes = std::array<size_t, sizeof...(Is)>{
        sizeof(typename std::tuple_element_t<Is, FieldTuple>::field_type)...};
    if (offsets[0] != 0) {
      return "first struct field defined incorrectly";
    }
    if (!(((Is == 0) || (offsets[Is] > offsets[Is - 1])) && ...)) {
      return "struct fields are out of order";
    }
    auto align_offset = [](size_t offset, size_t alignment) constexpr {
      return offset +
             (offset % alignment == 0 ? 0 : alignment - offset % alignment);
    };
    if (!(((Is == 0) ||
           (offsets[Is] <=
            align_offset(offsets[Is - 1] + sizes[Is - 1], alignments[Is]))) &&
          ...)) {
      return "struct field is missed in the middle";
    }
    if (align_offset(offsets.back() + sizes.back(), alignof(T)) != sizeof(T)) {
      return "struct field is missed at the end";
    }
  }
  return "";
}

// Runtime wrapper around `GetArollaStructFieldsError<T>` that converts any
// layout validation error into `absl::FailedPreconditionError`.
//
// Motivation: Allows unit tests (`VerifyArollaStructFieldsTest`) to directly
// test each malformed struct layout case (`MissedFirst`, `MissedMiddle`,
// `MissedEnd`, `OutOfOrder`) and assert on the resulting `absl::Status` message
// without triggering a hard compile-time `static_assert` failure.
template <class T, class FieldTuple, size_t... Is>
absl::Status VerifyArollaStructFields(const FieldTuple& fields,
                                      std::index_sequence<Is...> seq) {
  absl::string_view error = GetArollaStructFieldsError<T>(fields, seq);
  if (!error.empty()) {
    return absl::FailedPreconditionError(error);
  }
  return absl::OkStatus();
}

// Compile-time trait that evaluates to true iff every `StructField` element in
// `Tuple` is included in the Arolla QType (`kIsIncludedToArollaQType == true`,
// i.e., no field uses `AROLLA_SKIP_STRUCT_FIELD`).
//
// Motivation: Provides a compile-time fast path for `FilterStructFieldsTuple`
// so structs without skipped fields (the vast majority of Arolla structs)
// bypass `std::apply` and `std::tuple_cat` template instantiations.
template <typename Tuple>
struct AreAllStructFieldsIncluded : std::true_type {};

template <typename... Fields>
struct AreAllStructFieldsIncluded<std::tuple<Fields...>>
    : std::bool_constant<(Fields::kIsIncludedToArollaQType && ...)> {};

// Filters a tuple of `StructField` entries at compile time, removing any fields
// declared with `AROLLA_SKIP_STRUCT_FIELD` (`kIsIncludedToArollaQType ==
// false`) and returning a tuple of only the Arolla-visible fields.
//
// Motivation: Skipped fields must be listed in `ArollaStructFields()` so that
// `GetArollaStructFieldsError` can verify there are no unintended padding gaps
// or forgotten fields in `T`, but they must be stripped before exposing the
// struct's subfields to Arolla's QType and `FrameLayout` registration.
template <typename FieldTuple>
constexpr auto FilterStructFieldsTuple(const FieldTuple& fields) {
  if constexpr (std::tuple_size_v<FieldTuple> == 0 ||
                AreAllStructFieldsIncluded<FieldTuple>::value) {
    return fields;
  } else {
    auto filter_and_convert_to_tuple = [](auto struct_field) {
      using StructField = decltype(struct_field);
      if constexpr (StructField::kIsIncludedToArollaQType) {
        return std::tuple<StructField>{struct_field};
      } else {
        return std::tuple<>();
      }
    };
    return std::apply(
        [&](auto... struct_fields) {
          return std::tuple_cat(filter_and_convert_to_tuple(struct_fields)...);
        },
        fields);
  }
}

}  // namespace struct_field_impl

// Returns tuple of StructField for included into Arolla QType struct fields.
// Fields declared with AROLLA_SKIP_STRUCT_FIELD will *not* be present.
template <class T>
constexpr auto GetStructFields() {
  constexpr auto kRawFields =
      struct_field_impl::StructFieldTraits<T>::ArollaStructFields();
  constexpr absl::string_view kError =
      struct_field_impl::GetArollaStructFieldsError<T>(
          kRawFields,
          std::make_index_sequence<std::tuple_size_v<decltype(kRawFields)>>());
  static_assert(kError.empty(), "Invalid ArollaStructFields definition");
  return struct_field_impl::FilterStructFieldsTuple(kRawFields);
}

// Returns number of struct fields.
template <class T>
constexpr size_t StructFieldCount() {
  return std::tuple_size_v<decltype(struct_field_impl::FilterStructFieldsTuple(
      struct_field_impl::StructFieldTraits<T>::ArollaStructFields()))>;
}

// Returns true if there is at least one struct field.
template <class T>
constexpr bool HasStructFields() {
  return StructFieldCount<T>() != 0;
}

// AROLLA_DECLARE_STRUCT_FIELD(name) defines a StructField record
// corresponding to the field `name`.
//
// Must be used inside of ArollaStructFields with CppType defined.
//
// Examples:
//
// struct Foo {
//   float f1;
//   int i1;
//
//   constexpr static auto ArollaStructFields() {
//     using CppType = Foo;
//     return std::tuple{
//       AROLLA_DECLARE_STRUCT_FIELD(f1),
//       AROLLA_DECLARE_STRUCT_FIELD(i1),
//     };
//   }
// };
//
// struct Bar {
//   Foo foo1;
//   Foo foo2;
//   int bar3;
//
//   constexpr static auto ArollaStructFields() {
//     using CppType = Bar;
//     return std::tuple{
//       AROLLA_DECLARE_STRUCT_FIELD(foo1),
//       AROLLA_DECLARE_STRUCT_FIELD(foo2),
//       AROLLA_DECLARE_STRUCT_FIELD(bar3),
//     };
//   }
// };
#define AROLLA_DECLARE_STRUCT_FIELD(NAME)                        \
  ::arolla::StructField<decltype(CppType::NAME)> {               \
    .field_offset = offsetof(CppType, NAME), .field_name = #NAME \
  }

// AROLLA_SKIP_STRUCT_FIELD(name) defines a StructField record
// that will be treated as padding by Arolla library.
//
// Must be used inside of ArollaStructFields with CppType defined.
// It is useful to work with structs that contain fields with unsupported by
// Arolla and not needed for evaluation.
//
// We require to specify skipped fields explicitly to perform verification
// that all fields are listed. That helps to ensure that ArollaStructFields
// is updated together with the struct.
//
// NOTE: skipped fields still have to be std::is_standard_layout_v.
//
// Examples:
//
// struct Foo {
//   float f1;
//   void* unsupported;
//   int i1;
//
//   constexpr static auto ArollaStructFields() {
//     using CppType = Foo;
//     return std::tuple{
//       AROLLA_DECLARE_STRUCT_FIELD(f1),
//       AROLLA_SKIP_STRUCT_FIELD(unsupported),
//       AROLLA_DECLARE_STRUCT_FIELD(i1),
//     };
//   }
// };
#define AROLLA_SKIP_STRUCT_FIELD(NAME)                                  \
  ::arolla::StructField<decltype(CppType::NAME), /*kIsSkipped=*/true> { \
    .field_offset = offsetof(CppType, NAME), .field_name = #NAME        \
  }

}  // namespace arolla

#endif  // AROLLA_UTIL_STRUCT_FIELD_H_
