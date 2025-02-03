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
#ifndef AROLLA_UTIL_STRUCT_FIELD_H_
#define AROLLA_UTIL_STRUCT_FIELD_H_

#include <array>
#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/util/demangle.h"

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
template <typename T, typename Enabled = void>
struct StructFieldTraits {
  // ArollaStructFields defines a set of fields in the struct.
  // *All* the struct fields *must* be listed in order as
  // std::tuple<StructField<T>>.
  static constexpr auto ArollaStructFields() { return std::tuple(); }
};

// Specialization for types with static constexpr `ArollaStructFields` method.
template <typename T>
struct StructFieldTraits<
    T, std::enable_if_t<std::is_invocable_v<decltype(T::ArollaStructFields)>>> {
  static auto ArollaStructFields() { return T::ArollaStructFields(); }
};

template <class T, class FieldTuple, size_t... Is>
absl::Status VerifyArollaStructFields(
    ABSL_ATTRIBUTE_UNUSED const FieldTuple& fields,
    std::index_sequence<Is...>) {
  if constexpr (sizeof...(Is) != 0) {
    auto offsets =
        std::array<size_t, sizeof...(Is)>{std::get<Is>(fields).field_offset...};
    auto alignments = std::array<size_t, sizeof...(Is)>{
        alignof(typename std::tuple_element_t<Is, FieldTuple>::field_type)...};
    auto sizes = std::array<size_t, sizeof...(Is)>{
        sizeof(typename std::tuple_element_t<Is, FieldTuple>::field_type)...};
    if (offsets[0] != 0) {
      return absl::FailedPreconditionError(
          "first struct field defined incorrectly");
    }
    if (!(((Is == 0) || (offsets[Is] > offsets[Is - 1])) && ...)) {
      return absl::FailedPreconditionError("struct fields are out of order");
    }
    auto align_offset = [](size_t offset, size_t alignment) constexpr {
      return offset +
             (offset % alignment == 0 ? 0 : alignment - offset % alignment);
    };
    if (!(((Is == 0) ||
           (offsets[Is] <=
            align_offset(offsets[Is - 1] + sizes[Is - 1], alignments[Is]))) &&
          ...)) {
      return absl::FailedPreconditionError(
          "struct field is missed in the middle");
    }
    if (align_offset(offsets.back() + sizes.back(), alignof(T)) != sizeof(T)) {
      return absl::FailedPreconditionError("struct field is missed at the end");
    }
  }
  return absl::OkStatus();
}

}  // namespace struct_field_impl

// Returns tuple of StructField for included into Arolla QType struct fields.
// Fields declared with AROLLA_SKIP_STRUCT_FIELD will *not* be present.
template <class T>
const auto& GetStructFields() {
  ABSL_ATTRIBUTE_UNUSED static const bool once = [] {
    const auto fields =
        struct_field_impl::StructFieldTraits<T>::ArollaStructFields();
    constexpr size_t kSize = std::tuple_size_v<decltype(fields)>;
    CHECK_OK(struct_field_impl::VerifyArollaStructFields<T>(
        fields, std::make_index_sequence<kSize>()))
        << TypeName<T>();
    return true;
  }();

  // If field is included to QType, converts to std::tuple.
  // Otherwise returns empty tuple.
  auto filter_and_convert_to_tuple = [](auto struct_field) {
    using StructField = decltype(struct_field);
    if constexpr (StructField::kIsIncludedToArollaQType) {
      return std::tuple<StructField>{struct_field};
    } else {
      return std::tuple<>();
    }
  };

  // Filter out fields not included into QType.
  static const auto filtered_fields = std::apply(
      [&](auto... struct_fields) {
        return std::tuple_cat(filter_and_convert_to_tuple(struct_fields)...);
      },
      struct_field_impl::StructFieldTraits<T>::ArollaStructFields());

  return filtered_fields;
}

// Returns number of struct fields.
template <class T>
constexpr size_t StructFieldCount() {
  return std::tuple_size_v<std::decay_t<decltype(GetStructFields<T>())>>;
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
//   static auto ArollaStructFields() {
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
//   static auto ArollaStructFields() {
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
//   static auto ArollaStructFields() {
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
