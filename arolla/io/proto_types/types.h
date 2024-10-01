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
// Library to convert types in a proto to Arolla specific C++ types.

#ifndef AROLLA_IO_PROTO_TYPES_TYPES_H_
#define AROLLA_IO_PROTO_TYPES_TYPES_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bytes.h"

namespace arolla::proto {

// Selection of types for proto string fields.
enum class StringFieldType {
  kText = 0,   // ::arolla::Text
  kBytes = 1,  // ::arolla::Bytes
};

// Mapping from proto C++ type to Arolla C++ type.
// For convenience std::decay is applied to the type.
// All enums are assumed to be `int32_t`.
template <class T>
struct arolla_single_value {
  using type = std::conditional_t<std::is_enum_v<std::decay_t<T>>, int32_t,
                                  std::decay_t<T>>;
};

template <>
struct arolla_single_value<std::string> {
  using type = ::arolla::Bytes;
};

template <>
struct arolla_single_value<absl::string_view> {
  using type = ::arolla::Bytes;
};

template <>
struct arolla_single_value<absl::Cord> {
  using type = ::arolla::Bytes;
};

template <>
struct arolla_single_value<uint32_t> {
  using type = int64_t;
};

template <>
struct arolla_single_value<uint8_t> {
  using type = int32_t;
};

template <>
struct arolla_single_value<int8_t> {
  using type = int32_t;
};

template <>
struct arolla_single_value<uint16_t> {
  using type = int32_t;
};

template <>
struct arolla_single_value<int16_t> {
  using type = int32_t;
};

template <class T>
using arolla_single_value_t =
    typename arolla_single_value<std::decay_t<T>>::type;

// Mapping from proto C++ type to Arolla optional C++ type.
template <class T>
using arolla_optional_value_t =
    ::arolla::OptionalValue<arolla_single_value_t<T>>;

// Type used to represent sizes of repeated proto fields.
using arolla_size_t = int64_t;

// Cast from proto2 type to Arolla supported type.
// Returned value must be *implicitly* assignable to Arolla type.
// Example:
// arolla::Text t;
// t = ToArollaCompatibleType("abc");
template <class T>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline decltype(auto) ToArollaCompatibleType(
    T&& x) {
  if constexpr (std::is_same_v<std::decay_t<T>, absl::Cord>) {
    return std::string(std::forward<T>(x));
  } else {
    return std::forward<T>(x);
  }
}

namespace internal {

// helper type for creating overloaded lambdas
template <class... Ts>
struct overloaded : Ts... {
  using Ts::operator()...;
};
// explicit deduction guide (not needed as of C++20)
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

// helper classes for disambiguation and prioritization of overloaded functions
struct Rank1 {};
struct Rank0 : Rank1 {};

}  // namespace internal

// proto2 and proto3 compatible way to verify presence of the field.
// Macro expands to the code evaluating to the boolean
// * always true if `has_FIELD_NAME` is not present in the VAR
// * `VAR.has_FIELD_NAME()` otherwise
#define AROLLA_PROTO3_COMPATIBLE_HAS(VAR, FIELD_NAME)                          \
  [&]() {                                                                      \
    namespace internal = ::arolla::proto::internal;                            \
    static_assert(                                                             \
        std::is_same_v<decltype(std::declval<std::remove_cv_t<                 \
                                    std::remove_reference_t<decltype(VAR)>>>() \
                                    .clear_##FIELD_NAME()),                    \
                       void>,                                                  \
        "clear_##FIELD_NAME must be defined");                                 \
    return internal::overloaded{                                               \
        [&](internal::Rank0,                                                   \
            const auto& x) -> decltype(x.has_##FIELD_NAME()) {                 \
          return x.has_##FIELD_NAME();                                         \
        },                                                                     \
        [&](internal::Rank1, const auto& x) { return true; }}(                 \
        internal::Rank0{}, VAR);                                               \
  }()

namespace internal {

template <class T>
struct ContainerTraits {
  void Resize(T& container, size_t size) { container.resize(size); }
};

template <class T>
struct ContainerTraits<google::protobuf::RepeatedPtrField<T>> {
  void Resize(google::protobuf::RepeatedPtrField<T>& container, size_t size) {
    if (container.size() < size) {
      container.Reserve(size);
      while (container.size() < size) {
        container.Add();
      }
    } else if (container.size() > size) {
      container.erase(container.begin() + size, container.end());
    }
  }
};

}  // namespace internal

// Resize container to `size` elements. Equivalent to `container.resize(size)`.
template <class T>
void ResizeContainer(T& container, size_t size) {
  internal::ContainerTraits<T>().Resize(container, size);
}

}  // namespace arolla::proto

#endif  // AROLLA_IO_PROTO_TYPES_TYPES_H_
