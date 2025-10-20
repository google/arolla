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
/**
 * The fingerprint hash is designated to uniquely identify a value.
 *
 * Key properties:
 *
 *  1. Fingerprint equality must guarantee the equality of the corresponding
 *     objects within the given runtime.
 *
 *    Please note that a dynamic object's memory address is NOT suitable as
 *    an identifier because it can be allocated for another object later in
 *    runtime. A singleton's memory address is a valid exception.
 *
 *  2. Equivalence of the objects doesn't guarantee the fingerprint equality.
 *
 *    The missing values in an array may stay uninitialized, for performance
 *    reasons. It makes every sparse array potentially unique, even when
 *    the present values are fixed.
 *
 *  3. The stability of fingerprints is not guaranteed between runtimes
 *     (even between runs on the same hardware of the same binary).
 */
#ifndef AROLLA_UTIL_FINGERPRINT_H_
#define AROLLA_UTIL_FINGERPRINT_H_

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/meta.h"
#include "arolla/util/struct_field.h"
#include "arolla/util/types.h"

namespace arolla {

// Binary representation of a fingerprint value.
struct Fingerprint {
  absl::uint128 value;

  std::string AsString() const;

  // Hash value in one word (signed).
  signed_size_t PythonHash() const;
};

// Returns a random fingerprint.
Fingerprint RandomFingerprint();

// A helper class for computing Fingerprints.
//
// The implementation is based on CityHash.
//
// Please pay attention, that FingerprintHasherTraits<T> changes the behaviour
// of FingerprintHasher::Combine<T>, but it doesn't affect the types derived
// from T. For example, the behaviour of Combine<std::pair<T, T>> stays
// unaffected. It may cause issues. Please write unit-tests.
//
class FingerprintHasher {
 public:
  explicit FingerprintHasher(absl::string_view salt);

  // Returns the resulting fingerprint.
  Fingerprint Finish() &&;

  // Combines a list of values to the fingerprint state.
  template <typename... Args>
  FingerprintHasher& Combine(const Args&... args) &;

  // Combines a list of values to the fingerprint state.
  template <typename... Args>
  FingerprintHasher&& Combine(const Args&... args) &&;

  // Combines a span of values to the fingerprint state.
  template <typename SpanT>
  FingerprintHasher& CombineSpan(SpanT&& values) &;

  // Combines a span of values to the fingerprint state.
  template <typename SpanT>
  FingerprintHasher&& CombineSpan(SpanT&& values) &&;

  // Combines a raw byte sequence to the fingerprint state.
  //
  // NOTE: The hash function consumes the specified number of bytes from `data`.
  // It may not hash the `size` value.
  void CombineRawBytes(const void* data, size_t size);

 private:
  std::pair<uint64_t, uint64_t> state_;
};

namespace fingerprint_impl {

// Returns true if class has `void ArollaFingerprint(FingerprintHasher*) const`
// method.
template <typename T, class = void>
struct HasArollaFingerprintMethod : std::false_type {};

template <class T>
struct HasArollaFingerprintMethod<
    T, std::void_t<decltype(static_cast<void (T::*)(FingerprintHasher*) const>(
           &T::ArollaFingerprint))>> : std::true_type {};

}  // namespace fingerprint_impl

// Extension points for FingerprintHasher.
//
// 1. Define method
// void T::ArollaFingerprint(FingerprintHasher* hasher) const {
//   hasher->Combine(...);
// }
//
// 2. Specialize FingerprintHasherTraits
// This extension point provides a way to override FingerprintHasher behaviour
// for any specific type T.
//
// For example, FingerprintHasher doesn't support pointer types.
// You can override this behaviour using FingerpringHasherTraits:
//
//   template <>
//   struct FingerprintHasherTraits<std::shared_ptr<DecisionForest>> {
//     void operator()(FingerprintHasher* hasher,
//                     const std::shared_ptr<DecisionForest>& forest) {
//       hasher->Combine(forest->internal_state);
//     }
//   };
//
template <typename T>
struct FingerprintHasherTraits {
  FingerprintHasherTraits() = delete;
};

inline bool operator==(const Fingerprint& lhs, const Fingerprint& rhs) {
  return lhs.value == rhs.value;
}
inline bool operator!=(const Fingerprint& lhs, const Fingerprint& rhs) {
  return !(lhs == rhs);
}
inline bool operator<(const Fingerprint& lhs, const Fingerprint& rhs) {
  return lhs.value < rhs.value;
}
std::ostream& operator<<(std::ostream& ostream, const Fingerprint& fingerprint);

template <typename H>
H AbslHashValue(H state, const Fingerprint& fingerprint) {
  return H::combine(std::move(state), fingerprint.value);
}

template <typename... Args>
FingerprintHasher& FingerprintHasher::Combine(const Args&... args) & {
  auto combine = [this](const auto& arg) {
    using Arg = std::decay_t<decltype(arg)>;
    if constexpr (fingerprint_impl::HasArollaFingerprintMethod<Arg>::value) {
      arg.ArollaFingerprint(this);
    } else if constexpr (std::is_default_constructible_v<
                             FingerprintHasherTraits<Arg>>) {
      FingerprintHasherTraits<Arg>()(this, arg);
    } else if constexpr (std::is_arithmetic_v<Arg> || std::is_enum_v<Arg>) {
      CombineRawBytes(&arg, sizeof(arg));
    } else {
      static_assert(sizeof(Arg) == 0,
                    "Please, define `void "
                    "T::ArollaFingerprint(FingerprintHasher* hasher) const` "
                    "or specialise FingerprintHasherTraits for your type.");
    }
  };
  (combine(args), ...);
  return *this;
}

template <typename... Args>
FingerprintHasher&& FingerprintHasher::Combine(const Args&... args) && {
  Combine(args...);
  return std::move(*this);
}

template <typename SpanT>
FingerprintHasher& FingerprintHasher::CombineSpan(SpanT&& values) & {
  const auto span = absl::MakeConstSpan(values);
  using T = typename decltype(span)::value_type;
  Combine(values.size());
  if constexpr (std::is_default_constructible_v<FingerprintHasherTraits<T>>) {
    constexpr FingerprintHasherTraits<T> traits;
    for (const auto& x : values) {
      traits(this, x);
    }
  } else if constexpr (std::is_arithmetic_v<T> || std::is_enum_v<T>) {
    CombineRawBytes(values.data(), values.size() * sizeof(values[0]));
  } else {
    static_assert(sizeof(T) == 0,
                  "Please specialise FingerprintHasherTraits for your type.");
  }

  return *this;
}

template <typename SpanT>
FingerprintHasher&& FingerprintHasher::CombineSpan(SpanT&& values) && {
  CombineSpan(std::forward<SpanT>(values));
  return std::move(*this);
}

template <>
struct FingerprintHasherTraits<Fingerprint> {
  void operator()(FingerprintHasher* hasher, const Fingerprint& value) const {
    hasher->CombineRawBytes(&value.value, sizeof(value.value));
  }
};

template <>
struct FingerprintHasherTraits<std::string> {
  void operator()(FingerprintHasher* hasher, const std::string& value) const {
    hasher->Combine(value.size()).CombineRawBytes(value.data(), value.size());
  }
};

template <>
struct FingerprintHasherTraits<absl::string_view> {
  void operator()(FingerprintHasher* hasher, absl::string_view value) const {
    hasher->Combine(value.size()).CombineRawBytes(value.data(), value.size());
  }
};

// Specialization for classes that `HasStructFields<Struct>()`.
// Note `ArollaFingerprint` takes preference.
template <class Struct>
void CombineStructFields(FingerprintHasher* hasher, const Struct& value) {
  static_assert(HasStructFields<Struct>(), "no struct fields found");
  meta::foreach_tuple_element(
      GetStructFields<Struct>(), [&](const auto& struct_field) {
        hasher->Combine(*UnsafeGetStructFieldPtr(struct_field, &value));
      });
}

/// Disable FingerprintHasher for pointer types.

template <typename T>
struct FingerprintHasherTraits<T*> {
  static_assert(sizeof(T*) == 0,
                "Pointer values are runtime specific and not fingerprintable.");
};

template <typename T>
struct FingerprintHasherTraits<std::unique_ptr<T>> {
  static_assert(
      sizeof(std::unique_ptr<T>) == 0,
      "Unique pointer values are runtime specific and not fingerprintable.");
};

template <typename T>
struct FingerprintHasherTraits<std::shared_ptr<T>> {
  static_assert(
      sizeof(std::shared_ptr<T>) == 0,
      "Shared pointer values are runtime specific and not fingerprintable.");
};

// A helper macro for declaring FingerprintHasherTraits.
// Must be used within `arolla` namespace.
#define AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(CPP_TYPE)                   \
  template <>                                                                \
  struct FingerprintHasherTraits<CPP_TYPE> {                                 \
    void operator()(FingerprintHasher* hasher, const CPP_TYPE& value) const; \
  }

}  // namespace arolla

#endif  // AROLLA_UTIL_FINGERPRINT_H_
