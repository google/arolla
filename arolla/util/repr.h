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
#ifndef AROLLA_UTIL_REPR_H_
#define AROLLA_UTIL_REPR_H_

#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

#include "arolla/util/fingerprint.h"

namespace arolla {

// An object's string representation.
//
// This struct includes "precedence", that describes how tightly the left and
// right parts of the string are "bound" with the middle. It helps determine
// when to use brackets when displaying composite values, such as expressions.
struct ReprToken {
  struct Precedence {
    int8_t left = -1;
    int8_t right = -1;
  };
  // Highest precedence.
  static constexpr Precedence kHighest{-1, -1};

  // In particular means no brackets needed for subscription, slicing, call,
  // attribute reference:
  //
  //   value[index]
  //   value[index:index]
  //   value(args...)
  //   value.attribute
  static constexpr Precedence kSafeForSubscription = kHighest;

  // Safe for negation: -value.
  //
  // Note: Among the arithmetic operations, negation has one of the highest
  // precedences.
  static constexpr Precedence kSafeForNegation{0, 0};

  // Safe for arithmetics: value * value, value + value; but -(value).
  static constexpr Precedence kSafeForArithmetic{1, 1};

  // Subscription operator precedence.
  static constexpr Precedence kOpSubscription{0, -1};

  // Unary operator precedence, such as negation.
  static constexpr Precedence kOpUnary{1, 1};

  // Binary operator precedences.
  // Corresponds to: **
  static constexpr Precedence kOpPow{1, 2};
  // Corresponds to: *, /, //, %
  static constexpr Precedence kOpMul{3, 2};
  // Corresponds to: +, -
  static constexpr Precedence kOpAdd{5, 4};
  // Corresponds to: &
  static constexpr Precedence kOpAnd{7, 6};
  // Corresponds to: |
  static constexpr Precedence kOpOr{9, 8};
  // Corresponds to: <, <=, ==, !=, >=, >
  static constexpr Precedence kOpComparison{10, 10};

  // The operator precedence for foo[a:b:c] slicing operator.
  static constexpr Precedence kOpSlice{11, 11};

  std::string str;
  Precedence precedence = kHighest;
};

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(ReprToken::Precedence);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(ReprToken);

// Traits class that allows to customize the "official" string representation
// for type T.
//
// String representation you define should be information-rich and
// unambiguous, and generally it should follow the spirit of `repr()` builtin
// function in Python.
//
// NOTE: Please expect that this facility will be called many thousands of
// times per second. It means that generation of a string representation
// should be fast, and the resulting string should be modest in size.
//
template <typename T, typename Enabled = void>
struct ReprTraits {};

// Default implementation for types that implement ReprToken ArollaReprToken().
template <typename T>
struct ReprTraits<T, std::void_t<decltype(static_cast<ReprToken (T::*)() const>(
                         &T::ArollaReprToken))>> {
  ReprToken operator()(const T& value) const { return value.ArollaReprToken(); }
};

template <typename T>
auto GenReprToken(const T& value) -> decltype(ReprTraits<T>()(value)) {
  return ReprTraits<T>()(value);
}

template <typename T>
auto Repr(const T& value) -> decltype(GenReprToken(value).str) {
  return std::move(GenReprToken(value).str);
}

// Returns string representation for weak_float.
ReprToken GenReprTokenWeakFloat(double value);

// Template for declaring ReprTraits for a type.
//
// AROLLA_DECLARE_REPR must be used in a header within ::arolla namespace.
//
// Example:
//   AROLLA_DECLARE_REPR(Text);
//
#define AROLLA_DECLARE_REPR(CPP_TYPE)                  \
  template <>                                          \
  struct ReprTraits<CPP_TYPE> {                        \
    ReprToken operator()(const CPP_TYPE& value) const; \
  }

AROLLA_DECLARE_REPR(bool);
AROLLA_DECLARE_REPR(int32_t);
AROLLA_DECLARE_REPR(int64_t);
AROLLA_DECLARE_REPR(uint64_t);
AROLLA_DECLARE_REPR(float);
AROLLA_DECLARE_REPR(double);

// Special handling for std::vector<bool>::const_refererence.
// (currently needed for compatibility with clang++ -stdlib=libc++)
namespace repr_traits_vector_bool_ref_impl {
struct FakeStdVectorBoolConstRef;

using StdVectorBoolConstRef = std::conditional_t<
    std::is_same_v<bool, std::decay_t<std::vector<bool>::const_reference>>,
    repr_traits_vector_bool_ref_impl::FakeStdVectorBoolConstRef,
    std::decay_t<std::vector<bool>::const_reference>>;

}  // namespace repr_traits_vector_bool_ref_impl

template <>
struct ReprTraits<repr_traits_vector_bool_ref_impl::StdVectorBoolConstRef>
    : ReprTraits<bool> {};

}  // namespace arolla

#endif  // AROLLA_UTIL_REPR_H_
