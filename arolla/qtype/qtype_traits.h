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
#ifndef AROLLA_QTYPE_QTYPE_TRAITS_H_
#define AROLLA_QTYPE_QTYPE_TRAITS_H_

#include <type_traits>

#include "absl/base/attributes.h"
#include "absl/log/check.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/demangle.h"

namespace arolla {

// QTypeTraits is a way to deduce the QType corresponding to a C++ type during
// compilation.
// NOTE: Not all the QTypes have corresponding C++ types, e.g. Tuple does not.
//
// Do not use QTypeTraits<T>::type() directly, prefer GetQType<T>() that
// performs additional checks and generates better error messages.
//
// QTypeTraits<T> must provide a single function
//   static QTypePtr type();
// that returns QType for the template parameter T. To declare QTypeTraits
// specialization for your type use AROLLA_DECLARE_QTYPE(MyType) macro.
//
template <typename T>
struct QTypeTraits;

// has_qtype_traits_v<T> is true if QTypeTraits<T> specialization is included.
template <typename T, typename = void>
struct has_qtype_traits : std::false_type {};
template <typename T>
struct has_qtype_traits<T, std::void_t<decltype(QTypeTraits<T>::type())>>
    : std::true_type {};
template <typename T>
constexpr bool has_qtype_traits_v = has_qtype_traits<T>::value;

// Returns the QType corresponding to a C++ type T.
template <typename T>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline QTypePtr GetQType() {
  static_assert(
      has_qtype_traits_v<T>,
      "QTypeTraits<T> specialization is not included. #include file with "
      "QTypeTraits<T> expliclty to fix this problem. "
      "E.g., #include \"arolla/qtype/base_types.h\" for standard "
      "Arolla scalar and OptionalValue types.");
  DCHECK(typeid(T) == QTypeTraits<T>::type()->type_info())
      << "There is an error in the QType implementation for "
      << QTypeTraits<T>::type()->name();
  DCHECK(sizeof(T) <= QTypeTraits<T>::type()->type_layout().AllocSize())
      << "QType " << QTypeTraits<T>::type()->name()
      << " has too small frame layout to carry a value of C++ type "
      << TypeName<T>();
  return QTypeTraits<T>::type();
}

// Template for declaring QTypeTraits for a type.
//
// AROLLA_DECLARE_QTYPE must be used in a header within ::arolla namespace.
//
// The macro defines a QTypeTraits specialization with a type() static method
// that returns QTypePtr. You need to implement this method within corresponding
// .cc file.
//
// Example:
//
//   AROLLA_DECLARE_QTYPE(int);
//
// generates
//
//   template <>
//   struct QTypeTraits<int> {
//     static QTypePtr type();
//   };
//
#define AROLLA_DECLARE_QTYPE(/*CPP_TYPE*/...) \
  template <>                                 \
  struct QTypeTraits<__VA_ARGS__> {           \
    static QTypePtr type();                   \
  }

// QTypeTraits for `QTYPE` qtype.
template <>
struct QTypeTraits<QTypePtr> {
  static QTypePtr type() { return GetQTypeQType(); }
};

}  // namespace arolla

#endif  // AROLLA_QTYPE_QTYPE_TRAITS_H_
