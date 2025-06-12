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
#ifndef AROLLA_UTIL_META_H_
#define AROLLA_UTIL_META_H_

#include <cstddef>
#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"

// Utils for template metaprogramming.

namespace arolla::meta {

// Template class for storing a singular type.
//
// Note: Use a workaround with `using type = type_` to make `type::type`
//       property name work.
template <class T>
struct type_ {
  using type = T;
};

template <typename T>
using type = type_<T>;

// Template class for storing a list of types.
//
template <class... TypeTraits>
struct type_list {
  using tuple = std::tuple<TypeTraits...>;
};

// head_t<type_list<Ts...>> returns the first element of Ts... list.
//
template <class L>
struct head;

template <class T, class... Ts>
struct head<type_list<T, Ts...>> : type<T> {};

template <class L>
using head_t = typename head<L>::type;

// tail_t<type_list<Ts...>> returns all except the first element of Ts... list.
//
template <class L>
struct tail;

template <class T, class... Ts>
struct tail<type_list<T, Ts...>> {
  using type = type_list<Ts...>;
};

template <class L>
using tail_t = typename tail<L>::type;

// concat_t<type_list<Ts...>, type_list<Us...>> returns type_list<Ts..., Us...>
//
template <class L1, class L2>
struct concat;

template <class... Ts1, class... Ts2>
struct concat<type_list<Ts1...>, type_list<Ts2...>> {
  using type = type_list<Ts1..., Ts2...>;
};

template <class L1, class L2>
using concat_t = typename concat<L1, L2>::type;

// Apply a function to each type of the list.
// foreach_type(type_list<Ts...>, fn) -> fn(type<Ts>())...
template <typename Fn, typename... Ts>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline constexpr void foreach_type(
    type_list<Ts...>, Fn fn) {
  (fn(type<Ts>()), ...);
}

template <typename TypeList, typename Fn>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline constexpr void foreach_type(Fn fn) {
  foreach_type(TypeList(), fn);
}

// contains_v<type_list<Ts...>, T> is true iff T is in Ts... list.
//
template <typename TypeList, typename T>
struct contains : std::false_type {};

template <typename T, typename... Ts>
struct contains<type_list<Ts...>, T>
    : std::disjunction<std::is_same<T, Ts>...> {};

template <typename TypeList, typename T>
constexpr bool contains_v = contains<TypeList, T>::value;

// function_traits inspects callable to deduce its arity, argument and return
// types.
//
// function_traits for objects with operator() defined, which has only one
// overload (e. g. lambdas).
template <typename T>
struct function_traits : public function_traits<decltype(&T::operator())> {};

// function_traits for pointers to member const functions.
template <typename CLS, typename RES, typename... ARGs>
struct function_traits<RES (CLS::*)(ARGs...) const> {
  static constexpr int arity = sizeof...(ARGs);
  using arg_types = type_list<ARGs...>;
  using return_type = RES;
};

// function_traits for pointers to member non-const functions.
template <typename CLS, typename RES, typename... ARGs>
struct function_traits<RES (CLS::*)(ARGs...)> {
  static constexpr int arity = sizeof...(ARGs);
  using arg_types = type_list<ARGs...>;
  using return_type = RES;
};

// function_traits for function pointers.
template <typename RES, typename... ARGs>
struct function_traits<RES (*)(ARGs...)> {
  static constexpr int arity = sizeof...(ARGs);
  using arg_types = type_list<ARGs...>;
  using return_type = RES;
};

// function_traits for std::reference_wrapper<F>
template <typename F>
struct function_traits<std::reference_wrapper<F>> : function_traits<F> {};

// is_wrapped_with<X, Y>::value is true if Y is X<T> or false otherwise.
// Note: doesn't work with template aliases.
template <template <typename> class Wrapper, class T>
struct is_wrapped_with : public std::false_type {};

template <template <typename> class Wrapper, class T>
struct is_wrapped_with<Wrapper, Wrapper<T>> : public std::true_type {};

template <template <typename> class Wrapper, class T>
constexpr bool is_wrapped_with_v = is_wrapped_with<Wrapper, T>::value;

template <template <typename> class Wrapper, class T>
struct strip_template {
  using type = T;
};

template <template <typename> class Wrapper, class T>
struct strip_template<Wrapper, Wrapper<T>> {
  using type = T;
};

// Converts X<Y> to Y for given X. Doesn't affect other types.
// i.e. strip_template_t<X, X<Y>> -> Y
//      strip_template_t<X, Y> -> Y
// Note: doesn't work with template aliases.
template <template <typename> class Wrapper, class T>
using strip_template_t = typename strip_template<Wrapper, T>::type;

// (internal) Applies a function to each element of a tuple.
template <typename Tuple, typename Fn, size_t... Is>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline void foreach_tuple_element_impl(
    ABSL_ATTRIBUTE_UNUSED Tuple&& tuple, ABSL_ATTRIBUTE_UNUSED Fn&& fn,
    std::index_sequence<Is...>) {
  (fn(std::get<Is>(std::forward<Tuple>(tuple))), ...);
}

// Applies a function to each element of a tuple.
//
// foreach_tuple_element(tuple, fn) -> fn(std::get<0>(tuple))...
//
template <typename Tuple, typename Fn>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline void foreach_tuple_element(Tuple&& tuple,
                                                               Fn&& fn) {
  static_assert(std::tuple_size_v<std::decay_t<Tuple>> >= 0,
                "expected a tuple");
  foreach_tuple_element_impl(
      std::forward<Tuple>(tuple), std::forward<Fn>(fn),
      std::make_index_sequence<std::tuple_size_v<std::decay_t<Tuple>>>());
}

// (internal) Applies a function to each element type of a tuple.
template <typename Tuple, typename Fn, size_t... Is>
void foreach_tuple_element_type_impl(ABSL_ATTRIBUTE_UNUSED Fn&& fn,
                                     std::index_sequence<Is...>) {
  (fn(::arolla::meta::type<std::tuple_element_t<Is, Tuple>>()), ...);
}

// Applies a function to each element type of a tuple.
//
// foreach_tuple_element_type<Tuple>(fn)
//     -> fn(meta::type<std::tuple_element_t<0, Tuple>>())...
//
template <typename Tuple, typename Fn>
void foreach_tuple_element_type(Fn fn) {
  static_assert(std::tuple_size_v<Tuple> >= 0, "expected a tuple");
  foreach_tuple_element_type_impl<Tuple>(
      std::forward<Fn>(fn),
      std::make_index_sequence<std::tuple_size_v<Tuple>>());
}

// Checks if a function object is "transparent": accepts arguments of arbitrary
// types and uses perfect forwarding.
//
// The check is based on the presence of the member type `is_transparent`.
//
template <class, class = void>
struct is_transparent : std::false_type {};

template <class T>
struct is_transparent<T, std::void_t<typename T::is_transparent>>
    : std::true_type {};

template <typename T>
constexpr bool is_transparent_v = is_transparent<T>::value;

}  // namespace arolla::meta

#endif  // AROLLA_UTIL_META_H_
