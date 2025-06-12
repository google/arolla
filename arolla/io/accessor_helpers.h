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
#ifndef AROLLA_IO_ACCESSOR_HELPERS_H_
#define AROLLA_IO_ACCESSOR_HELPERS_H_

#include <cstddef>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

namespace arolla::accessor_helpers_impl {

template <class... NameAccessors>
class VariadicPackToNestedTupleImpl {
 private:
  template <size_t I>
  using AccessorType =
      std::remove_cv_t<std::remove_reference_t<typename std::tuple_element<
          I * 2 + 1, std::tuple<NameAccessors...>>::type>>;

  template <size_t I>
  AccessorType<I> Accessor() const {
    return std::get<I * 2 + 1>(name_accessors_);
  }

  template <size_t... Is>
  using NestedTupleType =
      std::tuple<std::pair<std::string, AccessorType<Is>>...>;

  template <size_t... Is>
  NestedTupleType<Is...> MakeNestedTupleImpl(std::index_sequence<Is...>) const {
    return std::make_tuple(std::make_pair(Name<Is>(), Accessor<Is>())...);
  }

  template <size_t I>
  std::string Name() const {
    return std::string(std::get<I * 2>(name_accessors_));
  }

 public:
  static_assert(
      sizeof...(NameAccessors) % 2 == 0,
      "NameAccessors must be formed as name, accessor, name, accessor, ...");
  static constexpr size_t kAccessorCount = sizeof...(NameAccessors) / 2;

  explicit VariadicPackToNestedTupleImpl(
      std::tuple<NameAccessors...> name_accessors)
      : name_accessors_(name_accessors) {}

  auto MakeNestedTuple() const
      -> decltype(MakeNestedTupleImpl(
          std::make_index_sequence<
              VariadicPackToNestedTupleImpl::kAccessorCount>())) {
    return MakeNestedTupleImpl(
        std::make_index_sequence<
            VariadicPackToNestedTupleImpl::kAccessorCount>());
  }

 private:
  std::tuple<NameAccessors...> name_accessors_;
};

// Returns nested tuple for name accessors tuple<pair<string, Fn0>, ...>.
// Number of parameters must be even.
// Pack must be formed in the following way:
// name, accessor, name, accessor, ...
// Names must be convertible to string.
template <class... NameAccessors>
auto ConvertNameAccessorsPackToNestedTuple(NameAccessors... name_accessors)
    -> decltype(VariadicPackToNestedTupleImpl<NameAccessors...>(
                    std::make_tuple(name_accessors...))
                    .MakeNestedTuple()) {
  return VariadicPackToNestedTupleImpl<NameAccessors...>(
             std::forward_as_tuple(name_accessors...))
      .MakeNestedTuple();
}

}  // namespace arolla::accessor_helpers_impl

#endif  // AROLLA_IO_ACCESSOR_HELPERS_H_
