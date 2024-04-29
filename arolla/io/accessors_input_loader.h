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
#ifndef AROLLA_IO_ACCESSORS_INPUT_LOADER_H_
#define AROLLA_IO_ACCESSORS_INPUT_LOADER_H_

#include <cstddef>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/io/accessor_helpers.h"
#include "arolla/io/input_loader.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/meta.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Invoke accessor and store result to the output.
// The first supported signature will be called:
// 1. void(const Input&, RawBufferFactory*, Output*)
// 2. void(const Input&, Output*)
// 3. Output(const Input&, RawBufferFactory*)
// 4. Output(const Input&)
template <class Accessor, class Input, class Output>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline void InvokeInputLoaderAccessor(
    const Accessor& accessor, const Input& input, RawBufferFactory* factory,
    Output* output) {
  if constexpr (std::is_invocable_v<const Accessor&, const Input&,
                                    RawBufferFactory*, Output*>) {
    // 1. void(const Input&, RawBufferFactory*, Output*)
    accessor(input, factory, output);
  } else if constexpr (std::is_invocable_v<const Accessor&, const Input&,
                                           Output*>) {
    // 2. void(const Input&, Output*)
    ((void)(factory));
    accessor(input, output);
  } else if constexpr (std::is_invocable_v<const Accessor&, const Input&,
                                           RawBufferFactory*>) {
    // 3. Output(const Input&, RawBufferFactory*)
    *output = accessor(input, factory);
  } else if constexpr (std::is_invocable_v<const Accessor&, const Input&>) {
    // 4. Output(const Input&)
    ((void)(factory));
    *output = accessor(input);
  }
}

namespace input_loader_impl {

// Determinate result type of the accessor.
template <class Accessor, class Input>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline auto InvokeInputLoaderAccessorTypeMeta() {
  if constexpr (std::is_invocable_v<const Accessor&, const Input&,
                                    RawBufferFactory*>) {
    // 3. Output(const Input&, RawBufferFactory*)
    return std::invoke_result<const Accessor&, const Input&,
                              RawBufferFactory*>();
  } else if constexpr (std::is_invocable_v<const Accessor&, const Input&>) {
    // 4. Output(const Input&)
    return std::invoke_result<const Accessor&, const Input&>();
  } else {
    using info = meta::function_traits<std::decay_t<Accessor>>;
    if constexpr (info::arity == 2) {
      // 2. void(const Input&, Output*)
      using Output = std::remove_pointer_t<
          std::tuple_element_t<1, typename info::arg_types::tuple>>;
      static_assert(std::is_invocable_v<const Accessor&, const Input&, Output*>,
                    "Unexpected accessor signature.");
      return meta::type<Output>();
    } else {
      // 1. void(const Input&, RawBufferFactory*, Output*)
      using Output = std::remove_pointer_t<
          std::tuple_element_t<2, typename info::arg_types::tuple>>;
      static_assert(std::is_invocable_v<const Accessor&, const Input&,
                                        RawBufferFactory*, Output*>,
                    "Unexpected accessor signature.");
      return meta::type<Output>();
    }
  }
}

}  // namespace input_loader_impl

// Result type of the accessor.
// Supported signatures:
// 1. void(const Input&, RawBufferFactory*, Output*)
// 2. void(const Input&, Output*)
// 3. Output(const Input&, RawBufferFactory*)
// 4. Output(const Input&)
// If more than one signature is implemented, output type need to be consistent.
template <class Accessor, class Input>
using InputLoaderAccessorResultType = std::decay_t<
    typename decltype(input_loader_impl::InvokeInputLoaderAccessorTypeMeta<
                      const Accessor&, const Input&>())::type>;

namespace input_loader_impl {

template <class Input, class NameAccessorsTuple>
class AccessorsInputLoader;

// Functor to conditionally set Accessor result to the given Slot.
template <class Input, class Accessor>
class Setter {
 public:
  using ResultType = InputLoaderAccessorResultType<Accessor, Input>;

  Setter(std::optional<FrameLayout::Slot<ResultType>> slot, Accessor accessor)
      : slot_(slot), accessor_(std::move(accessor)) {}

  static absl::StatusOr<Setter> Build(std::optional<TypedSlot> slot,
                                      const Accessor& accessor) {
    if (slot.has_value()) {
      ASSIGN_OR_RETURN(auto specific_slot, slot->ToSlot<ResultType>());
      return {Setter({specific_slot}, accessor)};
    } else {
      return {Setter(std::nullopt, accessor)};
    }
  }

  void operator()(const Input& input, FramePtr frame,
                  RawBufferFactory* factory) const {
    if (slot_.has_value()) {
      InvokeInputLoaderAccessor(accessor_, input, factory,
                                frame.GetMutable(*slot_));
    }
  }

 private:
  std::optional<FrameLayout::Slot<ResultType>> slot_;
  Accessor accessor_;
};

template <class Input, class... Accessors>
class AccessorsInputLoader<Input,
                           std::tuple<std::pair<std::string, Accessors>...>>
    final : public StaticInputLoader<Input> {
  using NameAccessorsTuple = std::tuple<std::pair<std::string, Accessors>...>;

 public:
  static absl::StatusOr<InputLoaderPtr<Input>> Build(
      NameAccessorsTuple accessors) {
    auto output_types_in_order = CreateOutputTypesInOrder(
        accessors,
        std::make_index_sequence<std::tuple_size<NameAccessorsTuple>::value>{});
    RETURN_IF_ERROR(ValidateDuplicatedNames(output_types_in_order));
    // Not using make_shared to avoid binary size blowup.
    return InputLoaderPtr<Input>(
        static_cast<InputLoader<Input>*>(new AccessorsInputLoader(
            std::move(accessors), std::move(output_types_in_order))));
  }

  absl::StatusOr<BoundInputLoader<Input>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const final {
    ASSIGN_OR_RETURN(auto slots, MaybeFindSlotsAndVerifyTypes(
                                     this->types_in_order(), output_slots));
    return BindImpl(
        std::move(slots),
        std::make_index_sequence<std::tuple_size<NameAccessorsTuple>::value>{});
  }

 private:
  explicit AccessorsInputLoader(
      NameAccessorsTuple accessors,
      std::vector<std::pair<std::string, QTypePtr>> output_types_in_order)
      : StaticInputLoader<Input>(std::move(output_types_in_order)),
        accessors_(std::move(accessors)) {}

  // Type of the Ith accessor.
  template <size_t I>
  using Accessor =
      std::tuple_element_t<1, std::tuple_element_t<I, NameAccessorsTuple>>;

  // Type of the Ith accessor.
  template <size_t I>
  using InputLoaderAccessorResultType =
      InputLoaderAccessorResultType<Accessor<I>, Input>;

  // Returns QType of the Ith accessor.
  template <size_t I>
  const Accessor<I>& GetAccessor() const {
    return std::get<1>(std::get<I>(accessors_));
  }

  // Returns QType of the Ith accessor.
  template <size_t I>
  static QTypePtr GetOutputType() {
    return GetQType<InputLoaderAccessorResultType<I>>();
  }

  template <size_t... Is>
  static std::vector<std::pair<std::string, QTypePtr>> CreateOutputTypesInOrder(
      const NameAccessorsTuple& accessors, std::index_sequence<Is...>) {
    return {{std::string(std::get<0>(std::get<Is>(accessors))),
             GetOutputType<Is>()}...};
  }

  template <size_t... Is>
  absl::StatusOr<BoundInputLoader<Input>> BindImpl(
      std::vector<std::optional<TypedSlot>> slots,
      std::index_sequence<Is...>) const {
    auto setters_or = LiftStatusUp(
        Setter<Input, Accessor<Is>>::Build(slots[Is], GetAccessor<Is>())...);
    ASSIGN_OR_RETURN(auto setters, setters_or);
    return BoundInputLoader<Input>(
        [setters_(std::move(setters))](
            // args are unused iff sizeof...(Is)==0.
            const Input& input ABSL_ATTRIBUTE_UNUSED,
            FramePtr frame ABSL_ATTRIBUTE_UNUSED,
            RawBufferFactory* factory ABSL_ATTRIBUTE_UNUSED) {
          (std::get<Is>(setters_)(input, frame, factory), ...);
          return absl::OkStatus();
        });
  }

  NameAccessorsTuple accessors_;
};

}  // namespace input_loader_impl

// Implementation of InputLoader based on accessors.
// NameAccessorsTuple is a tuple of pairs, more precisely
// std::tuple<std::pair<std::string, Accessors>...>.
// Due to compiler limitations std::tuple_size must be less than 300.
// Accessors must be functors with supported signatures:
// 1. Output(const Input&)
// 2. Output(const Input&, RawBufferFactory*)
// 3. void(const Input&, Output*)
// 4. void(const Input&, RawBufferFactory*, Output*)
// `Output` type of accessors must have QTypeTraits<Output> specialized.
//
// Creating tuple with specified requirements could be challenging, so
// there is CreateAccessorsInputLoader function to construct this class.
//
// struct MyInput {
//   int a;
//   double b;
// };
//
// ASSIGN_OR_RETURN(
//    InputLoaderPtr<MyInput> input_loader,
//    CreateAccessorsInputLoader<MyInput>(
//      "a", [](const MyInput& s) { return s.a; },  //
//      "b", [](const MyInput& s) { return s.b; },  //
//      "b**2", [](const MyInput& s, double* res) { res = s.b * s.b; },  //
//    ));
template <class Input, class NameAccessorsTuple>
using AccessorsInputLoader =
    input_loader_impl::AccessorsInputLoader<Input, NameAccessorsTuple>;

// Constructs AccessorsInputLoader from provided tuple.
// See comments for AccessorsInputLoader for more details.
template <class Input, class... Accessors>
absl::StatusOr<InputLoaderPtr<Input>> CreateAccessorsInputLoaderFromTuple(
    std::tuple<std::pair<std::string, Accessors>...> name_accessors) {
  return AccessorsInputLoader<Input, decltype(name_accessors)>::Build(
      std::move(name_accessors));
}

// Constructs AccessorsInputLoader from parameters pack:
// name, accessor, name, accessor, ...
// Number of parameters must be even. Names must be convertible to string.
// Accessors must be functors with supported signatures:
// 1. Output(const Input&)
// 2. Output(const Input&, RawBufferFactory*)
// 3. void(const Input&, Output*)
// 4. void(const Input&, RawBufferFactory*, Output*)
// `Output` type must have QTypeTraits<Output> specialized.
// See more details in AccessorsInputLoader comments.
// Due to compiler limitations function is compiled with up to 300 accessors.
template <class Input, class... NameAccessors>
absl::StatusOr<InputLoaderPtr<Input>> CreateAccessorsInputLoader(
    NameAccessors... name_accessors) {
  return CreateAccessorsInputLoaderFromTuple<Input>(
      accessor_helpers_impl::ConvertNameAccessorsPackToNestedTuple(
          name_accessors...));
}

}  // namespace arolla

#endif  // AROLLA_IO_ACCESSORS_INPUT_LOADER_H_
