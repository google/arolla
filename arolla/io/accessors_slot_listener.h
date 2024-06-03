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
#ifndef AROLLA_IO_ACCESSORS_SLOT_LISTENER_H_
#define AROLLA_IO_ACCESSORS_SLOT_LISTENER_H_

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/io/accessor_helpers.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/meta.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace slot_listener_impl {

// Input type for the accessor with signature void(const Input&, Output*).
template <class Accessor, class Output>
using slot_listener_accessor_input_t = std::decay_t<meta::head_t<
    typename meta::function_traits<std::decay_t<Accessor>>::arg_types>>;

// Functor to conditionally run Accessor on the given Slot.
template <class Output, class Accessor>
class Getter {
 public:
  using InputType = slot_listener_accessor_input_t<Accessor, Output>;

  Getter(std::optional<FrameLayout::Slot<InputType>> slot, Accessor accessor)
      : slot_(slot), accessor_(std::move(accessor)) {}

  static absl::StatusOr<Getter> Build(std::optional<TypedSlot> slot,
                                      const Accessor& accessor) {
    if (slot.has_value()) {
      ASSIGN_OR_RETURN(auto specific_slot, slot->ToSlot<InputType>());
      return {Getter({specific_slot}, accessor)};
    } else {
      return {Getter(std::nullopt, accessor)};
    }
  }

  void operator()(ConstFramePtr frame, Output* output) const {
    if (slot_.has_value()) {
      accessor_(frame.Get(*slot_), output);
    }
  }

 private:
  std::optional<FrameLayout::Slot<InputType>> slot_;
  Accessor accessor_;
};

template <class Input, class NameAccessorsTuple>
class AccessorsSlotListener;

template <class Output, class... Accessors>
class AccessorsSlotListener<Output,
                            std::tuple<std::pair<std::string, Accessors>...>>
    final : public StaticSlotListener<Output> {
  using NameAccessorsTuple = std::tuple<std::pair<std::string, Accessors>...>;

 public:
  static absl::StatusOr<std::unique_ptr<SlotListener<Output>>> Build(
      NameAccessorsTuple accessors) {
    auto loader =
        absl::WrapUnique(new AccessorsSlotListener(std::move(accessors)));
    RETURN_IF_ERROR(ValidateDuplicatedNames(loader->types_in_order()));
    return {std::move(loader)};
  }

 private:
  absl::StatusOr<BoundSlotListener<Output>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots)
      const final {
    ASSIGN_OR_RETURN(auto slots, MaybeFindSlotsAndVerifyTypes(
                                     this->types_in_order(), input_slots));
    return BindImpl(
        std::move(slots),
        std::make_index_sequence<std::tuple_size<NameAccessorsTuple>::value>{});
  }

  explicit AccessorsSlotListener(NameAccessorsTuple accessors)
      : StaticSlotListener<Output>(
            AccessorsSlotListener::CreateOutputTypesInOrder(
                accessors, std::make_index_sequence<
                               std::tuple_size<NameAccessorsTuple>::value>{})),
        accessors_(std::move(accessors)) {}

  // Type of the Ith accessor.
  template <size_t I>
  using Accessor =
      std::tuple_element_t<1, std::tuple_element_t<I, NameAccessorsTuple>>;

  // Type of the Ith accessor.
  template <size_t I>
  using SlotListenerAccessorInputType =
      slot_listener_impl::slot_listener_accessor_input_t<Accessor<I>, Output>;

  // Returns QType of the Ith accessor.
  template <size_t I>
  const Accessor<I>& GetAccessor() const {
    return std::get<1>(std::get<I>(accessors_));
  }

  // Returns QType of the Ith accessor.
  template <size_t I>
  static QTypePtr GetOutputType() {
    return GetQType<SlotListenerAccessorInputType<I>>();
  }

  template <size_t... Is>
  static std::vector<std::pair<std::string, QTypePtr>> CreateOutputTypesInOrder(
      const NameAccessorsTuple& accessors, std::index_sequence<Is...>) {
    return {{std::string(std::get<0>(std::get<Is>(accessors))),
             GetOutputType<Is>()}...};
  }

  template <size_t... Is>
  absl::StatusOr<BoundSlotListener<Output>> BindImpl(
      std::vector<std::optional<TypedSlot>> slots,
      std::index_sequence<Is...>) const {
    auto getters_or =
        LiftStatusUp(slot_listener_impl::Getter<Output, Accessor<Is>>::Build(
            slots[Is], GetAccessor<Is>())...);
    ASSIGN_OR_RETURN(auto getters, getters_or);
    return BoundSlotListener<Output>(
        [getters_(std::move(getters))](
            // args are unused iff sizeof...(Is)==0.
            ConstFramePtr frame ABSL_ATTRIBUTE_UNUSED,
            Output* output ABSL_ATTRIBUTE_UNUSED) {
          (std::get<Is>(getters_)(frame, output), ...);
          return absl::OkStatus();
        });
  }

  NameAccessorsTuple accessors_;
};

}  // namespace slot_listener_impl

// Implementation of SlotListener based on accessors.
// NameAccessorsTuple is a tuple of pairs, more precisely
// std::tuple<std::pair<std::string, Accessors>...>.
// Due to compiler limitations the tuple size is limited to several hundreds of
// elements. Accessors must be functors with supported signature
// void(const Input&, Output*)
// `Input` type of accessors must have QTypeTraits<Input> specialized.
//
// Use CreateAccessorsSlotListener syntactic sugar function to construct this
// class.
//
template <class Input, class NameAccessorsTuple>
using AccessorsSlotListener =
    slot_listener_impl::AccessorsSlotListener<Input, NameAccessorsTuple>;

// Constructs AccessorsSlotListener from provided tuple.
// See comments for AccessorsSlotListener for more details.
template <class Output, class... Accessors>
absl::StatusOr<std::unique_ptr<SlotListener<Output>>>
CreateAccessorsSlotListenerFromTuple(
    std::tuple<std::pair<std::string, Accessors>...> name_accessors) {
  return AccessorsSlotListener<Output, decltype(name_accessors)>::Build(
      std::move(name_accessors));
}

// Constructs AccessorsSlotListener from parameters pack:
// name, accessor, name, accessor, ...
// Number of parameters must be even. Names must be convertible to string.
// Accessors must be functors with supported signature
// void(const Input&, Output*)
// `Input` type of accessors must have QTypeTraits<Input> specialized.
// Due to compiler limitations up to several hundreds of accessors is supported.
//
// Usage example:
//
// struct MyOutput {
//   int a;
//   double b;
//   double b_squared;
// };
//
// ASSIGN_OR_RETURN(
//    std::unique_ptr<SlotListener<MyOutput>> input_loader,
//    CreateAccessorsSlotListener<MyOutput>(
//      "a", [](int a, MyOutput* o) { o->a = a; },  //
//      "b", [](double b, MyOutput* o) { o->b = b; o->b_squared = b * b; },  //
//    ));
//
template <class Output, class... NameAccessors>
absl::StatusOr<std::unique_ptr<SlotListener<Output>>>
CreateAccessorsSlotListener(NameAccessors... name_accessors) {
  return CreateAccessorsSlotListenerFromTuple<Output>(
      accessor_helpers_impl::ConvertNameAccessorsPackToNestedTuple(
          name_accessors...));
}

}  // namespace arolla

#endif  // AROLLA_IO_ACCESSORS_SLOT_LISTENER_H_
