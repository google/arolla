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
#ifndef AROLLA_IO_WILDCARD_INPUT_LOADER_H_
#define AROLLA_IO_WILDCARD_INPUT_LOADER_H_

#include <algorithm>
#include <functional>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/io/input_loader.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Invoke accessor and store result to the output.
// The first supported signature will be called:
// 1. void(const Input&, const Key&, RawBufferFactory*, Output*)
// 2. void(const Input&, const Key&, Output*)
// 3. Output(const Input&, const Key&, RawBufferFactory*)
// 4. Output(const Input&, const Key&)
template <class Accessor, class Input, class Key, class Output>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline absl::Status
InvokeWildcardInputLoaderAccessor(const Accessor& accessor, const Input& input,
                                  const Key& key, RawBufferFactory* factory,
                                  Output* output) {
  if constexpr (std::is_invocable_v<const Accessor&, const Input&, const Key&,
                                    RawBufferFactory*, Output*>) {
    // 1. void(const Input&, const Key&, RawBufferFactory*, Output*)
    accessor(input, key, factory, output);
  } else if constexpr (std::is_invocable_v<const Accessor&, const Input&,
                                           const Key&, Output*>) {
    // 2. void(const Input&, const Key&, Output*)
    ((void)(factory));
    accessor(input, key, output);
  } else if constexpr (std::is_invocable_v<const Accessor&, const Input&,
                                           const Key&, RawBufferFactory*>) {
    // 3. Output(const Input&, const Key&, RawBufferFactory*)
    if constexpr (IsStatusOrT<decltype(accessor(input, key, factory))>::value) {
      ASSIGN_OR_RETURN(*output, accessor(input, key, factory));
    } else {
      *output = accessor(input, key, factory);
    }
  } else if constexpr (std::is_invocable_v<const Accessor&, const Input&,
                                           const Key&>) {
    // 4. Output(const Input&, const Key&)
    ((void)(factory));
    if constexpr (IsStatusOrT<decltype(accessor(input, key))>::value) {
      ASSIGN_OR_RETURN(*output, accessor(input, key));
    } else {
      *output = accessor(input, key);
    }
  }
  return absl::OkStatus();
}

namespace input_loader_impl {

// Determinate result type of the wildcard accessor.
template <class Accessor, class Input, class Key>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline auto
InvokeWildcardInputLoaderAccessorTypeMeta() {
  if constexpr (std::is_invocable_v<const Accessor&, const Input&, const Key&,
                                    RawBufferFactory*>) {
    // 3. Output(const Input&, const Key& key, RawBufferFactory*)
    return std::invoke_result<const Accessor&, const Input&, const Key&,
                              RawBufferFactory*>();
  } else if constexpr (std::is_invocable_v<const Accessor&, const Input&,
                                           const Key&>) {
    // 4. Output(const Input&, const Key& key)
    return std::invoke_result<const Accessor&, const Input&, const Key&>();
  } else {
    using info = meta::function_traits<std::decay_t<Accessor>>;
    if constexpr (info::arity == 3) {
      // 2. void(const Input&, const Key& key, Output*)
      using Output = std::remove_pointer_t<
          std::tuple_element_t<2, typename info::arg_types::tuple>>;
      static_assert(std::is_invocable_v<const Accessor&, const Input&,
                                        const Key&, Output*>,
                    "Unexpected accessor signature.");
      return meta::type<Output>();
    } else {
      // 1. void(const Input&, const Key& key, RawBufferFactory*, Output*)
      using Output = std::remove_pointer_t<
          std::tuple_element_t<3, typename info::arg_types::tuple>>;
      static_assert(std::is_invocable_v<const Accessor&, const Input&,
                                        const Key&, RawBufferFactory*, Output*>,
                    "Unexpected accessor signature.");
      return meta::type<Output>();
    }
  }
}

// Constructs a function that extracts lookup key from a string based on the
// given format specification.
std::function<std::optional<std::string>(absl::string_view)> MakeNameToKeyFn(
    const absl::ParsedFormat<'s'>& format);

}  // namespace input_loader_impl

// Result type of the wildcard accessor.
// Supported signatures:
// 1. void(const Input&, const Key&, RawBufferFactory*, Output*)
// 2. void(const Input&, const Key&, Output*)
// 3. Output(const Input&, const Key&, RawBufferFactory*)
// 4. Output(const Input&, const Key&)
// If more than one signature is implemented, output type need to be consistent.
template <class Accessor, class Input, class Key>
using WildcardAccessorResultType = std::decay_t<strip_statusor_t<
    typename decltype(input_loader_impl::
                          InvokeWildcardInputLoaderAccessorTypeMeta<
                              const Accessor&, const Input&,
                              const Key&>())::type>>;

// Specialized callback class for implementations of
// WildcardInputLoader::CallbackAccessorFn. Should be called either with
// specific type or TypedRef.
class WildcardInputLoaderCallback {
 public:
  WildcardInputLoaderCallback(TypedSlot slot, FramePtr frame)
      : slot_(slot), frame_(frame) {}

  absl::Status operator()(TypedRef ref) const {
    RETURN_IF_ERROR(ref.CopyToSlot(slot_, frame_)) << kErrorContext;
    return absl::OkStatus();
  }

  template <class T>
  absl::Status operator()(const T& value) const {
    ASSIGN_OR_RETURN(auto slot, slot_.ToSlot<T>(), _ << kErrorContext);
    frame_.Set(slot, value);
    return absl::OkStatus();
  }

 private:
  static constexpr absl::string_view kErrorContext =
      "type provided in WildcardInputLoader construction doesn't match the one "
      "provided in WildcardInputLoaderCallback";
  TypedSlot slot_;
  FramePtr frame_;
};

// Loader for inputs with names provided at construction time.
// Useful for Input's like std::map<std::string, T>, where key is a part of
// the feature name.
template <class Input>
class WildcardInputLoader final : public InputLoader<Input> {
 public:
  // Accessor function calling callback exactly once with a single argument
  // (TypedRef or specific type) for ability to return different types.
  using CallbackAccessorFn =
      std::function<absl::Status(const Input& input, const std::string& key,
                                 WildcardInputLoaderCallback callback)>;

  // Creates WildcardInputLoader with string keys for feature names.
  //
  // Args:
  //  accessor: function StatusOr<OutT>(const Input&, const std::string&)
  //  name_format: used to convert lookup key to the input name.
  //
  // Returns:
  //   InputLoader with following properties:
  //      * GetOutputTypes keys are absl::StrFormat(name_format, key) for key
  //        from keys
  //      * GetOutputTypes values are all the same and equal to
  //        GetQType<WildcardAccessorResultType<OutT>>
  //      * Bind returns BoundInputLoader, which set to the corresponding slot
  //        accessor(Input, key)
  //
  // == Usage example ==
  // using Input = absl::flat_hash_map<std::string, int>;
  // auto accessor = [](const Input& input,
  //                    const std::string& key) -> absl::StatusOr<int> {
  //   auto it = input.find(key);
  //   return it == input.end() ? -1 : it->second;
  // };
  // ASSIGN_OR_RETURN(
  //     InputLoaderPtr<Input> input_loader,
  //     WildcardInputLoader<Input>::Build(
  //         accessor, {"a", "b"}, absl::ParsedFormat<'s'>("from_map_%s")));
  // FrameLayout::Builder layout_builder;
  // auto a_slot = layout_builder.AddSlot<int>();
  // auto b_slot = layout_builder.AddSlot<int>();
  // ASSIGN_OR_RETURN(
  //     BoundInputLoader<Input> bound_input_loader,
  //     input_loader->Bind({
  //         {"from_map_a", TypedSlot::FromSlot(a_slot)},
  //         {"from_map_b", TypedSlot::FromSlot(b_slot)},
  //     }));
  //
  template <class AccessFn>
  static absl::StatusOr<InputLoaderPtr<Input>> Build(
      AccessFn accessor,
      absl::ParsedFormat<'s'> name_format = absl::ParsedFormat<'s'>("%s")) {
    std::string name_suggestion = absl::StrFormat(name_format, "*");
    return BuildImpl(std::move(accessor),
                     input_loader_impl::MakeNameToKeyFn(name_format),
                     {std::move(name_suggestion)});
  }

  // Creates WildcardInputLoader with string keys for feature names.
  //
  // Args:
  //  accessor: function StatusOr<OutT>(const Input&, const std::string&)
  //  name2key: converts input name (absl::string_view) into a lookup key.
  //  name_suggestions: optional list of available input names / patterns to
  //    make the error messages more actionable.
  //
  // Returns:
  //   InputLoader with following properties:
  //      * GetOutputTypes keys are `key2name(key)` for `key` from `keys`
  //      * GetOutputTypes values are all the same and equal to
  //        GetQType<WildcardAccessorResultType<OutT>>
  //      * Bind returns BoundInputLoader, which set to the corresponding slot
  //        accessor(Input, key)
  //
  template <class AccessFn, class Name2KeyFn>
  static absl::StatusOr<InputLoaderPtr<Input>> Build(
      AccessFn accessor, Name2KeyFn name2key,
      std::vector<std::string> name_suggestions = {}) {
    return BuildImpl(std::move(accessor), name2key,
                     std::move(name_suggestions));
  }

  // Creates WildcardInputLoader from a functor calling
  // WildcardInputLoaderCallback with a specific type or TypedRef.
  // This gives an ability to create wildcard InputLoader with different QTypes.
  // Args:
  //  accessor: function
  //      Status(const Input&, const std::string&, WildcardInputLoaderCallback)
  //    that for the specified key calls calback with a value of type specified
  //    in key_types.
  //  key_types: map from the key to the expected QType.
  //  name_format: used to convert lookup key into the input name.
  //
  // Usage example:
  // using Input = std::pair<int, float>;
  // auto accessor = [](const Input& input,
  //                    const std::string& key,
  //                    WildcardInputLoaderCallback callback) -> absl::Status {
  //   if (key == "x") {
  //     return callback(input.first);
  //   }
  //   if (key == "y") {
  //     return callback(input.second);
  //   }
  //   return FailedPreconditionError(
  //       absl::StrFormat("key `%s` is not found", key));
  // };
  // ASSIGN_OR_RETURN(
  //     auto input_loader,
  //     WildcardInputLoader<Input>::BuildFromCallbackAccessorFn(
  //         accessor, {{"x", GetQType<int32_t>()}, {"y", GetQType<float>()}}));
  //
  static absl::StatusOr<InputLoaderPtr<Input>> BuildFromCallbackAccessorFn(
      CallbackAccessorFn accessor,
      absl::flat_hash_map<std::string, QTypePtr> key_types,
      absl::ParsedFormat<'s'> name_format = absl::ParsedFormat<'s'>("%s")) {
    std::vector<std::string> name_suggestions;
    name_suggestions.reserve(key_types.size());
    for (const auto& [key, _] : key_types) {
      name_suggestions.emplace_back(absl::StrFormat(name_format, key));
    }
    std::sort(name_suggestions.begin(), name_suggestions.end());
    return BuildFromCallbackImpl(
        std::move(accessor), input_loader_impl::MakeNameToKeyFn(name_format),
        [key_types = std::move(key_types)](absl::string_view name) {
          auto it = key_types.find(name);
          return it != key_types.end() ? it->second : nullptr;
        },
        std::move(name_suggestions));
  }

  // Creates WildcardInputLoader from a functor calling
  // WildcardInputLoaderCallback with a specific type or TypedRef.
  // This gives an ability to create wildcard InputLoader with different QTypes.
  //
  // Args:
  //  accessor: is the function Status(
  //      const Input&, const std::string&, WildcardInputLoaderCallback)
  //    that for the specified key calls calback with a value of type specified
  //    in key_types.
  //  name2key: converts input name (absl::string_view) into a lookup key (not
  //    necessary a string).
  //  key2type: returns type for each lookup key.
  //  name_suggestions: optional list of available input names / patterns to
  //    make the error messages more actionable.
  //
  template <typename Name2KeyFn, typename Key2TypeFn>
  static absl::StatusOr<InputLoaderPtr<Input>> BuildFromCallbackAccessorFn(
      CallbackAccessorFn accessor, Name2KeyFn name2key, Key2TypeFn key2type,
      std::vector<std::string> name_suggestions = {}) {
    return BuildFromCallbackImpl(std::move(accessor), std::move(name2key),
                                 std::move(key2type),
                                 std::move(name_suggestions));
  }

  absl::Nullable<const QType*> GetQTypeOf(absl::string_view name) const final {
    return get_qtype_of_fn_(name);
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    return name_suggestions_;
  }

 private:
  explicit WildcardInputLoader(
      std::function<absl::StatusOr<BoundInputLoader<Input>>(
          const absl::flat_hash_map<std::string, TypedSlot>&)>
          bind_fn,
      std::function<absl::Nullable<const QType*>(absl::string_view)>
          get_output_type_fn,
      std::vector<std::string> name_suggestions)
      : bind_fn_(std::move(bind_fn)),
        get_qtype_of_fn_(get_output_type_fn),
        name_suggestions_(std::move(name_suggestions)) {}

  absl::StatusOr<BoundInputLoader<Input>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const final {
    return bind_fn_(output_slots);
  }

  template <typename AccessFn, typename Name2KeyFn>
  static absl::StatusOr<InputLoaderPtr<Input>> BuildImpl(
      AccessFn accessor_fn, Name2KeyFn name2key,
      std::vector<std::string> name_suggestions) {
    using KeyT = meta::strip_template_t<
        std::optional,
        std::decay_t<decltype(name2key(std::declval<std::string>()))>>;
    using OutT = WildcardAccessorResultType<AccessFn, Input, KeyT>;
    auto get_output_qtype_fn = [name2key](absl::string_view name) {
      return name2key(name).has_value() ? GetQType<OutT>() : nullptr;
    };
    // Not using make_unique to avoid binary size blowup.
    return InputLoaderPtr<Input>(
        static_cast<InputLoader<Input>*>(new WildcardInputLoader(
            CreateBindFn<AccessFn, KeyT, Name2KeyFn>(std::move(accessor_fn),
                                                     std::move(name2key)),
            std::move(get_output_qtype_fn), std::move(name_suggestions))));
  }

  template <typename Key2TypeFn, typename Name2KeyFn>
  static absl::StatusOr<InputLoaderPtr<Input>> BuildFromCallbackImpl(
      CallbackAccessorFn accessor_fn, Name2KeyFn name2key, Key2TypeFn key2type,
      std::vector<std::string> name_suggestions) {
    auto get_output_qtype_fn = [name2key, key2type = std::move(key2type)](
                                   absl::string_view name) -> const QType* {
      auto key = name2key(name);
      return key.has_value() ? key2type(*key) : nullptr;
    };
    // Not using make_unique to avoid binary size blowup.
    return InputLoaderPtr<Input>(
        static_cast<InputLoader<Input>*>(new WildcardInputLoader(
            CreateBindFnFromCallbackAccessorFn(std::move(accessor_fn),
                                               std::move(name2key)),
            get_output_qtype_fn, std::move(name_suggestions))));
  }

  // Implement Bind as method, to avoid more template parameters in a class.
  // The lambda implicitly use AccessFn and Key template arguments.
  // Moving this code to the WildcardInputLoader::Bind will require adding
  // two more template parameters to the WildcardInputLoader class.
  // This will also require to have BuildWildcardInputLoader free function to
  // deduct these template parameters.
  template <typename AccessFn, typename Key, typename Name2KeyFn>
  static auto CreateBindFn(AccessFn accessor_fn, Name2KeyFn name2key) {
    return [accessor(std::move(accessor_fn)), name2key(std::move(name2key))](
               const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
               -> absl::StatusOr<BoundInputLoader<Input>> {
      using OutT = WildcardAccessorResultType<AccessFn, Input, Key>;
      std::vector<std::pair<Key, FrameLayout::Slot<OutT>>> keyed_slots;
      for (const auto& [slot_name, typed_slot] : output_slots) {
        auto key = name2key(slot_name);
        if (!key.has_value()) {
          continue;
        }
        ASSIGN_OR_RETURN(auto slot, typed_slot.template ToSlot<OutT>());
        keyed_slots.emplace_back(*std::move(key), slot);
      }
      // Make the order of accessor executions deterministic.
      std::sort(keyed_slots.begin(), keyed_slots.end(),
                [](const auto& a, const auto& b) { return a.first < b.first; });
      return BoundInputLoader<Input>(
          [keyed_slots_(keyed_slots), accessor_(accessor)](
              const Input& input, FramePtr frame,
              RawBufferFactory* factory) -> absl::Status {
            for (const auto& [key, slot] : keyed_slots_) {
              RETURN_IF_ERROR(InvokeWildcardInputLoaderAccessor(
                  accessor_, input, key, factory, frame.GetMutable(slot)));
            }
            return absl::OkStatus();
          });
    };
  }

  template <typename Name2KeyFn>
  static auto CreateBindFnFromCallbackAccessorFn(CallbackAccessorFn accessor_fn,
                                                 Name2KeyFn name2key) {
    return [accessor(std::move(accessor_fn)), name2key(std::move(name2key))](
               const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
               -> absl::StatusOr<BoundInputLoader<Input>> {
      std::vector<std::pair<std::string, TypedSlot>> keyed_slots;
      for (const auto& [slot_name, typed_slot] : output_slots) {
        if (auto key = name2key(slot_name); key.has_value()) {
          keyed_slots.emplace_back(*std::move(key), typed_slot);
        }
      }
      // Make the order of accessor executions deterministic.
      std::sort(keyed_slots.begin(), keyed_slots.end(),
                [](const auto& a, const auto& b) { return a.first < b.first; });
      return BoundInputLoader<Input>(  //
          [keyed_slots_(std::move(keyed_slots)),
           accessor_(std::move(accessor))](const Input& input, FramePtr frame,
                                           RawBufferFactory*) -> absl::Status {
            for (const auto& [key, slot] : keyed_slots_) {
              RETURN_IF_ERROR(accessor_(
                  input, key, WildcardInputLoaderCallback(slot, frame)))
                  << absl::StrFormat("key: `%s`", key);
            }
            return absl::OkStatus();
          });
    };
  }

  std::function<absl::StatusOr<BoundInputLoader<Input>>(
      const absl::flat_hash_map<std::string, TypedSlot>&)>
      bind_fn_;
  std::function<absl::Nullable<const QType*>(absl::string_view)>
      get_qtype_of_fn_;
  std::vector<std::string> name_suggestions_;
};

}  // namespace arolla

#endif  // AROLLA_IO_WILDCARD_INPUT_LOADER_H_
