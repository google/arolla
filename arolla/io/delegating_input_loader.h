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
#ifndef AROLLA_IO_DELEGATING_INPUT_LOADER_H_
#define AROLLA_IO_DELEGATING_INPUT_LOADER_H_

#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "arolla/io/input_loader.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace delegating_input_loader_impl {

// InputLoader delegating to another loader with input transformation.
template <class Input, class DelegateInputFullType>
class DelegatingInputLoader final : public InputLoader<Input> {
  using DelegateInput = std::decay_t<DelegateInputFullType>;

 public:
  // Prefer CreateDelegatingInputLoader or
  // CreateDelegatingInputLoaderWithCopyAllowed for type deduction.
  // Constructs from delegate_loader and accessor that have to return
  // `DelegateInputFullType`.
  static absl::StatusOr<InputLoaderPtr<Input>> Build(
      InputLoaderPtr<std::decay_t<DelegateInputFullType>> delegate_loader,
      std::function<DelegateInputFullType(const Input&)> accessor,
      std::string name_prefix) {
    // Not using make_shared to avoid binary size blowup.
    return InputLoaderPtr<Input>(
        static_cast<InputLoader<Input>*>(new DelegatingInputLoader(
            std::move(delegate_loader), accessor, std::move(name_prefix))));
  }

  absl::Nullable<const QType*> GetQTypeOf(absl::string_view name) const final {
    if (!absl::ConsumePrefix(&name, name_prefix_)) {
      return nullptr;
    }
    return delegate_loader_->GetQTypeOf(name);
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    std::vector<std::string> names = delegate_loader_->SuggestAvailableNames();
    for (auto& name : names) {
      name = absl::StrCat(name_prefix_, name);
    }
    return names;
  }

 private:
  DelegatingInputLoader(
      InputLoaderPtr<DelegateInput> delegate_loader,
      std::function<DelegateInputFullType(const Input&)> accessor,
      std::string name_prefix)
      : delegate_loader_(std::move(delegate_loader)),
        accessor_(accessor),
        name_prefix_(name_prefix) {}

  absl::StatusOr<BoundInputLoader<Input>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const final {
    absl::flat_hash_map<std::string, TypedSlot> delegate_output_slots;
    for (const auto& [name, slot] : output_slots) {
      absl::string_view delegate_name = name;
      if (!absl::ConsumePrefix(&delegate_name, name_prefix_)) {
        // Should be already validated in InputLoader::Bind().
        return absl::InternalError(
            absl::StrFormat("unexpected input name %s", name));
      }
      delegate_output_slots.emplace(delegate_name, slot);
    }
    ASSIGN_OR_RETURN(BoundInputLoader<DelegateInput> bound_delegate_loader,
                     delegate_loader_->Bind(delegate_output_slots));
    return BoundInputLoader<Input>(
        [bound_delegate_loader(std::move(bound_delegate_loader)),
         accessor(accessor_)](const Input& input, FramePtr frame,
                              RawBufferFactory* factory) -> absl::Status {
          return bound_delegate_loader(accessor(input), frame, factory);
        });
  }

  InputLoaderPtr<DelegateInput> delegate_loader_;
  std::function<DelegateInputFullType(const Input&)> accessor_;
  std::string name_prefix_;
};

}  // namespace delegating_input_loader_impl

// Creates InputLoader delegating to another loader with input transformation.
//
// Input transformation is specified by
//    std::function<const DelegateInput&(const Input&)>
// Note that result type must be `const&` to avoid copying.
// In case of lambda, output type *must* be specified explicitly like in
// example below.
//
// Names can be transformed from delegate_loader by
//     std::function<std::string(absl::string_view)> name_modifier.
//
// Usage example:
//
// struct Input {
//   OtherInput* x;
// };
//
// // Creating InputLoader that delegate reading from OtherInput and
// // adding to all names "prefix_".
// ASSIGN_OR_RETURN(
//   auto loader,
//   arolla::CreateDelegatingInputLoader<Input>(
//       delegate_loader,
//       [](const Input& input) -> const OtherInput& { return *input.x; },
//       [](absl::string_view name) { return absl::StrCat("prefix_", name); }));
//
template <class Input, class DelegateInput, class Accessor>
absl::StatusOr<InputLoaderPtr<Input>> CreateDelegatingInputLoader(
    InputLoaderPtr<DelegateInput> delegate_loader, const Accessor& accessor,
    std::string name_prefix = "") {
  static_assert(
      std::is_same_v<decltype(accessor(std::declval<Input>())),
                     const DelegateInput&>,
      "Accessor must have `const DelegateInput&` result type. In case of "
      "lambda specify output type explicitly, this would save copying. "
      "In case you are creating temporary object and copy is necessary or "
      "intended, use CreateDelegatingInputLoaderWithCopyAllowed.");
  return delegating_input_loader_impl::DelegatingInputLoader<
      Input, const DelegateInput&>::Build(std::move(delegate_loader), accessor,
                                          std::move(name_prefix));
}

// Version of `CreateDelegatingInputLoader` with allowed copy.
// Useful in case temporary object is being created or copy is cheap
// (e.g., absl::Span).
// In order to avoid copy make return type `const DelegateInput&` and use
// CreateDelegatingInputLoader.
// Usage example:
//
// struct Input {
//   std::vector<OtherInput> x;
// };
//
// // Creating InputLoader that delegate reading from OtherInput and
// // adding to all names "prefix_".
// ASSIGN_OR_RETURN(
//   auto loader,
//   arolla::CreateDelegatingInputLoader<Input>(
//       delegate_loader,
//       [](const Input& input) -> absl::Span<const OtherInput> {
//           return input.x;
//       },
//       [](absl::string_view name) { return absl::StrCat("prefix_", name); }));
//
template <class Input, class DelegateInput, class Accessor>
absl::StatusOr<InputLoaderPtr<Input>>
CreateDelegatingInputLoaderWithCopyAllowed(
    InputLoaderPtr<DelegateInput> delegate_loader, const Accessor& accessor,
    std::string name_prefix = "") {
  static_assert(
      std::is_same_v<decltype(accessor(std::declval<Input>())), DelegateInput>,
      "Accessor must have `DelegateInput` result type for "
      "CreateDelegatingInputLoaderWithCopyAllowed. In case accessor returns "
      "`const DelegateInput&`, use CreateDelegatingInputLoader.");
  return delegating_input_loader_impl::DelegatingInputLoader<
      Input, DelegateInput>::Build(std::move(delegate_loader), accessor,
                                   std::move(name_prefix));
}

}  // namespace arolla

#endif  // AROLLA_IO_DELEGATING_INPUT_LOADER_H_
