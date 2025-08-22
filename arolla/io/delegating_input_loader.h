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
#ifndef AROLLA_IO_DELEGATING_INPUT_LOADER_H_
#define AROLLA_IO_DELEGATING_INPUT_LOADER_H_

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/synchronization/mutex.h"
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
  static absl::StatusOr<std::unique_ptr<InputLoader<Input>>> Build(
      std::unique_ptr<InputLoader<std::decay_t<DelegateInputFullType>>>
          delegate_loader,
      std::function<DelegateInputFullType(const Input&)> accessor,
      std::string name_prefix) {
    // Not using make_unique to avoid binary size blowup.
    return std::unique_ptr<InputLoader<Input>>(
        static_cast<InputLoader<Input>*>(new DelegatingInputLoader(
            std::move(delegate_loader), accessor, std::move(name_prefix))));
  }

  const QType* absl_nullable GetQTypeOf(absl::string_view name) const final {
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
      std::unique_ptr<InputLoader<DelegateInput>> delegate_loader,
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

  std::unique_ptr<InputLoader<DelegateInput>> delegate_loader_;
  std::function<DelegateInputFullType(const Input&)> accessor_;
  std::string name_prefix_;
};

template <class Input>
using DelegateLoaderFactoryRawFn =
    absl::StatusOr<std::unique_ptr<InputLoader<Input>>>(absl::string_view);

template <class Input>
using DelegateLoaderFactory = std::function<DelegateLoaderFactoryRawFn<Input>>;

// InputLoader delegating to many loaders created on demand via factory
// function.
template <class Input>
class DynamicDelegatingInputLoader final : public InputLoader<Input> {
 public:
  // Prefer CreateDynamicDelegatingInputLoader for type deduction.
  static absl::StatusOr<std::unique_ptr<InputLoader<Input>>> Build(
      DelegateLoaderFactory<Input> delegate_loader_factory,
      absl::AnyInvocable<std::optional<std::string>(absl::string_view) const>
          name2key,
      std::vector<std::string> suggest_available_names) {
    // Not using make_unique to avoid binary size blowup.
    return std::unique_ptr<InputLoader<Input>>(
        static_cast<InputLoader<Input>*>(new DynamicDelegatingInputLoader(
            std::move(delegate_loader_factory), std::move(name2key),
            std::move(suggest_available_names))));
  }

  const QType* absl_nullable GetQTypeOf(absl::string_view name) const final {
    std::optional<std::string> key = name2key_(name);
    if (!key.has_value()) {
      return nullptr;
    }
    auto delegate_loader = GetDelegateLoader(*key);
    if (!delegate_loader.ok()) {
      return nullptr;
    }
    return (*delegate_loader)->GetQTypeOf(name);
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    return suggest_available_names_;
  }

 private:
  DynamicDelegatingInputLoader(
      DelegateLoaderFactory<Input> delegate_loader_factory,
      absl::AnyInvocable<std::optional<std::string>(absl::string_view) const>
          name2key,
      std::vector<std::string> suggest_available_names)
      : delegate_loader_factory_(std::move(delegate_loader_factory)),
        name2key_(std::move(name2key)),
        suggest_available_names_(std::move(suggest_available_names)) {}

  absl::StatusOr<BoundInputLoader<Input>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const final {
    absl::flat_hash_map<std::string,
                        absl::flat_hash_map<std::string, TypedSlot>>
        delegate_output_slots;
    for (const auto& [name, slot] : output_slots) {
      std::optional<std::string> key = name2key_(name);
      if (!key.has_value()) {
        // Should be already validated in InputLoader::Bind().
        return absl::InternalError(
            absl::StrFormat("unexpected input name %s", name));
      }
      delegate_output_slots[*key].emplace(name, slot);
    }
    std::vector<BoundInputLoader<Input>> bound_loaders;
    for (const auto& [key, delegate_output_slots] : delegate_output_slots) {
      ASSIGN_OR_RETURN(auto* delegate_loader, GetDelegateLoader(key));
      ASSIGN_OR_RETURN(BoundInputLoader<Input> bound_delegate_loader,
                       delegate_loader->Bind(delegate_output_slots));
      bound_loaders.push_back(std::move(bound_delegate_loader));
    }
    if (bound_loaders.empty()) {
      return absl::InternalError(
          "no slots were bound, must be processed in Bind");
    }
    // Avoid indirection in case only one loader is bound.
    if (bound_loaders.size() == 1) {
      return std::move(bound_loaders[0]);
    }
    return BoundInputLoader<Input>(
        [bound_loaders(std::move(bound_loaders))](
            const Input& input, FramePtr frame,
            RawBufferFactory* factory) -> absl::Status {
          return ChainInputLoader<Input>::InvokeBoundLoaders(
              bound_loaders, input, frame, factory);
        });
  }

  absl::StatusOr<const InputLoader<Input>*> GetDelegateLoader(
      absl::string_view key) const {
    absl::MutexLock lock(&mutex_);
    auto it = delegate_loaders_.find(key);
    if (it != delegate_loaders_.end()) {
      return it->second.get();
    }
    ASSIGN_OR_RETURN(auto loader, delegate_loader_factory_(key));
    const auto* result = loader.get();
    delegate_loaders_[key] = std::move(loader);
    return result;
  }

  DelegateLoaderFactory<Input> delegate_loader_factory_;
  absl::AnyInvocable<std::optional<std::string>(absl::string_view) const>
      name2key_;
  std::vector<std::string> suggest_available_names_;

  mutable absl::Mutex mutex_;
  mutable absl::flat_hash_map<std::string, std::unique_ptr<InputLoader<Input>>>
      delegate_loaders_ ABSL_GUARDED_BY(mutex_);
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
absl::StatusOr<std::unique_ptr<InputLoader<Input>>> CreateDelegatingInputLoader(
    std::unique_ptr<InputLoader<DelegateInput>> delegate_loader,
    const Accessor& accessor, std::string name_prefix = "") {
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
absl::StatusOr<std::unique_ptr<InputLoader<Input>>>
CreateDelegatingInputLoaderWithCopyAllowed(
    std::unique_ptr<InputLoader<DelegateInput>> delegate_loader,
    const Accessor& accessor, std::string name_prefix = "") {
  static_assert(
      std::is_same_v<decltype(accessor(std::declval<Input>())), DelegateInput>,
      "Accessor must have `DelegateInput` result type for "
      "CreateDelegatingInputLoaderWithCopyAllowed. In case accessor returns "
      "`const DelegateInput&`, use CreateDelegatingInputLoader.");
  return delegating_input_loader_impl::DelegatingInputLoader<
      Input, DelegateInput>::Build(std::move(delegate_loader), accessor,
                                   std::move(name_prefix));
}

// Creates InputLoader that delegates to dynamically created loaders.
//
// Delegate loaders are created on demand via factory function, which is
// expected to return InputLoader<Input>.
// Name2Key function is used to convert input name to a key used to create
// a loader.
//
// Usage example:
//
// struct Input {
//   int a;
//   int b;
// };
//
// ASSIGN_OR_RETURN(
//     auto loader,
//     arolla::CreateDynamicDelegatingInputLoader<Input>(
//         [](absl::string_view key)
//             -> absl::StatusOr<std::unique_ptr<InputLoader<Input>>> {
//           if (key == "a") {
//             return CreateAccessorsInputLoader<Input>(
//                 "get(a)", [](const Input& s) { return s.a; });
//           }
//           if (key == "b") {
//             return CreateAccessorsInputLoader<Input>(
//                 "get(b)", [](const Input& s) { return s.b; });
//           }
//           return absl::NotFoundError(absl::StrCat(key, " not found"));
//         },
//         [](absl::string_view name) -> std::optional<std::string> {
//           if (absl::StartsWith(name, "get(a)")) {
//             return "a";
//           }
//           if (absl::StartsWith(name, "get(b)")) {
//             return "b";
//           }
//           return std::nullopt;
//         },
//         /*suggest_available_names=*/{"get(a)", "get(b)"}));
//
template <class Input>
absl::StatusOr<std::unique_ptr<InputLoader<Input>>>
CreateDynamicDelegatingInputLoader(
    delegating_input_loader_impl::DelegateLoaderFactory<Input>
        delegate_loader_factory,
    absl::AnyInvocable<std::optional<std::string>(absl::string_view) const>
        name2key,
    std::vector<std::string> suggest_available_names) {
  return delegating_input_loader_impl::DynamicDelegatingInputLoader<
      Input>::Build(std::move(delegate_loader_factory), std::move(name2key),
                    std::move(suggest_available_names));
}

}  // namespace arolla

#endif  // AROLLA_IO_DELEGATING_INPUT_LOADER_H_
