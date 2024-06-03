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
#ifndef AROLLA_IO_INPUT_LOADER_H_
#define AROLLA_IO_INPUT_LOADER_H_

#include <algorithm>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/macros.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Bound interface for loading user data into EvaluationContext.
template <class Input>
class BoundInputLoader {
 public:
  using FnType = absl::AnyInvocable<absl::Status(const Input&, FramePtr,
                                                 RawBufferFactory*) const>;
  explicit BoundInputLoader(FnType&& fn) : fn_(std::forward<FnType>(fn)) {}

  absl::Status operator()(
      const Input& input, FramePtr frame,
      RawBufferFactory* factory = GetHeapBufferFactory()) const {
    return fn_(input, frame, factory);
  }

 private:
  FnType fn_;
};

// Non-templated base class for InputLoader<T>.
class InputLoaderBase {
 public:
  virtual ~InputLoaderBase() {}

  // Returns the type of the given input, or nullptr if the input is not
  // supported.
  virtual absl::Nullable<const QType*> GetQTypeOf(
      absl::string_view name) const = 0;

  // Returns a list of names or name patterns of the supported inputs. Used only
  // for error messages.
  virtual std::vector<std::string> SuggestAvailableNames() const = 0;

 protected:
  // Validates that all the names in `slots` are supported by the input loader
  // and their QTypes match.
  absl::Status ValidateSlotTypes(
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const;
  // Extracts the slots which the input loader supports out of `slots` map and
  // returns as a separate map.
  absl::flat_hash_map<std::string, TypedSlot> ExtractSupportedSlots(
      absl::Nonnull<absl::flat_hash_map<std::string, TypedSlot>*> slots) const;
};

// Loader interface for loading user data into a memory frame.
template <class T>
class InputLoader : public InputLoaderBase {
 public:
  using Input = T;

  // Binds InputLoader to the specific slots.
  //
  // Returned BoundInputLoader *must* initialize all specified `slots`
  // (potentially with missing values). The base class implementation validates
  // that the types of `slots` match `GetQTypeOf` results.
  //
  // Note a possible performance overhead for not populated keys.
  //
  absl::StatusOr<BoundInputLoader<Input>> Bind(
      // The slots corresponding to this Loader's outputs.
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const {
    RETURN_IF_ERROR(ValidateSlotTypes(slots));
    if (slots.empty()) {
      return BoundInputLoader<Input>(
          [](const Input&, FramePtr, RawBufferFactory*) {
            return absl::OkStatus();
          });
    }
    return BindImpl(slots);
  }

  // Binds InputLoader to the subset of output slots listed in GetOutputTypes.
  // All used slots will be removed from the provided map.
  absl::StatusOr<BoundInputLoader<Input>> PartialBind(
      absl::Nonnull<absl::flat_hash_map<std::string, TypedSlot>*> slots) const {
    return Bind(ExtractSupportedSlots(slots));
  }

 protected:
  // Implementation of Bind, which can assume that
  // 1. output_slots are not empty
  // 2. for each (name, slot) in output_slots `GetQTypeOf(name)` is not null and
  // is equal to `slot->GetType()`.
  virtual absl::StatusOr<BoundInputLoader<Input>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const = 0;
};

template <typename T>
using InputLoaderPtr = std::unique_ptr<InputLoader<T>>;

// A helper to construct type erased QType getter for an input loader. The
// functor will not own the input loader.
inline auto QTypeGetter(const InputLoaderBase& input_loader) {
  return [&input_loader](absl::string_view name) {
    return input_loader.GetQTypeOf(name);
  };
}

// Looks up input types in the input loader. Returns an error if any of the
// requested inputs is missing.
absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>> GetInputLoaderQTypes(
    const InputLoaderBase& input_loader, absl::Span<const std::string> names);

// A helper class to simplify InputLoader implementation if all the supported
// names / types are known during construction.
template <class T>
class StaticInputLoader : public InputLoader<T> {
 protected:
  // Constructs StaticInputLoader for the given <name, type> pairs. The original
  // order of the pairs will be preserved and available through types_in_order()
  // method.
  StaticInputLoader(
      std::initializer_list<std::pair<std::string, QTypePtr>> types_in_order)
      : StaticInputLoader(std::vector(types_in_order)) {}

  // Constructs StaticInputLoader for the given <name, type> pairs. The original
  // order of the pairs will be preserved and available through types_in_order()
  // method.
  explicit StaticInputLoader(
      std::vector<std::pair<std::string, QTypePtr>> types_in_order)
      : types_in_order_(std::move(types_in_order)),
        types_(types_in_order_.begin(), types_in_order_.end()) {}

  // Constructs StaticInputLoader for the given <name, type> pairs. The pairs
  // will be sorted by name and accessible via types_in_order() method.
  explicit StaticInputLoader(absl::flat_hash_map<std::string, QTypePtr> types)
      : types_in_order_(types.begin(), types.end()), types_(std::move(types)) {
    std::sort(types_in_order_.begin(), types_in_order_.end());
  }

  // Return all available types in the order they were specified.
  absl::Span<const std::pair<std::string, QTypePtr>> types_in_order() const {
    return types_in_order_;
  }

 public:
  absl::Nullable<const QType*> GetQTypeOf(absl::string_view name) const final {
    auto it = types_.find(name);
    return it != types_.end() ? it->second : nullptr;
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    std::vector<std::string> names;
    names.reserve(types_.size());
    for (const auto& [name, _] : types_in_order_) {
      names.emplace_back(name);
    }
    return names;
  }

 private:
  std::vector<std::pair<std::string, QTypePtr>> types_in_order_;
  absl::flat_hash_map<std::string, QTypePtr> types_;
};

namespace input_loader_impl {

// A not owning wrapper around InputLoader: use it in case if you guarantee that
// the wrapped InputLoader will outlive the wrapper.
template <typename T>
class NotOwningInputLoader final : public InputLoader<T> {
 public:
  explicit NotOwningInputLoader(const InputLoader<T>* input_loader)
      : input_loader_(input_loader) {}

  absl::Nullable<const QType*> GetQTypeOf(absl::string_view name) const final {
    return input_loader_->GetQTypeOf(name);
  }
  std::vector<std::string> SuggestAvailableNames() const {
    return input_loader_->SuggestAvailableNames();
  }

 private:
  absl::StatusOr<BoundInputLoader<T>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const final {
    return input_loader_->Bind(output_slots);
  }

  const InputLoader<T>* input_loader_;
};

}  // namespace input_loader_impl

// Creates a not owning wrapper around InputLoader: use it in case if you
// guarantee that the wrapped InputLoader will outlive the wrapper.
template <typename T>
InputLoaderPtr<T> MakeNotOwningInputLoader(const InputLoader<T>* input_loader) {
  return std::unique_ptr<InputLoader<T>>(
      new input_loader_impl::NotOwningInputLoader<T>(input_loader));
}

namespace input_loader_impl {

// A wrapper around InputLoader that holds shared ownership on the wrapped one.
template <typename T>
class SharedOwningInputLoader final : public InputLoader<T> {
 public:
  explicit SharedOwningInputLoader(
      std::shared_ptr<const InputLoader<T>> input_loader)
      : input_loader_(std::move(input_loader)) {}

  absl::Nullable<const QType*> GetQTypeOf(absl::string_view name) const final {
    return input_loader_->GetQTypeOf(name);
  }
  std::vector<std::string> SuggestAvailableNames() const {
    return input_loader_->SuggestAvailableNames();
  }

 private:
  absl::StatusOr<BoundInputLoader<T>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const final {
    return input_loader_->Bind(output_slots);
  }

  std::shared_ptr<const InputLoader<T>> input_loader_;
};

}  // namespace input_loader_impl

// Creates a not owning wrapper around InputLoader: use it in case if you
// guarantee that the wrapped InputLoader will outlive the wrapper.
template <typename T>
InputLoaderPtr<T> MakeSharedOwningInputLoader(
    std::shared_ptr<const InputLoader<T>> input_loader) {
  return std::unique_ptr<InputLoader<T>>(
      new input_loader_impl::SharedOwningInputLoader<T>(input_loader));
}

namespace input_loader_impl {

// A wrapper around InputLoader that supports only names for which filter_fn
// returned true.
template <typename T>
class FilteringInputLoader final : public InputLoader<T> {
 public:
  explicit FilteringInputLoader(
      std::unique_ptr<InputLoader<T>> input_loader,
      std::function<bool(absl::string_view)> filter_fn)
      : input_loader_(std::move(input_loader)),
        filter_fn_(std::move(filter_fn)) {}

  absl::Nullable<const QType*> GetQTypeOf(absl::string_view name) const final {
    if (filter_fn_(name)) {
      return input_loader_->GetQTypeOf(name);
    }
    return nullptr;
  }

  std::vector<std::string> SuggestAvailableNames() const {
    std::vector<std::string> suggested = input_loader_->SuggestAvailableNames();
    suggested.erase(std::remove_if(suggested.begin(), suggested.end(),
                                   std::not_fn(filter_fn_)),
                    suggested.end());
    return suggested;
  }

 private:
  absl::StatusOr<BoundInputLoader<T>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const final {
    // InputLoader::Bind takes care about filtration.
    return input_loader_->Bind(output_slots);
  }

  std::unique_ptr<InputLoader<T>> input_loader_;
  std::function<bool(absl::string_view)> filter_fn_;
};

}  // namespace input_loader_impl

// Creates an InputLoader that supports only that names from the original
// input_loader for which filter_fn returns true.
template <typename T>
std::unique_ptr<InputLoader<T>> MakeFilteringInputLoader(
    std::unique_ptr<InputLoader<T>> input_loader,
    std::function<bool(absl::string_view)> filter_fn) {
  return std::unique_ptr<InputLoader<T>>(
      new input_loader_impl::FilteringInputLoader<T>(std::move(input_loader),
                                                     std::move(filter_fn)));
}

// Creates an InputLoader that supports only that names from the original
// input_loader that are mentioned in allowed_names.
template <typename T>
std::unique_ptr<InputLoader<T>> MakeFilteringInputLoader(
    std::unique_ptr<InputLoader<T>> input_loader,
    absl::Span<const std::string> allowed_names) {
  return MakeFilteringInputLoader(
      std::move(input_loader),
      [allowed_names = absl::flat_hash_set<std::string>(allowed_names.begin(),
                                                        allowed_names.end())](
          absl::string_view input) { return allowed_names.contains(input); });
}

using OutputTypesSpan = absl::Span<const std::pair<std::string, QTypePtr>>;

// Returns an error iff names are duplicated.
absl::Status ValidateDuplicatedNames(OutputTypesSpan output_types_in_order);

// Bind list of InputLoader's partially in the given order.
// In case several InputLoader's expect the same key, the first will be bound.
// BoundInputLoader's with no bound slots are not included.
// Returns error if not all slots were bound.
template <class Input>
absl::StatusOr<std::vector<BoundInputLoader<Input>>> BindInputLoaderList(
    absl::Span<const InputLoaderPtr<Input>> loaders,
    const absl::flat_hash_map<std::string, TypedSlot>& output_slots) {
  std::vector<BoundInputLoader<Input>> bound_loaders;
  bound_loaders.reserve(loaders.size());
  auto partial_output_slots = output_slots;
  for (const auto& loader : loaders) {
    size_t slot_count = partial_output_slots.size();
    ASSIGN_OR_RETURN(auto bound_loader,
                     loader->PartialBind(&partial_output_slots));
    // Do not add empty loader to save a virtual call.
    if (slot_count != partial_output_slots.size()) {
      bound_loaders.push_back(std::move(bound_loader));
    }
  }
  if (!partial_output_slots.empty()) {
    return absl::FailedPreconditionError("not all slots were bound");
  }
  return bound_loaders;
}

// Input loader chaining several input loaders of the same type.
// This class is useful to simplify usage of several InputLoader's, which
// defined separately for some reasons. E.g., using CreateDelegatingInputLoader.
template <class Input>
class ChainInputLoader final : public InputLoader<Input> {
 public:
  // Creates ChainInputLoader, returns not OK on duplicated names.
  template <class... Loaders>
  static absl::StatusOr<InputLoaderPtr<Input>> Build(
      std::unique_ptr<Loaders>... loaders) {
    std::vector<InputLoaderPtr<Input>> loaders_vec;
    (loaders_vec.push_back(std::move(loaders)), ...);
    return Build(std::move(loaders_vec));
  }
  static absl::StatusOr<InputLoaderPtr<Input>> Build(
      std::vector<InputLoaderPtr<Input>> loaders) {
    // Not using make_shared to avoid binary size blowup.
    return InputLoaderPtr<Input>(static_cast<InputLoader<Input>*>(
        new ChainInputLoader(std::move(loaders))));
  }

  // Function to invoke bound loaders that can be customized externally.
  using InvokeBoundLoadersFn = std::function<absl::Status(
      absl::Span<const BoundInputLoader<Input>>, const Input& input,
      FramePtr frame, RawBufferFactory* factory)>;

  // Invokes all bound loaders sequentially.
  // It is a default implementation for InvokeBoundLoadersFn.
  static absl::Status InvokeBoundLoaders(
      absl::Span<const BoundInputLoader<Input>> bound_loaders,
      const Input& input, FramePtr frame, RawBufferFactory* factory) {
    for (const auto& loader : bound_loaders) {
      RETURN_IF_ERROR(loader(input, frame, factory));
    }
    return absl::OkStatus();
  }

  // Creates ChainInputLoader with customizable `invoke_bound_loaders` strategy.
  // This may run loaders in parallel or perform additional logging.
  // NOTE: as an optimization, this function is not going to be used
  // if 0 or 1 loaders will be required.
  static absl::StatusOr<InputLoaderPtr<Input>> Build(
      std::vector<InputLoaderPtr<Input>> loaders,
      InvokeBoundLoadersFn invoke_bound_loaders_fn) {
    // Not using make_shared to avoid binary size blowup.
    return InputLoaderPtr<Input>(
        static_cast<InputLoader<Input>*>(new ChainInputLoader(
            std::move(loaders), std::move(invoke_bound_loaders_fn))));
  }

  absl::Nullable<const QType*> GetQTypeOf(absl::string_view name) const final {
    for (const auto& loader : loaders_) {
      if (auto qtype = loader->GetQTypeOf(name); qtype != nullptr) {
        return qtype;
      }
    }
    return nullptr;
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    std::vector<std::string> names;
    for (const auto& loader : loaders_) {
      auto available = loader->SuggestAvailableNames();
      names.insert(names.end(), available.begin(), available.end());
    }
    return names;
  }

 private:
  explicit ChainInputLoader(std::vector<InputLoaderPtr<Input>> loaders)
      : loaders_(std::move(loaders)) {}

  explicit ChainInputLoader(std::vector<InputLoaderPtr<Input>> loaders,
                            InvokeBoundLoadersFn invoke_bound_loaders_fn)
      : loaders_(std::move(loaders)),
        invoke_bound_loaders_fn_(std::move(invoke_bound_loaders_fn)) {}

  absl::StatusOr<BoundInputLoader<Input>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const final {
    ASSIGN_OR_RETURN(std::vector<BoundInputLoader<Input>> bound_loaders,
                     BindInputLoaderList<Input>(loaders_, output_slots));
    if (bound_loaders.empty()) {
      return absl::InternalError(
          "no slots were bound, must be processed in Bind");
    }
    // Avoid indirection in case only one loader is bound.
    if (bound_loaders.size() == 1) {
      return std::move(bound_loaders[0]);
    }
    if (invoke_bound_loaders_fn_) {
      return BoundInputLoader<Input>(
          absl::bind_front(invoke_bound_loaders_fn_, std::move(bound_loaders)));
    }
    return BoundInputLoader<Input>(
        [bound_loaders(std::move(bound_loaders))](
            const Input& input, FramePtr frame,
            RawBufferFactory* factory) -> absl::Status {
          return ChainInputLoader<Input>::InvokeBoundLoaders(
              bound_loaders, input, frame, factory);
        });
  }

  std::vector<InputLoaderPtr<Input>> loaders_;
  InvokeBoundLoadersFn invoke_bound_loaders_fn_ = nullptr;
};

}  // namespace arolla

#endif  // AROLLA_IO_INPUT_LOADER_H_
