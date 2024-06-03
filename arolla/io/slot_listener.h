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
#ifndef AROLLA_IO_SLOT_LISTENER_H_
#define AROLLA_IO_SLOT_LISTENER_H_

#include <algorithm>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/die_if_null.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Function bound to the concrete slots, copying data from the frame
// to the specified output.
template <class OutputT>
using BoundSlotListener =
    absl::AnyInvocable<absl::Status(ConstFramePtr, OutputT*) const>;

// Non-templated base class for SlotListener<T>.
class SlotListenerBase {
 public:
  virtual ~SlotListenerBase() = default;

  // Returns the type of the given output, or nullptr if the output is not
  // supported. Optional argument `desired_qtype` allows SlotListener to support
  // multiple QTypes for the same name (e.g. by applying casting when copying
  // data) depending on what is requested.
  virtual absl::Nullable<const QType*> GetQTypeOf(
      absl::string_view name,
      absl::Nullable<const QType*> desired_qtype) const = 0;
  absl::Nullable<const QType*> GetQTypeOf(absl::string_view name) const {
    return GetQTypeOf(name, nullptr);
  }

  // Returns a list of names or name patterns of the supported outputs. Used
  // only for error messages.
  virtual std::vector<std::string> SuggestAvailableNames() const = 0;

 protected:
  // Returns a subset of `slots` that are supported by the slot listener.
  absl::flat_hash_map<std::string, TypedSlot> FindSupportedSlots(
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const;

  // Validates that all the names in `slots` are supported by the slot listener
  // and their QTypes match.
  absl::Status ValidateSlotTypes(
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const;
};

// Interface for creating callback bound to the specific slots.
// Created callback could log information or store it to the Output.
template <class T>
class SlotListener : public SlotListenerBase {
 public:
  using Output = T;

  // Bind SlotListener to the specific slots.
  // Values for corresponding keys from GetTypes() and from slots
  // must have the same QType.
  // Keys not present in slots will not be listened. Note a possible
  // performance overhead for not populated keys.
  // If slots key is missed in GetTypes() error should be returned.
  absl::StatusOr<BoundSlotListener<Output>> Bind(
      // The slots corresponding to this Loader's outputs.
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const {
    RETURN_IF_ERROR(ValidateSlotTypes(slots));
    if (slots.empty()) {
      return BoundSlotListener<Output>(
          [](ConstFramePtr, Output*) { return absl::OkStatus(); });
    }
    return BindImpl(slots);
  }

  // Bind SlotListener to the subset of slots listed in GetTypes.
  // Returns std::nullopt if the used subset is empty.
  absl::StatusOr<std::optional<BoundSlotListener<Output>>> PartialBind(
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const {
    absl::flat_hash_map<std::string, TypedSlot> partial_slots =
        FindSupportedSlots(slots);
    if (partial_slots.empty()) {
      return std::nullopt;
    }
    return Bind(partial_slots);
  }

 protected:
  // Implementation of Bind, which can assume that
  // 1. `slots` are not empty
  // 2. there are no slots with names not present in `GetTypes`
  // 3. each name from `GetTypes` is either missing in slots
  //    or has correct type.
  virtual absl::StatusOr<BoundSlotListener<Output>> BindImpl(
      // The slots corresponding to this SlotListener's inputs.
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const = 0;
};

// A helper class to simplify InputLoader implementation if all the supported
// names / types are known during construction.
template <class T>
class StaticSlotListener : public SlotListener<T> {
 public:
  using Output = T;

  // Constructs StaticSlotListener for the given <name, type> pairs. The
  // original order of the pairs will be preserved and available through
  // types_in_order() method.
  StaticSlotListener(
      std::initializer_list<std::pair<std::string, QTypePtr>> types_in_order)
      : StaticSlotListener(std::vector(types_in_order)) {}

  // Constructs StaticSlotListener for the given <name, type> pairs. The
  // original order of the pairs will be preserved and available through
  // types_in_order() method.
  explicit StaticSlotListener(
      std::vector<std::pair<std::string, QTypePtr>> types_in_order)
      : types_in_order_(std::move(types_in_order)),
        types_(types_in_order_.begin(), types_in_order_.end()) {}

  // Constructs StaticSlotListener for the given <name, type> pairs. The pairs
  // will be sorted by name and accessible via types_in_order() method.
  explicit StaticSlotListener(absl::flat_hash_map<std::string, QTypePtr> types)
      : types_in_order_(types.begin(), types.end()), types_(std::move(types)) {
    std::sort(types_in_order_.begin(), types_in_order_.end());
  }

  absl::Nullable<const QType*> GetQTypeOf(
      absl::string_view name, absl::Nullable<const QType*>) const final {
    auto it = types_.find(name);
    return it != types_.end() ? it->second : nullptr;
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    std::vector<std::string> names;
    names.reserve(types_in_order_.size());
    for (const auto& [name, _] : types_in_order_) {
      names.emplace_back(name);
    }
    return names;
  }

  // Return all available types in the order they were specified.
  absl::Span<const std::pair<std::string, QTypePtr>> types_in_order() const {
    return types_in_order_;
  }

 private:
  std::vector<std::pair<std::string, QTypePtr>> types_in_order_;
  absl::flat_hash_map<std::string, QTypePtr> types_;
};

namespace slot_listener_impl {

// A not owning wrapper around SlotListener: use it in case if you guarantee
// that the wrapped SlotListener will outlive the wrapper.
template <typename T>
class NotOwningSlotListener final : public SlotListener<T> {
 public:
  explicit NotOwningSlotListener(const SlotListener<T>* slot_listener)
      : slot_listener_(ABSL_DIE_IF_NULL(slot_listener)) {}

  absl::Nullable<const QType*> GetQTypeOf(
      absl::string_view name,
      absl::Nullable<const QType*> desired_qtype) const final {
    return slot_listener_->GetQTypeOf(name, desired_qtype);
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    return slot_listener_->SuggestAvailableNames();
  }

 private:
  absl::StatusOr<BoundSlotListener<T>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const final {
    return slot_listener_->Bind(slots);
  }

  const SlotListener<T>* slot_listener_;
};

}  // namespace slot_listener_impl

// Creates a not owning wrapper around SlotListener: use it in case if you
// guarantee that the wrapped SlotListener will outlive the wrapper.
template <typename T>
std::unique_ptr<SlotListener<T>> MakeNotOwningSlotListener(
    const SlotListener<T>* slot_listener) {
  return std::unique_ptr<SlotListener<T>>(
      new slot_listener_impl::NotOwningSlotListener<T>(slot_listener));
}

namespace slot_listener_impl {

// Wapper around SlotListener that owns the wrapped one via a shared_ptr.
template <typename T>
class SharedOwningSlotListener final : public SlotListener<T> {
 public:
  explicit SharedOwningSlotListener(
      std::shared_ptr<const SlotListener<T>> slot_listener)
      : slot_listener_(std::move(ABSL_DIE_IF_NULL(slot_listener))) {}

  absl::Nullable<const QType*> GetQTypeOf(
      absl::string_view name,
      absl::Nullable<const QType*> desired_qtype) const final {
    return slot_listener_->GetQTypeOf(name, desired_qtype);
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    return slot_listener_->SuggestAvailableNames();
  }

 private:
  absl::StatusOr<BoundSlotListener<T>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const final {
    return slot_listener_->Bind(slots);
  }

  std::shared_ptr<const SlotListener<T>> slot_listener_;
};

}  // namespace slot_listener_impl

// Creates a wapper around SlotListener that owns the wrapped one via a
// shared_ptr.
template <typename T>
std::unique_ptr<SlotListener<T>> MakeSharedOwningSlotListener(
    std::shared_ptr<const SlotListener<T>> slot_listener) {
  return std::unique_ptr<SlotListener<T>>(
      new slot_listener_impl::SharedOwningSlotListener<T>(
          std::move(slot_listener)));
}

}  // namespace arolla

#endif  // AROLLA_IO_SLOT_LISTENER_H_
