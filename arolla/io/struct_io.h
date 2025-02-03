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
#ifndef AROLLA_IO_STRUCT_IO_H_
#define AROLLA_IO_STRUCT_IO_H_

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace struct_io_impl {

class StructIO {
 public:
  // Keys of `frame_slots` must be a subset of `struct_slots` keys.
  explicit StructIO(
      const absl::flat_hash_map<std::string, TypedSlot>& struct_slots,
      const absl::flat_hash_map<std::string, TypedSlot>& frame_slots);

  void CopyStructToFrame(const void* struct_ptr, FramePtr frame) const;
  void CopyFrameToStruct(ConstFramePtr frame, void* struct_ptr) const;

 private:
  using Offsets = std::vector<std::pair<size_t, size_t>>;
  Offsets offsets_bool_;
  Offsets offsets_32bits_;
  Offsets offsets_64bits_;
  absl::flat_hash_map<QTypePtr, Offsets> offsets_other_;
};

std::vector<std::string> SuggestAvailableNames(
    const absl::flat_hash_map<std::string, TypedSlot>& slots);

absl::Status ValidateStructSlots(
    const absl::flat_hash_map<std::string, TypedSlot>& slots,
    size_t struct_size);

}  // namespace struct_io_impl

// InputLoader that copies values from a struct by offsets and doesn't require
// codegeneration. See struct_io_test.cc for a usage example.
template <typename T>
class StructInputLoader final : public InputLoader<T> {
 public:
  static absl::StatusOr<InputLoaderPtr<T>> Create(
      absl::flat_hash_map<std::string, TypedSlot> struct_slots) {
    RETURN_IF_ERROR(
        struct_io_impl::ValidateStructSlots(struct_slots, sizeof(T)));
    return InputLoaderPtr<T>(new StructInputLoader(std::move(struct_slots)));
  }

  absl::Nullable<const QType*> GetQTypeOf(absl::string_view name) const final {
    auto it = struct_slots_.find(name);
    return it != struct_slots_.end() ? it->second.GetType() : nullptr;
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    return struct_io_impl::SuggestAvailableNames(struct_slots_);
  }

 private:
  explicit StructInputLoader(
      absl::flat_hash_map<std::string, TypedSlot> struct_slots)
      : struct_slots_(std::move(struct_slots)) {}

  absl::StatusOr<BoundInputLoader<T>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const final {
    return BoundInputLoader<T>(
        [io = struct_io_impl::StructIO(struct_slots_, slots)](
            const T& input, FramePtr frame, RawBufferFactory*) -> absl::Status {
          io.CopyStructToFrame(&input, frame);
          return absl::OkStatus();
        });
  }

  absl::flat_hash_map<std::string, TypedSlot> struct_slots_;
};

// SlotListener that copies values to a struct by offsets and doesn't require
// codegeneration. See struct_io_test.cc for a usage example.
template <typename T>
class StructSlotListener final : public SlotListener<T> {
 public:
  static absl::StatusOr<std::unique_ptr<SlotListener<T>>> Create(
      absl::flat_hash_map<std::string, TypedSlot> struct_slots) {
    RETURN_IF_ERROR(
        struct_io_impl::ValidateStructSlots(struct_slots, sizeof(T)));
    return std::unique_ptr<SlotListener<T>>(
        new StructSlotListener(std::move(struct_slots)));
  }

  absl::Nullable<const QType*> GetQTypeOf(
      absl::string_view name, absl::Nullable<const QType*>) const final {
    auto it = struct_slots_.find(name);
    return it != struct_slots_.end() ? it->second.GetType() : nullptr;
  }

  std::vector<std::string> SuggestAvailableNames() const final {
    return struct_io_impl::SuggestAvailableNames(struct_slots_);
  }

 private:
  explicit StructSlotListener(
      absl::flat_hash_map<std::string, TypedSlot> struct_slots)
      : struct_slots_(std::move(struct_slots)) {}

  absl::StatusOr<BoundSlotListener<T>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& slots) const final {
    return BoundSlotListener<T>(
        [io = struct_io_impl::StructIO(struct_slots_, slots)](
            ConstFramePtr frame, T* output) -> absl::Status {
          io.CopyFrameToStruct(frame, output);
          return absl::OkStatus();
        });
  }

  absl::flat_hash_map<std::string, TypedSlot> struct_slots_;
};

}  // namespace arolla

#endif  // AROLLA_IO_STRUCT_IO_H_
