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
#include "arolla/memory/frame.h"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <tuple>
#include <typeindex>
#include <typeinfo>
#include <utility>

#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//strings/str_cat.h"
#include "arolla/util/algorithms.h"
#include "arolla/util/memory.h"

namespace arolla {

std::type_index FrameLayout::FieldFactory::type_index() const { return type_; }

void FrameLayout::FieldFactory::Add(size_t offset) {
  offsets_.push_back(offset);
}

void FrameLayout::FieldFactory::AddDerived(
    const FieldFactory& derived_factory) {
  DCHECK(type_index() == derived_factory.type_index());
  for (size_t cur_offset : derived_factory.offsets_) {
    offsets_.push_back(cur_offset);
  }
}

FrameLayout::FieldFactory FrameLayout::FieldFactory::Derive(
    size_t offset) const {
  FieldFactory res = *this;
  for (size_t& cur_offset : res.offsets_) {
    cur_offset += offset;
  }
  return res;
}

void FrameLayout::FieldInitializers::AddOffsetToFactory(
    size_t offset, FieldFactory empty_factory) {
  auto it = type2factory.find(empty_factory.type_index());
  if (it == type2factory.end()) {
    bool inserted;
    std::tie(it, inserted) =
        type2factory.emplace(empty_factory.type_index(), factories.size());
    factories.push_back(std::move(empty_factory));
  }
  DCHECK_LT(it->second, factories.size());
  if (it->second < factories.size()) {  // defensive check
    factories[it->second].Add(offset);
  }
}

void FrameLayout::FieldInitializers::AddDerived(
    size_t offset, const FieldInitializers& derived_initializers) {
  for (const auto& [derived_tpe, derived_id] :
       derived_initializers.type2factory) {
    const auto& derived_factory = derived_initializers.factories[derived_id];
    if (auto it = type2factory.find(derived_tpe); it != type2factory.end()) {
      factories[it->second].AddDerived(derived_factory.Derive(offset));
    } else {
      type2factory.emplace(derived_tpe, factories.size());
      factories.push_back(derived_factory.Derive(offset));
    }
  }
}

// Allocates storage in the layout for holding a sub-frame.
FrameLayout::Slot<void> FrameLayout::Builder::AddSubFrame(
    const FrameLayout& subframe) {
  alloc_size_ = RoundUp(alloc_size_, subframe.AllocAlignment().value);
  size_t offset = alloc_size_;
  alloc_size_ += subframe.AllocSize();
  alloc_alignment_ =
      std::max(alloc_alignment_, subframe.AllocAlignment().value);
  initializers_.AddDerived(offset, subframe.initializers_);
#ifndef NDEBUG
  for (const auto& [field_offset, field_type] : subframe.registered_fields_) {
    registered_fields_.emplace(offset + field_offset, field_type);
  }
#endif
  return FrameLayout::Slot<void>(offset);
}

absl::Status FrameLayout::Builder::RegisterUnsafeSlot(
    size_t byte_offset, size_t byte_size, const std::type_info& type) {
  return RegisterSlot(byte_offset, byte_size, type);
}

absl::Status FrameLayout::Builder::RegisterSlot(size_t byte_offset,
                                                size_t byte_size,
                                                const std::type_info& type,
                                                bool allow_duplicates) {
  if (byte_offset == FrameLayout::Slot<float>::kUninitializedOffset) {
    return absl::FailedPreconditionError(
        "unable to register uninitialized slot");
  }
  if (byte_offset > alloc_size_ || byte_size > alloc_size_ - byte_offset) {
    return absl::FailedPreconditionError(absl::StrCat(
        "unable to register slot after the end of alloc, offset: ", byte_offset,
        ", size: ", byte_size, ", alloc size: ", alloc_size_));
  }
#ifndef NDEBUG
  if (!registered_fields_.emplace(byte_offset, std::type_index(type)).second &&
      !allow_duplicates) {
    return absl::FailedPreconditionError(absl::StrCat(
        "slot is already registered ", byte_offset, " ", type.name()));
  }
#endif
  return absl::OkStatus();
}

}  // namespace arolla
