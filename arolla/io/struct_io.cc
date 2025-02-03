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
#include "arolla/io/struct_io.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::struct_io_impl {

std::vector<std::string> SuggestAvailableNames(
    const absl::flat_hash_map<std::string, TypedSlot>& slots) {
  std::vector<std::string> names;
  names.reserve(slots.size());
  for (const auto& [name, _] : slots) {
    names.emplace_back(name);
  }
  return names;
}

absl::Status ValidateStructSlots(
    const absl::flat_hash_map<std::string, TypedSlot>& slots,
    size_t struct_size) {
  for (const auto& [name, slot] : slots) {
    if (slot.byte_offset() + slot.GetType()->type_layout().AllocSize() >
        struct_size) {
      return absl::InvalidArgumentError(
          absl::StrCat("slot '", name, "' is not within the struct"));
    }
  }
  return absl::OkStatus();
}

StructIO::StructIO(
    const absl::flat_hash_map<std::string, TypedSlot>& struct_slots,
    const absl::flat_hash_map<std::string, TypedSlot>& frame_slots) {
  QTypePtr b = GetQType<bool>();
  std::vector<QTypePtr> types32{GetQType<float>(), GetQType<int32_t>()};
  std::vector<QTypePtr> types64{GetQType<double>(), GetQType<int64_t>(),
                                GetQType<uint64_t>(), GetOptionalQType<float>(),
                                GetOptionalQType<int32_t>()};
  static_assert(sizeof(OptionalValue<float>) == 8);
  static_assert(sizeof(OptionalValue<int32_t>) == 8);
  static_assert(std::is_trivially_copyable_v<OptionalValue<float>>);
  static_assert(std::is_trivially_copyable_v<OptionalValue<int32_t>>);
  for (const auto& [name, frame_slot] : frame_slots) {
    QTypePtr t = frame_slot.GetType();
    size_t struct_offset = struct_slots.at(name).byte_offset();
    size_t frame_offset = frame_slot.byte_offset();
    if (t == b) {
      offsets_bool_.emplace_back(struct_offset, frame_offset);
    } else if (absl::c_find(types32, t) != types32.end()) {
      DCHECK_EQ(t->type_layout().AllocSize(), 4);
      offsets_32bits_.emplace_back(struct_offset, frame_offset);
    } else if (absl::c_find(types64, t) != types64.end()) {
      DCHECK_EQ(t->type_layout().AllocSize(), 8);
      offsets_64bits_.emplace_back(struct_offset, frame_offset);
    } else {
      offsets_other_[t].emplace_back(struct_offset, frame_offset);
    }
  }
  // Optimization; doesn't affect the behavior. The idea is that sorting should
  // reduce cache misses when accessing a huge struct.
  std::sort(offsets_bool_.begin(), offsets_bool_.end());
  std::sort(offsets_32bits_.begin(), offsets_32bits_.end());
  std::sort(offsets_64bits_.begin(), offsets_64bits_.end());
  for (auto& [_, v] : offsets_other_) {
    std::sort(v.begin(), v.end());
  }
  // NOTE: Idea for future optimization: find sequential 32bits
  // offsets (i.e. o1.first+4==o2.first && o1.second+4==o2.second) and move them
  // to offsets_64bits_.
  // NOTE: Consider to concatenate all offsets*_ into a single
  // vector and a starting offset for each size.
}

void StructIO::CopyStructToFrame(const void* struct_ptr, FramePtr frame) const {
  const char* src_base = reinterpret_cast<const char*>(struct_ptr);
  for (const auto& [src, dst] : offsets_bool_) {
    std::memcpy(frame.GetRawPointer(dst), src_base + src, sizeof(bool));
  }
  for (const auto& [src, dst] : offsets_32bits_) {
    std::memcpy(frame.GetRawPointer(dst), src_base + src, 4);
  }
  for (const auto& [src, dst] : offsets_64bits_) {
    std::memcpy(frame.GetRawPointer(dst), src_base + src, 8);
  }
  for (const auto& [t, offsets] : offsets_other_) {
    for (const auto& [src, dst] : offsets) {
      t->UnsafeCopy(src_base + src, frame.GetRawPointer(dst));
    }
  }
}

void StructIO::CopyFrameToStruct(ConstFramePtr frame, void* struct_ptr) const {
  char* dst_base = reinterpret_cast<char*>(struct_ptr);
  for (const auto& [dst, src] : offsets_bool_) {
    std::memcpy(dst_base + dst, frame.GetRawPointer(src), sizeof(bool));
  }
  for (const auto& [dst, src] : offsets_32bits_) {
    std::memcpy(dst_base + dst, frame.GetRawPointer(src), 4);
  }
  for (const auto& [dst, src] : offsets_64bits_) {
    std::memcpy(dst_base + dst, frame.GetRawPointer(src), 8);
  }
  for (const auto& [t, offsets] : offsets_other_) {
    for (const auto& [dst, src] : offsets) {
      t->UnsafeCopy(frame.GetRawPointer(src), dst_base + dst);
    }
  }
}

}  // namespace arolla::struct_io_impl
