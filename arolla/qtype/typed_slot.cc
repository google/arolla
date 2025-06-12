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
#include "arolla/qtype/typed_slot.h"

#include <algorithm>
#include <optional>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/demangle.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace {

std::string TypeMismatchError(absl::string_view name, QTypePtr expected_type,
                              QTypePtr actual_type) {
  return absl::StrFormat("%s{expected:%s, actual:%s}", name,
                         expected_type->name(), actual_type->name());
}

absl::Status SlotTypesError(std::vector<std::string> missed_slots,
                            std::vector<std::string> type_mismatch,
                            std::vector<std::string> unwanted_slots) {
  if (missed_slots.empty() && type_mismatch.empty() && unwanted_slots.empty()) {
    return absl::OkStatus();
  }

  std::string msg = "slots/types match errors:";

  if (!missed_slots.empty()) {
    std::sort(missed_slots.begin(), missed_slots.end());
    msg +=
        absl::StrFormat("missed slots: %s;", absl::StrJoin(missed_slots, ","));
  }
  if (!type_mismatch.empty()) {
    std::sort(type_mismatch.begin(), type_mismatch.end());
    msg += absl::StrFormat("slot types mismatch: %s;",
                           absl::StrJoin(type_mismatch, ","));
  }
  if (!unwanted_slots.empty()) {
    std::sort(unwanted_slots.begin(), unwanted_slots.end());
    msg += absl::StrFormat("unwanted slots: %s;",
                           absl::StrJoin(unwanted_slots, ","));
  }
  return absl::FailedPreconditionError(msg);
}

}  // namespace

std::vector<QTypePtr> SlotsToTypes(absl::Span<const TypedSlot> slots) {
  std::vector<QTypePtr> types;
  types.reserve(slots.size());
  for (const auto& slot : slots) {
    types.push_back(slot.GetType());
  }
  return types;
}

absl::Status TypedSlot::VerifyType(const std::type_info& tpe) const {
  if (GetType()->type_info() != tpe) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "slot type does not match C++ type: expected %s, got %s", TypeName(tpe),
        TypeName(GetType()->type_info())));
  }
  return absl::OkStatus();
}

absl::flat_hash_map<std::string, QTypePtr> SlotsToTypes(
    const absl::flat_hash_map<std::string, TypedSlot>& slots) {
  absl::flat_hash_map<std::string, QTypePtr> types;
  types.reserve(slots.size());
  for (const auto& kv : slots) {
    types[kv.first] = kv.second.GetType();
  }
  return types;
}

std::vector<TypedSlot> AddSlots(absl::Span<const QTypePtr> types,
                                FrameLayout::Builder* layout_builder) {
  std::vector<TypedSlot> slots;
  slots.reserve(types.size());
  for (const auto* type : types) {
    slots.push_back(AddSlot(type, layout_builder));
  }
  return slots;
}

std::vector<std::pair<std::string, TypedSlot>> AddNamedSlots(
    absl::Span<const std::pair<std::string, QTypePtr>> types,
    FrameLayout::Builder* layout_builder) {
  std::vector<std::pair<std::string, TypedSlot>> slots;
  slots.reserve(types.size());
  for (const auto& [name, type] : types) {
    slots.emplace_back(name, AddSlot(type, layout_builder));
  }
  return slots;
}

absl::flat_hash_map<std::string, TypedSlot> AddSlotsMap(
    const absl::flat_hash_map<std::string, const QType*>& types,
    FrameLayout::Builder* layout_builder) {
  absl::flat_hash_map<std::string, TypedSlot> slots;
  slots.reserve(types.size());
  for (const auto& name_type : types) {
    slots.insert({name_type.first, AddSlot(name_type.second, layout_builder)});
  }
  return slots;
}

absl::Status RegisterUnsafeSlots(absl::Span<const TypedSlot> slots,
                                 FrameLayout::Builder* layout_builder) {
  for (const auto& slot : slots) {
    RETURN_IF_ERROR(layout_builder->RegisterUnsafeSlot(
        slot.byte_offset(), slot.GetType()->type_layout().AllocSize(),
        slot.GetType()->type_info()));
  }
  return absl::OkStatus();
}

absl::Status RegisterUnsafeSlotsMap(
    const absl::flat_hash_map<std::string, TypedSlot>& slots,
    FrameLayout::Builder* layout_builder) {
  for (const auto& name_slot : slots) {
    const auto& slot = name_slot.second;
    RETURN_IF_ERROR(layout_builder->RegisterUnsafeSlot(
        slot.byte_offset(), slot.GetType()->type_layout().AllocSize(),
        slot.GetType()->type_info()));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::vector<std::optional<TypedSlot>>>
MaybeFindSlotsAndVerifyTypes(
    absl::Span<const std::pair<std::string, QTypePtr>> types_in_order,
    const absl::flat_hash_map<std::string, TypedSlot>& slots) {
  std::vector<std::string> type_mismatch;
  std::vector<std::optional<TypedSlot>> res_slots;
  res_slots.reserve(types_in_order.size());
  for (const auto& [name, type] : types_in_order) {
    auto it = slots.find(name);
    if (it == slots.end()) {
      res_slots.push_back(std::nullopt);
      continue;
    }
    res_slots.push_back({it->second});
    if (it->second.GetType() != type) {
      type_mismatch.push_back(
          TypeMismatchError(name, type, it->second.GetType()));
    }
  }
  RETURN_IF_ERROR(SlotTypesError(/*missed_slots=*/{}, std::move(type_mismatch),
                                 /*unwanted_slots=*/{}));
  return {std::move(res_slots)};
}

absl::StatusOr<std::vector<TypedSlot>> FindSlotsAndVerifyTypes(
    absl::Span<const std::pair<std::string, QTypePtr>> types_in_order,
    const absl::flat_hash_map<std::string, TypedSlot>& slots) {
  std::vector<std::string> missed_slots;
  std::vector<std::string> type_mismatch;
  std::vector<TypedSlot> res_slots;
  res_slots.reserve(types_in_order.size());
  for (const auto& [name, type] : types_in_order) {
    auto it = slots.find(name);
    if (it == slots.end()) {
      missed_slots.push_back(name);
      continue;
    }
    res_slots.push_back({it->second});
    if (it->second.GetType() != type) {
      type_mismatch.push_back(
          TypeMismatchError(name, type, it->second.GetType()));
    }
  }
  RETURN_IF_ERROR(SlotTypesError(std::move(missed_slots),
                                 std::move(type_mismatch),
                                 /*unwanted_slots=*/{}));
  return {std::move(res_slots)};
}

absl::Status VerifySlotTypes(
    const absl::flat_hash_map<std::string, QTypePtr>& types,
    const absl::flat_hash_map<std::string, TypedSlot>& slots,
    bool verify_unwanted_slots, bool verify_missed_slots) {
  std::vector<std::string> missed_slots;
  std::vector<std::string> type_mismatch;
  std::vector<std::string> unwanted_slots;
  for (const auto& [name, type] : types) {
    auto it = slots.find(name);
    if (it == slots.end()) {
      if (verify_missed_slots) {
        missed_slots.push_back(name);
      }
      continue;
    }
    if (it->second.GetType() != type) {
      type_mismatch.push_back(
          TypeMismatchError(name, type, it->second.GetType()));
    }
  }
  if (verify_unwanted_slots) {
    for (const auto& [name, _] : slots) {
      if (!types.contains(name)) {
        unwanted_slots.push_back(name);
      }
    }
  }
  return SlotTypesError(std::move(missed_slots), std::move(type_mismatch),
                        std::move(unwanted_slots));
}

}  // namespace arolla
