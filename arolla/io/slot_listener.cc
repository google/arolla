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
#include "arolla/io/slot_listener.h"

#include <set>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/string.h"

namespace arolla {

absl::Status SlotListenerBase::ValidateSlotTypes(
    const absl::flat_hash_map<std::string, TypedSlot>& slots) const {
  absl::flat_hash_map<std::string, QTypePtr> types;
  types.reserve(slots.size());
  std::set<absl::string_view> unknown_types;
  for (const auto& [name, slot] : slots) {
    if (auto qtype = GetQTypeOf(name, slot.GetType()); qtype != nullptr) {
      types.emplace(name, qtype);
    } else {
      unknown_types.emplace(name);
    }
  }
  if (!unknown_types.empty()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unknown outputs: %s (available: %s)",
        Truncate(absl::StrJoin(unknown_types, ", "), 200),
        Truncate(absl::StrJoin(SuggestAvailableNames(), ", "), 200)));
  }
  return VerifySlotTypes(types, slots,
                         /*verify_unwanted_slots=*/true,
                         /*verify_missed_slots=*/false);
}

absl::flat_hash_map<std::string, TypedSlot>
SlotListenerBase::FindSupportedSlots(
    const absl::flat_hash_map<std::string, TypedSlot>& slots) const {
  absl::flat_hash_map<std::string, TypedSlot> partial_slots;
  for (const auto& [name, slot] : slots) {
    if (GetQTypeOf(name, slot.GetType()) != nullptr) {
      partial_slots.emplace(name, slot);
    }
  }
  return partial_slots;
}
}  // namespace arolla
