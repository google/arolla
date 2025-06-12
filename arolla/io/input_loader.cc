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
#include "arolla/io/input_loader.h"

#include <algorithm>
#include <cstddef>
#include <set>
#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

absl::Status ValidateDuplicatedNames(OutputTypesSpan output_types) {
  absl::flat_hash_map<std::string, size_t> names_count;
  std::vector<std::string> duplicated_names;
  for (const auto& [name, type] : output_types) {
    size_t& count = names_count[name];
    if (count == 1) {
      duplicated_names.push_back(name);
    }
    ++count;
  }
  if (duplicated_names.empty()) {
    return absl::OkStatus();
  }
  std::sort(duplicated_names.begin(), duplicated_names.end());
  return absl::FailedPreconditionError(
      absl::StrCat("accessors have duplicated names: ",
                   absl::StrJoin(duplicated_names, ", ")));
}

absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>> GetInputLoaderQTypes(
    const InputLoaderBase& input_loader, absl::Span<const std::string> names) {
  absl::flat_hash_map<std::string, QTypePtr> types;
  types.reserve(names.size());
  std::set<absl::string_view> unknown_types;
  for (const auto& name : names) {
    if (auto qtype = input_loader.GetQTypeOf(name); qtype != nullptr) {
      types.emplace(name, qtype);
    } else {
      unknown_types.emplace(name);
    }
  }
  if (!unknown_types.empty()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unknown inputs: %s (available: %s)",
        Truncate(absl::StrJoin(unknown_types, ", "), 200),
        Truncate(absl::StrJoin(input_loader.SuggestAvailableNames(), ", "),
                 200)));
  }
  return types;
}

absl::Status InputLoaderBase::ValidateSlotTypes(
    const absl::flat_hash_map<std::string, TypedSlot>& slots) const {
  std::vector<std::string> names;
  names.reserve(slots.size());
  for (const auto& [name, _] : slots) {
    names.emplace_back(name);
  }
  ASSIGN_OR_RETURN(auto types, GetInputLoaderQTypes(*this, names));
  return VerifySlotTypes(types, slots,
                         /*verify_unwanted_slots=*/true,
                         /*verify_missed_slots=*/false);
}

absl::flat_hash_map<std::string, TypedSlot>
InputLoaderBase::ExtractSupportedSlots(
    absl::flat_hash_map<std::string, TypedSlot>* absl_nonnull slots) const {
  absl::flat_hash_map<std::string, TypedSlot> partial_slots;
  for (const auto& [name, slot] : *slots) {
    if (GetQTypeOf(name) == nullptr) {
      continue;
    }
    partial_slots.emplace(name, slot);
  }
  for (const auto& [name, _] : partial_slots) {
    slots->erase(name);
  }
  return partial_slots;
}

}  // namespace arolla
