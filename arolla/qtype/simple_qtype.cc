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
#include "arolla/qtype/simple_qtype.h"

#include <cstdint>
#include <optional>
#include <string>

#include "absl//status/status.h"
#include "absl//strings/str_cat.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"

namespace arolla {

absl::Status SimpleQType::InitNameMap() {
  name2index_.reserve(field_names_.size());
  for (const auto& field_name : field_names_) {
    if (bool inserted =
            name2index_.emplace(field_name, name2index_.size()).second;
        !inserted) {
      return absl::FailedPreconditionError(absl::StrCat(
          "duplicated name field for QType ", name(), ": ", field_name));
    }
  }
  return absl::OkStatus();
}

absl::Span<const std::string> SimpleQType::GetFieldNames() const {
  return field_names_;
}

std::optional<int64_t> SimpleQType::GetFieldIndexByName(
    absl::string_view field_name) const {
  if (auto it = name2index_.find(field_name); it != name2index_.end()) {
    return it->second;
  }
  return std::nullopt;
}

}  // namespace arolla
