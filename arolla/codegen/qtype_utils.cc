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
#include "arolla/codegen/qtype_utils.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"

namespace arolla::codegen {

std::vector<std::pair<std::string, QTypePtr>>
NamedQTypeVectorBuilder::Build() && {
  return std::move(types_);
}

void NamedQTypeVectorBuilder::AddFromCommonPrefixWithPrevious(
    size_t length, const char* suffix, QTypePtr qtype) {
  absl::string_view suffix_str(suffix);
  CHECK_LE(suffix_str.size(), length);
  size_t prefix_length = length - suffix_str.size();
  absl::string_view previous_name =
      types_.empty() ? "" : absl::string_view(types_.back().first);
  CHECK_LE(prefix_length, previous_name.size());
  types_.emplace_back(
      absl::StrCat(previous_name.substr(0, prefix_length), suffix_str), qtype);
}

std::vector<std::pair<std::string, QTypePtr>> BuildNamedQTypeVector(
    absl::Span<const uint16_t> name_lengths, const char* concatenated_suffixes,
    absl::Span<QTypePtr (*const)()> qtype_getters) {
  CHECK_EQ(name_lengths.size(), qtype_getters.size());
  NamedQTypeVectorBuilder builder;
  const char* suffix = concatenated_suffixes;
  for (size_t i = 0; i < name_lengths.size(); ++i) {
    builder.AddFromCommonPrefixWithPrevious(name_lengths[i], suffix,
                                            qtype_getters[i]());
    suffix += std::strlen(suffix) + 1;
  }
  return std::move(builder).Build();
}

}  // namespace arolla::codegen
