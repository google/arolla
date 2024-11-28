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
#include "arolla/io/wildcard_input_loader.h"

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <utility>

#include "absl//log/check.h"
#include "absl//strings/str_format.h"
#include "absl//strings/string_view.h"
#include "absl//strings/strip.h"

namespace arolla::input_loader_impl {

std::function<std::optional<std::string>(absl::string_view)> MakeNameToKeyFn(
    const absl::ParsedFormat<'s'>& format) {
  constexpr absl::string_view kUniqueString =
      "unique_string_5a7cf4c5ed2d49068302b641bad242aa";
  auto formatted = absl::StrFormat(format, kUniqueString);
  size_t prefix_end = formatted.find(kUniqueString);
  // Should never happen because ParsedFormat guarantees it.
  DCHECK(prefix_end != absl::string_view::npos);
  std::string prefix = formatted.substr(0, prefix_end);

  size_t suffix_begin = prefix_end + kUniqueString.size();
  DCHECK(suffix_begin <= formatted.size());
  std::string suffix = formatted.substr(suffix_begin);

  return [prefix = std::move(prefix), suffix = std::move(suffix)](
             absl::string_view name) -> std::optional<std::string> {
    if (!absl::ConsumePrefix(&name, prefix)) {
      return std::nullopt;
    }
    if (!absl::ConsumeSuffix(&name, suffix)) {
      return std::nullopt;
    }
    return std::string(name);
  };
}

}  // namespace arolla::input_loader_impl
