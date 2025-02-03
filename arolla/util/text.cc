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
#include "arolla/util/text.h"

#include <cstddef>
#include <string>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {
namespace {

absl::string_view Utf8CopyFirstNCodePoints(size_t n, absl::string_view data) {
  size_t offset = 0;
  for (; n > 0 && offset < data.size(); --n) {
    const auto byte = data[offset];
    if ((byte & 0x80) == 0) {
      offset += 1;
    } else if ((byte & 0xe0) == 0xc0) {
      offset += 2;
    } else if ((byte & 0xf0) == 0xe0) {
      offset += 3;
    } else if ((byte & 0xf8) == 0xf0) {
      offset += 4;
    } else {
      offset += 1;  // broken byte
    }
  }
  return data.substr(0, offset);
}

}  // namespace

ReprToken ReprTraits<Text>::operator()(const Text& value) const {
  constexpr size_t kTextAbbrevLimit = 120;
  ReprToken result;
  auto text = value.view();
  auto prefix = Utf8CopyFirstNCodePoints(kTextAbbrevLimit, text);
  if (prefix.size() == text.size()) {
    result.str = absl::StrCat("'", absl::Utf8SafeCHexEscape(text), "'");
  } else {
    result.str = absl::StrCat("'", absl::Utf8SafeCHexEscape(prefix),
                              "... (TEXT of ", text.size(), " bytes total)'");
  }
  return result;
}

void FingerprintHasherTraits<Text>::operator()(FingerprintHasher* hasher,
                                               const Text& value) const {
  hasher->Combine(value.view());
}

}  // namespace arolla
