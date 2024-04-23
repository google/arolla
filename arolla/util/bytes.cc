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
#include "arolla/util/bytes.h"

#include <cstddef>
#include <string>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/util/repr.h"

namespace arolla {

ReprToken ReprTraits<Bytes>::operator()(const Bytes& value) const {
  constexpr size_t kBytesAbbrevLimit = 120;
  ReprToken result;
  absl::string_view bytes = value;
  if (bytes.size() <= kBytesAbbrevLimit) {
    result.str = absl::StrCat("b'", absl::CHexEscape(bytes), "'");
  } else {
    result.str =
        absl::StrCat("b'", absl::CHexEscape(bytes.substr(0, kBytesAbbrevLimit)),
                     "... (", bytes.size(), " bytes total)'");
  }
  return result;
}

}  // namespace arolla
