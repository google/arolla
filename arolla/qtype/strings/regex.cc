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
#include "arolla/qtype/strings/regex.h"

#include <memory>
#include <ostream>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "re2/re2.h"

namespace arolla {

absl::StatusOr<Regex> Regex::FromPattern(absl::string_view pattern) {
  auto value = std::make_shared<RE2>(pattern, RE2::Quiet);
  if (value->ok()) {
    return Regex(std::move(value));
  } else {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Invalid regular expression: \"%s\"; %s.", pattern, value->error()));
  }
}

const RE2& Regex::default_value() {
  // A pattern which doesn't match anything.
  static LazyRE2 value = {".^"};
  return *value;
}

std::ostream& operator<<(std::ostream& stream, const Regex& regex) {
  if (regex.has_value()) {
    return stream << "Regex{\"" << regex.value().pattern() << "\"}";
  } else {
    return stream << "Regex{}";
  }
}

void FingerprintHasherTraits<Regex>::operator()(FingerprintHasher* hasher,
                                                const Regex& value) const {
  hasher->Combine(value.value().pattern());
}

AROLLA_DEFINE_SIMPLE_QTYPE(REGEX, Regex);
AROLLA_DEFINE_OPTIONAL_QTYPE(REGEX, Regex);

}  // namespace arolla
