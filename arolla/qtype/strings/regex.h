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
#ifndef AROLLA_QTYPE_STRINGS_REGEX_H_
#define AROLLA_QTYPE_STRINGS_REGEX_H_

#include <memory>
#include <ostream>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "re2/re2.h"

namespace arolla {

// A compiled, valid regular expression for use in RL2.
class Regex {
 public:
  // Default constructor creates a valid regular expression which matches
  // nothing.
  Regex() {}

  // Returns a Regex corresponding to `pattern`, or an error if `pattern` does
  // not contain a valid regular expression.
  static absl::StatusOr<Regex> FromPattern(absl::string_view pattern);

  // Returns true if this Regex was initialized to a non-default value.
  bool has_value() const { return impl_ != nullptr; }

  // Returns the compiled RE2 representation of this Regex. If this Regex was
  // default-initialized, the returned RE2 is valid, but will not match any
  // character sequence.
  const RE2& value() const { return has_value() ? *impl_ : default_value(); }

 private:
  // Returns a default regular expression which matches nothing.
  static const RE2& default_value();

  explicit Regex(std::shared_ptr<RE2> impl) : impl_(std::move(impl)) {
    DCHECK(impl_->ok());
  }

  std::shared_ptr<const RE2> impl_;
};

std::ostream& operator<<(std::ostream& stream, const Regex& regex);

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(Regex);
AROLLA_DECLARE_SIMPLE_QTYPE(REGEX, Regex);
AROLLA_DECLARE_OPTIONAL_QTYPE(REGEX, Regex);

}  // namespace arolla

#endif  // AROLLA_QTYPE_STRINGS_REGEX_H_
