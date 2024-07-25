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
#include <string>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

// Interface for regular expression matching, modeled after RE2.
//
// The RE2 library has a significant binary size. Therefore, we use this
// interface to define a REGEX qtype while keeping RE2 as an optional linkage
// dependency. If the final binary doesn't use any RE2 symbols, the linker
// should be able to strip the dependency.
//
class Regex {
 public:
  Regex() = default;

  Regex(const Regex&) = delete;
  Regex& operator=(const Regex&) = delete;

  virtual ~Regex() = default;

  // The string specification for this regex.
  virtual absl::string_view pattern() const = 0;

  // Return the number of capturing groups in the pattern.
  virtual int NumberOfCapturingGroups() const = 0;

  // Returns true if `text` contains the pattern.
  virtual bool PartialMatch(absl::string_view text) const = 0;

  // Returns true if `text` contains the pattern and stores the value of
  // the first capturing group in `match`.
  virtual bool PartialMatch(absl::string_view text,
                            std::string* match) const = 0;
};

using RegexPtr = std::shared_ptr<const Regex>;

// Returns a regular expression for the given pattern.
absl::StatusOr<absl::Nonnull<RegexPtr>> CompileRegex(absl::string_view pattern);

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(RegexPtr);
AROLLA_DECLARE_REPR(RegexPtr);

AROLLA_DECLARE_QTYPE(RegexPtr);

}  // namespace arolla

#endif  // AROLLA_QTYPE_STRINGS_REGEX_H_
