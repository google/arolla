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
#include <string>

#include "absl/base/nullability.h"
#include "absl/container/inlined_vector.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "re2/re2.h"

namespace arolla {
namespace {

class RE2Regex final : public Regex {
 public:
  explicit RE2Regex(absl::string_view pattern) : re2_(pattern, RE2::Quiet) {}

  bool ok() const { return re2_.ok(); }

  absl::string_view error() const { return re2_.error(); }

  absl::string_view pattern() const final { return re2_.pattern(); }

  int NumberOfCapturingGroups() const final {
    return re2_.NumberOfCapturingGroups();
  }

  bool PartialMatch(absl::string_view text) const final {
    return re2_.PartialMatch(text, re2_);
  }

  bool PartialMatch(absl::string_view text, std::string* match) const final {
    return RE2::PartialMatch(text, re2_, match);
  }

  void FindAll(absl::string_view text,
               absl::FunctionRef<void(absl::Span<const absl::string_view>)>
                   callback) const final {
    // Most of the code here serves to set up the state for the while loop.
    // The RE2 API is a bit cumbersome to use here, because we don't know
    // the number of capturing groups at compile time. The RE2::FindAndConsumeN
    // function takes a pointer to an array of RE2::Arg pointers, where each
    // RE2::Arg takes a pointer to string_view that will be updated with the
    // value of a capturing group during a match.
    // The loop body is pretty simple: just invoke the callback with the current
    // string_view values of the capturing groups. The next iteration will set
    // new values for the string_views.

    int n_groups = re2_.NumberOfCapturingGroups();
    absl::InlinedVector<absl::string_view, 10> capture_group_values(n_groups);
    absl::InlinedVector<RE2::Arg, 10> match_args_(n_groups);
    absl::InlinedVector<const RE2::Arg*, 10> match_arg_ptrs_(n_groups);

    for (int i = 0; i < n_groups; ++i) {
      match_args_[i] = RE2::Arg(&capture_group_values[i]);
      match_arg_ptrs_[i] = &match_args_[i];
    }

    absl::string_view* input = &text;
    while (
        RE2::FindAndConsumeN(input, re2_, match_arg_ptrs_.data(), n_groups)) {
      callback(capture_group_values);
    }
  }

  int GlobalReplace(std::string* str, absl::string_view rewrite) const final {
    return RE2::GlobalReplace(str, re2_, rewrite);
  }

 private:
  RE2 re2_;
};

}  // namespace

absl::StatusOr</*absl_nonnull*/ RegexPtr> CompileRegex(absl::string_view pattern) {
  auto result = std::make_shared<RE2Regex>(pattern);
  if (result->ok()) {
    return result;
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "invalid regular expression: `", pattern, "`; ", result->error()));
}

void FingerprintHasherTraits<RegexPtr>::operator()(
    FingerprintHasher* hasher, const RegexPtr& value) const {
  if (value != nullptr) {
    hasher->Combine(value->pattern());
  }
}

ReprToken ReprTraits<RegexPtr>::operator()(const RegexPtr& value) const {
  if (value == nullptr) {
    return {"regex{}"};
  }
  return {absl::StrCat("regex{`", value->pattern(), "`}")};
}

AROLLA_DEFINE_SIMPLE_QTYPE(REGEX, RegexPtr)

}  // namespace arolla
