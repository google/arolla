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
#include "arolla/qexpr/operators/strings/strings.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "unicode/bytestream.h"
#include "unicode/casemap.h"
#include "unicode/errorcode.h"
#include "unicode/stringoptions.h"
#include "unicode/umachine.h"
#include "unicode/utf8.h"
#include "double-conversion/double-to-string.h"
#include "double-conversion/utils.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/strings/regex.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

absl::Status ValidateUtf8(absl::string_view bytes) {
  if (bytes.size() > size_t{std::numeric_limits<int32_t>::max()}) {
    return absl::UnimplementedError("string is too long to convert to UTF-8");
  }
  int32_t offset = 0;
  while (offset < bytes.size()) {
    UChar32 character;
    int32_t previous_offset = offset;
    U8_NEXT(bytes.data(), offset, bytes.size(), character);
    if (character < 0) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "invalid UTF-8 sequence at position %d", previous_offset));
    }
  }
  return absl::OkStatus();
}

// Implementation of strings.replace. `codepoint_length` must be a function
// `(char) -> size_t` returning codepoint length in bytes.
template <typename CharLengthFn>
absl::StatusOr<std::string> ReplaceImpl(absl::string_view s,
                                        absl::string_view old_sub,
                                        absl::string_view new_sub,
                                        OptionalValue<int64_t> max_subs,
                                        CharLengthFn codepoint_length) {
  // For consistency with Python's str.replace we treat max_subs < 0 as None.
  int64_t count = max_subs.present && max_subs.value >= 0
                      ? max_subs.value
                      : std::numeric_limits<int64_t>::max();

  std::string res;
  size_t offset = 0;
  if (old_sub.empty()) {
    // Special handling for empty 'old_sub'.
    if (count-- > 0) {
      absl::StrAppend(&res, new_sub);
    }
    while ((count-- > 0) && (offset < s.length())) {
      size_t length = codepoint_length(s, offset);
      // Should be guaranteed by U8_FWD_1 below.
      DCHECK_LE(offset + length, s.size());
      absl::StrAppend(&res, s.substr(offset, length), new_sub);
      offset += length;
    }
  } else {
    while (count-- > 0) {
      const size_t start = s.find(old_sub, offset);
      if (start == std::string::npos) break;
      absl::StrAppend(&res, s.substr(offset, start - offset), new_sub);
      offset = start + old_sub.size();
    }
  }
  res.append(s.begin() + offset, s.end());
  return res;
}

}  // namespace

absl::StatusOr<Text> LowerOp::operator()(
    absl::string_view in, std::optional<absl::string_view> locale) const {
  std::string result;
  icu::StringByteSink<std::string> sink(&result);
  icu::ErrorCode error_code;

  const char* locale_ptr = locale.has_value() ? locale->data() : nullptr;
  icu::CaseMap::utf8ToLower(locale_ptr, U_FOLD_CASE_DEFAULT,
                            absl::string_view(in), sink,
                            /* edits = */ nullptr, error_code);
  if (error_code.isFailure()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "utf8ToLower failed with error: %s", error_code.errorName()));
  }
  return Text(std::move(result));
}

absl::StatusOr<Text> UpperOp::operator()(
    absl::string_view in, std::optional<absl::string_view> locale) const {
  std::string result;
  icu::StringByteSink<std::string> sink(&result);
  icu::ErrorCode error_code;
  const char* locale_ptr = locale.has_value() ? locale->data() : nullptr;
  icu::CaseMap::utf8ToUpper(locale_ptr, U_FOLD_CASE_DEFAULT,
                            absl::string_view(in), sink,
                            /* edits = */ nullptr, error_code);
  if (error_code.isFailure()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "utf8ToUpper failed with error: %s", error_code.errorName()));
  }
  return Text(std::move(result));
}

absl::StatusOr<Text> DecodeOp::operator()(absl::string_view s) const {
  RETURN_IF_ERROR(ValidateUtf8(s));
  return Text(s);
}

absl::StatusOr<std::string> BytesReplaceOp::operator()(
    absl::string_view s, absl::string_view old_sub, absl::string_view new_sub,
    OptionalValue<int64_t> max_subs) const {
  return ReplaceImpl(s, old_sub, new_sub, max_subs,
                     [](absl::string_view str, size_t offset) { return 1; });
}

absl::StatusOr<std::string> TextReplaceOp::operator()(
    absl::string_view s, absl::string_view old_sub, absl::string_view new_sub,
    OptionalValue<int64_t> max_subs) const {
  return ReplaceImpl(s, old_sub, new_sub, max_subs,
                     [](absl::string_view str, size_t offset) {
                       size_t new_offset = offset;
                       U8_FWD_1(str.data(), new_offset, str.size());
                       return new_offset - offset;
                     });
}

absl::StatusOr</*absl_nonnull*/ RegexPtr> CompileRegexOp::operator()(
    absl::string_view pattern) const {
  return CompileRegex(pattern);
}

OptionalUnit ContainsRegexOp::operator()(absl::string_view text,
                                         const RegexPtr& regex) const {
  return OptionalUnit(regex != nullptr && regex->PartialMatch(text));
}

OptionalUnit ContainsRegexOp::operator()(OptionalValue<absl::string_view> text,
                                         const RegexPtr& regex) const {
  return OptionalUnit(text.present && regex != nullptr &&
                      regex->PartialMatch(text.value));
}

absl::StatusOr<OptionalValue<Text>> ExtractRegexOp::operator()(
    const Text& text, const RegexPtr& regex) const {
  if (regex == nullptr) {
    return std::nullopt;
  }
  if (regex->NumberOfCapturingGroups() != 1) {
    return absl::InvalidArgumentError(
        absl::StrFormat("ExtractRegexOp expected regular expression with "
                        "exactly one capturing group; got `%s` which "
                        "contains %d capturing groups",
                        regex->pattern(), regex->NumberOfCapturingGroups()));
  }
  std::string match;
  if (regex->PartialMatch(text.view(), &match)) {
    return Text(std::move(match));
  }
  return std::nullopt;
}

absl::StatusOr<OptionalValue<Text>> ExtractRegexOp::operator()(
    const OptionalValue<Text>& text, const RegexPtr& regex) const {
  if (text.present) {
    return (*this)(text.value, regex);
  }
  return std::nullopt;
}

absl::StatusOr<OptionalValue<Text>> ReplaceAllRegexOp::operator()(
    const Text& text, const RegexPtr& regex, const Text& rewrite) const {
  if (regex == nullptr) {
    return std::nullopt;
  }
  std::string str = std::string(text.view());
  regex->GlobalReplace(&str, rewrite.view());
  return Text(std::move(str));
}

absl::StatusOr<OptionalValue<Text>> ReplaceAllRegexOp::operator()(
    const OptionalValue<Text>& text, const RegexPtr& regex,
    const OptionalValue<Text>& rewrite) const {
  if (text.present && rewrite.present) {
    return (*this)(text.value, regex, rewrite.value);
  }
  return std::nullopt;
}

namespace {

template <class T>
Text SignedIntegerToText(T x) {
  return Text(absl::StrFormat("%d", x));
}

}  // namespace

Text AsTextOp::operator()(absl::string_view s) const {
  return Text(absl::StrFormat("b'%s'", absl::Utf8SafeCHexEscape(s)));
}
Text AsTextOp::operator()(const Bytes& x) const {
  return operator()(absl::string_view(x));
}
Text AsTextOp::operator()(Unit) const { return Text("present"); }
Text AsTextOp::operator()(int32_t x) const { return SignedIntegerToText(x); }
Text AsTextOp::operator()(int64_t x) const { return SignedIntegerToText(x); }
Text AsTextOp::operator()(uint64_t x) const {
  return Text(absl::StrFormat("%d", x));
}
Text AsTextOp::operator()(bool x) const {
  return x ? Text("true") : Text("false");
}

Text AsTextOp::operator()(float x) const {
  static const absl::NoDestructor<double_conversion::DoubleToStringConverter>
      converter(double_conversion::DoubleToStringConverter::NO_FLAGS, "inf",
                "nan",
                /*exponent_character=*/'e',
                /*decimal_in_shortest_low=*/-6, /*decimal_in_shortest_high=*/21,
                /*max_leading_padding_zeroes_in_precision_mode=*/6,
                /*max_trailing_padding_zeroes_in_precision_mode=*/0);
  char buf[128];
  double_conversion::StringBuilder builder(buf, sizeof(buf));
  converter->ToShortestSingle(x, &builder);
  return Text(builder.Finalize());
}

Text AsTextOp::operator()(double x) const {
  static const absl::NoDestructor<double_conversion::DoubleToStringConverter>
      converter(double_conversion::DoubleToStringConverter::NO_FLAGS, "inf",
                "nan",
                /*exponent_character=*/'e',
                /*decimal_in_shortest_low=*/-6, /*decimal_in_shortest_high=*/21,
                /*max_leading_padding_zeroes_in_precision_mode=*/6,
                /*max_trailing_padding_zeroes_in_precision_mode=*/0);
  char buf[128];
  double_conversion::StringBuilder builder(buf, sizeof(buf));
  converter->ToShortest(x, &builder);
  return Text(builder.Finalize());
}

Text TextAsTextOp::operator()(absl::string_view s) const { return Text(s); }
Text TextAsTextOp::operator()(const Text& s) const { return s; }

}  // namespace arolla
