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
#ifndef AROLLA_QEXPR_OPERATORS_STRINGS_STRINGS_H_
#define AROLLA_QEXPR_OPERATORS_STRINGS_STRINGS_H_

#include <bitset>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <system_error>  // NOLINT(build/c++11): needed for absl::from_chars.
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/charconv.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "icu4c/source/common/unicode/uchar.h"
#include "icu4c/source/common/unicode/unistr.h"
#include "icu4c/source/common/unicode/utf16.h"
#include "icu4c/source/common/unicode/utf8.h"
#include "icu4c/source/common/unicode/utypes.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/strings/regex.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Returns transformation of `in` to lower-case.
struct LowerOp {
  absl::StatusOr<Text> operator()(
      absl::string_view in,
      std::optional<absl::string_view> locale = std::nullopt) const;

  absl::StatusOr<Text> operator()(const Text& in) const {
    return this->operator()(absl::string_view(in));
  }

  absl::StatusOr<Text> operator()(const Text& in, const Text& locale) const {
    return this->operator()(absl::string_view(in), absl::string_view(locale));
  }
};

// Returns transformation of `in` to upper-case.
struct UpperOp {
  absl::StatusOr<Text> operator()(
      absl::string_view in,
      std::optional<absl::string_view> locale = std::nullopt) const;

  absl::StatusOr<Text> operator()(const Text& in) const {
    return this->operator()(absl::string_view(in));
  }

  absl::StatusOr<Text> operator()(const Text& in, const Text& locale) const {
    return this->operator()(absl::string_view(in), absl::string_view(locale));
  }
};

// Replaces up to `max_subs` occurrences of `old_sub` within `s` with
// `new_sub`. If `max_subs` is missing, then there is no limit on the number
// of substitutions.
//
// Note that this has behavior similar to Python's string replace in that
// if the search string is empty, the original string is fenced with the
// replacement string, for example:
//
//   replace("ab", "", "-") returns "-a-b-",
//
struct ReplaceOp {
  absl::StatusOr<std::string> operator()(absl::string_view s,
                                         absl::string_view old_sub,
                                         absl::string_view new_sub,
                                         OptionalValue<int32_t> max_subs) const;

  template <typename StringType>
  absl::StatusOr<StringType> operator()(const StringType& str,
                                        const StringType& old_sub,
                                        const StringType& new_sub,
                                        OptionalValue<int32_t> max_subs) const {
    ASSIGN_OR_RETURN(
        auto result,
        this->operator()(absl::string_view(str), absl::string_view(old_sub),
                         absl::string_view(new_sub), max_subs));

    return StringType(result);
  }
};

// strings.lstrip eliminates leading whitespaces,
// or leading specified characters.
struct LStripOp {
  Bytes operator()(const Bytes& bytes,
                   const OptionalValue<Bytes>& chars) const {
    auto do_lstrip = [](absl::string_view s, auto strip_test) {
      const char* b = s.data();
      const char* e = s.data() + s.length() - 1;
      while (b <= e && strip_test(*b)) {
        ++b;
      }
      return absl::string_view(b, e - b + 1);
    };
    if (chars.present) {
      std::bitset<256> char_set(256);
      for (char c : absl::string_view(chars.value)) {
        char_set.set(static_cast<int>(c));
      }
      return Bytes(do_lstrip(bytes, [&](char c) { return char_set.test(c); }));
    } else {
      return Bytes(
          do_lstrip(bytes, [&](char c) { return absl::ascii_isspace(c); }));
    }
  }

  Text operator()(const Text& text, const OptionalValue<Text>& chars) const {
    auto do_lstrip = [](absl::string_view s, auto strip_test) {
      int pref = 0;
      for (int i = 0; i < s.length();) {
        UChar32 c;
        U8_NEXT(s.data(), i, s.length(), c);
        if (!strip_test(c)) {
          break;
        }
        pref = i;
      }
      return Text(absl::string_view(s.data() + pref, s.length() - pref));
    };
    if (!chars.present) {
      return Text(
          do_lstrip(text, [](UChar32 c) { return u_isUWhiteSpace(c); }));
    }
    absl::string_view chars_bytes = chars.value;

    // TODO: Can we create the set only once for an array?
    absl::flat_hash_set<UChar32> set;
    for (int i = 0; i < chars_bytes.length();) {
      UChar32 c;
      U8_NEXT(chars_bytes.data(), i, chars_bytes.length(), c);
      set.insert(c);
    }
    return Text(do_lstrip(text, [&](UChar32 c) { return set.contains(c); }));
  }
};

// strings.rstrip eliminates trailing whitespaces,
// or trailing specified characters.
struct RStripOp {
  Bytes operator()(const Bytes& bytes,
                   const OptionalValue<Bytes>& chars) const {
    auto do_rstrip = [](absl::string_view s, auto strip_test) {
      const char* b = s.data();
      const char* e = s.data() + s.length() - 1;
      while (b <= e && strip_test(*e)) {
        --e;
      }
      return absl::string_view(b, e - b + 1);
    };
    if (chars.present) {
      std::bitset<256> char_set(256);
      for (char c : absl::string_view(chars.value)) {
        char_set.set(static_cast<int>(c));
      }
      return Bytes(do_rstrip(bytes, [&](char c) { return char_set.test(c); }));
    } else {
      return Bytes(
          do_rstrip(bytes, [&](char c) { return absl::ascii_isspace(c); }));
    }
  }

  Text operator()(const Text& text, const OptionalValue<Text>& chars) const {
    auto do_rstrip = [](absl::string_view s, auto strip_test) {
      int len = s.length();
      for (int i = s.length(); i > 0;) {
        UChar32 c;
        U8_PREV(s.data(), 0, i, c);
        if (!strip_test(c)) {
          break;
        }
        len = i;
      }
      return Text(absl::string_view(s.data(), len));
    };
    if (!chars.present) {
      return Text(
          do_rstrip(text, [](UChar32 c) { return u_isUWhiteSpace(c); }));
    }
    absl::string_view chars_bytes = chars.value;

    // TODO: Can we create the set only once for an array?
    absl::flat_hash_set<UChar32> set;
    for (int i = 0; i < chars_bytes.length();) {
      UChar32 c;
      U8_NEXT(chars_bytes.data(), i, chars_bytes.length(), c);
      set.insert(c);
    }
    return Text(do_rstrip(text, [&](UChar32 c) { return set.contains(c); }));
  }
};

// strings.strip eliminates leading and trailing whitespaces, or leading and
// trailing specified characters.
struct StripOp {
  absl::StatusOr<Bytes> operator()(const Bytes& bytes,
                                   const OptionalValue<Bytes>& chars) const {
    return RStripOp()(LStripOp()(bytes, chars), chars);
  }

  absl::StatusOr<Text> operator()(const Text& text,
                                  const OptionalValue<Text>& chars) const {
    return RStripOp()(LStripOp()(text, chars), chars);
  }
};

// Returns the length in bytes of a Bytes object.
struct BytesLengthOp {
  int32_t operator()(absl::string_view s) const { return s.length(); }

  int32_t operator()(const Bytes& bytes) const {
    return this->operator()(absl::string_view(bytes));
  }
};

// Returns the length in code-points of a Text object.
struct TextLengthOp {
  int32_t operator()(absl::string_view s) const {
    return icu::UnicodeString::fromUTF8(s).countChar32();
  }

  int32_t operator()(const Text& text) const {
    return this->operator()(absl::string_view(text));
  }
};

// strings.as_text operator implementation.
struct AsTextOp {
  Text operator()(absl::string_view s) const;
  Text operator()(const Bytes& x) const;
  Text operator()(Unit) const;
  Text operator()(int32_t x) const;
  Text operator()(int64_t x) const;
  Text operator()(uint64_t x) const;
  Text operator()(float x) const;
  Text operator()(double x) const;
  Text operator()(bool x) const;
};

// strings.as_text operator implementation for Text argument. Extracted into a
// separate functor to avoid conflict with Bytes version.
// TODO: This operator is identity, eliminate it in compilation.
struct TextAsTextOp {
  Text operator()(absl::string_view s) const;
  Text operator()(const Text& s) const;
};

// strings.encode operator. Supports only UTF-8 for now.
struct EncodeOp {
  Bytes operator()(absl::string_view s) const { return Bytes(s); }
  Bytes operator()(const Text& text) const {
    return Bytes(absl::string_view(text));
  }
};

// strings.decode operator. Supports only UTF-8 for now. As we are using UTF-8
// inside Text as well, it just verifies UTF-8 correctness of the input.
// TODO: Support encoding argument (except the default UTF-8).
struct DecodeOp {
  absl::StatusOr<Text> operator()(absl::string_view s) const;
  auto operator()(const Bytes& bytes) const {
    return (*this)(absl::string_view(bytes));
  }
};

// Compile `pattern` into a regular expression. Returns an error if `pattern`
// is not a valid regular expression.
struct CompileRegexOp {
  absl::StatusOr<Regex> operator()(const Text& pattern) const {
    return Regex::FromPattern(pattern);
  }
};

// Returns kPresent if `s` contains the regular expression contained in
// `regexp`.
struct ContainsRegexOp {
  OptionalUnit operator()(const Text& text, const Regex& regexp) const;
  OptionalUnit operator()(const OptionalValue<Text>& text,
                          const Regex& regexp) const {
    return text.present ? (*this)(text.value, regexp) : kMissing;
  }
};

// Given a `regexp` with a single capturing group, if `text` contains the
// pattern represented by `regexp`, then return the matched value from the
// capturing group, otherwise missing. Returns an error if `regexp` does not
// contain exactly one capturing group.
struct ExtractRegexOp {
  absl::StatusOr<OptionalValue<Text>> operator()(const Text& text,
                                                 const Regex& regexp) const;
  absl::StatusOr<OptionalValue<Text>> operator()(
      const OptionalValue<Text>& text, const Regex& regexp) const {
    return text.present ? (*this)(text.value, regexp) : OptionalValue<Text>{};
  }
};

// Parses a floating number from a string representation.
// Returns true if the parsing was successful.
//
// What is supported:
//  * fixed point (0.456) and scientific (4.56e-1) decimal notations
//  * leading '+' or '-' sign
//  * special values: 'nan', 'inf', 'infinity' (parsing is case insensitive)
//
// Values that overflow FloatT are treated as infinity. Values that underflow
// FloatT are treated as zeros.
//
// What is not supported:
//  * no leading or trailing junk is allowed, including whitespaces
//  * no octals numbers supported; "0755" gets parsed as 755
//  * no hex numbers supported; parsing "0xabc" fails
//
template <typename FloatT>
bool ParseFloatT(absl::string_view str, FloatT& result) {
  // Skip `+` sign
  if (!str.empty() && str[0] == '+') {
    str.remove_prefix(1);
    if (!str.empty() && str[0] == '-') {
      return false;  // Forbid: +-
    }
  }
  // Forbid hexadecimals: 0x, 0X, -0x, -0X
  if (str.size() >= 2 && (str[1] == 'x' || str[1] == 'X')) {
    return false;
    if (str.size() >= 3 && (str[2] == 'x' || str[2] == 'X')) {
      return false;
    }
  }
  // Invoke the parser.
  auto [ptr, ec] =
      absl::from_chars(str.data(), str.data() + str.size(), result);
  if (ptr != str.data() + str.size()) {
    return false;  // not all characters consumed
  }
  // from_chars() with DR 3081's current wording will return max() on
  // overflow. We return infinity instead.
  if (ec == std::errc::result_out_of_range) {
    if (result > 1.0) {
      result = std::numeric_limits<FloatT>::infinity();
    } else if (result < -1.0) {
      result = -std::numeric_limits<FloatT>::infinity();
    }
    return true;
  }
  return ec == std::errc();
}

// Parses an integer number from a string representation.
// Returns true if the parsing was successful.
//
// What is supported:
//  * decimal format (456)
//  * leading '+' or '-' sign
//
// IntT overflow is treated as an error.
//
// What is not supported:
//  * no leading or trailing junk is allowed, including whitespaces
//  * no octals numbers supported; "0755" gets parsed as 755
//  * no hex numbers supported; parsing "0xabc" fails
//
template <typename IntT>
bool ParseIntT(absl::string_view str, IntT& result) {
  // Skip `+` sign
  if (!str.empty() && str[0] == '+') {
    str.remove_prefix(1);
    if (!str.empty() && str[0] == '-') {
      return false;  // Forbid: +-
    }
  }
  auto [ptr, ec] =
      std::from_chars(str.data(), str.data() + str.size(), result, 10);
  return ec == std::errc() && ptr == str.data() + str.size();
}

// strings.parse_float32: Converts Bytes/Text to Float32.
//
// See doc for ParseFloatT() above for the number format details.
struct StringsParseFloat32 {
  absl::StatusOr<float> operator()(absl::string_view str) const {
    float result;
    if (ParseFloatT(str, result)) {
      return result;
    }
    return absl::InvalidArgumentError(
        absl::StrCat("unable to parse FLOAT32: ", str));
  }

  auto operator()(const ::arolla::Bytes& s) const {
    return (*this)(absl::string_view(s));
  }
  auto operator()(const ::arolla::Text& s) const {
    return (*this)(absl::string_view(s));
  }
};

// strings.parse_float64: Converts Bytes/Text to Float64.
//
// See doc for ParseFloatT() above for the number format details.
struct StringsParseFloat64 {
  absl::StatusOr<double> operator()(absl::string_view str) const {
    double result;
    if (ParseFloatT(str, result)) {
      return result;
    }
    return absl::InvalidArgumentError(
        absl::StrCat("unable to parse FLOAT64: ", str));
  }

  auto operator()(const ::arolla::Bytes& s) const {
    return (*this)(absl::string_view(s));
  }
  auto operator()(const ::arolla::Text& s) const {
    return (*this)(absl::string_view(s));
  }
};

// strings.parse_int32: Converts Bytes/Text to Int32.
//
// See doc for ParseIntT() above for the number format details.
struct StringsParseInt32 {
  absl::StatusOr<int32_t> operator()(absl::string_view str) const {
    int32_t result;
    if (ParseIntT(str, result)) {
      return result;
    }
    return absl::InvalidArgumentError(
        absl::StrCat("unable to parse INT32: ", str));
  }

  auto operator()(const ::arolla::Bytes& s) const {
    return (*this)(absl::string_view(s));
  }
  auto operator()(const ::arolla::Text& s) const {
    return (*this)(absl::string_view(s));
  }
};

// strings.parse_int64: Converts Bytes/Text to Int64.
//
// See doc for ParseIntT() above for the number format details.
struct StringsParseInt64 {
  absl::StatusOr<int64_t> operator()(absl::string_view str) const {
    int64_t result;
    if (ParseIntT(str, result)) {
      return result;
    }
    return absl::InvalidArgumentError(
        absl::StrCat("unable to parse INT64: ", str));
  }

  auto operator()(const ::arolla::Bytes& s) const {
    return (*this)(absl::string_view(s));
  }
  auto operator()(const ::arolla::Text& s) const {
    return (*this)(absl::string_view(s));
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_STRINGS_STRINGS_H_
