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
#ifndef AROLLA_UTIL_STRING_H_
#define AROLLA_UTIL_STRING_H_

#include <cstddef>
#include <string>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace arolla {

// (ascii) Determines whether the given character is an alphabetic character.
constexpr bool IsAlpha(char c) {
  return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z');
}

// (ascii) Determines whether the given character is a decimal digit character.
constexpr bool IsDigit(char c) { return '0' <= c && c <= '9'; }

// (ascii) Determines whether the given character is an alphanumeric character.
constexpr bool IsAlnum(char c) { return IsAlpha(c) || IsDigit(c); }

// Determines whether the given string holds a valid identifier.
constexpr bool IsIdentifier(absl::string_view str) {
  if (str.empty()) {
    return false;
  }
  if (str[0] != '_' && !IsAlpha(str[0])) {
    return false;
  }
  for (size_t i = 1; i < str.size(); ++i) {
    if (str[i] != '_' && !IsAlnum(str[i])) {
      return false;
    }
  }
  return true;
}

// Determines whether the given string holds a chain of valid identifiers.
constexpr bool IsQualifiedIdentifier(absl::string_view str) {
  bool fail_flag = false;
  bool ends_with_token_flag = false;
  for (char ch : str) {
    if (ends_with_token_flag) {
      if (ch == '.') {
        ends_with_token_flag = false;
      } else if (ch != '_' && !IsAlnum(ch)) {
        fail_flag = true;
      }
    } else {
      if (ch != '_' && !IsAlpha(ch)) {
        fail_flag = true;
      }
      ends_with_token_flag = true;
    }
  }
  return !fail_flag && ends_with_token_flag;
}

// Returns if the string has the specified prefix.
constexpr bool starts_with(absl::string_view text, absl::string_view prefix) {
  return text.substr(0, prefix.size()) == prefix;
}

// Returns the delimiter if the first parameter is `false`. The first parameter
// is always reset to `false` after the function is called.
constexpr absl::string_view NonFirstComma(bool& is_first_call,
                                          absl::string_view delimiter = ", ") {
  return (is_first_call ? (is_first_call = false, "") : delimiter);
}

// If the string is longer than max_length chars — truncates it adding "..." to
// the end. `max_lendgth` must be > 3.
// NOTE: the function does NOT respect UTF-8 and can cut half of a codepoint.
std::string Truncate(std::string str, size_t max_length);

// Returns ".key" when `key` is an identifier and "['key']"" otherwise.
inline std::string ContainerAccessString(absl::string_view key) {
  if (IsIdentifier(key)) {
    return absl::StrCat(".", key);
  } else {
    // Use Utf8SafeCHexEscape() because it preserves utf8, and also
    // it's compatible with Python escaping format for strings.
    return absl::StrCat("['", absl::Utf8SafeCHexEscape(key), "']");
  }
}

}  // namespace arolla

#endif  // AROLLA_UTIL_STRING_H_
