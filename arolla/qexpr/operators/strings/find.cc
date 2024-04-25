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
#include "arolla/qexpr/operators/strings/find.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <string>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {

namespace {

// Adjust start/end string indexes to be non-negative values wihin the range
// [0, s.size()]. Missing indexes are set to the corresponding bondary value,
// and negative indexes are set relative to the end of the range, Python style.
// Return value indicates whether the indexes represent a valid (possibly empty)
// sub-interval of the string's range.
bool AdjustIndexes(int64_t ssize, OptionalValue<int64_t>& start,
                   OptionalValue<int64_t>& end) {
  if (!start.present) {
    start.value = 0;
  } else if (start.value < 0) {
    start.value = std::max<int64_t>(start.value + ssize, 0);
  }

  if (!end.present) {
    end.value = ssize;
  } else if (end.value < 0) {
    end.value += ssize;
  } else {
    end.value = std::min(end.value, ssize);
  }

  // Return true if interval is valid.
  return start.value <= end.value;
}

// Returns number of bytes in a UTF-8 encoded codepoint at `src`.
inline int32_t UTF8CharLen(const char* src) {
  return "\1\1\1\1\1\1\1\1\1\1\1\1\2\2\3\4"[(*src & 0xFF) >> 4];
}

// Returns a vector of byte offsets corresponding to the sequence of codepoints
// in the UTF8 string `str`. The length of the returned vector is equal to the
// number of complete codepoints in `str` plus 1, with the i'th value containing
// the offset of the i'th codepoint, plus one extra value containing str.size().
std::vector<int32_t> UTF8StringIndex(absl::string_view str) {
  std::vector<int32_t> index;
  // At most one codepoint per byte; could be fewer, but value is short-lived,
  // so it doesn't matter.
  index.reserve(str.length() + 1);
  const char* data = str.data();
  int32_t offset = 0;
  while (offset < str.size()) {
    index.push_back(offset);
    offset += UTF8CharLen(data + offset);
  }
  if (offset > str.size()) {
    // UTF8 string is invalid; final code-point is incomplete.
    index.pop_back();
  }

  // Having an extra value at the end is useful for converting offsets
  // corresponding to the end of the string.
  index.push_back(str.length());
  return index;
}

}  // namespace

OptionalUnit ContainsOp::operator()(absl::string_view str,
                                    absl::string_view substr) const {
  return OptionalUnit{absl::StrContains(str, substr)};
}

int32_t SubstringOccurrenceCountOp::operator()(absl::string_view str,
                                               absl::string_view substr) const {
  if (substr.empty()) {
    return str.length() + 1;
  }
  int count = 0;
  absl::string_view::size_type curr = 0;
  while (absl::string_view::npos != (curr = str.find(substr, curr))) {
    ++count;
    ++curr;
  }
  return count;
}

namespace {
OptionalValue<int64_t> FindSubstring(absl::string_view str,
                                     absl::string_view substr, int64_t start,
                                     int64_t end) {
  OptionalValue<int64_t> result;
  auto pos = str.substr(start, end - start).find(substr);
  if (pos != absl::string_view::npos) {
    result = static_cast<int64_t>(pos) + start;
  } else if (substr.empty()) {
    result = start;
  }
  return result;
}
}  // namespace

OptionalValue<int64_t> FindSubstringOp::operator()(
    const Bytes& str, const Bytes& substr, OptionalValue<int64_t> start,
    OptionalValue<int64_t> end) const {
  if (AdjustIndexes(str.size(), start, end)) {
    return FindSubstring(str, substr, start.value, end.value);
  } else {
    return {};
  }
}

OptionalValue<int64_t> FindSubstringOp::operator()(
    const Text& str, const Text& substr, OptionalValue<int64_t> start,
    OptionalValue<int64_t> end) const {
  auto index = UTF8StringIndex(absl::string_view(str));
  if (AdjustIndexes(index.size() - 1, start, end)) {
    auto byte_offset =
        FindSubstring(absl::string_view(str), absl::string_view(substr),
                      index[start.value], index[end.value]);
    if (byte_offset.present) {
      return std::distance(
          index.begin(),
          std::lower_bound(index.begin(), index.end(), byte_offset.value));
    }
  }
  return {};
}

namespace {
OptionalValue<int64_t> FindLastSubstring(absl::string_view str,
                                         absl::string_view substr,
                                         int64_t start, int64_t end) {
  OptionalValue<int64_t> result;
  auto pos = str.substr(start, end - start).rfind(substr);
  if (pos != absl::string_view::npos) {
    result = static_cast<int64_t>(pos) + start;
  } else if (substr.empty()) {
    result = start;
  }
  return result;
}
}  // namespace

OptionalValue<int64_t> FindLastSubstringOp::operator()(
    const Bytes& str, const Bytes& substr, OptionalValue<int64_t> start,
    OptionalValue<int64_t> end) const {
  if (AdjustIndexes(str.size(), start, end)) {
    return FindLastSubstring(str, substr, start.value, end.value);
  } else {
    return {};
  }
}

OptionalValue<int64_t> FindLastSubstringOp::operator()(
    const Text& str, const Text& substr, OptionalValue<int64_t> start,
    OptionalValue<int64_t> end) const {
  auto index = UTF8StringIndex(absl::string_view(str));
  if (AdjustIndexes(index.size() - 1, start, end)) {
    auto byte_offset =
        FindLastSubstring(absl::string_view(str), absl::string_view(substr),
                          index[start.value], index[end.value]);
    if (byte_offset.present) {
      return std::distance(
          index.begin(),
          std::lower_bound(index.begin(), index.end(), byte_offset.value));
    }
  }
  return {};
}

namespace {

// Returns substring of `str` in the byte offset range [start, end).
std::string Substring(absl::string_view str, int64_t start, int64_t end) {
  return std::string(str.substr(start, end - start));
}

}  // namespace

absl::string_view SubstringOp::operator()(absl::string_view str,
                                          OptionalValue<int64_t> start,
                                          OptionalValue<int64_t> end) const {
  if (AdjustIndexes(str.size(), start, end)) {
    return str.substr(start.value, end.value - start.value);
  } else {
    return "";
  }
}

Bytes SubstringOp::operator()(const Bytes& str, OptionalValue<int64_t> start,
                              OptionalValue<int64_t> end) const {
  return Bytes((*this)(absl::string_view(str), start, end));
}

Text SubstringOp::operator()(const Text& str, OptionalValue<int64_t> start,
                             OptionalValue<int64_t> end) const {
  auto index = UTF8StringIndex(absl::string_view(str));
  std::string substr;
  if (AdjustIndexes(index.size() - 1, start, end)) {
    substr =
        Substring(absl::string_view(str), index[start.value], index[end.value]);
  }
  return Text(substr);
}

}  // namespace arolla
