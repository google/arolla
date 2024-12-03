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
#ifndef AROLLA_QEXPR_OPERATORS_STRINGS_FIND_H_
#define AROLLA_QEXPR_OPERATORS_STRINGS_FIND_H_

// String operators related to finding or extracting a substring.

#include <cstdint>

#include "absl//strings/string_view.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {

// Returns kPresent if `str` contains `substr`.
struct ContainsOp {
  OptionalUnit operator()(absl::string_view str,
                          absl::string_view substr) const;

  template <typename StringT>
  OptionalUnit operator()(const StringT& str, const StringT& substr) const {
    return this->operator()(absl::string_view(str), absl::string_view(substr));
  }
};

// Counts occurrences of 'substr' in 'str'. If 'substr' is empty, returns
// one greater than the length of 'str'. In particular, if 'str' and 'substr'
// are both empty, returns 1.
struct SubstringOccurrenceCountOp {
  int64_t operator()(absl::string_view str, absl::string_view substr) const;

  template <typename StringT>
  int64_t operator()(const StringT& str, const StringT& substr) const {
    return this->operator()(absl::string_view(str), absl::string_view(substr));
  }
};

class FindSubstringOp {
 public:
  // Returns the offset of the first occurrence of `substr` in `str` within
  // the offset range of `[start, end)`, or failure_value if not found. The
  // units of `start`, `end`, and the return value are all byte offsets if
  // `StringT` is `Bytes` and codepoint offsets if `StringT` is `Text`.
  //
  // StringT is Bytes or Text
  // ResultT is int64_t or OptionalValue<int64_t>
  template <typename StringT, typename ResultT>
  ResultT operator()(const StringT& str, const StringT& substr,
                     OptionalValue<int64_t> start, OptionalValue<int64_t> end,
                     ResultT failure_value) const {
    auto tmp = this->operator()(str, substr, start, end);
    return (tmp.present) ? tmp.value : failure_value;
  }

 private:
  OptionalValue<int64_t> operator()(const Bytes& str, const Bytes& substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;

  OptionalValue<int64_t> operator()(const Text& str, const Text& substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;
};

class FindLastSubstringOp {
 public:
  // Returns the offset of the last occurrence of `substr` in `str` within
  // the offset range of `[start, end)`, or failure_value if not found. The
  // units of `start`, `end`, and the return value are all byte offsets if
  // `StringT` is `Bytes` and codepoint offsets if `StringT` is `Text`.
  //
  // StringT is Bytes or Text
  // ResultT is int64_t or OptionalValue<int64_t>
  template <typename StringT, typename ResultT>
  ResultT operator()(const StringT& str, const StringT& substr,
                     OptionalValue<int64_t> start, OptionalValue<int64_t> end,
                     ResultT failure_value) const {
    auto tmp = this->operator()(str, substr, start, end);
    return (tmp.present) ? tmp.value : failure_value;
  }

 private:
  OptionalValue<int64_t> operator()(const Bytes& str, const Bytes& substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;

  OptionalValue<int64_t> operator()(const Text& str, const Text& substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;
};

// Returns the substring of `str` over the range `[start, end)`. `start` and
// `end` represent byte offsets if `str` type is Bytes, and codepoint offsets
// if `str` type is `Text`.
struct SubstringOp {
  absl::string_view operator()(absl::string_view str,
                               OptionalValue<int64_t> start,
                               OptionalValue<int64_t> end) const;

  Bytes operator()(const Bytes& str, OptionalValue<int64_t> start,
                   OptionalValue<int64_t> end) const;

  Text operator()(const Text& str, OptionalValue<int64_t> start,
                  OptionalValue<int64_t> end) const;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_STRINGS_FIND_H_
