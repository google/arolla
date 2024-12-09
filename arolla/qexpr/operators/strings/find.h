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

// strings.contains operator implementation.
struct ContainsOp {
  OptionalUnit operator()(absl::string_view str,
                          absl::string_view substr) const;

  template <typename StringT>
  OptionalUnit operator()(const StringT& str, const StringT& substr) const {
    return this->operator()(absl::string_view(str), absl::string_view(substr));
  }
};

// strings.count operator implementation.
struct SubstringOccurrenceCountOp {
  int64_t operator()(absl::string_view str, absl::string_view substr) const;

  template <typename StringT>
  int64_t operator()(const StringT& str, const StringT& substr) const {
    return this->operator()(absl::string_view(str), absl::string_view(substr));
  }
};

// strings.find operator implementation on Bytes.
class BytesFindSubstringOp {
 public:
  OptionalValue<int64_t> operator()(absl::string_view str,
                                    absl::string_view substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;
  OptionalValue<int64_t> operator()(const Bytes& str, const Bytes& substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;
};

// strings.find operator implementation on Text.
class TextFindSubstringOp {
 public:
  OptionalValue<int64_t> operator()(absl::string_view str,
                                    absl::string_view substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;
  OptionalValue<int64_t> operator()(const Text& str, const Text& substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;
};

// strings.rfind operator implementation on Bytes.
class BytesFindLastSubstringOp {
 public:
  OptionalValue<int64_t> operator()(absl::string_view str,
                                    absl::string_view substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;
  OptionalValue<int64_t> operator()(const Bytes& str, const Bytes& substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;
};

// strings.rfind operator implementation on Text.
class TextFindLastSubstringOp {
 public:
  OptionalValue<int64_t> operator()(absl::string_view str,
                                    absl::string_view substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;
  OptionalValue<int64_t> operator()(const Text& str, const Text& substr,
                                    OptionalValue<int64_t> start,
                                    OptionalValue<int64_t> end) const;
};

// strings.substr operator implementation on Bytes.
struct BytesSubstringOp {
  Bytes operator()(absl::string_view str, OptionalValue<int64_t> start,
                   OptionalValue<int64_t> end) const;
  Bytes operator()(const Bytes& str, OptionalValue<int64_t> start,
                   OptionalValue<int64_t> end) const;
};

// strings.substr operator implementation on Text.
struct TextSubstringOp {
  Text operator()(absl::string_view str, OptionalValue<int64_t> start,
                  OptionalValue<int64_t> end) const;
  Text operator()(const Text& str, OptionalValue<int64_t> start,
                  OptionalValue<int64_t> end) const;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_STRINGS_FIND_H_
