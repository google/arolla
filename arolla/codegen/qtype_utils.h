// Copyright 2025 Google LLC
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
#ifndef AROLLA_CODEGEN_QTYPE_UTILS_H_
#define AROLLA_CODEGEN_QTYPE_UTILS_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"

namespace arolla::codegen {

// Low binary size builder for `std::vector<std::pair<std::string, QTypePtr>>`.
// This class hiding code for `std::pair` and `std::string` construction.
// Overall it is saving ~190 bytes of binary size per element.
class NamedQTypeVectorBuilder {
 public:
  NamedQTypeVectorBuilder() = default;

  std::vector<std::pair<std::string, QTypePtr>> Build() &&;

  // Adds an element. The name will have the provided length.
  // If suffix is shorter than length, the rest will be taken from the previous
  // name's prefix.
  void AddFromCommonPrefixWithPrevious(
      size_t length,
      const char* suffix, QTypePtr qtype);

 private:
  std::vector<std::pair<std::string, QTypePtr>> types_;
};

// Builds `std::vector<std::pair<std::string, QTypePtr>>` from prefix-compressed
// name metadata and QType getters.
//
// `name_lengths` contains the full length of each name.
// `concatenated_suffixes` contains null-terminated suffixes for each name
// concatenated sequentially. If a suffix is shorter than `name_lengths[i]`, the
// prefix is taken from the previous name.
// `qtype_getters` contains function pointers returning the `QTypePtr` for each
// element.
std::vector<std::pair<std::string, QTypePtr>> BuildNamedQTypeVector(
    absl::Span<const uint16_t> name_lengths, const char* concatenated_suffixes,
    absl::Span<QTypePtr (*const)()> qtype_getters);

}  // namespace arolla::codegen

#endif  // AROLLA_CODEGEN_QTYPE_UTILS_H_
