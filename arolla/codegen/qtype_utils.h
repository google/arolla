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
#ifndef AROLLA_CODEGEN_QTYPE_UTILS_H_
#define AROLLA_CODEGEN_QTYPE_UTILS_H_

#include <string>
#include <utility>
#include <vector>

#include "arolla/qtype/qtype.h"

namespace arolla::codegen {

// Low binary size builder for `std::vector<std::pair<std::string, QTypePtr>>`.
// This class hiding code for `std::pair` and `std::string` construction.
// Overall it is saving ~190 bytes of binary size per element.
class NamedQTypeVectorBuilder {
 public:
  NamedQTypeVectorBuilder() = default;

  // Adds an element to the end of the vector.
  void Add(const char* name, QTypePtr qtype);

  std::vector<std::pair<std::string, QTypePtr>> Build() &&;

 private:
  std::vector<std::pair<std::string, QTypePtr>> types_;
};

}  // namespace arolla::codegen

#endif  // AROLLA_CODEGEN_QTYPE_UTILS_H_
