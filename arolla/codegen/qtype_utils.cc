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
#include "arolla/codegen/qtype_utils.h"

#include <string>
#include <utility>
#include <vector>

#include "arolla/qtype/qtype.h"

namespace arolla::codegen {

void NamedQTypeVectorBuilder::Add(const char* name, QTypePtr qtype) {
  types_.emplace_back(name, qtype);
}

std::vector<std::pair<std::string, QTypePtr>>
NamedQTypeVectorBuilder::Build() && {
  return std::move(types_);
}

}  // namespace arolla::codegen
