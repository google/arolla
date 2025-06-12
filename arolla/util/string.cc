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
#include "arolla/util/string.h"

#include <cstddef>
#include <string>

#include "absl/log/check.h"

namespace arolla {

std::string Truncate(std::string str, size_t max_length) {
  DCHECK_GT(max_length, 3);
  if (str.size() > max_length) {
    str.resize(max_length);
    str.replace(max_length - 3, 3, "...");
  }
  return str;
}

}  // namespace arolla
