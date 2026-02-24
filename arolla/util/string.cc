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

#include <algorithm>
#include <cstddef>
#include <string>

#include "absl/log/check.h"
#include "absl/strings/string_view.h"

namespace arolla {

std::string Truncate(std::string str, size_t max_length) {
  DCHECK_GT(max_length, 3);
  if (str.size() > max_length) {
    str.resize(max_length);
    str.replace(max_length - 3, 3, "...");
  }
  return str;
}

bool IsWithinOneTypo(absl::string_view lhs, absl::string_view rhs) {
  const size_t n = lhs.size();
  const size_t m = rhs.size();
  const size_t l =
      std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end()).first -
      lhs.begin();
  const size_t r =
      std::mismatch(lhs.rbegin(), lhs.rend() - l, rhs.rbegin(), rhs.rend() - l)
          .first -
      lhs.rbegin();
  // Handle insertion, deletion, and substitution.
  if (n - l - r <= 1 && m - l - r <= 1) {
    return true;
  }
  // Handle transposition.
  return (l + r + 2 == n && n == m && lhs[l] == rhs[l + 1] &&
          lhs[l + 1] == rhs[l]);
}

}  // namespace arolla
