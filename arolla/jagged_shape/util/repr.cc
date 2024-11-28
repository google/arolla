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
#include "arolla/jagged_shape/util/repr.h"

#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>
#include <utility>

#include "absl//algorithm/container.h"
#include "absl//strings/str_cat.h"
#include "absl//types/span.h"
#include "arolla/util/string.h"

namespace arolla {

std::string CompactSplitPointsAsSizesRepr(
    absl::Span<const int64_t> split_points, size_t max_part_size) {
  if (split_points.size() <= 1) {
    return "[]";
  }
  // If there is a common size, simply print it.
  int64_t size = split_points[1] - split_points[0];
  if (absl::c_adjacent_find(split_points, [size](int64_t a, int64_t b) {
        return b - a != size;
      }) == split_points.end()) {
    return absl::StrCat(size);
  }
  // Otherwise, print "all" sizes.
  std::ostringstream result;
  result << "[";
  bool first = true;
  const auto sizes_size = split_points.size() - 1;
  if (sizes_size <= 2 * max_part_size) {
    for (size_t i = 0; i < sizes_size; ++i) {
      result << NonFirstComma(first) << split_points[i + 1] - split_points[i];
    }
  } else {
    for (size_t i = 0; i < max_part_size; ++i) {
      result << NonFirstComma(first) << split_points[i + 1] - split_points[i];
    }
    result << NonFirstComma(first) << "...";
    for (size_t i = sizes_size - max_part_size; i < sizes_size; ++i) {
      result << NonFirstComma(first) << split_points[i + 1] - split_points[i];
    }
  }
  result << "]";
  return std::move(result).str();
}

}  // namespace arolla
