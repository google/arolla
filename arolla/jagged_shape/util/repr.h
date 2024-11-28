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
#ifndef AROLLA_JAGGED_SHAPE_UTIL_REPR_H_
#define AROLLA_JAGGED_SHAPE_UTIL_REPR_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "absl//types/span.h"

namespace arolla {

// Returns a compact representation of `split_points` using sizes. If the splits
// are all of the same size, a single value is used instead of multiple. Lists
// of sizes are truncated with `max_part_size` values shown from the front and
// back of the span.
//
// Examples:
//   CompactSplitPointsAsSizesRepr({0, 1, 2, 3, 6, 7}, /*max_part_size=*/2)
//     -> "[1, 1, ..., 3, 1]"
//   CompactSplitPointsAsSizesRepr({0, 1, 2, 3}, /*max_part_size=*/2)
//     -> "1"
std::string CompactSplitPointsAsSizesRepr(
    absl::Span<const int64_t> split_points, size_t max_part_size);

}  // namespace arolla

#endif  // AROLLA_JAGGED_SHAPE_UTIL_REPR_H_
