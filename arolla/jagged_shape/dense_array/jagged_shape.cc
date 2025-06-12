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
#include "arolla/jagged_shape/dense_array/jagged_shape.h"

#include <sstream>
#include <utility>

#include "arolla/jagged_shape/util/repr.h"
#include "arolla/util/repr.h"
#include "arolla/util/string.h"

namespace arolla {

ReprToken ReprTraits<JaggedDenseArrayShape>::operator()(
    const JaggedDenseArrayShape& value) const {
  std::ostringstream result;
  result << "JaggedShape(";
  bool first = true;
  for (const auto& edge : value.edges()) {
    result << NonFirstComma(first)
           << CompactSplitPointsAsSizesRepr(edge.edge_values().values.span(),
                                            /*max_part_size=*/3);
  }
  result << ")";
  return ReprToken{std::move(result).str()};
}

}  // namespace arolla
