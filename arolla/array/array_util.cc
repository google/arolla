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
#include "arolla/array/array_util.h"

#include <cstdint>
#include <vector>

#include "arolla/array/array.h"
#include "arolla/util/unit.h"

namespace arolla {

std::vector<int64_t> ArrayFirstPresentIds(const Array<Unit>& array,
                                          int64_t max_count) {
  std::vector<int64_t> res;
  if (max_count <= 0) {
    return res;
  }
  res.reserve(max_count);
  if (array.IsDenseForm() || array.HasMissingIdValue()) {
    int64_t index = 0;
    while (index < array.size() && res.size() < max_count) {
      if (array[index].present) res.push_back(index);
      index++;
    }
  } else {
    int64_t offset = 0;
    while (offset < array.dense_data().size() && res.size() < max_count) {
      if (array.dense_data().present(offset)) {
        res.push_back(array.id_filter().IdsOffsetToId(offset));
      }
      offset++;
    }
  }
  return res;
}

std::vector<int64_t> ArrayLastPresentIds(const Array<Unit>& array,
                                         int64_t max_count) {
  std::vector<int64_t> res;
  if (max_count <= 0) {
    return res;
  }
  res.reserve(max_count);
  if (array.IsDenseForm() || array.HasMissingIdValue()) {
    int64_t index = array.size() - 1;
    while (index >= 0 && res.size() < max_count) {
      if (array[index].present) res.push_back(index);
      index--;
    }
  } else {
    int64_t offset = array.dense_data().size() - 1;
    while (offset >= 0 && res.size() < max_count) {
      if (array.dense_data().present(offset)) {
        res.push_back(array.id_filter().IdsOffsetToId(offset));
      }
      offset--;
    }
  }
  return res;
}

}  // namespace arolla
