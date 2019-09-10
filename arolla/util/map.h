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
#ifndef AROLLA_UTIL_MAP_H_
#define AROLLA_UTIL_MAP_H_

#include <vector>
#include <algorithm>

namespace arolla {

// Returns sorted vector of map keys.
// Useful for simple deterministic iteration over unordered containers.
template <class Map>
std::vector<typename Map::key_type> SortedMapKeys(
    const Map& map) {
  std::vector<typename Map::key_type> result;
  result.reserve(map.size());
  for (const auto& item : map) {
    result.push_back(item.first);
  }
  std::sort(result.begin(), result.end());
  return result;
}

}  // namespace arolla

#endif  // AROLLA_UTIL_MAP_H_
