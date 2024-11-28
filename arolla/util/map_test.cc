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
#include "arolla/util/map.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//container/flat_hash_map.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::TypedEq;

struct V {};

TEST(SortedMapKeys, small) {
  EXPECT_THAT(SortedMapKeys(absl::flat_hash_map<int, V>{}), ElementsAre());
  EXPECT_THAT(SortedMapKeys(absl::flat_hash_map<char, V>{{0, V{}}}),
              ElementsAre(TypedEq<char>(0)));
  EXPECT_THAT(SortedMapKeys(
                  absl::flat_hash_map<std::string, V>{{"1", V{}}, {"0", V{}}}),
              ElementsAre("0", "1"));
}

TEST(SortedMapKeys, big) {
  absl::flat_hash_map<std::string, V> m;
  for (int i = 1000; i != 10000; ++i) {
    m.emplace(std::to_string(i), V{});
  }
  std::vector<std::string> keys = SortedMapKeys(m);
  EXPECT_EQ(keys.size(), 9000);
  auto it = keys.begin();
  for (int i = 1000; i != 10000; ++i, ++it) {
    EXPECT_EQ(*it, std::to_string(i));
  }
}

}  // namespace
}  // namespace arolla
