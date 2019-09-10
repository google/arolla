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
#include "arolla/util/memory.h"

#include <cstdint>
#include <vector>

#include "gtest/gtest.h"

namespace arolla {
namespace {

TEST(Memory, IsAlignedPtr) {
  std::vector<char> range(128, 0);
  for (auto& ref : range) {
    EXPECT_EQ(IsAlignedPtr(32, &ref),
              reinterpret_cast<uintptr_t>(&ref) % 32 == 0);
  }
}

TEST(Memory, AlignedAlloc) {
  std::vector<MallocPtr> ptrs;
  for (int i = 0; i < 100; ++i) {
    ptrs.push_back(AlignedAlloc(Alignment{64}, 3));
  }
  for (const auto& ptr : ptrs) {
    EXPECT_TRUE(IsAlignedPtr(64, ptr.get()));
  }
  EXPECT_NE(AlignedAlloc(Alignment{1}, 0).get(), nullptr);
  EXPECT_NE(AlignedAlloc(Alignment{1}, 64).get(), nullptr);
}

}  // namespace
}  // namespace arolla
