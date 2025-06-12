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
#include "arolla/util/switch_index.h"

#include <string>

#include "gtest/gtest.h"

namespace arolla::testing {
namespace {

template <int N>
void test_switch_index() {
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(std::to_string(i), switch_index<N>(i, [i](auto arg) {
                constexpr int constexpr_i = arg();
                EXPECT_EQ(i, constexpr_i);
                return std::to_string(constexpr_i);
              }));
  }
}

TEST(SwitchIndex, switch_index_32) { test_switch_index<32>(); }

TEST(SwitchIndex, switch_index_64) { test_switch_index<64>(); }

}  // namespace
}  // namespace arolla::testing
