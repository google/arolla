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
#include "arolla/util/demangle.h"

#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace arolla {
namespace {

using ::testing::Eq;
using ::testing::MatchesRegex;

TEST(DemangleTest, TypeName) {
  EXPECT_THAT(TypeName<int>(), Eq("int"));
  EXPECT_THAT(TypeName<int32_t>(), Eq("int"));
  EXPECT_THAT(TypeName<std::vector<int>>(), MatchesRegex("std::.*vector.*"));
}

}  // namespace
}  // namespace arolla
