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
#include <variant>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash_testing.h"
#include "arolla/memory/buffer.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Not;

using Void = std::monostate;

TEST(VoidBuffer, Simple) {
  VoidBuffer buffer(4);
  EXPECT_TRUE(buffer.is_owner());
  EXPECT_THAT(buffer, Not(IsEmpty()));
  EXPECT_THAT(buffer.size(), Eq(4));
  EXPECT_THAT(buffer.front(), Eq(Void{}));
  EXPECT_THAT(buffer.back(), Eq(Void()));
  EXPECT_THAT(buffer, ElementsAre(Void(), Void(), Void(), Void()));
  EXPECT_EQ(buffer, VoidBuffer(4));
  EXPECT_NE(buffer, VoidBuffer(5));
  EXPECT_THAT(buffer.Slice(1, 2), Eq(VoidBuffer(2)));
}

TEST(VoidBuffer, SupportsAbslHash) {
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(
      {Buffer<Void>(0), Buffer<Void>(10)}));
}

}  // namespace
}  // namespace arolla
