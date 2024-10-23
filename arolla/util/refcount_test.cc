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
#include "arolla/util/refcount.h"

#include <type_traits>

#include "gtest/gtest.h"

namespace arolla {
namespace {

TEST(RefcountTest, Copyable) {
  EXPECT_FALSE(std::is_copy_constructible_v<Refcount>);
  EXPECT_FALSE(std::is_move_constructible_v<Refcount>);
  EXPECT_FALSE(std::is_copy_assignable_v<Refcount>);
  EXPECT_FALSE(std::is_move_assignable_v<Refcount>);
}

TEST(RefcountTest, Decrement) {
  {
    Refcount refcount;
    EXPECT_FALSE(refcount.decrement());
  }
  {
    Refcount refcount;
    EXPECT_FALSE(refcount.skewed_decrement());
  }
}

TEST(RefcountTest, IncrementDecrement) {
  constexpr int N = 10;
  {
    Refcount refcount;
    for (int i = 0; i < N; ++i) {
      refcount.increment();
    }
    for (int i = 0; i < N; ++i) {
      ASSERT_TRUE(refcount.decrement());
    }
    ASSERT_FALSE(refcount.decrement());
  }
  {
    Refcount refcount;
    for (int i = 0; i < N; ++i) {
      refcount.increment();
    }
    for (int i = 0; i < N; ++i) {
      ASSERT_TRUE(refcount.skewed_decrement());
    }
    ASSERT_FALSE(refcount.skewed_decrement());
  }
}

}  // namespace
}  // namespace arolla
