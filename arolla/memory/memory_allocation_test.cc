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
#include "arolla/memory/memory_allocation.h"

#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "arolla/memory/frame.h"

namespace arolla {
namespace {

struct DeleteCounter {
  ~DeleteCounter() { ++deletions; }
  static int deletions;
};
int DeleteCounter::deletions = 0;

TEST(MemoryAllocationTest, TestEmptyValues) {
  FrameLayout::Builder builder;
  // Object with non-trivial destructor.
  auto slot = builder.AddSlot<std::unique_ptr<DeleteCounter>>();
  auto layout = std::move(builder).Build();

  ASSERT_EQ(DeleteCounter::deletions, 0);

  MemoryAllocation alloc(&layout);
  EXPECT_TRUE(alloc.IsValid());
  auto owned_ptr = std::make_unique<DeleteCounter>();
  auto ptr = owned_ptr.get();
  alloc.frame().Set(slot, std::move(owned_ptr));
  EXPECT_EQ(alloc.frame().Get(slot).get(), ptr);

  // Test move constructor.
  MemoryAllocation new_alloc(std::move(alloc));
  EXPECT_TRUE(new_alloc.IsValid());
  EXPECT_FALSE(alloc.IsValid());  // NOLINT(bugprone-use-after-move)
  EXPECT_EQ(new_alloc.frame().Get(slot).get(), ptr);
  EXPECT_EQ(DeleteCounter::deletions, 0);

  // Test move assignment.
  MemoryAllocation newer_alloc(&layout);
  EXPECT_TRUE(newer_alloc.IsValid());
  newer_alloc.frame().Set(slot, std::make_unique<DeleteCounter>());
  newer_alloc = std::move(new_alloc);
  EXPECT_TRUE(newer_alloc.IsValid());
  EXPECT_FALSE(new_alloc.IsValid());  // NOLINT(bugprone-use-after-move)
  EXPECT_EQ(newer_alloc.frame().Get(slot).get(), ptr);
  EXPECT_EQ(DeleteCounter::deletions, 1);
}

}  // namespace
}  // namespace arolla
