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
#include "arolla/codegen/io/testing/test_empty_slot_listener.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/proto/test.pb.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace arolla {
namespace {

using ::arolla::testing::IsOk;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::IsNull;

TEST(EmptySlotListenerTest, TestGetListener) {
  auto slot_listener = ::my_namespace::GetEmptySlotListener();
  EXPECT_THAT(slot_listener->GetQTypeOf("foo"), IsNull());
  EXPECT_THAT(slot_listener->SuggestAvailableNames(), IsEmpty());

  FrameLayout::Builder layout_builder;
  ASSERT_OK_AND_ASSIGN(
      BoundSlotListener<testing_namespace::Root> bound_slot_listener,
      slot_listener->Bind({}));
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();
  testing_namespace::Root root;
  EXPECT_THAT(bound_slot_listener(frame, &root), IsOk());
  EXPECT_THAT(root.ByteSizeLong(), Eq(0));
}

}  // namespace
}  // namespace arolla
