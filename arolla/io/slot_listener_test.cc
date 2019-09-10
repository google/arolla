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
#include "arolla/io/slot_listener.h"

#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/io/accessors_slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

using ::testing::Eq;

struct TestStruct {
  int a;
  double b;
};

TEST(SlotListenerTest, MakeNotOwningSlotListener) {
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SlotListener<TestStruct>> wrapped_listener,
      CreateAccessorsSlotListener<TestStruct>(
          "a", [](int a, TestStruct* s) { s->a = a; }));

  std::unique_ptr<SlotListener<TestStruct>> not_owning_listener =
      MakeNotOwningSlotListener(wrapped_listener.get());

  EXPECT_THAT(not_owning_listener->GetQTypeOf("a"),
              Eq(wrapped_listener->GetQTypeOf("a")));
  EXPECT_THAT(not_owning_listener->SuggestAvailableNames(),
              Eq(wrapped_listener->SuggestAvailableNames()));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  ASSERT_OK_AND_ASSIGN(
      BoundSlotListener<TestStruct> bound_slot_listener,
      not_owning_listener->Bind({{"a", TypedSlot::FromSlot(a_slot)}}));

  MemoryAllocation alloc(&memory_layout);
  alloc.frame().Set(a_slot, 57);
  TestStruct s;
  ASSERT_OK(bound_slot_listener(alloc.frame(), &s));
  EXPECT_EQ(s.a, 57);
}

TEST(SlotListenerTest, MakeSharedOwningSlotListener) {
  std::unique_ptr<SlotListener<TestStruct>> shared_owning_listener;

  {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<const SlotListener<TestStruct>> wrapped_listener,
        CreateAccessorsSlotListener<TestStruct>(
            "a", [](int a, TestStruct* s) { s->a = a; }));
    shared_owning_listener = MakeSharedOwningSlotListener(wrapped_listener);
    EXPECT_THAT(shared_owning_listener->GetQTypeOf("a"),
                Eq(wrapped_listener->GetQTypeOf("a")));
    EXPECT_THAT(shared_owning_listener->SuggestAvailableNames(),
                Eq(wrapped_listener->SuggestAvailableNames()));
  }

  // wrapped_listener goes out of scope, but it is still owned by
  // shared_owning_listener.

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  ASSERT_OK_AND_ASSIGN(
      BoundSlotListener<TestStruct> bound_slot_listener,
      shared_owning_listener->Bind({{"a", TypedSlot::FromSlot(a_slot)}}));

  MemoryAllocation alloc(&memory_layout);
  alloc.frame().Set(a_slot, 57);
  TestStruct s;
  ASSERT_OK(bound_slot_listener(alloc.frame(), &s));
  EXPECT_EQ(s.a, 57);
}

}  // namespace
}  // namespace arolla
