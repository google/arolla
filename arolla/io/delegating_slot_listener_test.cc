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
#include "arolla/io/delegating_slot_listener.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/io/accessors_slot_listener.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::Eq;

struct TestStruct {
  int a;
  double b;
};

struct OuterTestStruct {
  TestStruct* a;
  int b;
};

TEST(SlotListenerTest, DelegateSlotListener) {
  ASSERT_OK_AND_ASSIGN(auto listener_struct,
                       CreateAccessorsSlotListener<TestStruct>(
                           "a", [](int a, TestStruct* s) { s->a = a; },  //
                           "b", [](double b, TestStruct* s) { s->b = b; }));
  auto accessor = [](OuterTestStruct* s) -> TestStruct* { return s->a; };
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SlotListener<OuterTestStruct>> delegate_slot_listener,
      CreateDelegatingSlotListener<OuterTestStruct>(
          MakeNotOwningSlotListener(listener_struct.get()), accessor));
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SlotListener<OuterTestStruct>>
          renamed_delegate_slot_listener,
      CreateDelegatingSlotListener<OuterTestStruct>(
          MakeNotOwningSlotListener(listener_struct.get()), accessor,
          /*name_prefix=*/"p_"));
  for (const auto& [prefix, slot_listener] : std::vector<
           std::pair<std::string, const SlotListener<OuterTestStruct>*>>{
           {"", delegate_slot_listener.get()},
           {"p_", renamed_delegate_slot_listener.get()}}) {
    EXPECT_THAT(slot_listener->GetQTypeOf(prefix + "a"),
                Eq(GetQType<int32_t>()));
    EXPECT_THAT(slot_listener->GetQTypeOf(prefix + "b"),
                Eq(GetQType<double>()));
    EXPECT_THAT(slot_listener->SuggestAvailableNames(),
                ElementsAre(prefix + "a", prefix + "b"));
    FrameLayout::Builder layout_builder;
    auto a_slot = layout_builder.AddSlot<int>();
    auto b_slot = layout_builder.AddSlot<double>();
    ASSERT_OK_AND_ASSIGN(BoundSlotListener<OuterTestStruct> bound_slot_listener,
                         slot_listener->Bind({
                             {prefix + "a", TypedSlot::FromSlot(a_slot)},
                             {prefix + "b", TypedSlot::FromSlot(b_slot)},
                         }));
    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    alloc.frame().Set(a_slot, 5);
    alloc.frame().Set(b_slot, 3.5);

    TestStruct ts;
    OuterTestStruct ots{&ts, -1};
    ASSERT_OK(bound_slot_listener(alloc.frame(), &ots));
    EXPECT_EQ(ts.a, 5);
    EXPECT_EQ(ts.b, 3.5);
  }
}

}  // namespace
}  // namespace arolla
