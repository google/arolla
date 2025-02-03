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
#include "arolla/io/chain_slot_listener.h"

#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/io/accessors_slot_listener.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;

struct TestStruct {
  int a;
  double b;
  int c;
};

TEST(SlotListenerTest, ChainSlotListenerErrors) {
  ASSERT_OK_AND_ASSIGN(auto loader1,
                       CreateAccessorsSlotListener<TestStruct>(
                           "a", [](int a, TestStruct* s) { s->a = a; },  //
                           "b", [](double b, TestStruct* s) { s->b = b; }));
  ASSERT_OK_AND_ASSIGN(
      auto loader2,
      CreateAccessorsSlotListener<TestStruct>(
          // The type is different from the previous definition for "a" slot.
          "a", [](double b, TestStruct* s) { s->b = b; }));
  ASSERT_OK_AND_ASSIGN(auto chain_listener,
                       ChainSlotListener<TestStruct>::Build(
                           std::move(loader1), std::move(loader2)));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<double>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  EXPECT_THAT(
      chain_listener->Bind({
          {"a", TypedSlot::FromSlot(a_slot)},
          {"b", TypedSlot::FromSlot(b_slot)},
      }),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("slots/types match errors:slot types mismatch: "
                         "a{expected:FLOAT64, actual:INT32}")));
}

TEST(SlotListenerTest, ChainSlotListener) {
  auto create_listener1 = []() {
    return CreateAccessorsSlotListener<TestStruct>(
               "a", [](int a, TestStruct* s) { s->a = a; })
        .value();
  };
  auto create_listener2 = []() {
    return CreateAccessorsSlotListener<TestStruct>(
               "b", [](double b, TestStruct* s) { s->b = b; },  //
               "a", [](int a, TestStruct* s) { s->c = a; })
        .value();
  };
  ASSERT_OK_AND_ASSIGN(auto chain_listener1,
                       ChainSlotListener<TestStruct>::Build(
                           create_listener1(), create_listener2()));
  std::vector<std::unique_ptr<SlotListener<TestStruct>>> listeners;
  listeners.push_back(create_listener1());
  listeners.push_back(create_listener2());
  ASSERT_OK_AND_ASSIGN(
      auto chain_listener2,
      ChainSlotListener<TestStruct>::Build(std::move(listeners)));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<double>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  for (auto chain_listener : {chain_listener1.get(), chain_listener2.get()}) {
    EXPECT_THAT(chain_listener->GetQTypeOf("a"), Eq(GetQType<int>()));
    EXPECT_THAT(chain_listener->GetQTypeOf("b"), Eq(GetQType<double>()));
    EXPECT_THAT(chain_listener->SuggestAvailableNames(),
                ElementsAre("a", "b", "a"));

    ASSERT_OK_AND_ASSIGN(BoundSlotListener<TestStruct> bound_chain_listener,
                         chain_listener->Bind({
                             {"a", TypedSlot::FromSlot(a_slot)},
                             {"b", TypedSlot::FromSlot(b_slot)},
                         }));

    MemoryAllocation alloc(&memory_layout);

    alloc.frame().Set(a_slot, 5);
    alloc.frame().Set(b_slot, 3.5);
    TestStruct s;
    ASSERT_OK(bound_chain_listener(alloc.frame(), &s));
    EXPECT_EQ(s.a, 5);
    EXPECT_EQ(s.b, 3.5);
    EXPECT_EQ(s.c, 5);
  }
}

}  // namespace
}  // namespace arolla
