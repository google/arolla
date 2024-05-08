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
#include <array>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/codegen/io/testing/test_sharded_slot_listener_set.h"
#include "arolla/codegen/io/testing/test_slot_listener_set.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::testing::Eq;

TEST(InputLoaderTest, TestGetProtoLoader) {
  auto listener1 = ::aaa::GetListener1();
  auto listener2 = ::bbb::GetListener2();
  auto sharded_listener1 = ::aaa::GetShardedListener1();
  auto sharded_listener2 = ::bbb::GetShardedListener2();

  EXPECT_THAT(listener1->GetQTypeOf("a5"), Eq(GetQType<int>()));
  EXPECT_THAT(listener1->GetQTypeOf("a3"), Eq(GetQType<int>()));
  EXPECT_THAT(listener2->GetQTypeOf("a2"), Eq(GetQType<float>()));
  EXPECT_THAT(listener2->GetQTypeOf("a3"), Eq(GetQType<float>()));

  EXPECT_THAT(sharded_listener1->GetQTypeOf("a5"), Eq(GetQType<int>()));
  EXPECT_THAT(sharded_listener1->GetQTypeOf("a3"), Eq(GetQType<int>()));
  EXPECT_THAT(sharded_listener2->GetQTypeOf("a2"), Eq(GetQType<float>()));
  EXPECT_THAT(sharded_listener2->GetQTypeOf("a3"), Eq(GetQType<float>()));

  FrameLayout::Builder layout_builder;
  auto i1_slot = layout_builder.AddSlot<int>();
  auto i2_slot = layout_builder.AddSlot<int>();
  auto f1_slot = layout_builder.AddSlot<float>();
  auto f2_slot = layout_builder.AddSlot<float>();

  ASSERT_OK_AND_ASSIGN(auto bound_listener1,
                       listener1->Bind({
                           {"a5", TypedSlot::FromSlot(i1_slot)},
                           {"a3", TypedSlot::FromSlot(i2_slot)},
                       }));
  ASSERT_OK_AND_ASSIGN(auto bound_listener2,
                       listener2->Bind({
                           {"a2", TypedSlot::FromSlot(f1_slot)},
                           {"a3", TypedSlot::FromSlot(f2_slot)},
                       }));
  ASSERT_OK_AND_ASSIGN(auto bound_sharded_listener1,
                       sharded_listener1->Bind({
                           {"a5", TypedSlot::FromSlot(i1_slot)},
                           {"a3", TypedSlot::FromSlot(i2_slot)},
                       }));
  ASSERT_OK_AND_ASSIGN(auto bound_sharded_listener2,
                       sharded_listener2->Bind({
                           {"a2", TypedSlot::FromSlot(f1_slot)},
                           {"a3", TypedSlot::FromSlot(f2_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  frame.Set(i1_slot, 1);
  frame.Set(i2_slot, 2);
  frame.Set(f1_slot, 3);
  frame.Set(f2_slot, 4);

  ::std::array<int, 10> out1a;
  ::std::array<float, 5> out2a;
  ::std::array<int, 10> out1b;
  ::std::array<float, 5> out2b;

  ASSERT_OK(bound_listener1(frame, &out1a));
  ASSERT_OK(bound_listener2(frame, &out2a));
  ASSERT_OK(bound_sharded_listener1(frame, &out1b));
  ASSERT_OK(bound_sharded_listener2(frame, &out2b));

  EXPECT_EQ(out1a[5], 1);
  EXPECT_EQ(out1a[3], 2);
  EXPECT_EQ(out2a[2], 3);
  EXPECT_EQ(out2a[3], 4);

  EXPECT_EQ(out1b[5], 1);
  EXPECT_EQ(out1b[3], 2);
  EXPECT_EQ(out2b[2], 3);
  EXPECT_EQ(out2b[3], 4);
}

}  // namespace
}  // namespace arolla
