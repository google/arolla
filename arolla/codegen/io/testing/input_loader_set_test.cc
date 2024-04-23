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
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/codegen/io/testing/test_input_loader_set_spec_by_value.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::arolla::testing::InputLoaderSupports;

TEST(InputLoaderTest, TestGetLoader) {
  auto i32 = GetQType<int>();
  auto f64 = GetQType<double>();
  for (const auto& input_loader :
       {::my_namespace::ShardedLoader(), ::my_namespace::NonShardedLoader()}) {
    EXPECT_THAT(input_loader,
                InputLoaderSupports(
                    {{"self", i32}, {"bit[\"0\"]", i32}, {"double", f64}}));

    FrameLayout::Builder layout_builder;
    auto self_slot = layout_builder.AddSlot<int>();
    auto bit0_slot = layout_builder.AddSlot<int>();
    auto double_slot = layout_builder.AddSlot<double>();
    ASSERT_OK_AND_ASSIGN(BoundInputLoader<int> bound_input_loader,
                         input_loader->Bind({
                             {"self", TypedSlot::FromSlot(self_slot)},
                             {"bit[\"0\"]", TypedSlot::FromSlot(bit0_slot)},
                             {"double", TypedSlot::FromSlot(double_slot)},
                         }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();

    ASSERT_OK(bound_input_loader(19, frame));
    EXPECT_EQ(frame.Get(self_slot), 19);
    EXPECT_EQ(frame.Get(bit0_slot), 1);
    EXPECT_EQ(frame.Get(double_slot), 19.0);
  }
}

}  // namespace
}  // namespace arolla
