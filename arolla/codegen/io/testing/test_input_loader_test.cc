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
#include "arolla/codegen/io/testing/test_input_loader.h"

#include <array>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/codegen/io/testing/test_generator_based_input_loader.h"
#include "arolla/codegen/io/testing/test_input_loader_over_sharded.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::arolla::testing::InputLoaderSupports;
using ::testing::Eq;
using ::testing::NotNull;

TEST(InputLoaderTest, TestGetLoader) {
  auto i32 = GetQType<int>();
  auto f64 = GetQType<double>();
  for (const auto& input_loader :
       {::my_namespace::GetLoader(), ::my_namespace::GetOverShardedLoader()}) {
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

TEST(InputLoaderTest, GetOverShardedLoader) {
  std::vector<std::string> input_names = {"bit[\"0\"]", "double", "self"};
  auto ref_loader = ::my_namespace::GetLoader();
  auto shard_loaders = ::my_namespace::GetOverShardedLoader_Shards();
  ASSERT_EQ(shard_loaders.size(), input_names.size());

  for (size_t shard_id = 0; shard_id < input_names.size(); ++shard_id) {
    const auto& name = input_names[shard_id];
    ASSERT_THAT(shard_loaders[shard_id]->GetQTypeOf(name), NotNull())
        << "shard_id=" << shard_id << " name=" << name;
    EXPECT_THAT(shard_loaders[shard_id]->GetQTypeOf(name),
                Eq(ref_loader->GetQTypeOf(name)))
        << "shard_id=" << shard_id << " name=" << name;
  }
}

TEST(InputLoaderTest, TestGetAccessorsGeneratedArrayLoader) {
  using Input = std::array<int, 10>;
  auto i32 = GetQType<int>();
  const auto& input_loader = ::my_namespace::GetAccessorsGeneratedArrayLoader();
  EXPECT_THAT(input_loader, InputLoaderSupports({{"zero", i32},
                                                 {"one", i32},
                                                 {"a_2", i32},
                                                 {"a_3", i32},
                                                 {"a_4", i32},
                                                 {"f_5", i32},
                                                 {"f_6", i32}}));

  FrameLayout::Builder layout_builder;
  std::vector<FrameLayout::Slot<int>> slots;
  for (size_t i = 0; i != 7; ++i) {
    slots.push_back(layout_builder.AddSlot<int>());
  }
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_input_loader,
                       input_loader->Bind({
                           {"zero", TypedSlot::FromSlot(slots[0])},
                           {"one", TypedSlot::FromSlot(slots[1])},
                           {"a_2", TypedSlot::FromSlot(slots[2])},
                           {"a_3", TypedSlot::FromSlot(slots[3])},
                           {"a_4", TypedSlot::FromSlot(slots[4])},
                           {"f_5", TypedSlot::FromSlot(slots[5])},
                           {"f_6", TypedSlot::FromSlot(slots[6])},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ASSERT_OK(bound_input_loader(Input{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, frame));
  for (size_t i = 0; i != 7; ++i) {
    EXPECT_EQ(frame.Get(slots[i]), i + 1);
  }
}

}  // namespace
}  // namespace arolla
