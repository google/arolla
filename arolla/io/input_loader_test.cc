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
#include "arolla/io/input_loader.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::InputLoaderSupports;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::IsNull;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

struct TestStruct {
  int a;
  double b;
};

TEST(InputLoaderTest, GetInputLoaderTypes) {
  ASSERT_OK_AND_ASSIGN(auto loader,
                       CreateAccessorsInputLoader<TestStruct>(
                           "a", [](const TestStruct& s) { return s.a; },  //
                           "b", [](const TestStruct& s) { return s.b; }));

  EXPECT_THAT(GetInputLoaderQTypes(*loader, {}), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(
      GetInputLoaderQTypes(*loader, {"a"}),
      IsOkAndHolds(UnorderedElementsAre(Pair("a", GetQType<int32_t>()))));
  EXPECT_THAT(
      GetInputLoaderQTypes(*loader, {"a", "b"}),
      IsOkAndHolds(UnorderedElementsAre(Pair("a", GetQType<int32_t>()),
                                        Pair("b", GetQType<double>()))));
  EXPECT_THAT(GetInputLoaderQTypes(*loader, {"a", "b", "c"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unknown inputs: c (available: a, b)"));
}

TEST(InputLoaderTest, ChainInputLoaderConflict) {
  ASSERT_OK_AND_ASSIGN(auto loader1,
                       CreateAccessorsInputLoader<TestStruct>(
                           "a", [](const TestStruct& s) { return s.a; },  //
                           "b", [](const TestStruct& s) { return s.b; }));
  ASSERT_OK_AND_ASSIGN(auto loader2,
                       CreateAccessorsInputLoader<TestStruct>(
                           // The name "b" conflicts with loader1.
                           "b", [](const TestStruct& s) { return 2 * s.b; },  //
                           "c", [](const TestStruct& s) { return s.b * s.b; }));
  ASSERT_OK_AND_ASSIGN(auto chain_loader,
                       ChainInputLoader<TestStruct>::Build(std::move(loader1),
                                                           std::move(loader2)));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<double>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  ASSERT_OK_AND_ASSIGN(
      BoundInputLoader<TestStruct> bound_input_loader,
      chain_loader->Bind({{"a", TypedSlot::FromSlot(a_slot)},
                          {"b", TypedSlot::FromSlot(b_slot)}}));

  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));

  // If loader2 would be used for b_slot, the result would be 7.0.
  EXPECT_EQ(alloc.frame().Get(b_slot), 3.5);
}

TEST(InputLoaderTest, MakeNotOwningInputLoader) {
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputLoader<TestStruct>> wrapped_loader,
                       CreateAccessorsInputLoader<TestStruct>(
                           "a", [](const TestStruct& s) { return s.a; }));

  std::unique_ptr<InputLoader<TestStruct>> not_owning_loader =
      MakeNotOwningInputLoader(wrapped_loader.get());

  EXPECT_THAT(not_owning_loader->GetQTypeOf("a"), Eq(GetQType<int32_t>()));
  EXPECT_THAT(not_owning_loader->GetQTypeOf("b"), IsNull());

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  ASSERT_OK_AND_ASSIGN(
      BoundInputLoader<TestStruct> bound_input_loader,
      not_owning_loader->Bind({{"a", TypedSlot::FromSlot(a_slot)}}));

  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
}

TEST(InputLoaderTest, MakeSharedOwningInputLoader) {
  std::unique_ptr<InputLoader<TestStruct>> shared_owning_loader;

  {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<const InputLoader<TestStruct>> wrapped_loader,
        CreateAccessorsInputLoader<TestStruct>(
            "a", [](const TestStruct& s) { return s.a; }));
    shared_owning_loader = MakeSharedOwningInputLoader(wrapped_loader);
  }

  // wrapped_loader goes out of scope, but it is still owned by
  // shared_owning_loader.

  EXPECT_THAT(shared_owning_loader->GetQTypeOf("a"), Eq(GetQType<int32_t>()));
  EXPECT_THAT(shared_owning_loader->GetQTypeOf("b"), IsNull());

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  ASSERT_OK_AND_ASSIGN(
      BoundInputLoader<TestStruct> bound_input_loader,
      shared_owning_loader->Bind({{"a", TypedSlot::FromSlot(a_slot)}}));

  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
}

TEST(InputLoaderTest, BindInputLoaderList) {
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<double>();
  auto c_slot = layout_builder.AddSlot<double>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  std::vector<std::unique_ptr<InputLoader<TestStruct>>> input_loaders;
  ASSERT_OK_AND_ASSIGN(input_loaders.emplace_back(),
                       CreateAccessorsInputLoader<TestStruct>(
                           "a", [](const TestStruct& s) { return s.a; }));
  ASSERT_OK_AND_ASSIGN(input_loaders.emplace_back(),
                       CreateAccessorsInputLoader<TestStruct>(
                           "b", [](const TestStruct& s) { return s.b; }));
  ASSERT_OK_AND_ASSIGN(input_loaders.emplace_back(),
                       CreateAccessorsInputLoader<TestStruct>(
                           // Duplicated names are ignored.
                           "b", [](const TestStruct& s) { return int{0}; },  //
                           "c", [](const TestStruct& s) { return s.b * s.b; }));
  ASSERT_OK_AND_ASSIGN(
      std::vector<BoundInputLoader<TestStruct>> bound_input_loaders,
      BindInputLoaderList<TestStruct>(input_loaders,
                                      {
                                          {"a", TypedSlot::FromSlot(a_slot)},
                                          {"b", TypedSlot::FromSlot(b_slot)},
                                          {"c", TypedSlot::FromSlot(c_slot)},
                                      }));
  MemoryAllocation alloc(&memory_layout);

  TestStruct input{5, 3.5};
  for (const auto& bound_input_loader : bound_input_loaders) {
    ASSERT_OK(bound_input_loader(input, alloc.frame()));
  }
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 3.5);
  EXPECT_EQ(alloc.frame().Get(c_slot), 3.5 * 3.5);
}

TEST(InputLoaderTest, BindInputLoaderListErrors) {
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<double>();
  auto c_slot = layout_builder.AddSlot<double>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  std::vector<std::unique_ptr<InputLoader<TestStruct>>> input_loaders;
  ASSERT_OK_AND_ASSIGN(input_loaders.emplace_back(),
                       CreateAccessorsInputLoader<TestStruct>(
                           "a", [](const TestStruct& s) { return s.a; }));
  ASSERT_OK_AND_ASSIGN(input_loaders.emplace_back(),
                       CreateAccessorsInputLoader<TestStruct>(
                           "b", [](const TestStruct& s) { return s.b; }));
  EXPECT_THAT(
      BindInputLoaderList<TestStruct>(input_loaders,
                                      {
                                          {"a", TypedSlot::FromSlot(a_slot)},
                                          {"b", TypedSlot::FromSlot(b_slot)},
                                          {"c", TypedSlot::FromSlot(c_slot)},
                                      }),
      StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("not all")));
}

TEST(InputLoaderTest, FilteringInputLoader) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  ASSERT_OK_AND_ASSIGN(auto inner_loader,
                       CreateAccessorsInputLoader<TestStruct>(
                           "a", [](const TestStruct& s) { return s.a; },  //
                           "b", [](const TestStruct& s) { return s.b; }));

  EXPECT_THAT(inner_loader->GetQTypeOf("a"), Eq(i32));
  EXPECT_THAT(inner_loader->GetQTypeOf("b"), Eq(f64));

  auto filtered_loader =
      MakeFilteringInputLoader(std::move(inner_loader), {"a"});

  EXPECT_THAT(filtered_loader->GetQTypeOf("a"), Eq(i32));
  EXPECT_THAT(filtered_loader->GetQTypeOf("b"), IsNull());

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<double>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  EXPECT_THAT(filtered_loader->Bind({{"a", TypedSlot::FromSlot(a_slot)},
                                     {"b", TypedSlot::FromSlot(b_slot)}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unknown inputs: b (available: a)"));

  ASSERT_OK_AND_ASSIGN(
      BoundInputLoader<TestStruct> bound_input_loader,
      filtered_loader->Bind({{"a", TypedSlot::FromSlot(a_slot)}}));

  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
}

TEST(InputLoaderTest, ChainInputLoader) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  std::unique_ptr<InputLoader<TestStruct>> chain_input_loader;
  {  // scope to delete loader*. They are still owned by chain_input_loader.
    ASSERT_OK_AND_ASSIGN(auto loader1,
                         CreateAccessorsInputLoader<TestStruct>(
                             "a", [](const TestStruct& s) { return s.a; }));
    ASSERT_OK_AND_ASSIGN(auto loader2,
                         CreateAccessorsInputLoader<TestStruct>(
                             "b", [](const TestStruct& s) { return s.b; }));
    ASSERT_OK_AND_ASSIGN(
        auto loader3, CreateAccessorsInputLoader<TestStruct>(
                          "c", [](const TestStruct& s) { return s.b * s.b; }));
    ASSERT_OK_AND_ASSIGN(
        chain_input_loader,
        ChainInputLoader<TestStruct>::Build(
            std::move(loader1), std::move(loader2), std::move(loader3)));
  }
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<double>();
  auto c_slot = layout_builder.AddSlot<double>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  EXPECT_THAT(*chain_input_loader,
              InputLoaderSupports({{"a", i32}, {"b", f64}, {"c", f64}}));

  ASSERT_OK_AND_ASSIGN(BoundInputLoader<TestStruct> bound_input_loader,
                       chain_input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},
                           {"b", TypedSlot::FromSlot(b_slot)},
                           {"c", TypedSlot::FromSlot(c_slot)},
                       }));

  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 3.5);
  EXPECT_EQ(alloc.frame().Get(c_slot), 3.5 * 3.5);
}

TEST(InputLoaderTest, ChainInputLoaderFactoryPropagated) {
  auto qbool = GetQType<bool>();
  std::unique_ptr<InputLoader<TestStruct>> input_loader;
  UnsafeArenaBufferFactory global_factory1(1000);
  UnsafeArenaBufferFactory global_factory2(1000);
  {  // scope to delete loader*. They are still owned by input_loader.
    ASSERT_OK_AND_ASSIGN(auto loader1, CreateAccessorsInputLoader<TestStruct>(
                                           "a", [&](const TestStruct&,
                                                    RawBufferFactory* factory) {
                                             return factory == &global_factory1;
                                           }));
    ASSERT_OK_AND_ASSIGN(auto loader2, CreateAccessorsInputLoader<TestStruct>(
                                           "b", [&](const TestStruct&,
                                                    RawBufferFactory* factory) {
                                             return factory == &global_factory2;
                                           }));
    ASSERT_OK_AND_ASSIGN(
        input_loader, ChainInputLoader<TestStruct>::Build(std::move(loader1),
                                                          std::move(loader2)));
  }
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<bool>();
  auto b_slot = layout_builder.AddSlot<bool>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  EXPECT_THAT(input_loader, InputLoaderSupports({{"a", qbool}, {"b", qbool}}));

  ASSERT_OK_AND_ASSIGN(BoundInputLoader<TestStruct> bound_input_loader,
                       input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},
                           {"b", TypedSlot::FromSlot(b_slot)},
                       }));

  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame(), &global_factory1));
  EXPECT_TRUE(alloc.frame().Get(a_slot));
  EXPECT_FALSE(alloc.frame().Get(b_slot));

  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame(), &global_factory2));
  EXPECT_FALSE(alloc.frame().Get(a_slot));
  EXPECT_TRUE(alloc.frame().Get(b_slot));
}

TEST(InputLoaderTest, ChainInputLoaderWithCustomInvoke) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  std::unique_ptr<InputLoader<TestStruct>> chain_input_loader;

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<double>();
  auto c_slot = layout_builder.AddSlot<double>();
  FrameLayout memory_layout = std::move(layout_builder).Build();
  int64_t number_of_loaders = -1;

  {  // scope to delete loader*. They are still owned by chain_input_loader.
    std::vector<std::unique_ptr<InputLoader<TestStruct>>> input_loaders;
    ASSERT_OK_AND_ASSIGN(input_loaders.emplace_back(),
                         CreateAccessorsInputLoader<TestStruct>(
                             "a", [](const TestStruct& s) { return s.a; }));
    ASSERT_OK_AND_ASSIGN(input_loaders.emplace_back(),
                         CreateAccessorsInputLoader<TestStruct>(
                             "b", [](const TestStruct& s) { return s.b; }));
    ASSERT_OK_AND_ASSIGN(
        input_loaders.emplace_back(),
        CreateAccessorsInputLoader<TestStruct>(
            "c", [](const TestStruct& s) { return s.b * s.b; }));
    ASSERT_OK_AND_ASSIGN(
        chain_input_loader,
        ChainInputLoader<TestStruct>::Build(
            std::move(input_loaders),
            [&number_of_loaders](
                absl::Span<const BoundInputLoader<TestStruct>> loaders,
                const TestStruct& input, FramePtr frame,
                RawBufferFactory* factory) {
              number_of_loaders = loaders.size();
              return ChainInputLoader<TestStruct>::InvokeBoundLoaders(
                  loaders, input, frame, factory);
            }));
    EXPECT_THAT(*chain_input_loader,
                InputLoaderSupports({{"a", i32}, {"b", f64}, {"c", f64}}));
  }

  BoundInputLoader<TestStruct> bound_input_loader(nullptr);
  {
    // BoundInputLoader should own all necessary information from InputLoader.
    ASSERT_OK_AND_ASSIGN(bound_input_loader,
                         chain_input_loader->Bind({
                             {"a", TypedSlot::FromSlot(a_slot)},
                             {"b", TypedSlot::FromSlot(b_slot)},
                             {"c", TypedSlot::FromSlot(c_slot)},
                         }));
  }

  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
  EXPECT_EQ(number_of_loaders, 3);
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 3.5);
  EXPECT_EQ(alloc.frame().Get(c_slot), 3.5 * 3.5);
}

TEST(InputLoaderTest, ChainInputLoaderWithCustomInvokeOptimized) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  std::unique_ptr<InputLoader<TestStruct>> chain_input_loader;

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  FrameLayout memory_layout = std::move(layout_builder).Build();

  int64_t number_of_loaders = -1;

  {  // scope to delete loader*. They are still owned by chain_input_loader.
    std::vector<std::unique_ptr<InputLoader<TestStruct>>> input_loaders;
    ASSERT_OK_AND_ASSIGN(input_loaders.emplace_back(),
                         CreateAccessorsInputLoader<TestStruct>(
                             "a", [](const TestStruct& s) { return s.a; }));
    ASSERT_OK_AND_ASSIGN(input_loaders.emplace_back(),
                         CreateAccessorsInputLoader<TestStruct>(
                             "b", [](const TestStruct& s) { return s.b; }));
    ASSERT_OK_AND_ASSIGN(
        chain_input_loader,
        ChainInputLoader<TestStruct>::Build(
            std::move(input_loaders),
            [&number_of_loaders](
                absl::Span<const BoundInputLoader<TestStruct>> loaders,
                const TestStruct& input, FramePtr frame,
                RawBufferFactory* factory) {
              number_of_loaders = loaders.size();
              return ChainInputLoader<TestStruct>::InvokeBoundLoaders(
                  loaders, input, frame, factory);
            }));
    EXPECT_THAT(*chain_input_loader,
                InputLoaderSupports({{"a", i32}, {"b", f64}}));
  }

  BoundInputLoader<TestStruct> bound_input_loader(nullptr);
  {
    // BoundInputLoader should own all necessary information from InputLoader.
    ASSERT_OK_AND_ASSIGN(bound_input_loader,
                         chain_input_loader->Bind({
                             {"a", TypedSlot::FromSlot(a_slot)},
                         }));
  }

  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
  // Not invoked since single BoundInputLoader.
  EXPECT_EQ(number_of_loaders, -1);
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
}

}  // namespace
}  // namespace arolla
