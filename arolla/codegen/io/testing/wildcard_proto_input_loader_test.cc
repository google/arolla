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
#include <optional>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/codegen/io/testing/test_proto_qtype.h"
#include "arolla/codegen/io/testing/test_wildcard_proto_input_loader.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/naming/table.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::arolla::testing::InputLoaderSupports;
using ::testing::ElementsAre;
using ::testing::IsEmpty;

TEST(InputLoaderTest, GetWildcardMapIntProtoLoader) {
  using OInt = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<OInt>();
  std::string x_name(
      naming::TablePath().Column(naming::MapAccess("map_int", "x")).FullName());
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       ::my_namespace::GetWildcardMapIntProtoLoader());
  ASSERT_TRUE(input_loader != nullptr);
  EXPECT_THAT(input_loader, InputLoaderSupports({{x_name, oi32}}));
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OInt>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {x_name, TypedSlot::FromSlot(x_slot)},
                       }));
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt());
  r.mutable_map_int()->insert({{"y", 8}});
  EXPECT_EQ(frame.Get(x_slot), OInt());
  r.mutable_map_int()->insert({{"x", 7}});
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(7));
}

TEST(InputLoaderTest, GetWildcardMapIntNoRenamingProtoLoader) {
  using OInt = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<OInt>();
  for (const auto& factory_fn :
       {::my_namespace::GetWildcardMapIntNoRenamingProtoLoader,
        ::my_namespace::GetWildcardMapIntNoRenamingProtoLoader2}) {
    ASSERT_OK_AND_ASSIGN(auto input_loader, factory_fn());
    ASSERT_TRUE(input_loader != nullptr);
    EXPECT_THAT(input_loader, InputLoaderSupports({{"x", oi32}}));
    FrameLayout::Builder layout_builder;
    auto x_slot = layout_builder.AddSlot<OInt>();
    ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                         input_loader->Bind({
                             {"x", TypedSlot::FromSlot(x_slot)},
                         }));
    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();

    ::testing_namespace::Root r;
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_EQ(frame.Get(x_slot), OInt());
    r.mutable_map_int()->insert({{"y", 8}});
    EXPECT_EQ(frame.Get(x_slot), OInt());
    r.mutable_map_int()->insert({{"x", 7}});
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_EQ(frame.Get(x_slot), OInt(7));
  }
}

TEST(InputLoaderTest, GetWildcardMapInnerAProtoLoader) {
  using OInt = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<OInt>();
  std::string x_name(naming::TablePath()
                         .Child(naming::MapAccess("map_string_inner", "x"))
                         .Column("a")
                         .FullName());
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       ::my_namespace::GetWildcardMapInnerAProtoLoader());
  ASSERT_TRUE(input_loader != nullptr);
  EXPECT_THAT(input_loader, InputLoaderSupports({{x_name, oi32}}));
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OInt>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {x_name, TypedSlot::FromSlot(x_slot)},
                       }));
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt());
  ::testing_namespace::Inner& x_inner = (*r.mutable_map_string_inner())["x"];
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt());
  x_inner.set_a(7);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(7));
}

TEST(InputLoaderTest, GetWildcardMapInnerPtrProtoLoader) {
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       ::my_namespace::GetWildcardMapInnerPtrProtoLoader());
  ASSERT_TRUE(input_loader != nullptr);
  EXPECT_THAT(input_loader,
              InputLoaderSupports(
                  {{"/map_string_inner[\"x\"]",
                    GetOptionalQType<testing_namespace::InnerRawPtr>()}}));
  FrameLayout::Builder layout_builder;
  auto x_slot =
      layout_builder.AddSlot<OptionalValue<testing_namespace::InnerRawPtr>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {"/map_string_inner[\"x\"]", TypedSlot::FromSlot(x_slot)},
      }));
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), std::nullopt);
  (*r.mutable_map_string_inner())["x"];
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), &r.map_string_inner().at("x"));
}

TEST(InputLoaderTest, GetWildcardMapInnerAsProtoLoader) {
  using DAInt = ::arolla::DenseArray<int>;
  auto daint = GetDenseArrayQType<int>();
  std::string x_name(naming::TablePath()
                         .Child(naming::MapAccess("map_string_inner", "x"))
                         .Column("as")
                         .FullName());
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       ::my_namespace::GetWildcardMapInnerAsProtoLoader());
  ASSERT_TRUE(input_loader != nullptr);
  EXPECT_THAT(input_loader, InputLoaderSupports({{x_name, daint}}));
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<DAInt>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {x_name, TypedSlot::FromSlot(x_slot)},
                       }));
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(x_slot), IsEmpty());
  ::testing_namespace::Inner& x_inner = (*r.mutable_map_string_inner())["x"];
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(x_slot), IsEmpty());
  x_inner.add_as(7);
  x_inner.add_as(8);
  x_inner.add_as(10);
  ASSERT_OK(bound_input_loader(r, frame));
  {
    DAInt x = frame.Get(x_slot);
    EXPECT_THAT(x, ElementsAre(7, 8, 10));
    EXPECT_TRUE(x.is_owned());
  }
  UnsafeArenaBufferFactory factory(1024);
  ASSERT_OK(bound_input_loader(r, frame, &factory));
  {
    DAInt x = frame.Get(x_slot);
    EXPECT_THAT(x, ElementsAre(7, 8, 10));
    EXPECT_FALSE(x.is_owned());
  }
}

}  // namespace
}  // namespace arolla
