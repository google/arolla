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
#include "arolla/codegen/io/testing/input_loader_with_wildcards.h"

#include <optional>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/naming/table.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"

namespace arolla {
namespace {

using ::arolla::testing::InputLoaderSupports;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;

TEST(InputLoaderTest, InputLoaderWithWildcards) {
  std::string ys_name(naming::TablePath().Column("ys").FullName());
  std::string foo_name(naming::TablePath()
                           .Column(naming::MapAccess("map_int", "foo"))
                           .FullName());
  std::string bar_name(naming::TablePath()
                           .Column(naming::MapAccess("map_int", "bar"))
                           .FullName());
  std::string inner_foo_name(
      naming::TablePath()
          .Child(naming::MapAccess("map_string_inner", "foo"))
          .Column("as")
          .FullName());

  auto input_loader = ::my_namespace::GetWildcardMapProtoLoader();

  ASSERT_TRUE(input_loader != nullptr);
  EXPECT_THAT(input_loader, InputLoaderSupports(
                                {{ys_name, GetDenseArrayQType<int>()},
                                 {foo_name, GetOptionalQType<int>()},
                                 {bar_name, GetOptionalQType<int>()},
                                 {inner_foo_name, GetDenseArrayQType<int>()}}));

  FrameLayout::Builder layout_builder;
  auto ys_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto foo_slot = layout_builder.AddSlot<OptionalValue<int>>();
  auto inner_foo_slot = layout_builder.AddSlot<DenseArray<int>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {ys_name, TypedSlot::FromSlot(ys_slot)},
          {foo_name, TypedSlot::FromSlot(foo_slot)},
          {inner_foo_name, TypedSlot::FromSlot(inner_foo_slot)},
      }));
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_slot), IsEmpty());
  EXPECT_THAT(frame.Get(foo_slot), Eq(std::nullopt));
  EXPECT_THAT(frame.Get(inner_foo_slot), IsEmpty());

  r.add_ys(57);
  ::testing_namespace::Inner& foo_inner =
      (*r.mutable_map_string_inner())["foo"];
  foo_inner.add_as(7);
  foo_inner.add_as(8);
  foo_inner.add_as(10);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_slot), ElementsAre(57));
  EXPECT_THAT(frame.Get(inner_foo_slot), ElementsAre(7, 8, 10));

  (*r.mutable_map_int())["foo"] = 57;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(foo_slot), Eq(57));
}

}  // namespace
}  // namespace arolla
