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
#include "arolla/codegen/io/testing/test_struct_input_loader.h"

#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/codegen/io/testing/test_native_struct.h"
#include "arolla/codegen/io/testing/test_proto_qtype.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

using ::arolla::testing::InputLoaderSupports;

TEST(InputLoaderTest, TestGetProtoLoader) {
  using OInt = ::arolla::OptionalValue<int>;
  using obytes = ::arolla::OptionalValue<Bytes>;
  using otext = ::arolla::OptionalValue<Text>;
  auto oi32 = GetQType<OInt>();
  auto obytes_qtype = GetQType<obytes>();
  auto otext_qtype = GetQType<otext>();
  const auto& input_loader = ::my_namespace::GetStructLoader();
  EXPECT_THAT(input_loader,
              InputLoaderSupports(
                  {{"/x", oi32},
                   {"raw_bytes", obytes_qtype},
                   {"str", otext_qtype},
                   {"/inner/a", oi32},
                   {"/inner_proto/a", oi32},
                   {"/inner_proto",
                    GetOptionalQType<testing_namespace::InnerRawPtr>()}}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OInt>();
  auto str_slot = layout_builder.AddSlot<otext>();
  auto raw_bytes_slot = layout_builder.AddSlot<obytes>();
  auto a_struct_slot = layout_builder.AddSlot<OInt>();
  auto a_proto_slot = layout_builder.AddSlot<OInt>();
  auto proto_ptr_slot =
      layout_builder.AddSlot<OptionalValue<testing_namespace::InnerRawPtr>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {"/x", TypedSlot::FromSlot(x_slot)},
          {"str", TypedSlot::FromSlot(str_slot)},
          {"raw_bytes", TypedSlot::FromSlot(raw_bytes_slot)},
          {"/inner/a", TypedSlot::FromSlot(a_struct_slot)},
          {"/inner_proto/a", TypedSlot::FromSlot(a_proto_slot)},
          {"/inner_proto", TypedSlot::FromSlot(proto_ptr_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  testing_namespace::RootNativeStruct r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(0));
  EXPECT_EQ(frame.Get(str_slot), otext(""));
  EXPECT_EQ(frame.Get(raw_bytes_slot), obytes(""));
  EXPECT_EQ(frame.Get(a_struct_slot), OInt());
  EXPECT_EQ(frame.Get(a_proto_slot), OInt());
  EXPECT_EQ(frame.Get(proto_ptr_slot),
            OptionalValue<testing_namespace::InnerRawPtr>(&r.inner_proto));

  r.x = 57;
  r.str = "fifty";
  r.raw_bytes = "seven";
  r.inner_proto.set_a(75);
  testing_namespace::InnerNativeStruct inner{.a = 19};
  r.inner = &inner;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(57));
  EXPECT_EQ(frame.Get(str_slot), otext("fifty"));
  EXPECT_EQ(frame.Get(raw_bytes_slot), obytes("seven"));
  EXPECT_EQ(frame.Get(a_struct_slot), OInt(19));
  EXPECT_EQ(frame.Get(a_proto_slot), OInt(75));
  EXPECT_EQ(frame.Get(proto_ptr_slot),
            OptionalValue<testing_namespace::InnerRawPtr>(&r.inner_proto));

  // Test that old values are overridden by default values
  r = {};
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(0));
  EXPECT_EQ(frame.Get(str_slot), otext(""));
  EXPECT_EQ(frame.Get(raw_bytes_slot), obytes(""));
  EXPECT_EQ(frame.Get(a_struct_slot), OInt());
  EXPECT_EQ(frame.Get(a_proto_slot), OInt());
  EXPECT_EQ(frame.Get(proto_ptr_slot),
            OptionalValue<testing_namespace::InnerRawPtr>(&r.inner_proto));
}

}  // namespace
}  // namespace arolla
