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
#include "arolla/codegen/io/testing/test_struct_slot_listener.h"

#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/codegen/io/testing/test_native_struct.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/naming/table.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

using ::testing::Eq;
using ::testing::UnorderedElementsAre;

TEST(InputLoaderTest, TestGetProtoLoader) {
  using OInt = ::arolla::OptionalValue<int>;
  using obytes = ::arolla::OptionalValue<Bytes>;
  using otext = ::arolla::OptionalValue<Text>;
  auto oi32 = GetQType<OInt>();
  auto obytes_qtype = GetQType<obytes>();
  auto otext_qtype = GetQType<otext>();
  auto listener = ::my_namespace::GetStructListener();
  std::string x_name(naming::TablePath().Column("x").FullName());
  std::string a_struct_name(naming::TablePath("inner").Column("a").FullName());
  std::string a_proto_name(
      naming::TablePath("inner_proto").Column("a").FullName());

  EXPECT_THAT(listener->GetQTypeOf(x_name), Eq(oi32));
  EXPECT_THAT(listener->GetQTypeOf("raw_bytes"), Eq(obytes_qtype));
  EXPECT_THAT(listener->GetQTypeOf("str"), Eq(otext_qtype));
  EXPECT_THAT(listener->GetQTypeOf(a_struct_name), Eq(oi32));
  EXPECT_THAT(listener->GetQTypeOf(a_proto_name), Eq(oi32));
  EXPECT_THAT(listener->SuggestAvailableNames(),
              UnorderedElementsAre(x_name, "raw_bytes", "str", a_struct_name,
                                   a_proto_name));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OInt>();
  auto str_slot = layout_builder.AddSlot<otext>();
  auto raw_bytes_slot = layout_builder.AddSlot<obytes>();
  auto a_struct_slot = layout_builder.AddSlot<OInt>();
  auto a_proto_slot = layout_builder.AddSlot<OInt>();
  ASSERT_OK_AND_ASSIGN(auto bound_listener,
                       listener->Bind({
                           {x_name, TypedSlot::FromSlot(x_slot)},
                           {"str", TypedSlot::FromSlot(str_slot)},
                           {"raw_bytes", TypedSlot::FromSlot(raw_bytes_slot)},
                           {a_struct_name, TypedSlot::FromSlot(a_struct_slot)},
                           {a_proto_name, TypedSlot::FromSlot(a_proto_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  testing_namespace::RootNativeStruct r;
  ASSERT_OK(bound_listener(frame, &r));
  EXPECT_EQ(r.x, 0);
  EXPECT_EQ(r.str, "");
  EXPECT_EQ(r.raw_bytes, "");
  EXPECT_EQ(r.inner, nullptr);
  EXPECT_FALSE(r.inner_proto.has_a());

  frame.Set(a_struct_slot, 19);
  // No crash if pointer is nullptr
  ASSERT_OK(bound_listener(frame, &r));
  EXPECT_EQ(r.inner, nullptr);

  testing_namespace::InnerNativeStruct inner;
  r.inner = &inner;
  ASSERT_OK(bound_listener(frame, &r));
  EXPECT_EQ(r.inner, &inner);
  EXPECT_EQ(r.inner->a, 19);

  frame.Set(x_slot, OInt(57));
  frame.Set(str_slot, Text("fifty"));
  frame.Set(raw_bytes_slot, Bytes("seven"));
  frame.Set(a_proto_slot, OInt(75));
  ASSERT_OK(bound_listener(frame, &r));
  EXPECT_EQ(r.x, 57);
  EXPECT_EQ(r.str, "fifty");
  EXPECT_EQ(r.raw_bytes, "seven");
  EXPECT_EQ(r.inner->a, 19);
  EXPECT_EQ(r.inner_proto.a(), 75);

  // Test that old values are preserved if optionals are missing.
  // This behavior is a bit dangerous in case of proto reusage.
  // This may leak data across evaluations.
  MemoryAllocation alloc2(&memory_layout);
  frame = alloc2.frame();
  r.x = 91;
  ASSERT_OK(bound_listener(frame, &r));
  EXPECT_EQ(r.x, 91);
  EXPECT_EQ(r.str, "fifty");
  EXPECT_EQ(r.raw_bytes, "seven");
  EXPECT_EQ(r.inner->a, 19);
  EXPECT_EQ(r.inner_proto.a(), 75);
}

}  // namespace
}  // namespace arolla
