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
#include "arolla/codegen/io/testing/test_proto_slot_listener.h"

#include <optional>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/codegen/io/testing/test_array_proto_slot_listener.h"
#include "arolla/codegen/io/testing/test_sharded_slot_listener.h"
#include "arolla/codegen/io/testing/test_sized_slot_listener.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/proto_types/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/naming/table.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/proto/testing/test_extension.pb.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::MatchesRegex;

TEST(InputLoaderTest, TestGetProtoSlotListenerErrors) {
  using oint = ::arolla::OptionalValue<int>;
  FrameLayout::Builder layout_builder;
  auto int_slot = layout_builder.AddSlot<oint>();
  EXPECT_THAT(::my_namespace::GetProtoSlotListener()
                  ->Bind({
                      {"a", TypedSlot::FromSlot(int_slot)},
                  })
                  .status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unknown outputs: a")));
  EXPECT_THAT(
      ::my_namespace::GetProtoSlotListener()
          ->Bind({
              {"str", TypedSlot::FromSlot(int_slot)},
          })
          .status(),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("slot types mismatch: str{expected:OPTIONAL_TEXT, "
                         "actual:OPTIONAL_INT32}")));
}

TEST(InputLoaderTest, TestGetProtoSlotListener) {
  using oint = ::arolla::OptionalValue<int>;
  using obytes = ::arolla::OptionalValue<Bytes>;
  using otext = ::arolla::OptionalValue<Text>;

  FrameLayout::Builder layout_builder;
  std::string x_name(std::string(naming::TablePath().Column("x").FullName()));
  auto x_slot = layout_builder.AddSlot<oint>();
  auto str_slot = layout_builder.AddSlot<otext>();
  auto raw_bytes_slot = layout_builder.AddSlot<obytes>();
  auto a_slot = layout_builder.AddSlot<oint>();
  auto ys_0_slot = layout_builder.AddSlot<oint>();
  auto inners_0_a_slot = layout_builder.AddSlot<oint>();
  auto map_int_a_slot = layout_builder.AddSlot<oint>();
  std::string broken_name(naming::TablePath().Column("BrOkEn_CaSe").FullName());
  auto broken_case_slot = layout_builder.AddSlot<oint>();

  // Bind all listeners
  auto slot_listener = ::my_namespace::GetProtoSlotListener();
  EXPECT_THAT(slot_listener->GetQTypeOf(x_name), Eq(GetQType<oint>()));
  EXPECT_THAT(slot_listener->GetQTypeOf("str"), Eq(GetQType<otext>()));
  EXPECT_THAT(slot_listener->GetQTypeOf("raw_bytes"), Eq(GetQType<obytes>()));
  EXPECT_THAT(slot_listener->GetQTypeOf("inner__a"), Eq(GetQType<oint>()));
  EXPECT_THAT(slot_listener->GetQTypeOf("ys_0"), Eq(GetQType<oint>()));
  EXPECT_THAT(slot_listener->GetQTypeOf("inners_0__a"), Eq(GetQType<oint>()));
  EXPECT_THAT(slot_listener->GetQTypeOf("map_int_a"), Eq(GetQType<oint>()));
  EXPECT_THAT(slot_listener->GetQTypeOf(broken_name), Eq(GetQType<oint>()));
  ASSERT_OK_AND_ASSIGN(
      auto bound_listener,
      slot_listener->Bind({
          {x_name, TypedSlot::FromSlot(x_slot)},
          {"str", TypedSlot::FromSlot(str_slot)},
          {"raw_bytes", TypedSlot::FromSlot(raw_bytes_slot)},
          {"inner__a", TypedSlot::FromSlot(a_slot)},
          {"ys_0", TypedSlot::FromSlot(ys_0_slot)},
          {"inners_0__a", TypedSlot::FromSlot(inners_0_a_slot)},
          {"map_int_a", TypedSlot::FromSlot(map_int_a_slot)},
          {broken_name, TypedSlot::FromSlot(broken_case_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_listener(frame, &r));
  // All values are missed, so nothing should be set
  EXPECT_FALSE(r.has_x());
  EXPECT_FALSE(r.has_str());
  EXPECT_FALSE(r.has_raw_bytes());
  EXPECT_FALSE(r.inner().has_a());
  EXPECT_EQ(r.map_int_size(), 0);
  EXPECT_EQ(r.ys_size(), 0);
  EXPECT_EQ(r.inners_size(), 0);
  EXPECT_FALSE(r.has_broken_case());

  frame.Set(x_slot, oint(19));
  frame.Set(str_slot, Text("19"));
  frame.Set(raw_bytes_slot, Bytes("57"));
  frame.Set(a_slot, oint(17));
  frame.Set(map_int_a_slot, oint(27));
  frame.Set(ys_0_slot, oint(57));
  frame.Set(inners_0_a_slot, oint(37));
  frame.Set(broken_case_slot, oint(75));

  ASSERT_OK(bound_listener(frame, &r));
  EXPECT_EQ(r.x(), 19);
  EXPECT_EQ(r.str(), "19");
  EXPECT_EQ(r.raw_bytes(), "57");
  EXPECT_EQ(r.inner().a(), 17);
  EXPECT_EQ(r.map_int().at("a"), 27);
  // on index access we do not extend size of the repeated field
  EXPECT_EQ(r.ys_size(), 0);      // we set only if idx exist
  EXPECT_EQ(r.inners_size(), 0);  // we set only if idx exist
  EXPECT_EQ(r.broken_case(), 75);

  // If repeated field has enough size we set value.
  r.add_ys(0);
  r.add_inners();
  ASSERT_OK(bound_listener(frame, &r));
  EXPECT_EQ(r.ys(0), 57);
  EXPECT_EQ(r.inners(0).a(), 37);
}

TEST(InputLoaderTest, TestGetArrayProtoSlotListener) {
  using aint = ::arolla::DenseArray<int>;

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<aint>();
  auto z_slot = layout_builder.AddSlot<aint>();
  auto nested_z_slot = layout_builder.AddSlot<aint>();

  // Bind all listeners
  auto slot_listener = ::my_namespace::GetArrayProtoSlotListener();
  EXPECT_THAT(slot_listener->GetQTypeOf("inners__a"), Eq(GetQType<aint>()));
  EXPECT_THAT(slot_listener->GetQTypeOf("inners2__z"), Eq(GetQType<aint>()));
  EXPECT_THAT(slot_listener->GetQTypeOf("inners__inners2__z"),
              Eq(GetQType<aint>()));
  ASSERT_OK_AND_ASSIGN(
      auto bound_listener,
      slot_listener->Bind({
          {"inners__a", TypedSlot::FromSlot(a_slot)},
          {"inners2__z", TypedSlot::FromSlot(z_slot)},
          {"inners__inners2__z", TypedSlot::FromSlot(nested_z_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_listener(frame, &r));
  // All values are missed, so nothing should be set
  EXPECT_EQ(r.inners_size(), 0);
  EXPECT_FALSE(r.has_inner());

  r = ::testing_namespace::Root();
  frame.Set(a_slot, CreateDenseArray<int>({19, std::nullopt}));
  EXPECT_THAT(
      bound_listener(frame, &r),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex("unexpected.*/inners/a.*proto.*0.*array.*2")));
  EXPECT_EQ(r.inners_size(), 0);  // no resize happen
  EXPECT_FALSE(r.has_inner());

  r.add_inners();
  r.add_inners();
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 2);
  EXPECT_THAT(r.inners(0).a(), 19);
  EXPECT_FALSE(r.inners(1).has_a());

  frame.Set(z_slot, CreateDenseArray<int>({std::nullopt, 17}));
  r.mutable_inner()->add_inners2();
  r.mutable_inner()->add_inners2();
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inner().inners2_size(), 2);
  EXPECT_FALSE(r.inner().inners2(0).has_z());
  EXPECT_THAT(r.inner().inners2(1).z(), 17);

  frame.Set(nested_z_slot, CreateDenseArray<int>({15, std::nullopt, 11}));
  r.mutable_inners(0)->add_inners2();
  r.mutable_inners(0)->add_inners2();
  r.mutable_inners(1)->add_inners2();
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 2);
  ASSERT_EQ(r.inners(0).inners2_size(), 2);
  EXPECT_THAT(r.inners(0).inners2(0).z(), 15);
  EXPECT_FALSE(r.inners(0).inners2(1).has_z());
  ASSERT_EQ(r.inners(1).inners2_size(), 1);
  EXPECT_THAT(r.inners(1).inners2(0).z(), 11);
}

TEST(InputLoaderTest, TestGetProtoSizedSlotListenerSingleValueSize) {
  for (const auto& slot_listener :
       {::my_namespace::GetProtoSizedSlotListener(),
        ::my_namespace::GetShardedProtoSizedSlotListener()}) {
    FrameLayout::Builder layout_builder;
    auto a_slot = layout_builder.AddSlot<DenseArray<int>>();
    auto inner_size_slot = layout_builder.AddSlot<DenseArrayShape>();

    // Bind all listeners
    EXPECT_THAT(slot_listener->GetQTypeOf("/inners/a"),
                Eq(GetDenseArrayQType<int>()));
    EXPECT_THAT(slot_listener->GetQTypeOf("/inners/@size"),
                Eq(GetQType<DenseArrayShape>()));
    ASSERT_OK_AND_ASSIGN(
        auto bound_listener,
        slot_listener->Bind({
            {"/inners/a", TypedSlot::FromSlot(a_slot)},
            {"/inners/@size", TypedSlot::FromSlot(inner_size_slot)},
        }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();

    ::testing_namespace::Root r;
    ASSERT_OK(bound_listener(frame, &r));
    // All values are missed, so nothing should be set
    EXPECT_EQ(r.inners_size(), 0);
    EXPECT_FALSE(r.has_inner());

    r = ::testing_namespace::Root();
    frame.Set(inner_size_slot, DenseArrayShape{.size = 0});
    frame.Set(a_slot, CreateDenseArray<int>({std::nullopt, 17}));
    EXPECT_THAT(
        bound_listener(frame, &r),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                MatchesRegex("unexpected.*/inners/a.*proto.*0.*array.*2")));
    EXPECT_EQ(r.inners_size(), 0);  // no resize happen
    EXPECT_FALSE(r.has_inner());

    frame.Set(inner_size_slot, DenseArrayShape{.size = 2});
    ASSERT_OK(bound_listener(frame, &r));
    EXPECT_EQ(r.inners_size(), 2);
    EXPECT_FALSE(r.inners(0).has_a());
    EXPECT_EQ(r.inners(1).a(), 17);

    // increase size
    frame.Set(inner_size_slot, DenseArrayShape{.size = 3});
    frame.Set(a_slot, CreateDenseArray<int>({13, std::nullopt, 15}));
    ASSERT_OK(bound_listener(frame, &r));
    EXPECT_EQ(r.inners_size(), 3);
    EXPECT_EQ(r.inners(0).a(), 13);
    EXPECT_EQ(r.inners(1).a(), 17);  // value is not cleared
    EXPECT_EQ(r.inners(2).a(), 15);

    // decrease size
    frame.Set(inner_size_slot, DenseArrayShape{.size = 1});
    frame.Set(a_slot, CreateDenseArray<int>({11}));
    ASSERT_OK(bound_listener(frame, &r));
    EXPECT_EQ(r.inners_size(), 1);
    EXPECT_EQ(r.inners(0).a(), 11);
  }
}

TEST(InputLoaderTest, TestGetProtoSizedSlotListenerRepeatedSize) {
  for (const auto& slot_listener :
       {::my_namespace::GetProtoSizedSlotListener(),
        ::my_namespace::GetShardedProtoSizedSlotListener()}) {
    FrameLayout::Builder layout_builder;
    auto z_slot = layout_builder.AddSlot<DenseArray<int>>();
    auto inner_size_slot = layout_builder.AddSlot<DenseArrayShape>();
    auto inners2_size_slot =
        layout_builder.AddSlot<DenseArray<proto::arolla_size_t>>();

    // Bind all listeners
    EXPECT_THAT(slot_listener->GetQTypeOf("/inners/inners2/z"),
                Eq(GetDenseArrayQType<int>()));
    EXPECT_THAT(slot_listener->GetQTypeOf("/inners/@size"),
                Eq(GetQType<DenseArrayShape>()));
    EXPECT_THAT(slot_listener->GetQTypeOf("/inners/inners2/@size"),
                Eq(GetDenseArrayQType<proto::arolla_size_t>()));
    ASSERT_OK_AND_ASSIGN(
        auto bound_listener,
        slot_listener->Bind({
            {"/inners/@size", TypedSlot::FromSlot(inner_size_slot)},
            {"/inners/inners2/z", TypedSlot::FromSlot(z_slot)},
            {"/inners/inners2/@size", TypedSlot::FromSlot(inners2_size_slot)},
        }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();

    ::testing_namespace::Root r;
    ASSERT_OK(bound_listener(frame, &r));
    // All values are missed, so nothing should be set
    EXPECT_EQ(r.inners_size(), 0);

    r = ::testing_namespace::Root();
    frame.Set(inner_size_slot, DenseArrayShape{.size = 2});
    frame.Set(inners2_size_slot,
              CreateDenseArray<proto::arolla_size_t>({2, 3}));
    frame.Set(z_slot, CreateDenseArray<int>({13, std::nullopt}));
    EXPECT_THAT(
        bound_listener(frame, &r),
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            MatchesRegex("unexpected.*/inners/inners2/z.*proto.*5.*array.*2")));
    // inners was successfully resized (not guaranteed)
    EXPECT_EQ(r.inners_size(), 2);
    // inners2 was successfully resized (not guaranteed)
    EXPECT_EQ(r.inners(0).inners2_size(), 2);
    EXPECT_EQ(r.inners(1).inners2_size(), 3);
    frame.Set(z_slot,
              CreateDenseArray<int>({13, std::nullopt, 14, std::nullopt, 15}));

    r = ::testing_namespace::Root();
    ASSERT_OK(bound_listener(frame, &r));
    EXPECT_EQ(r.inners_size(), 2);
    EXPECT_EQ(r.inners(0).inners2_size(), 2);
    EXPECT_EQ(r.inners(0).inners2(0).z(), 13);
    EXPECT_FALSE(r.inners(0).inners2(1).has_z());
    EXPECT_EQ(r.inners(1).inners2_size(), 3);
    EXPECT_EQ(r.inners(1).inners2(0).z(), 14);
    EXPECT_FALSE(r.inners(1).inners2(1).has_z());
    EXPECT_EQ(r.inners(1).inners2(2).z(), 15);

    // change size
    frame.Set(inner_size_slot, DenseArrayShape{.size = 2});
    frame.Set(inners2_size_slot,
              CreateDenseArray<proto::arolla_size_t>({3, 1}));
    frame.Set(z_slot,
              CreateDenseArray<int>({std::nullopt, -1, std::nullopt, -2}));
    ASSERT_OK(bound_listener(frame, &r));
    EXPECT_EQ(r.inners_size(), 2);
    EXPECT_EQ(r.inners(0).inners2_size(), 3);
    EXPECT_EQ(r.inners(0).inners2(0).z(), 13);  // value is not cleared
    EXPECT_EQ(r.inners(0).inners2(1).z(), -1);
    EXPECT_FALSE(r.inners(0).inners2(2).has_z());
    EXPECT_EQ(r.inners(1).inners2_size(), 1);
    EXPECT_EQ(r.inners(1).inners2(0).z(), -2);
  }
}

TEST(InputLoaderTest, TestGetArrayProtoSlotListenerWithMap) {
  using aint = ::arolla::DenseArray<int>;

  FrameLayout::Builder layout_builder;
  auto in_map_a_slot = layout_builder.AddSlot<aint>();

  // Bind all listeners
  auto slot_listener = ::my_namespace::GetArrayProtoSlotListener();
  EXPECT_THAT(slot_listener->GetQTypeOf("in_map_a"), Eq(GetQType<aint>()));
  ASSERT_OK_AND_ASSIGN(auto bound_listener,
                       slot_listener->Bind({
                           {"in_map_a", TypedSlot::FromSlot(in_map_a_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_listener(frame, &r));
  // All values are missed, so nothing should be set
  EXPECT_EQ(r.inners_size(), 0);

  frame.Set(in_map_a_slot, CreateDenseArray<int>({19, std::nullopt}));
  EXPECT_THAT(bound_listener(frame, &r),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex("unexpected.*/inners/root_reference/"
                                    "map_string_inner.*proto.*0.*array.*2")));
  EXPECT_EQ(r.inners_size(), 0);  // no resize happen

  r.add_inners();
  r.add_inners();
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 2);
  EXPECT_THAT(r.inners(0).root_reference().map_string_inner().at("a").a(), 19);
  EXPECT_FALSE(r.inners(1).root_reference().map_string_inner().contains("a"));
}

TEST(InputLoaderTest,
     TestGetArrayProtoSlotListenerWithArrayInTheMiddleOfLastPath) {
  using aint = ::arolla::DenseArray<int>;

  FrameLayout::Builder layout_builder;
  auto in_array_z_slot = layout_builder.AddSlot<aint>();

  // Bind all listeners
  auto slot_listener = ::my_namespace::GetArrayProtoSlotListener();
  EXPECT_THAT(slot_listener->GetQTypeOf("in_array_z"), Eq(GetQType<aint>()));
  ASSERT_OK_AND_ASSIGN(auto bound_listener,
                       slot_listener->Bind({
                           {"in_array_z", TypedSlot::FromSlot(in_array_z_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_listener(frame, &r));
  // All values are missed, so nothing should be set
  EXPECT_EQ(r.inners_size(), 0);

  frame.Set(in_array_z_slot, CreateDenseArray<int>({19, std::nullopt, 17}));
  EXPECT_THAT(
      bound_listener(frame, &r),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               MatchesRegex(".*/inners/inners2.*/z.*proto.*0.*array.*3")));
  EXPECT_EQ(r.inners_size(), 0);  // no resize happen

  r.add_inners();
  r.add_inners();
  r.add_inners();
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 3);
  EXPECT_EQ(r.inners(0).inners2_size(), 0);  // we do not resize
  EXPECT_EQ(r.inners(1).inners2_size(), 0);  // we do not resize
  EXPECT_EQ(r.inners(2).inners2_size(), 0);  // we do not resize

  r.mutable_inners(2)->add_inners2();
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 3);
  EXPECT_EQ(r.inners(0).inners2_size(), 0);  // we do not resize
  EXPECT_EQ(r.inners(1).inners2_size(), 0);  // we do not resize
  ASSERT_EQ(r.inners(2).inners2_size(), 1);
  EXPECT_EQ(r.inners(2).inners2(0).z(), 17);

  r.mutable_inners(0)->add_inners2();
  r.mutable_inners(2)->mutable_inners2(0)->clear_z();
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 3);
  ASSERT_EQ(r.inners(0).inners2_size(), 1);
  EXPECT_EQ(r.inners(0).inners2(0).z(), 19);
  EXPECT_EQ(r.inners(1).inners2_size(), 0);  // we do not resize
  ASSERT_EQ(r.inners(2).inners2_size(), 1);
  EXPECT_EQ(r.inners(2).inners2(0).z(), 17);

  r.mutable_inners(1)->add_inners2();
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 3);
  ASSERT_EQ(r.inners(0).inners2_size(), 1);
  EXPECT_EQ(r.inners(0).inners2(0).z(), 19);
  ASSERT_EQ(r.inners(1).inners2_size(), 1);
  EXPECT_FALSE(r.inners(1).inners2(0).has_z());
  ASSERT_EQ(r.inners(2).inners2_size(), 1);
  EXPECT_EQ(r.inners(2).inners2(0).z(), 17);
}

TEST(InputLoaderTest, TestGetArrayProtoSlotListenerWithArrayInEndOfLastPath) {
  using aint = ::arolla::DenseArray<int>;

  FrameLayout::Builder layout_builder;
  auto in_array_as_slot = layout_builder.AddSlot<aint>();

  // Bind all listeners
  auto slot_listener = ::my_namespace::GetArrayProtoSlotListener();
  EXPECT_THAT(slot_listener->GetQTypeOf("in_array_as"), GetQType<aint>());
  ASSERT_OK_AND_ASSIGN(
      auto bound_listener,
      slot_listener->Bind({
          {"in_array_as", TypedSlot::FromSlot(in_array_as_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_listener(frame, &r));
  // All values are missed, so nothing should be set
  EXPECT_EQ(r.inners_size(), 0);

  frame.Set(in_array_as_slot, CreateDenseArray<int>({19, std::nullopt, 17}));
  EXPECT_THAT(bound_listener(frame, &r),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       MatchesRegex(".*/inners/as.*proto.*0.*array.*3")));
  EXPECT_EQ(r.inners_size(), 0);  // no resize happen

  r.add_inners();
  r.add_inners();
  r.add_inners();
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 3);
  EXPECT_EQ(r.inners(0).as_size(), 0);  // we do not resize
  EXPECT_EQ(r.inners(1).as_size(), 0);  // we do not resize
  EXPECT_EQ(r.inners(2).as_size(), 0);  // we do not resize

  r.mutable_inners(2)->add_as(-1);
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 3);
  EXPECT_EQ(r.inners(0).as_size(), 0);  // we do not resize
  EXPECT_EQ(r.inners(1).as_size(), 0);  // we do not resize
  ASSERT_EQ(r.inners(2).as_size(), 1);
  EXPECT_EQ(r.inners(2).as(0), 17);

  r.mutable_inners(0)->add_as(-1);
  r.mutable_inners(2)->set_as(0, -1);
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 3);
  ASSERT_EQ(r.inners(0).as_size(), 1);
  EXPECT_EQ(r.inners(0).as(0), 19);
  EXPECT_EQ(r.inners(1).as_size(), 0);  // we do not resize
  ASSERT_EQ(r.inners(2).as_size(), 1);
  EXPECT_EQ(r.inners(2).as(0), 17);

  r.mutable_inners(1)->add_as(-1);
  ASSERT_OK(bound_listener(frame, &r));
  ASSERT_EQ(r.inners_size(), 3);
  ASSERT_EQ(r.inners(0).as_size(), 1);
  EXPECT_EQ(r.inners(0).as(0), 19);
  ASSERT_EQ(r.inners(1).as_size(), 1);
  EXPECT_EQ(r.inners(1).as(0), -1);  // not modified
  ASSERT_EQ(r.inners(2).as_size(), 1);
  EXPECT_EQ(r.inners(2).as(0), 17);
}

TEST(InputLoaderTest, TestGetProtoSlotListenerExtensions) {
  using OInt = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<OInt>();

  std::string x_name(naming::TablePath()
                         .Column(naming::ProtoExtensionAccess(
                             "testing_extension_namespace.extension_x_int32"))
                         .FullName());
  std::string inner_x_name(
      naming::TablePath()
          .Child("inner")
          .Child(naming::ProtoExtensionAccess(
              "testing_extension_namespace.InnerExtension.inner_ext"))
          .Column("inner_extension_x_int32")
          .FullName());

  // Bind all listeners
  auto slot_listener = ::my_namespace::GetProtoSlotListener();
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OInt>();
  auto inner_x_slot = layout_builder.AddSlot<OInt>();
  EXPECT_THAT(slot_listener->GetQTypeOf(x_name), Eq(oi32));
  EXPECT_THAT(slot_listener->GetQTypeOf(inner_x_name), Eq(oi32));
  ASSERT_OK_AND_ASSIGN(auto bound_listener,
                       slot_listener->Bind({
                           {x_name, TypedSlot::FromSlot(x_slot)},
                           {inner_x_name, TypedSlot::FromSlot(inner_x_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_listener(frame, &r));
  // All values are missed, so nothing should be set
  EXPECT_FALSE(
      r.HasExtension(::testing_extension_namespace::extension_x_int32));
  EXPECT_FALSE(r.inner()
                   .GetExtension(
                       ::testing_extension_namespace::InnerExtension::inner_ext)
                   .has_inner_extension_x_int32());

  frame.Set(x_slot, 19);
  ASSERT_OK(bound_listener(frame, &r));
  EXPECT_EQ(r.GetExtension(::testing_extension_namespace::extension_x_int32),
            19);
  EXPECT_FALSE(r.inner()
                   .GetExtension(
                       ::testing_extension_namespace::InnerExtension::inner_ext)
                   .has_inner_extension_x_int32());

  frame.Set(inner_x_slot, 57);
  r = {};
  ASSERT_OK(bound_listener(frame, &r));
  EXPECT_EQ(r.GetExtension(::testing_extension_namespace::extension_x_int32),
            19);
  EXPECT_EQ(r.inner()
                .GetExtension(
                    ::testing_extension_namespace::InnerExtension::inner_ext)
                .inner_extension_x_int32(),
            57);
}

}  // namespace
}  // namespace arolla
