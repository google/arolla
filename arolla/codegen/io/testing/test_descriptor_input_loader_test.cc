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
#include "arolla/codegen/io/testing/test_descriptor_input_loader.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/codegen/io/testing/test_dense_array_extensions_input_loader.h"
#include "arolla/codegen/io/testing/test_descriptor_input_loader_nested_message_type.h"
#include "arolla/codegen/io/testing/test_descriptor_input_loader_with_no_repeated.h"
#include "arolla/codegen/io/testing/test_descriptor_span_input_loader.h"
#include "arolla/codegen/io/testing/test_descriptor_subset_input_loader.h"
#include "arolla/codegen/io/testing/test_descriptor_with_extensions_input_loader.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/proto_types/types.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/naming/table.h"
#include "arolla/proto/testing/test_extension.pb.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

using ::arolla::proto::arolla_size_t;
using ::arolla::testing::InputLoaderSupports;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::IsNull;
using ::testing::Not;

TEST(InputLoaderTest, TestSinglePointDescriptorBasedLoader) {
  using OI32 = ::arolla::OptionalValue<int>;
  using OBytes = ::arolla::OptionalValue<Bytes>;
  using OText = ::arolla::OptionalValue<Text>;
  auto oi32 = GetQType<OI32>();
  auto obytes = GetQType<OBytes>();
  auto otext = GetQType<OText>();
  const auto& input_loader = ::my_namespace::GetDescriptorBasedLoader();
  std::string x_def_name(naming::TablePath().Column("x").FullName());
  std::string x_enum_def_name(naming::TablePath().Column("x_enum").FullName());
  std::string str_def_name(naming::TablePath().Column("str").FullName());
  std::string raw_bytes_def_name(
      naming::TablePath().Column("raw_bytes").FullName());
  std::string proto3_int32_name(
      naming::TablePath("proto3").Column("non_optional_i32").FullName());
  std::string x_alt_name("x");
  std::string str_alt_name("str");
  EXPECT_THAT(input_loader, InputLoaderSupports({{x_def_name, oi32},
                                                 {x_alt_name, oi32},
                                                 {x_enum_def_name, oi32},
                                                 {str_def_name, otext},
                                                 {str_alt_name, obytes},
                                                 {raw_bytes_def_name, obytes},
                                                 {proto3_int32_name, oi32}}));

  FrameLayout::Builder layout_builder;
  auto x_def_slot = layout_builder.AddSlot<OI32>();
  auto x_alt_slot = layout_builder.AddSlot<OI32>();
  auto x_enum_slot = layout_builder.AddSlot<OI32>();
  auto str_slot = layout_builder.AddSlot<OText>();
  auto str_alt_slot = layout_builder.AddSlot<OBytes>();
  auto raw_bytes_slot = layout_builder.AddSlot<OBytes>();
  auto proto3_int32_slot = layout_builder.AddSlot<OI32>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {x_def_name, TypedSlot::FromSlot(x_def_slot)},
          {x_alt_name, TypedSlot::FromSlot(x_alt_slot)},
          {x_enum_def_name, TypedSlot::FromSlot(x_enum_slot)},
          {str_def_name, TypedSlot::FromSlot(str_slot)},
          {str_alt_name, TypedSlot::FromSlot(str_alt_slot)},
          {raw_bytes_def_name, TypedSlot::FromSlot(raw_bytes_slot)},
          {proto3_int32_name, TypedSlot::FromSlot(proto3_int32_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.set_x(19);
  r.set_x_enum(::testing_namespace::Root::SECOND_VALUE);
  r.set_str("abc");
  r.set_raw_bytes("cba");
  r.mutable_proto3()->set_non_optional_i32(71);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_def_slot), 19);
  EXPECT_EQ(frame.Get(x_alt_slot), 19);
  EXPECT_EQ(frame.Get(x_enum_slot),
            OI32(::testing_namespace::Root::SECOND_VALUE));
  EXPECT_EQ(frame.Get(str_slot), Text("abc"));
  EXPECT_EQ(frame.Get(raw_bytes_slot), Bytes("cba"));
  EXPECT_EQ(frame.Get(str_alt_slot), Bytes("abc"));
  EXPECT_EQ(frame.Get(str_alt_slot), Bytes("abc"));
  EXPECT_EQ(frame.Get(str_alt_slot), Bytes("abc"));
  EXPECT_EQ(frame.Get(proto3_int32_slot), 71);
  r.clear_x();
  r.clear_x_enum();
  r.clear_str();
  r.clear_raw_bytes();
  r.mutable_proto3()->clear_non_optional_i32();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_def_slot), std::nullopt);
  EXPECT_EQ(frame.Get(x_alt_slot), std::nullopt);
  EXPECT_EQ(frame.Get(x_enum_slot), std::nullopt);
  EXPECT_EQ(frame.Get(str_slot), std::nullopt);
  EXPECT_EQ(frame.Get(raw_bytes_slot), std::nullopt);
  EXPECT_EQ(frame.Get(str_alt_slot), std::nullopt);
  EXPECT_EQ(frame.Get(proto3_int32_slot), 0);
  r.clear_proto3();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(proto3_int32_slot), std::nullopt);
}

TEST(InputLoaderTest,
     ClearBothSizesAndOptionalsRequestedSingleValueInputsCorrectly) {
  using OI32 = ::arolla::OptionalValue<int>;
  const auto& input_loader = ::my_namespace::GetDescriptorBasedLoader();
  std::string inner_a_name(
      naming::TablePath().Child("inner").Column("a").FullName());
  std::string inner_as_size_name(
      naming::TablePath().Child("inner").Size("as").FullName());
  std::string inner2_zs_size_name(
      naming::TablePath().Child("inner").Child("inner2").Size("zs").FullName());

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OI32>();
  auto as_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  auto zs_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind(
          {{inner_a_name, TypedSlot::FromSlot(a_slot)},
           {inner_as_size_name, TypedSlot::FromSlot(as_size_slot)},
           {inner2_zs_size_name, TypedSlot::FromSlot(zs_size_slot)}}));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  auto fill_garbage = [&]() {
    frame.Set(a_slot, -1);
    frame.Set(as_size_slot, DenseArrayShape{999});
    frame.Set(zs_size_slot, DenseArrayShape{9999});
  };

  ::testing_namespace::Root r;
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(as_size_slot), DenseArrayShape{0});
  EXPECT_EQ(frame.Get(zs_size_slot), DenseArrayShape{0});

  auto* inner = r.mutable_inner();
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(as_size_slot), DenseArrayShape{0});
  EXPECT_EQ(frame.Get(zs_size_slot), DenseArrayShape{0});

  auto* inner2 = inner->mutable_inner2();
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(as_size_slot), DenseArrayShape{0});
  EXPECT_EQ(frame.Get(zs_size_slot), DenseArrayShape{0});

  inner->add_as(-1);
  inner2->add_zs(-1);
  inner2->add_zs(-1);
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(as_size_slot), DenseArrayShape{1});
  EXPECT_EQ(frame.Get(zs_size_slot), DenseArrayShape{2});
}

TEST(InputLoaderTest,
     ClearOptionalsWithoutSizesRequestedSingleValueInputsCorrectly) {
  using OI32 = ::arolla::OptionalValue<int>;
  const auto& input_loader = ::my_namespace::GetDescriptorBasedLoader();
  std::string inner_a_name(
      naming::TablePath().Child("inner").Column("a").FullName());
  std::string inner2_z_name(naming::TablePath()
                                .Child("inner")
                                .Child("inner2")
                                .Column("z")
                                .FullName());

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OI32>();
  auto z_slot = layout_builder.AddSlot<OI32>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({{inner_a_name, TypedSlot::FromSlot(a_slot)},
                          {inner2_z_name, TypedSlot::FromSlot(z_slot)}}));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  auto fill_garbage = [&]() {
    frame.Set(a_slot, -1);
    frame.Set(z_slot, -1);
  };

  ::testing_namespace::Root r;
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(z_slot), std::nullopt);

  auto* inner = r.mutable_inner();
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(z_slot), std::nullopt);

  auto* inner2 = inner->mutable_inner2();
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(z_slot), std::nullopt);

  inner->set_a(5);
  inner2->set_z(7);
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), 5);
  EXPECT_EQ(frame.Get(z_slot), 7);
}

TEST(InputLoaderTest,
     ClearSizesNoOptionalsRequestedSingleValueInputsCorrectly) {
  const auto& input_loader = ::my_namespace::GetDescriptorBasedLoader();
  std::string inner_as_size_name(
      naming::TablePath().Child("inner").Size("as").FullName());
  std::string inner2_zs_size_name(
      naming::TablePath().Child("inner").Child("inner2").Size("zs").FullName());

  FrameLayout::Builder layout_builder;
  auto as_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  auto zs_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind(
          {{inner_as_size_name, TypedSlot::FromSlot(as_size_slot)},
           {inner2_zs_size_name, TypedSlot::FromSlot(zs_size_slot)}}));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  auto fill_garbage = [&]() {
    frame.Set(as_size_slot, DenseArrayShape{999});
    frame.Set(zs_size_slot, DenseArrayShape{9999});
  };

  ::testing_namespace::Root r;
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(as_size_slot), DenseArrayShape{0});
  EXPECT_EQ(frame.Get(zs_size_slot), DenseArrayShape{0});

  auto* inner = r.mutable_inner();
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(as_size_slot), DenseArrayShape{0});
  EXPECT_EQ(frame.Get(zs_size_slot), DenseArrayShape{0});

  auto* inner2 = inner->mutable_inner2();
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(as_size_slot), DenseArrayShape{0});
  EXPECT_EQ(frame.Get(zs_size_slot), DenseArrayShape{0});

  inner->add_as(-1);
  inner2->add_zs(-1);
  inner2->add_zs(-1);
  fill_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(as_size_slot), DenseArrayShape{1});
  EXPECT_EQ(frame.Get(zs_size_slot), DenseArrayShape{2});
}

TEST(InputLoaderTest, TestSinglePointDescriptorBasedSubsetLoader) {
  using OI32 = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<OI32>();
  const auto& input_loader = ::my_namespace::GetDescriptorBasedSubsetLoader();
  std::string proto3_int32_name(
      naming::TablePath("proto3").Column("non_optional_i32").FullName());
  EXPECT_THAT(input_loader, InputLoaderSupports({{proto3_int32_name, oi32}}));
  std::string x_name(naming::TablePath().Column("x").FullName());
  EXPECT_THAT(input_loader, Not(InputLoaderSupports({{x_name, oi32}})));

  FrameLayout::Builder layout_builder;
  auto proto3_int32_slot = layout_builder.AddSlot<OI32>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {proto3_int32_name, TypedSlot::FromSlot(proto3_int32_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.mutable_proto3()->set_non_optional_i32(71);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(proto3_int32_slot), 71);
  r.mutable_proto3()->clear_non_optional_i32();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(proto3_int32_slot), 0);
  r.clear_proto3();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(proto3_int32_slot), std::nullopt);
}

TEST(InputLoaderTest, TestSinglePointDescriptorBasedSpanLoader) {
  using DAI32 = ::arolla::DenseArray<int>;
  auto dai32 = GetQType<DAI32>();
  const auto& input_loader = ::my_namespace::GetDescriptorBasedSpanLoader();
  std::string x_name(naming::TablePath().Column("x").FullName());
  EXPECT_THAT(input_loader, InputLoaderSupports({{x_name, dai32}}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<DAI32>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {x_name, TypedSlot::FromSlot(x_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r1;
  r1.set_x(17);
  ASSERT_OK(bound_input_loader({r1}, frame));
  EXPECT_THAT(frame.Get(x_slot), ElementsAre(17));
  ::testing_namespace::Root r2;
  r2.set_x(71);
  ASSERT_OK(bound_input_loader({r1, r2}, frame));
  EXPECT_THAT(frame.Get(x_slot), ElementsAre(17, 71));
  r1.clear_x();
  ASSERT_OK(bound_input_loader({r1}, frame));
  EXPECT_THAT(frame.Get(x_slot), ElementsAre(std::nullopt));
}

TEST(InputLoaderTest, TestNoRepeatedDescriptorBasedLoader) {
  using OI32 = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<OI32>();
  const auto& input_loader =
      ::my_namespace::GetDescriptorBasedLoaderWithoutRepeated();
  std::string x_def_name(naming::TablePath().Column("x").FullName());
  std::string x9_def_name(naming::TablePath().Column("x9").FullName());
  EXPECT_THAT(input_loader, InputLoaderSupports({{x_def_name, oi32}}));
  std::string floats_size_name(
      naming::TablePath().Size("repeated_floats").FullName());
  std::string floats_name(
      naming::TablePath().Column("repeated_floats").FullName());
  EXPECT_THAT(input_loader->GetQTypeOf(floats_size_name), IsNull());
  EXPECT_THAT(input_loader->GetQTypeOf(floats_name), IsNull());

  FrameLayout::Builder layout_builder;
  auto x_def_slot = layout_builder.AddSlot<OI32>();
  auto x9_def_slot = layout_builder.AddSlot<OI32>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {x_def_name, TypedSlot::FromSlot(x_def_slot)},
                           {x9_def_name, TypedSlot::FromSlot(x9_def_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.set_x(19);
  r.set_x9(23);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_def_slot), 19);
  EXPECT_EQ(frame.Get(x9_def_slot), 23);
  r.clear_x();
  r.clear_x9();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_def_slot), std::nullopt);
  EXPECT_EQ(frame.Get(x9_def_slot), std::nullopt);
}

TEST(InputLoaderTest, TestSharding) {
  std::vector<std::string> inputs = {
      "/BrOkEn_CaSe",
      "/inner/a",
      "/inner/inner2/z",
      "/inner/raw_bytes",
      "/inner/str",
      "/private",
      "/proto3/non_optional_i32",
      "/raw_bytes",
      "/str",
      "/x",
      "/x0",
      "/x1",
      "/x2",
      "/x3",
      "/x4",
      "/x5",
      "/x6",
      "/x7",
      "/x8",
      "/x9",
      "/x_double",
      "/x_enum",
      "/x_float",
      "/x_int64",
      "/x_uint32",
      "/x_uint64"};

  const auto& main_input_loader =
      ::my_namespace::GetDescriptorBasedLoaderWithoutRepeated();
  const auto& input_loader_shards =
      ::my_namespace::GetDescriptorBasedLoaderWithoutRepeated_Shards();

  for (size_t i = 0; i != inputs.size(); ++i) {
    std::optional<size_t> shard_for_input;
    for (size_t shard_id = 0; shard_id != input_loader_shards.size();
         ++shard_id) {
      auto qtype = input_loader_shards[shard_id]->GetQTypeOf(inputs[i]);
      if (qtype != nullptr) {
        ASSERT_THAT(shard_for_input, Eq(std::nullopt))
            << "duplicated input " << inputs[i] << " shard_id=" << shard_id
            << " shard_id=" << *shard_for_input;
        shard_for_input = shard_id;
        EXPECT_THAT(qtype, Eq(main_input_loader->GetQTypeOf(inputs[i])))
            << "shard_id=" << shard_id << " input=" << inputs[i];
      }
    }
    ASSERT_THAT(shard_for_input, Not(Eq(std::nullopt)))
        << "no shard provides input " << inputs[i];
  }
}

TEST(InputLoaderTest, TestMultiValueDescriptorBasedLoader) {
  const auto& input_loader = ::my_namespace::GetDescriptorBasedLoader();
  std::string floats_size_name(
      naming::TablePath().Size("repeated_floats").FullName());
  std::string floats_name(
      naming::TablePath().Column("repeated_floats").FullName());
  std::string strs_def_name(
      naming::TablePath().Column("repeated_str").FullName());
  std::string raw_bytes_name(
      naming::TablePath().Column("repeated_raw_bytes").FullName());
  std::string strs_alt_name("repeated_str");
  EXPECT_THAT(
      input_loader,
      InputLoaderSupports({{floats_name, GetDenseArrayQType<float>()},
                           {floats_size_name, GetQType<DenseArrayShape>()},
                           {strs_def_name, GetDenseArrayQType<Text>()},
                           {strs_alt_name, GetDenseArrayQType<Bytes>()},
                           {raw_bytes_name, GetDenseArrayQType<Bytes>()}}));

  FrameLayout::Builder layout_builder;
  auto floats_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto floats_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  auto strs_slot = layout_builder.AddSlot<DenseArray<Text>>();
  auto strs_alt_slot = layout_builder.AddSlot<DenseArray<Bytes>>();
  auto raw_bytes_slot = layout_builder.AddSlot<DenseArray<Bytes>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {floats_name, TypedSlot::FromSlot(floats_slot)},
          {floats_size_name, TypedSlot::FromSlot(floats_size_slot)},
          {strs_def_name, TypedSlot::FromSlot(strs_slot)},
          {strs_alt_name, TypedSlot::FromSlot(strs_alt_slot)},
          {raw_bytes_name, TypedSlot::FromSlot(raw_bytes_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.add_repeated_floats(19.0f);
  r.add_repeated_floats(3.0f);

  r.add_repeated_str("abc");
  r.add_repeated_str("xyz");
  r.add_repeated_str("qwe");

  r.add_repeated_raw_bytes("cba");
  r.add_repeated_raw_bytes("zyx");

  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(floats_slot), ElementsAre(19.0f, 3.0f));
  EXPECT_THAT(frame.Get(floats_size_slot).size, 2);
  EXPECT_THAT(frame.Get(strs_slot),
              ElementsAre(Text("abc"), Text("xyz"), Text("qwe")));
  EXPECT_THAT(frame.Get(raw_bytes_slot),
              ElementsAre(Bytes("cba"), Bytes("zyx")));
  EXPECT_THAT(frame.Get(strs_alt_slot),
              ElementsAre(Bytes("abc"), Bytes("xyz"), Bytes("qwe")));
  r.clear_repeated_floats();
  r.clear_repeated_str();
  r.clear_repeated_raw_bytes();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(floats_slot), IsEmpty());
  EXPECT_THAT(frame.Get(strs_slot), IsEmpty());
  EXPECT_THAT(frame.Get(raw_bytes_slot), IsEmpty());
  EXPECT_THAT(frame.Get(strs_alt_slot), IsEmpty());
}

TEST(InputLoaderTest, TestMultiValueDescriptorBasedLoaderSizes) {
  const auto& input_loader = ::my_namespace::GetDescriptorBasedLoader();
  std::string inners_as_size_name(
      naming::TablePath("inners").Size("as").FullName());
  std::string inners_inner2_zs_size_name(
      naming::TablePath("inners").Child("inner2").Size("zs").FullName());
  EXPECT_THAT(
      input_loader,
      InputLoaderSupports({
          {inners_as_size_name, GetDenseArrayQType<arolla_size_t>()},
          {inners_inner2_zs_size_name, GetDenseArrayQType<arolla_size_t>()},
      }));

  FrameLayout::Builder layout_builder;
  auto inners_as_size_slot =
      layout_builder.AddSlot<DenseArray<arolla_size_t>>();
  auto inners_inner2_zs_size_slot =
      layout_builder.AddSlot<DenseArray<arolla_size_t>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {inners_as_size_name, TypedSlot::FromSlot(inners_as_size_slot)},
          {inners_inner2_zs_size_name,
           TypedSlot::FromSlot(inners_inner2_zs_size_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(inners_as_size_slot), IsEmpty());

  {
    auto* i = r.add_inners();
    i->add_as(3);
    i->add_as(1);
    auto* i2 = i->mutable_inner2();
    i2->add_zs(2);
    i2->add_zs(7);
    i2->add_zs(1);
  }
  r.add_inners();
  {
    auto* i = r.add_inners();
    i->add_as(4);
    auto* i2 = i->mutable_inner2();
    i2->add_zs(8);
    i2->add_zs(2);
  }
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(inners_as_size_slot), ElementsAre(2, 0, 1));
  EXPECT_THAT(frame.Get(inners_inner2_zs_size_slot), ElementsAre(3, 0, 2));
}

TEST(InputLoaderTest, TestExtensionProtoLoader) {
  using OInt = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<OInt>();
  const auto& input_loader = ::my_namespace::GetDescriptorWithExtensionLoader();
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
  std::string inners_x_name(
      naming::TablePath()
          .Child("inners")
          .Child(naming::ProtoExtensionAccess(
              "testing_extension_namespace.InnerExtension.inner_ext"))
          .Column("inner_extension_x_int32")
          .FullName());
  EXPECT_THAT(input_loader, InputLoaderSupports(
                                {{x_name, oi32},
                                 {inner_x_name, oi32},
                                 {inners_x_name, GetDenseArrayQType<int>()}}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OInt>();
  auto inner_x_slot = layout_builder.AddSlot<OInt>();
  auto inners_x_slot = layout_builder.AddSlot<DenseArray<int>>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {x_name, TypedSlot::FromSlot(x_slot)},
                           {inner_x_name, TypedSlot::FromSlot(inner_x_slot)},
                           {inners_x_name, TypedSlot::FromSlot(inners_x_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt());
  EXPECT_EQ(frame.Get(inner_x_slot), OInt());
  EXPECT_THAT(frame.Get(inners_x_slot), IsEmpty());

  r.SetExtension(::testing_extension_namespace::extension_x_int32, 5);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(5));
  EXPECT_EQ(frame.Get(inner_x_slot), OInt());
  EXPECT_THAT(frame.Get(inners_x_slot), IsEmpty());

  r.mutable_inner()
      ->MutableExtension(
          ::testing_extension_namespace::InnerExtension::inner_ext)
      ->set_inner_extension_x_int32(7);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(5));
  EXPECT_EQ(frame.Get(inner_x_slot), OInt(7));
  EXPECT_THAT(frame.Get(inners_x_slot), IsEmpty());

  r.add_inners()
      ->MutableExtension(
          ::testing_extension_namespace::InnerExtension::inner_ext)
      ->set_inner_extension_x_int32(57);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(5));
  EXPECT_EQ(frame.Get(inner_x_slot), OInt(7));
  EXPECT_THAT(frame.Get(inners_x_slot), ElementsAre(57));
}

TEST(InputLoaderTest,
     TestExtensionDenseArrayProtoLoaderMissedParentOfExtension) {
  const auto& input_loader = ::my_namespace::GetDenseArrayExtensionLoader();
  EXPECT_THAT(input_loader, InputLoaderSupports({{"inners_rr_inner_ext_xs",
                                                  GetDenseArrayQType<int>()}}));

  FrameLayout::Builder layout_builder;
  auto xs_slot = layout_builder.AddSlot<DenseArray<int>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {"inners_rr_inner_ext_xs", TypedSlot::FromSlot(xs_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(xs_slot), IsEmpty());

  r.add_inners();
  r.add_inners()->mutable_root_reference();
  r.add_inners()->mutable_root_reference()->mutable_inner();
  r.add_inners()->mutable_root_reference()->mutable_inner()->MutableExtension(
      ::testing_extension_namespace::InnerExtension::inner_ext);
  {
    auto* ext =
        r.add_inners()
            ->mutable_root_reference()
            ->mutable_inner()
            ->MutableExtension(
                ::testing_extension_namespace::InnerExtension::inner_ext);
    ext->add_repeated_inner_extension_x_int32(3);
    ext->add_repeated_inner_extension_x_int32(1);
    ext->add_repeated_inner_extension_x_int32(4);
  }

  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(xs_slot), ElementsAre(3, 1, 4));
}

TEST(InputLoaderTest, TestNestedMessageTypeLoader) {
  const auto& input_loader =
      ::my_namespace::GetDescriptorBasedLoaderNestedMessageType();

  FrameLayout::Builder layout_builder;
  auto z_def_slot = layout_builder.AddSlot<::arolla::OptionalValue<int>>();
  std::string z_def_name(naming::TablePath().Column("z").FullName());
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {z_def_name, TypedSlot::FromSlot(z_def_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Inner::Inner2 i;
  i.set_z(1234567);
  ASSERT_OK(bound_input_loader(i, frame));
  EXPECT_EQ(frame.Get(z_def_slot), 1234567);
}

TEST(InputLoaderTest, TestDenseArrayProtoLoaderNestedRepeated) {
  const auto& input_loader = ::my_namespace::GetDescriptorBasedLoader();
  EXPECT_THAT(input_loader,
              InputLoaderSupports(
                  {{"inners__inners2__z", GetDenseArrayQType<int32_t>()},
                   {"inners__inner2__zs", GetDenseArrayQType<int32_t>()}}));

  FrameLayout::Builder layout_builder;
  auto z_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto zs_slot = layout_builder.AddSlot<DenseArray<int>>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {"inners__inners2__z", TypedSlot::FromSlot(z_slot)},
                           {"inners__inner2__zs", TypedSlot::FromSlot(zs_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;

  {
    auto* i = r.add_inners();
    i->add_inners2()->set_z(3);
    i->add_inners2()->set_z(1);
    i->mutable_inner2()->add_zs(2);
    i->mutable_inner2()->add_zs(7);
  }
  {
    auto* i = r.add_inners();
    i->add_inners2()->set_z(4);
    i->add_inners2()->set_z(1);
  }
  {
    auto* i = r.add_inners();
    i->add_inners2()->set_z(5);
    i->add_inners2()->set_z(9);
    i->add_inners2()->set_z(2);
    i->mutable_inner2()->add_zs(1);
    i->mutable_inner2()->add_zs(8);
    i->mutable_inner2()->add_zs(2);
    i->mutable_inner2()->add_zs(8);
  }

  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(z_slot), ElementsAre(3, 1, 4, 1, 5, 9, 2));
  EXPECT_THAT(frame.Get(zs_slot), ElementsAre(2, 7, 1, 8, 2, 8));
}

TEST(InputLoaderTest, TestDenseArrayProtoLoaderRepeatedAfterMessage) {
  const auto& input_loader = ::my_namespace::GetDescriptorBasedLoader();
  EXPECT_THAT(input_loader,
              InputLoaderSupports(
                  {{"inner__inners2__z", GetDenseArrayQType<int32_t>()},
                   {"inners__inner2__z", GetDenseArrayQType<int32_t>()},
                   {"inners__inner2__zs", GetDenseArrayQType<int32_t>()}}));

  FrameLayout::Builder layout_builder;
  auto z1_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto z2_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto z3_slot = layout_builder.AddSlot<DenseArray<int>>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {"inner__inners2__z", TypedSlot::FromSlot(z1_slot)},
                           {"inners__inner2__z", TypedSlot::FromSlot(z2_slot)},
                           {"inners__inner2__zs", TypedSlot::FromSlot(z3_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;

  {
    auto* i = r.add_inners()->mutable_inner2();
    i->set_z(3);
    i->add_zs(2);
  }
  r.add_inners();
  {
    auto* i = r.add_inners()->mutable_inner2();
    i->set_z(1);
    i->add_zs(7);
    i->add_zs(1);
  }
  {
    auto* i = r.add_inners()->mutable_inner2();
    i->set_z(4);
    i->add_zs(8);
    i->add_zs(2);
  }
  r.add_inners()->mutable_inner2();

  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(z1_slot), IsEmpty());
  EXPECT_THAT(frame.Get(z2_slot),
              ElementsAre(3, std::nullopt, 1, 4, std::nullopt));
  EXPECT_THAT(frame.Get(z3_slot), ElementsAre(2, 7, 1, 8, 2));

  r.mutable_inner();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(z1_slot), IsEmpty());

  r.mutable_inner()->add_inners2()->set_z(2);
  r.mutable_inner()->add_inners2();
  r.mutable_inner()->add_inners2()->set_z(7);
  r.mutable_inner()->add_inners2()->set_z(1);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(z1_slot), ElementsAre(2, std::nullopt, 7, 1));
}

}  // namespace
}  // namespace arolla
