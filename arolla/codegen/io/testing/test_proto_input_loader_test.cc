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
#include "arolla/codegen/io/testing/test_proto_input_loader.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/codegen/io/testing/test_dense_array_repeated_proto_input_loader.h"
#include "arolla/codegen/io/testing/test_dense_array_single_field_repeated_proto_input_loader.h"
#include "arolla/codegen/io/testing/test_proto_extension_input_loader.h"
#include "arolla/codegen/io/testing/test_proto_input_loader_with_duplicated_configuration.h"
#include "arolla/codegen/io/testing/test_proto_qtype.h"
#include "arolla/codegen/io/testing/test_repeated_proto_input_loader.h"
#include "arolla/codegen/io/testing/test_repeated_proto_input_loader_with_no_branch_for_intemediate.h"
#include "arolla/codegen/io/testing/test_repeated_proto_input_loader_with_parent_intermediate_node_collected.h"
#include "arolla/codegen/io/testing/test_scalar_accessor_with_default_value_input_loader.h"
#include "arolla/codegen/io/testing/test_span_proto_input_loader.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/proto_types/types.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/naming/table.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/proto/testing/test_extension.pb.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {
namespace {

using ::arolla::testing::InputLoaderSupports;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;

TEST(InputLoaderTest, TestGetProtoLoader) {
  using OInt = ::arolla::OptionalValue<int>;
  using obytes = ::arolla::OptionalValue<Bytes>;
  using otext = ::arolla::OptionalValue<Text>;
  auto oi32 = GetQType<OInt>();
  auto obytes_qtype = GetQType<obytes>();
  auto otext_qtype = GetQType<otext>();
  const auto& input_loader = ::my_namespace::GetProtoLoader();
  EXPECT_THAT(input_loader,
              InputLoaderSupports(
                  {{"/x", oi32},
                   {"raw_bytes", obytes_qtype},
                   {"str[\"_\"]", otext_qtype},
                   {"inner__a", oi32},
                   {"ys_0", oi32},
                   {"inners_0__a", oi32},
                   {"map_int_a", oi32},
                   {"/BrOkEn_CaSe", oi32},
                   {"/private", oi32},
                   {"x_enum", oi32},
                   {"/inners[0]",
                    GetOptionalQType<testing_namespace::InnerRawPtr>()}}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OInt>();
  auto str_slot = layout_builder.AddSlot<otext>();
  auto raw_bytes_slot = layout_builder.AddSlot<obytes>();
  auto a_slot = layout_builder.AddSlot<OInt>();
  auto ys_0_slot = layout_builder.AddSlot<OInt>();
  auto inners_0_a_slot = layout_builder.AddSlot<OInt>();
  auto map_int_a_slot = layout_builder.AddSlot<OInt>();
  auto broken_case_slot = layout_builder.AddSlot<OInt>();
  auto private_slot = layout_builder.AddSlot<OInt>();
  auto x_enum_slot = layout_builder.AddSlot<OInt>();
  auto inners_0_ptr_slot =
      layout_builder.AddSlot<OptionalValue<testing_namespace::InnerRawPtr>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {"/x", TypedSlot::FromSlot(x_slot)},
          {"str[\"_\"]", TypedSlot::FromSlot(str_slot)},
          {"raw_bytes", TypedSlot::FromSlot(raw_bytes_slot)},
          {"inner__a", TypedSlot::FromSlot(a_slot)},
          {"ys_0", TypedSlot::FromSlot(ys_0_slot)},
          {"inners_0__a", TypedSlot::FromSlot(inners_0_a_slot)},
          {"map_int_a", TypedSlot::FromSlot(map_int_a_slot)},
          {"/BrOkEn_CaSe", TypedSlot::FromSlot(broken_case_slot)},
          {"/private", TypedSlot::FromSlot(private_slot)},
          {"x_enum", TypedSlot::FromSlot(x_enum_slot)},
          {"/inners[0]", TypedSlot::FromSlot(inners_0_ptr_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.set_x(5);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(5));
  EXPECT_EQ(frame.Get(str_slot), otext());
  EXPECT_EQ(frame.Get(raw_bytes_slot), obytes());
  EXPECT_EQ(frame.Get(a_slot), OInt());
  EXPECT_EQ(frame.Get(ys_0_slot), OInt());
  EXPECT_EQ(frame.Get(inners_0_a_slot), OInt());
  EXPECT_EQ(frame.Get(map_int_a_slot), OInt());
  EXPECT_EQ(frame.Get(broken_case_slot), OInt());
  EXPECT_EQ(frame.Get(private_slot), OInt());
  EXPECT_EQ(frame.Get(x_enum_slot), OInt());
  EXPECT_EQ(frame.Get(inners_0_ptr_slot), std::nullopt);

  ASSERT_FALSE(r.has_inner());
  r.mutable_inner();
  ASSERT_TRUE(r.has_inner());
  EXPECT_EQ(frame.Get(a_slot), OInt());

  r.mutable_inner()->set_a(17);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), OInt(17));

  r.add_ys(21);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(ys_0_slot), OInt(21));

  r.add_inners()->set_a(11);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(inners_0_a_slot), OInt(11));

  r.set_str("abc");
  r.set_raw_bytes("cba");
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(str_slot), otext(Text("abc")));
  EXPECT_EQ(frame.Get(raw_bytes_slot), obytes(Bytes("cba")));
  EXPECT_EQ(frame.Get(inners_0_ptr_slot), &r.inners(0));

  r.mutable_map_int()->insert({"b", 7});
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(map_int_a_slot), OInt());
  r.mutable_map_int()->insert({"a", 5});
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(map_int_a_slot), OInt(5));

  r.set_broken_case(57);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(broken_case_slot), OInt(57));

  r.set_private_(37);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(private_slot), OInt(37));

  r.set_x_enum(::testing_namespace::Root::SECOND_VALUE);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_enum_slot),
            OInt(::testing_namespace::Root::SECOND_VALUE));

  // Test that old values are overridden by nullopt
  r = ::testing_namespace::Root();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt());
  EXPECT_EQ(frame.Get(str_slot), otext());
  EXPECT_EQ(frame.Get(raw_bytes_slot), obytes());
  EXPECT_EQ(frame.Get(a_slot), OInt());
  EXPECT_EQ(frame.Get(ys_0_slot), OInt());
  EXPECT_EQ(frame.Get(inners_0_a_slot), OInt());
  EXPECT_EQ(frame.Get(map_int_a_slot), OInt());
  EXPECT_EQ(frame.Get(broken_case_slot), OInt());
  EXPECT_EQ(frame.Get(x_enum_slot), OInt());
  EXPECT_EQ(frame.Get(inners_0_ptr_slot), std::nullopt);
}

TEST(InputLoaderTest, TestGetSingleValueWithDefaultProtoLoader) {
  const auto& input_loader =
      ::my_namespace::GetSingleValueWithDefaultProtoLoader();
  EXPECT_THAT(input_loader,
              InputLoaderSupports({{"x0", GetQType<int32_t>()},
                                   {"x1", GetQType<int32_t>()},
                                   {"xf3", GetQType<float>()},
                                   {"xf4", GetQType<float>()},
                                   {"a", GetOptionalQType<int32_t>()},
                                   {"xrr", GetOptionalQType<int32_t>()}}));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OptionalValue<int32_t>>();
  auto xrr_slot = layout_builder.AddSlot<OptionalValue<int32_t>>();
  auto x0_slot = layout_builder.AddSlot<int32_t>();
  auto x1_slot = layout_builder.AddSlot<int32_t>();
  auto xf3_slot = layout_builder.AddSlot<float>();
  auto xf4_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},      //
                           {"xrr", TypedSlot::FromSlot(xrr_slot)},  //
                           {"x0", TypedSlot::FromSlot(x0_slot)},    //
                           {"x1", TypedSlot::FromSlot(x1_slot)},    //
                           {"xf3", TypedSlot::FromSlot(xf3_slot)},  //
                           {"xf4", TypedSlot::FromSlot(xf4_slot)},  //
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  auto fill_with_garbage = [&]() {
    frame.Set(xrr_slot, -99);
    frame.Set(a_slot, -77);
    frame.Set(x0_slot, -1);
    frame.Set(x1_slot, -2);
    frame.Set(xf3_slot, -3.0f);
    frame.Set(xf4_slot, -4.0f);
  };

  ::testing_namespace::Root r;

  fill_with_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(xrr_slot), std::nullopt);
  EXPECT_EQ(frame.Get(x0_slot), 0);
  EXPECT_EQ(frame.Get(x1_slot), 1);
  EXPECT_EQ(frame.Get(xf3_slot), 3.0f);
  EXPECT_EQ(frame.Get(xf4_slot), 4.0f);

  r.set_x(5);
  fill_with_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(xrr_slot), std::nullopt);
  EXPECT_EQ(frame.Get(x0_slot), 5);
  EXPECT_EQ(frame.Get(x1_slot), 5);
  EXPECT_EQ(frame.Get(xf3_slot), 3.0f);
  EXPECT_EQ(frame.Get(xf4_slot), 4.0f);

  auto* inner2 = r.mutable_inner()->mutable_inner2();
  fill_with_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(xrr_slot), std::nullopt);
  EXPECT_EQ(frame.Get(x0_slot), 5);
  EXPECT_EQ(frame.Get(x1_slot), 5);
  EXPECT_EQ(frame.Get(xf3_slot), 3.0f);
  EXPECT_EQ(frame.Get(xf4_slot), 4.0f);

  inner2->mutable_root_reference()->set_x_float(7.0f);
  fill_with_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), std::nullopt);
  EXPECT_EQ(frame.Get(xrr_slot), std::nullopt);
  EXPECT_EQ(frame.Get(x0_slot), 5);
  EXPECT_EQ(frame.Get(x1_slot), 5);
  EXPECT_EQ(frame.Get(xf3_slot), 7.0f);
  EXPECT_EQ(frame.Get(xf4_slot), 7.0f);

  r.mutable_inner()->mutable_root_reference()->mutable_inner()->set_a(13);
  inner2->mutable_root_reference()->set_x(19);
  fill_with_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(a_slot), 13);
  EXPECT_EQ(frame.Get(xrr_slot), 19);
  EXPECT_EQ(frame.Get(x0_slot), 5);
  EXPECT_EQ(frame.Get(x1_slot), 5);
  EXPECT_EQ(frame.Get(xf3_slot), 7.0f);
  EXPECT_EQ(frame.Get(xf4_slot), 7.0f);
}

TEST(InputLoaderTest, TestGetSingleValueWithDefaultProtoLoaderTopRequested) {
  const auto& input_loader =
      ::my_namespace::GetSingleValueWithDefaultProtoLoader();

  FrameLayout::Builder layout_builder;
  auto x1_slot = layout_builder.AddSlot<int32_t>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {"x1", TypedSlot::FromSlot(x1_slot)},  //
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  auto fill_with_garbage = [&]() { frame.Set(x1_slot, -2); };

  ::testing_namespace::Root r;

  fill_with_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x1_slot), 1);

  r.set_x(5);
  fill_with_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x1_slot), 5);
}

TEST(InputLoaderTest, TestGetSingleValueWithDefaultProtoLoaderDeepRequested) {
  const auto& input_loader =
      ::my_namespace::GetSingleValueWithDefaultProtoLoader();

  FrameLayout::Builder layout_builder;
  auto xf3_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {"xf3", TypedSlot::FromSlot(xf3_slot)},  //
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  auto fill_with_garbage = [&]() { frame.Set(xf3_slot, -3.0f); };

  ::testing_namespace::Root r;

  fill_with_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(xf3_slot), 3.0f);

  auto* inner2 = r.mutable_inner()->mutable_inner2();
  fill_with_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(xf3_slot), 3.0f);

  inner2->mutable_root_reference()->set_x_float(7.0f);
  fill_with_garbage();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(xf3_slot), 7.0f);
}

TEST(InputLoaderTest, GetProtoLoaderWithDuplicatedConfiguration) {
  using OInt = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<OInt>();
  const auto& input_loader =
      ::my_namespace::GetProtoLoaderWithDuplicatedConfiguration();
  std::string x_name(naming::TablePath().Column("x").FullName());
  EXPECT_THAT(input_loader, InputLoaderSupports({{x_name, oi32}, {"y", oi32}}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OInt>();
  auto y_slot = layout_builder.AddSlot<OInt>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({{x_name, TypedSlot::FromSlot(x_slot)},
                          {"y", TypedSlot::FromSlot(y_slot)}}));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt());
  EXPECT_EQ(frame.Get(y_slot), OInt());
  r.set_x(5);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(5));
  EXPECT_EQ(frame.Get(y_slot), OInt(5));
}

TEST(InputLoaderTest, TestExtensionProtoLoader) {
  using OInt = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<OInt>();
  const auto& input_loader = ::my_namespace::GetExtensionProtoLoader();
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
  EXPECT_THAT(input_loader,
              InputLoaderSupports({{x_name, oi32}, {inner_x_name, oi32}}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<OInt>();
  auto inner_x_slot = layout_builder.AddSlot<OInt>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {x_name, TypedSlot::FromSlot(x_slot)},
                           {inner_x_name, TypedSlot::FromSlot(inner_x_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt());
  EXPECT_EQ(frame.Get(inner_x_slot), OInt());

  r.SetExtension(::testing_extension_namespace::extension_x_int32, 5);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(5));
  EXPECT_EQ(frame.Get(inner_x_slot), OInt());

  r.mutable_inner()
      ->MutableExtension(
          ::testing_extension_namespace::InnerExtension::inner_ext)
      ->set_inner_extension_x_int32(7);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_slot), OInt(5));
  EXPECT_EQ(frame.Get(inner_x_slot), OInt(7));
}

TEST(InputLoaderTest, TestGetSpanProtoLoader) {
  using DAInt = ::arolla::DenseArray<int>;
  using DAText = ::arolla::DenseArray<Text>;
  using DASize = ::arolla::DenseArrayShape;
  auto dai32 = GetQType<DAInt>();
  auto datext_qtype = GetQType<DAText>();
  auto dasize_qtype = GetQType<DASize>();
  const auto& input_loader = ::my_namespace::GetSpanProtoLoader();
  std::string x_name(naming::TablePath().Column("x").FullName());
  std::string size_name(naming::TablePath().Size("").FullName());
  EXPECT_THAT(input_loader, InputLoaderSupports({{x_name, dai32},
                                                 {"str", datext_qtype},
                                                 {size_name, dasize_qtype}}));

  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<DAInt>();
  auto str_slot = layout_builder.AddSlot<DAText>();
  auto size_slot = layout_builder.AddSlot<DASize>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({{x_name, TypedSlot::FromSlot(x_slot)},
                          {"str", TypedSlot::FromSlot(str_slot)},
                          {size_name, TypedSlot::FromSlot(size_slot)}}));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r1;
  r1.set_x(5);
  ::testing_namespace::Root r2;
  r2.set_x(7);
  ASSERT_OK(bound_input_loader({r1, r2}, frame));
  EXPECT_THAT(frame.Get(x_slot), ElementsAre(5, 7));
  EXPECT_THAT(frame.Get(size_slot), DASize{2});

  r1.set_str("abc");
  r2.set_str("cba");
  ASSERT_OK(bound_input_loader({r1, r2}, frame));
  EXPECT_THAT(frame.Get(str_slot), ElementsAre(Text("abc"), Text("cba")));
  EXPECT_THAT(frame.Get(size_slot), DASize{2});

  // Test that old values are overridden
  r1 = ::testing_namespace::Root();
  ASSERT_OK(bound_input_loader({r1}, frame));
  EXPECT_THAT(frame.Get(x_slot), ElementsAre(std::nullopt));
  EXPECT_THAT(frame.Get(str_slot), ElementsAre(std::nullopt));
  EXPECT_THAT(frame.Get(size_slot), DASize{1});
}

TEST(InputLoaderTest, TestkProtoLoader_Full) {
  auto ai32 = GetDenseArrayQType<int>();
  auto asz = GetDenseArrayQType<proto::arolla_size_t>();
  auto sz = GetQType<DenseArrayShape>();
  auto ab = GetDenseArrayQType<Bytes>();
  auto at = GetDenseArrayQType<Text>();
  const auto& input_loader = ::my_namespace::GetRepeatedProtoLoader();
  EXPECT_THAT(input_loader, InputLoaderSupports({{"ys", ai32},
                                                 {"inner__as", ai32},
                                                 {"inners__as", ai32},
                                                 {"inners__a", ai32},
                                                 {"inners__inner2__z", ai32},
                                                 {"inners1__as", ai32},
                                                 {"inners__as1", ai32},
                                                 {"inners_size", sz},
                                                 {"inners__as_size", asz},
                                                 {"inners__raw_bytes", ab},
                                                 {"inners__str", at},
                                                 {"map_inner__keys", ai32},
                                                 {"map_inner__value_a", ai32},
                                                 {"map_inner_size", sz}}));

  FrameLayout::Builder layout_builder;
  auto ys_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inner_as_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_as_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_a_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_z_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners1_as_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_as1_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_str_slot = layout_builder.AddSlot<DenseArray<Text>>();
  auto inners_raw_bytes_slot = layout_builder.AddSlot<DenseArray<Bytes>>();
  auto inners_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  auto inners_as_size_slot =
      layout_builder.AddSlot<DenseArray<proto::arolla_size_t>>();
  auto map_inner_keys_slot = layout_builder.AddSlot<DenseArray<int32_t>>();
  auto map_inner_values_a_slot = layout_builder.AddSlot<DenseArray<int32_t>>();
  auto map_inner_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {"ys", TypedSlot::FromSlot(ys_slot)},
          {"inner__as", TypedSlot::FromSlot(inner_as_slot)},
          {"inners__as", TypedSlot::FromSlot(inners_as_slot)},
          {"inners__a", TypedSlot::FromSlot(inners_a_slot)},
          {"inners__inner2__z", TypedSlot::FromSlot(inners_z_slot)},
          {"inners1__as", TypedSlot::FromSlot(inners1_as_slot)},
          {"inners__as1", TypedSlot::FromSlot(inners_as1_slot)},
          {"inners__str", TypedSlot::FromSlot(inners_str_slot)},
          {"inners__raw_bytes", TypedSlot::FromSlot(inners_raw_bytes_slot)},
          {"inners_size", TypedSlot::FromSlot(inners_size_slot)},
          {"inners__as_size", TypedSlot::FromSlot(inners_as_size_slot)},
          {"map_inner__keys", TypedSlot::FromSlot(map_inner_keys_slot)},
          {"map_inner__value_a", TypedSlot::FromSlot(map_inner_values_a_slot)},
          {"map_inner_size", TypedSlot::FromSlot(map_inner_size_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.add_ys(5);
  r.add_ys(7);

  r.mutable_inner()->add_as(3);
  r.mutable_inner()->add_as(5);
  r.mutable_inner()->add_as(7);

  auto* inners0 = r.add_inners();
  inners0->add_as(5);
  inners0->set_a(3);
  inners0->mutable_inner2()->set_z(5);
  auto* inners1 = r.add_inners();
  inners1->add_as(7);
  inners1->add_as(9);
  inners1->set_a(7);
  inners1->mutable_inner2()->set_z(7);

  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_slot), ElementsAre(5, 7));
  EXPECT_THAT(frame.Get(inner_as_slot), ElementsAre(3, 5, 7));
  EXPECT_THAT(frame.Get(inners_as_slot), ElementsAre(5, 7, 9));
  EXPECT_THAT(frame.Get(inners_a_slot), ElementsAre(3, 7));
  EXPECT_THAT(frame.Get(inners_z_slot), ElementsAre(5, 7));
  EXPECT_THAT(frame.Get(inners1_as_slot), ElementsAre(7, 9));
  EXPECT_THAT(frame.Get(inners_as1_slot), ElementsAre(std::nullopt, 9));
  EXPECT_THAT(frame.Get(inners_size_slot).size, Eq(2));
  EXPECT_THAT(frame.Get(inners_as_size_slot), ElementsAre(1, 2));
  EXPECT_THAT(frame.Get(map_inner_keys_slot), IsEmpty());
  EXPECT_THAT(frame.Get(map_inner_keys_slot), IsEmpty());
  EXPECT_THAT(frame.Get(map_inner_values_a_slot), IsEmpty());
  EXPECT_THAT(frame.Get(map_inner_size_slot).size, Eq(0));

  (*r.mutable_map_inner())[19].set_a(13);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(map_inner_keys_slot), ElementsAre(19));
  EXPECT_THAT(frame.Get(map_inner_values_a_slot), ElementsAre(13));
  EXPECT_THAT(frame.Get(map_inner_size_slot).size, Eq(1));

  (*r.mutable_map_inner())[17].set_a(13);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(map_inner_keys_slot), ElementsAre(17, 19));
  EXPECT_THAT(frame.Get(map_inner_size_slot).size, Eq(2));

  // Test loading DenseArray<Bytes>
  inners0->set_raw_bytes("fifty");
  inners1->set_raw_bytes("seven");
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(inners_raw_bytes_slot),
              ElementsAre(Bytes("fifty"), Bytes("seven")));

  // Test loading DenseArray<Text>
  inners0->set_str("fifty");
  inners1->set_str("seven");
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(inners_str_slot),
              ElementsAre(Text("fifty"), Text("seven")));
}

// Test that repeated field in separate branch processed correctly.
TEST(InputLoaderTest, TestRepeatedIndependent) {
  auto ai32 = GetDenseArrayQType<int>();
  const auto& input_loader = ::my_namespace::GetRepeatedProtoLoader();
  EXPECT_THAT(input_loader, InputLoaderSupports({{"sr3_inners_as", ai32}}));

  FrameLayout::Builder layout_builder;
  auto sr3_inners_as_slot = layout_builder.AddSlot<DenseArray<int>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {"sr3_inners_as", TypedSlot::FromSlot(sr3_inners_as_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  auto* sr = r.mutable_self_reference()
                 ->mutable_self_reference()
                 ->mutable_self_reference();
  {
    auto* i = sr->add_inners();
    i->add_as(3);
  }
  {
    auto* i = sr->add_inners();
    i->add_as(1);
    i->add_as(4);
  }

  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(sr3_inners_as_slot), ElementsAre(3, 1, 4));
}

TEST(InputLoaderTest, TestkProtoLoader_Sparse) {
  auto ai32 = GetDenseArrayQType<int>();
  auto asz = GetDenseArrayQType<proto::arolla_size_t>();
  auto sz = GetQType<DenseArrayShape>();
  auto ab = GetDenseArrayQType<Bytes>();
  auto at = GetDenseArrayQType<Text>();
  const auto& input_loader = ::my_namespace::GetRepeatedProtoLoader();
  EXPECT_THAT(
      input_loader,
      InputLoaderSupports(
          {{"ys", ai32},
           {"inner__as", ai32},
           {"inners__as", ai32},
           {"inners__a", ai32},
           {"inners__inner2__z", ai32},
           {"inners1__as", ai32},
           {"inners__as1", ai32},
           {"inners__raw_bytes", ab},
           {"inners__str", at},
           {"inners_size", sz},
           {"inners__as_size", asz},
           {"map_inner__keys", ai32},
           {"map_inner__value_a", ai32},
           {"map_inner_size", sz},
           {"/inners", GetDenseArrayQType<testing_namespace::InnerRawPtr>()}}));

  FrameLayout::Builder layout_builder;
  auto inner_as_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_as_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_a_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_z_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_str_slot = layout_builder.AddSlot<DenseArray<Text>>();
  auto inners_raw_bytes_slot = layout_builder.AddSlot<DenseArray<Bytes>>();
  auto inners_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  auto inners_as_size_slot =
      layout_builder.AddSlot<DenseArray<proto::arolla_size_t>>();
  auto inners_ptr_slot =
      layout_builder.AddSlot<DenseArray<testing_namespace::InnerRawPtr>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {"inner__as", TypedSlot::FromSlot(inner_as_slot)},
          {"inners__as", TypedSlot::FromSlot(inners_as_slot)},
          {"inners__a", TypedSlot::FromSlot(inners_a_slot)},
          {"inners__str", TypedSlot::FromSlot(inners_str_slot)},
          {"inners__raw_bytes", TypedSlot::FromSlot(inners_raw_bytes_slot)},
          {"inners__inner2__z", TypedSlot::FromSlot(inners_z_slot)},
          {"inners_size", TypedSlot::FromSlot(inners_size_slot)},
          {"inners__as_size", TypedSlot::FromSlot(inners_as_size_slot)},
          {"/inners", TypedSlot::FromSlot(inners_ptr_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();

  {
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();
    ::testing_namespace::Root r;
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(inner_as_slot), IsEmpty());
    EXPECT_THAT(frame.Get(inners_size_slot).size, Eq(0));
    EXPECT_THAT(frame.Get(inners_as_size_slot), IsEmpty());
    EXPECT_THAT(frame.Get(inners_ptr_slot), IsEmpty());
  }

  {
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();
    ::testing_namespace::Root r;
    r.add_inners();
    auto* inners1 = r.add_inners();
    inners1->add_as(7);
    inners1->add_as(9);
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(inners_as_slot), ElementsAre(7, 9));
    EXPECT_THAT(frame.Get(inners_size_slot).size, Eq(2));
    EXPECT_THAT(frame.Get(inners_as_size_slot), ElementsAre(0, 2));
    EXPECT_THAT(frame.Get(inners_ptr_slot),
                ElementsAre(&r.inners(0), &r.inners(1)));
  }

  {
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();
    ::testing_namespace::Root r;
    r.add_inners();
    auto* inners1 = r.add_inners();
    inners1->set_a(7);
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(inners_a_slot), ElementsAre(std::nullopt, 7));
    EXPECT_THAT(frame.Get(inners_size_slot).size, Eq(2));
    EXPECT_THAT(frame.Get(inners_as_size_slot), ElementsAre(0, 0));
  }

  {
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();
    ::testing_namespace::Root r;
    auto* inners0 = r.add_inners();
    inners0->set_a(5);
    r.add_inners();
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(inners_a_slot), ElementsAre(5, std::nullopt));
    EXPECT_THAT(frame.Get(inners_size_slot).size, Eq(2));
    EXPECT_THAT(frame.Get(inners_as_size_slot), ElementsAre(0, 0));
  }

  {
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();
    ::testing_namespace::Root r;
    r.add_inners();
    auto* inners1 = r.add_inners();
    inners1->mutable_inner2()->set_z(7);
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(inners_z_slot), ElementsAre(std::nullopt, 7));
  }

  {
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();
    ::testing_namespace::Root r;
    auto* inners0 = r.add_inners();
    inners0->mutable_inner2();
    auto* inners1 = r.add_inners();
    inners1->mutable_inner2()->set_z(7);
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(inners_z_slot), ElementsAre(std::nullopt, 7));
  }

  // Test loading DenseArray<Bytes>
  {
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();
    ::testing_namespace::Root r;
    r.add_inners();
    r.add_inners()->set_raw_bytes("fifty");
    r.add_inners();
    r.add_inners()->set_raw_bytes("seven");
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(inners_raw_bytes_slot),
                ElementsAre(std::nullopt, Bytes("fifty"), std::nullopt,
                            Bytes("seven")));
  }

  // Test loading DenseArray<Text>
  {
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();
    ::testing_namespace::Root r;
    r.add_inners();
    r.add_inners()->set_str("fifty");
    r.add_inners();
    r.add_inners()->set_str("seven");
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(
        frame.Get(inners_str_slot),
        ElementsAre(std::nullopt, Text("fifty"), std::nullopt, Text("seven")));
  }
}

TEST(InputLoaderTest, TestkDenseArrayProtoLoader_Full) {
  const auto& input_loader = ::my_namespace::GetDenseArrayRepeatedProtoLoader();
  EXPECT_THAT(
      input_loader,
      InputLoaderSupports(
          {{"ys", GetDenseArrayQType<int32_t>()},
           {"inner__as", GetDenseArrayQType<int32_t>()},
           {"inners__raw_bytes", GetDenseArrayQType<Bytes>()},
           {"inners__str", GetDenseArrayQType<Text>()},
           {"inners__as_size", GetDenseArrayQType<proto::arolla_size_t>()}}));

  FrameLayout::Builder layout_builder;
  auto ys_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inner_as_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_str_slot = layout_builder.AddSlot<DenseArray<Text>>();
  auto inners_raw_bytes_slot = layout_builder.AddSlot<DenseArray<Bytes>>();
  auto inners_as_size_slot =
      layout_builder.AddSlot<DenseArray<proto::arolla_size_t>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {"ys", TypedSlot::FromSlot(ys_slot)},
          {"inner__as", TypedSlot::FromSlot(inner_as_slot)},
          {"inners__str", TypedSlot::FromSlot(inners_str_slot)},
          {"inners__raw_bytes", TypedSlot::FromSlot(inners_raw_bytes_slot)},
          {"inners__as_size", TypedSlot::FromSlot(inners_as_size_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.add_ys(5);
  r.add_ys(7);

  r.mutable_inner()->add_as(3);
  r.mutable_inner()->add_as(5);
  r.mutable_inner()->add_as(7);

  {
    auto* i = r.add_inners();
    i->set_str("fifty");
    i->set_raw_bytes("seven");
    i->add_as(1);
    i->add_as(2);
  }
  {
    auto* i = r.add_inners();
    i->set_str("seven");
    i->set_raw_bytes("fifty");
    i->add_as(1);
  }

  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_slot),
              ElementsAre(OptionalValue<int>(5), OptionalValue<int>(7)));
  EXPECT_THAT(frame.Get(inner_as_slot),
              ElementsAre(OptionalValue<int>(3), OptionalValue<int>(5),
                          OptionalValue<int>(7)));
  EXPECT_THAT(frame.Get(inners_raw_bytes_slot),
              ElementsAre(OptionalValue<Bytes>(Bytes("seven")),
                          OptionalValue<Bytes>(Bytes("fifty"))));
  EXPECT_THAT(frame.Get(inners_str_slot),
              ElementsAre(OptionalValue<Text>(Text("fifty")),
                          OptionalValue<Text>(Text("seven"))));
  EXPECT_THAT(frame.Get(inners_as_size_slot), ElementsAre(2, 1));
}

TEST(InputLoaderTest, TestDenseArrayProtoLoader) {
  const auto& input_loader = ::my_namespace::GetDenseArrayRepeatedProtoLoader();
  EXPECT_THAT(
      input_loader,
      InputLoaderSupports(
          {{"ys", GetDenseArrayQType<int32_t>()},
           {"inner__as", GetDenseArrayQType<int32_t>()},
           {"inners__raw_bytes", GetDenseArrayQType<Bytes>()},
           {"inners__str", GetDenseArrayQType<Text>()},
           {"inners__as_size", GetDenseArrayQType<proto::arolla_size_t>()},
           {"inners_size", GetQType<DenseArrayShape>()}}));

  FrameLayout::Builder layout_builder;
  auto ys_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inner_as_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners_str_slot = layout_builder.AddSlot<DenseArray<Text>>();
  auto inners_raw_bytes_slot = layout_builder.AddSlot<DenseArray<Bytes>>();
  auto inners_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  auto inners_as_size_slot =
      layout_builder.AddSlot<DenseArray<proto::arolla_size_t>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {"ys", TypedSlot::FromSlot(ys_slot)},
          {"inner__as", TypedSlot::FromSlot(inner_as_slot)},
          {"inners__as_size", TypedSlot::FromSlot(inners_as_size_slot)},
          {"inners__str", TypedSlot::FromSlot(inners_str_slot)},
          {"inners__raw_bytes", TypedSlot::FromSlot(inners_raw_bytes_slot)},
          {"inners_size", TypedSlot::FromSlot(inners_size_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.add_ys(5);
  r.add_ys(7);

  r.mutable_inner()->add_as(3);
  r.mutable_inner()->add_as(5);
  r.mutable_inner()->add_as(7);

  {
    auto* i = r.add_inners();
    i->set_str("fifty");
    i->set_raw_bytes("seven");
    i->add_as(9);
    i->add_as(15);
  }
  {
    auto* i = r.add_inners();
    i->set_str("seven");
    i->add_as(3);
  }

  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_slot), ElementsAre(5, 7));
  EXPECT_THAT(frame.Get(inner_as_slot), ElementsAre(3, 5, 7));
  EXPECT_THAT(frame.Get(inners_raw_bytes_slot),
              ElementsAre("seven", std::nullopt));
  EXPECT_THAT(frame.Get(inners_str_slot), ElementsAre("fifty", "seven"));
  EXPECT_THAT(frame.Get(inners_size_slot).size, 2);
  EXPECT_THAT(frame.Get(inners_as_size_slot), ElementsAre(2, 1));
}

TEST(InputLoaderTest, TestDenseArraySingleFieldProtoLoader) {
  const auto& input_loader =
      ::my_namespace::GetDenseArraySingleFieldRepeatedProtoLoader();
  EXPECT_THAT(input_loader,
              InputLoaderSupports({{"ys", GetDenseArrayQType<int32_t>()}}));

  FrameLayout::Builder layout_builder;
  auto ys_slot = layout_builder.AddSlot<DenseArray<int>>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {"ys", TypedSlot::FromSlot(ys_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_slot), IsEmpty());

  r.add_ys(5);
  r.add_ys(7);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_slot), ElementsAre(5, 7));
}

TEST(InputLoaderTest, TestDenseArrayIntermediateVerify) {
  const auto& input_loader =
      ::my_namespace::GetRepeatedProtoLoaderWithNoBranchesForIntermediate();
  EXPECT_THAT(input_loader,
              InputLoaderSupports(
                  {{"inners/a", GetDenseArrayQType<int32_t>()},
                   {"inners/rr/x", GetDenseArrayQType<int32_t>()},
                   {"inners/rr/sr/x", GetDenseArrayQType<int32_t>()},
                   {"inners/rr/sr/x64", GetDenseArrayQType<int64_t>()}}));

  // Bind only inners/a and make sure that inners/rr and inners/rr/sr
  // are processed properly.
  // Ideally we want to test that it is not being collected, but it is
  // hard using public interface.
  {
    FrameLayout::Builder layout_builder;
    auto a_slot = layout_builder.AddSlot<DenseArray<int>>();
    ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                         input_loader->Bind({
                             {"inners/a", TypedSlot::FromSlot(a_slot)},
                         }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();

    ::testing_namespace::Root r;
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(a_slot), IsEmpty());

    r.add_inners()->set_a(5);
    r.add_inners()->set_a(7);
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(a_slot), ElementsAre(5, 7));

    // set mutable self references so that there is something to collect for
    // inners/rr/sr
    r.mutable_inners(0)->mutable_root_reference()->MutableExtension(
        ::testing_extension_namespace::root_reference);
    r.mutable_inners(1)->mutable_root_reference()->MutableExtension(
        ::testing_extension_namespace::root_reference);
    frame.Set(a_slot, CreateDenseArray<int>({-5, -7}));  // garbage
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(a_slot), ElementsAre(5, 7));
  }
  // Bind only inners/rr/x and make sure that inners/rr, inners/rr/sr and inners
  // are processed properly.
  // Currently `inners` are still collected, although not used.
  // Ideally we want to avoid inners collection and test it using benchmark.
  {
    FrameLayout::Builder layout_builder;
    auto x_slot = layout_builder.AddSlot<DenseArray<int>>();
    ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                         input_loader->Bind({
                             {"inners/rr/x", TypedSlot::FromSlot(x_slot)},
                         }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();

    ::testing_namespace::Root r;
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(x_slot), IsEmpty());

    r.add_inners()->mutable_root_reference()->set_x(5);
    r.add_inners()->mutable_root_reference()->set_x(7);
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(x_slot), ElementsAre(5, 7));

    // set mutable self references so that there is something to collect for
    // inners/rr/sr
    r.mutable_inners(0)->mutable_root_reference()->MutableExtension(
        ::testing_extension_namespace::root_reference);
    r.mutable_inners(1)->mutable_root_reference()->MutableExtension(
        ::testing_extension_namespace::root_reference);
    frame.Set(x_slot, CreateDenseArray<int>({-5, -7}));  // garbage
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_THAT(frame.Get(x_slot), ElementsAre(5, 7));
  }
}

// Testing Special case, when we avoid collection of intermediate node and start
// from the ancestor for performance reasons.
TEST(InputLoaderTest, TestDenseArrayIntermediateParentNodeVerify) {
  const auto& input_loader = ::my_namespace::
      GetRepeatedProtoLoaderWithParentIntermediateNodeCollection();
  EXPECT_THAT(input_loader,
              InputLoaderSupports({
                  {"inners/rr/inner/a", GetDenseArrayQType<int32_t>()},
                  {"inners/rr/inner/a0", GetDenseArrayQType<int32_t>()},
                  {"inners/rr/inners0/a", GetDenseArrayQType<int32_t>()},
                  {"inners/rr/inners0/a0", GetDenseArrayQType<int32_t>()},
                  {"inners/rr/inners1/a", GetDenseArrayQType<int32_t>()},
                  {"inners/rr/inners1/a0", GetDenseArrayQType<int32_t>()},
              }));

  FrameLayout::Builder layout_builder;
  auto inner_a_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners0_a_slot = layout_builder.AddSlot<DenseArray<int>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {"inners/rr/inner/a", TypedSlot::FromSlot(inner_a_slot)},
          {"inners/rr/inners0/a", TypedSlot::FromSlot(inners0_a_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(inner_a_slot), IsEmpty());
  EXPECT_THAT(frame.Get(inners0_a_slot), IsEmpty());

  r.add_inners()->mutable_root_reference()->mutable_inner()->set_a(5);
  r.add_inners()->mutable_root_reference()->mutable_inner()->set_a(7);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(inner_a_slot), ElementsAre(5, 7));
  EXPECT_THAT(frame.Get(inners0_a_slot),
              ElementsAre(std::nullopt, std::nullopt));

  r.mutable_inners(0)->mutable_root_reference()->add_inners()->set_a(7);
  r.mutable_inners(1)->mutable_root_reference()->add_inners()->set_a(5);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(inner_a_slot), ElementsAre(5, 7));
  EXPECT_THAT(frame.Get(inners0_a_slot), ElementsAre(7, 5));
}

}  // namespace
}  // namespace arolla
