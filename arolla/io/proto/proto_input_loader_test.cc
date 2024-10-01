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
#include "arolla/io/proto/proto_input_loader.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "google/protobuf/message.h"
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
using ::testing::IsNull;

template <typename T>
class ProtoLoaderTest : public ::testing::Test {
 public:
  using StringType = T;
};

using StringTypes = ::testing::Types<Text, Bytes>;
TYPED_TEST_SUITE(ProtoLoaderTest, StringTypes);

TYPED_TEST(ProtoLoaderTest, LoadScalars) {
  using StringType = TypeParam;
  proto::StringFieldType string_type = std::is_same_v<StringType, Text>
                                           ? proto::StringFieldType::kText
                                           : proto::StringFieldType::kBytes;
  ASSERT_OK_AND_ASSIGN(
      auto input_loader_ptr,
      ProtoFieldsLoader::Create(::testing_namespace::Root::descriptor(),
                                string_type));
  const InputLoader<google::protobuf::Message>& input_loader = *input_loader_ptr;
  using OInt = ::arolla::OptionalValue<int>;
  using OBytes = ::arolla::OptionalValue<Bytes>;
  using OText = ::arolla::OptionalValue<StringType>;
  auto oi32 = GetQType<OInt>();
  auto obytes = GetQType<OBytes>();
  auto otxt = GetQType<OText>();
  std::string x_def_name(naming::TablePath().Column("x").FullName());
  std::string inner_a_def_name(
      naming::TablePath("inner").Column("a").FullName());
  std::string inner_inner2_z_def_name(
      naming::TablePath("inner").Child("inner2").Column("z").FullName());
  std::string str_def_name(naming::TablePath().Column("str").FullName());
  std::string raw_bytes_def_name(
      naming::TablePath().Column("raw_bytes").FullName());
  EXPECT_THAT(input_loader,
              InputLoaderSupports({{x_def_name, oi32},
                                   {inner_a_def_name, oi32},
                                   {inner_inner2_z_def_name, oi32},
                                   {str_def_name, otxt},
                                   {raw_bytes_def_name, obytes}}));

  FrameLayout::Builder layout_builder;
  auto x_def_slot = layout_builder.AddSlot<OInt>();
  auto inner_a_def_slot = layout_builder.AddSlot<OInt>();
  auto inner_inner2_z_def_slot = layout_builder.AddSlot<OInt>();
  auto str_def_slot = layout_builder.AddSlot<OText>();
  auto raw_bytes_def_slot = layout_builder.AddSlot<OBytes>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader.Bind({
          {x_def_name, TypedSlot::FromSlot(x_def_slot)},
          {inner_a_def_name, TypedSlot::FromSlot(inner_a_def_slot)},
          {inner_inner2_z_def_name,
           TypedSlot::FromSlot(inner_inner2_z_def_slot)},
          {str_def_name, TypedSlot::FromSlot(str_def_slot)},
          {raw_bytes_def_name, TypedSlot::FromSlot(raw_bytes_def_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.set_x(19);
  r.set_str("3");
  r.set_raw_bytes("37");
  r.mutable_inner()->set_a(57);
  r.mutable_inner()->mutable_inner2()->set_z(2);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_def_slot), 19);
  EXPECT_EQ(frame.Get(inner_a_def_slot), 57);
  EXPECT_EQ(frame.Get(inner_inner2_z_def_slot), 2);
  EXPECT_EQ(frame.Get(str_def_slot), StringType("3"));
  EXPECT_EQ(frame.Get(raw_bytes_def_slot), arolla::Bytes("37"));
  r.clear_x();
  r.clear_str();
  r.clear_inner();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(x_def_slot), std::nullopt);
  EXPECT_EQ(frame.Get(inner_a_def_slot), std::nullopt);
  EXPECT_EQ(frame.Get(inner_inner2_z_def_slot), std::nullopt);
  EXPECT_EQ(frame.Get(str_def_slot), std::nullopt);
}

TEST(ProtoFieldsLoaderTest, ProtopathIndexAccess) {
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      ProtoFieldsLoader::Create(::testing_namespace::Root::descriptor()));
  using oint = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<oint>();
  std::string ys_def_name(
      naming::TablePath().Column(naming::ArrayAccess("ys", 0)).FullName());
  std::string inners_a_def_name(naming::TablePath()
                                    .Child(naming::ArrayAccess("inners", 0))
                                    .Column("a")
                                    .FullName());
  std::string inners_as_def_name(naming::TablePath()
                                     .Child(naming::ArrayAccess("inners", 1))
                                     .Column(naming::ArrayAccess("as", 0))
                                     .FullName());
  EXPECT_THAT(input_loader, InputLoaderSupports({{ys_def_name, oi32},
                                                 {inners_a_def_name, oi32},
                                                 {inners_as_def_name, oi32}}));

  FrameLayout::Builder layout_builder;
  auto ys_def_slot = layout_builder.AddSlot<oint>();
  auto inners_a_def_slot = layout_builder.AddSlot<oint>();
  auto inners_as_def_slot = layout_builder.AddSlot<oint>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {ys_def_name, TypedSlot::FromSlot(ys_def_slot)},
          {inners_a_def_name, TypedSlot::FromSlot(inners_a_def_slot)},
          {inners_as_def_name, TypedSlot::FromSlot(inners_as_def_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.add_ys(19);
  r.add_inners()->set_a(17);
  r.add_inners()->add_as(57);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(ys_def_slot), 19);
  EXPECT_EQ(frame.Get(inners_a_def_slot), 17);
  EXPECT_EQ(frame.Get(inners_as_def_slot), 57);
  r.clear_ys();
  r.clear_inners();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_EQ(frame.Get(ys_def_slot), std::nullopt);
  EXPECT_EQ(frame.Get(inners_a_def_slot), std::nullopt);
  EXPECT_EQ(frame.Get(inners_as_def_slot), std::nullopt);
}

TEST(ProtoFieldsLoaderTest, ProtopathRepeatedAccess) {
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      ProtoFieldsLoader::Create(::testing_namespace::Root::descriptor()));
  using OInt = ::arolla::OptionalValue<int>;
  using DAInt = arolla::DenseArray<int>;
  auto dai32 = GetDenseArrayQType<int>();
  std::string ys_def_name(naming::TablePath().Column("ys").FullName());
  std::string inners_a_def_name(
      naming::TablePath().Child("inners").Column("a").FullName());
  std::string inners_as_def_name(
      naming::TablePath().Child("inners").Column("as").FullName());
  EXPECT_THAT(input_loader, InputLoaderSupports({{ys_def_name, dai32},
                                                 {inners_a_def_name, dai32},
                                                 {inners_as_def_name, dai32}}));

  FrameLayout::Builder layout_builder;
  auto ys_def_slot = layout_builder.AddSlot<DAInt>();
  auto inners_a_def_slot = layout_builder.AddSlot<DAInt>();
  auto inners_as_def_slot = layout_builder.AddSlot<DAInt>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {ys_def_name, TypedSlot::FromSlot(ys_def_slot)},
          {inners_a_def_name, TypedSlot::FromSlot(inners_a_def_slot)},
          {inners_as_def_name, TypedSlot::FromSlot(inners_as_def_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.add_ys(19);
  r.add_ys(3);
  auto inners_0 = r.add_inners();
  inners_0->set_a(17);
  inners_0->add_as(57);
  inners_0->add_as(37);
  r.add_inners();
  auto inners_2 = r.add_inners();
  inners_2->set_a(3);
  inners_2->add_as(17);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_def_slot), ElementsAre(OInt{19}, OInt{3}));
  EXPECT_THAT(frame.Get(inners_a_def_slot),
              ElementsAre(OInt{17}, std::nullopt, OInt{3}));
  EXPECT_THAT(frame.Get(inners_as_def_slot),
              ElementsAre(OInt{57}, OInt{37}, OInt{17}));
  r.clear_ys();
  r.clear_inners();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_def_slot), IsEmpty());
  EXPECT_THAT(frame.Get(inners_a_def_slot), IsEmpty());
  EXPECT_THAT(frame.Get(inners_as_def_slot), IsEmpty());
}

TEST(SizeAccessLoaderTest, ProtopathRepeatedSizeAccess) {
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      ProtoFieldsLoader::Create(::testing_namespace::Root::descriptor()));

  using Size = proto::arolla_size_t;
  using VSize = ::arolla::DenseArray<Size>;
  auto root_size = GetQType<DenseArrayShape>();
  auto v_size = GetDenseArrayQType<Size>();
  std::string ys_size_name(naming::TablePath().Size("ys").FullName());
  std::string inners_size_name(naming::TablePath().Size("inners").FullName());
  std::string inners_as_size_name(
      naming::TablePath().Child("inners").Size("as").FullName());
  EXPECT_THAT(*input_loader,
              InputLoaderSupports({{ys_size_name, root_size},
                                   {inners_size_name, root_size},
                                   {inners_as_size_name, v_size}}));

  FrameLayout::Builder layout_builder;
  auto ys_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  auto inners_size_slot = layout_builder.AddSlot<DenseArrayShape>();
  auto inners_as_size_slot = layout_builder.AddSlot<VSize>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({
          {ys_size_name, TypedSlot::FromSlot(ys_size_slot)},
          {inners_size_name, TypedSlot::FromSlot(inners_size_slot)},
          {inners_as_size_name, TypedSlot::FromSlot(inners_as_size_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.add_ys(19);
  r.add_ys(3);
  auto inners_0 = r.add_inners();
  inners_0->add_as(57);
  inners_0->add_as(37);
  r.add_inners();
  auto inners_2 = r.add_inners();
  inners_2->add_as(17);
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_size_slot), Eq(DenseArrayShape{.size = 2}));
  EXPECT_THAT(frame.Get(inners_size_slot), Eq(DenseArrayShape{.size = 3}));
  EXPECT_THAT(frame.Get(inners_as_size_slot), ElementsAre(2, 0, 1));
  r.clear_ys();
  r.clear_inners();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(ys_size_slot), Eq(DenseArrayShape{.size = 0}));
  EXPECT_THAT(frame.Get(inners_size_slot), Eq(DenseArrayShape{.size = 0}));
  EXPECT_THAT(frame.Get(inners_as_size_slot), IsEmpty());
}

TYPED_TEST(ProtoLoaderTest, LoadDenseArrays) {
  using StringType = TypeParam;
  proto::StringFieldType string_type = std::is_same_v<StringType, Text>
                                           ? proto::StringFieldType::kText
                                           : proto::StringFieldType::kBytes;
  ASSERT_OK_AND_ASSIGN(
      auto input_loader_ptr,
      ProtoFieldsLoader::Create(::testing_namespace::Root::descriptor(),
                                string_type));

  const InputLoader<google::protobuf::Message>& input_loader = *input_loader_ptr;
  using OText = ::arolla::OptionalValue<StringType>;
  using OBytes = ::arolla::OptionalValue<Bytes>;
  using DAText = ::arolla::DenseArray<StringType>;
  using DABytes = ::arolla::DenseArray<Bytes>;
  std::string str_name(naming::TablePath().Column("repeated_str").FullName());
  std::string bytes_name(
      naming::TablePath().Column("repeated_raw_bytes").FullName());
  EXPECT_THAT(input_loader,
              InputLoaderSupports({{str_name, GetQType<DAText>()},
                                   {bytes_name, GetQType<DABytes>()}}));

  FrameLayout::Builder layout_builder;
  auto str_slot = layout_builder.AddSlot<DAText>();
  auto bytes_slot = layout_builder.AddSlot<DABytes>();
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader.Bind({
                           {str_name, TypedSlot::FromSlot(str_slot)},
                           {bytes_name, TypedSlot::FromSlot(bytes_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  *r.add_repeated_str() = "19";
  *r.add_repeated_str() = "3";
  *r.add_repeated_raw_bytes() = "3";
  *r.add_repeated_raw_bytes() = "19";
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(str_slot),
              ElementsAre(OText{StringType{"19"}}, OText{StringType{"3"}}));
  EXPECT_THAT(frame.Get(bytes_slot),
              ElementsAre(OBytes{Bytes{"3"}}, OBytes{Bytes{"19"}}));
  r.clear_repeated_str();
  r.clear_repeated_raw_bytes();
  ASSERT_OK(bound_input_loader(r, frame));
  EXPECT_THAT(frame.Get(str_slot), IsEmpty());
  EXPECT_THAT(frame.Get(bytes_slot), IsEmpty());
}

TEST(SizeAccessErrorsLoaderTest, CreateFromProtopathsErrors) {
  ASSERT_OK_AND_ASSIGN(
      auto input_loader_ptr,
      ProtoFieldsLoader::Create(::testing_namespace::Root::descriptor()));

  EXPECT_THAT(input_loader_ptr->GetQTypeOf(""), IsNull());
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/"), IsNull());
  // Missing leading "/".
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("x"), IsNull());
  // Non-existing field.
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/i_am_not_here"), IsNull());
  // "x" is non-repeated field.
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/x[:]"), IsNull());
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/x[0]"), IsNull());
  // Accessing subfields of int fields.
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/x/y"), IsNull());
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/ys/x"), IsNull());

  // Incorrect index.
  for (auto ppath : {"/ys[]", "/ys[-1]", "/ys[a]", "/ys[0x0]", "/ys[\"0\"]",
                     "/ys[00]", "/ys[ 0 ]"}) {
    EXPECT_THAT(input_loader_ptr->GetQTypeOf(ppath), IsNull())
        << "ppath=" << ppath;
  }
}

TEST(SizeAccessErrorsLoaderTest, CreateFromSizeProtopathsErrors) {
  ASSERT_OK_AND_ASSIGN(
      auto input_loader_ptr,
      ProtoFieldsLoader::Create(::testing_namespace::Root::descriptor()));

  // Non-existing field.
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/i_am_not_here/@size"), IsNull());
  // Incorrect size accessor.
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/@size"), IsNull());
  // Size access of a scalar field.
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/x/@size"), IsNull());
  // Size access of a scalar value of a repeated field.
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/ys[0]/@size"), IsNull());
  // Size access in the middle of the protopath.
  EXPECT_THAT(input_loader_ptr->GetQTypeOf("/inners/@size/a"), IsNull());
}

}  // namespace
}  // namespace arolla
