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
#include <cstdint>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor_database.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/proto/proto_input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/proto/testing/test_proto3.pb.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::InputLoaderSupports;
using ::testing::HasSubstr;

TEST(OptionalLoaderTest, WrongProtoMessage) {
  const auto* orig_descriptor = ::testing_namespace::Proto3::descriptor();
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       ProtoFieldsLoader::Create(orig_descriptor));
  using OInt = ::arolla::OptionalValue<int>;
  auto oi32 = GetQType<OInt>();
  EXPECT_THAT(input_loader, InputLoaderSupports({{"/non_optional_i32", oi32}}));

  FrameLayout::Builder layout_builder;
  auto slot = layout_builder.AddSlot<OInt>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({{"/non_optional_i32", TypedSlot::FromSlot(slot)}}));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  EXPECT_THAT(bound_input_loader(::testing_namespace::Root(), frame),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("message must have the same descriptor")));
}

TEST(OptionalLoaderTest, PoolProtoMessage) {
  const auto* orig_descriptor = ::testing_namespace::Proto3::descriptor();
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       ProtoFieldsLoader::Create(orig_descriptor));
  auto oi32 = GetOptionalQType<int32_t>();
  EXPECT_THAT(input_loader, InputLoaderSupports({{"/non_optional_i32", oi32}}));

  FrameLayout::Builder layout_builder;
  auto slot = layout_builder.AddSlot<::arolla::OptionalValue<int>>();
  ASSERT_OK_AND_ASSIGN(
      auto bound_input_loader,
      input_loader->Bind({{"/non_optional_i32", TypedSlot::FromSlot(slot)}}));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Proto3 r;
  r.set_non_optional_i32(57);
  {
    ASSERT_OK(bound_input_loader(r, frame));
    EXPECT_EQ(frame.Get(slot), 57);
    frame.Set(slot, -77);  // garbage
  }

  // Creates DescriptorPool with only `Proto3` proto.
  google::protobuf::SimpleDescriptorDatabase db;
  google::protobuf::FileDescriptorProto file_proto;
  orig_descriptor->file()->CopyTo(&file_proto);
  db.Add(file_proto);
  auto pool = std::make_unique<google::protobuf::DescriptorPool>(&db);

  const auto* pool_descriptor =
      pool->FindMessageTypeByName(orig_descriptor->full_name());
  auto dynamic_message_factory =
      std::make_unique<google::protobuf::DynamicMessageFactory>();
  const google::protobuf::Message* default_pool_message =
      dynamic_message_factory->GetPrototype(pool_descriptor);
  {  // protos from other pool are not accepted
    std::unique_ptr<google::protobuf::Message> msg(default_pool_message->New());
    ASSERT_TRUE(msg->ParseFromString(r.SerializeAsString()));
    EXPECT_THAT(bound_input_loader(*msg, frame),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         HasSubstr("message must have the same descriptor")));
  }

  // Recreate `input_loader` based on pool descriptor.
  ASSERT_OK_AND_ASSIGN(input_loader,
                       ProtoFieldsLoader::Create(pool_descriptor));
  EXPECT_THAT(input_loader, InputLoaderSupports({{"/non_optional_i32", oi32}}));
  ASSERT_OK_AND_ASSIGN(
      bound_input_loader,
      input_loader->Bind({{"/non_optional_i32", TypedSlot::FromSlot(slot)}}));

  {  // normal protos are not accepted anymore
    EXPECT_THAT(bound_input_loader(r, frame),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         HasSubstr("message must have the same descriptor")));
  }
  {  // parsed via pool are possible to read
    std::unique_ptr<google::protobuf::Message> msg(default_pool_message->New());
    ASSERT_TRUE(msg->ParseFromString(r.SerializeAsString()));
    ASSERT_OK(bound_input_loader(*msg, frame));
    EXPECT_EQ(frame.Get(slot), 57);
    frame.Set(slot, -77);  // garbage
  }

  dynamic_message_factory.reset();
  pool.reset();
  {  // should work normally even if descriptor is destroyed
    EXPECT_THAT(bound_input_loader(r, frame),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         HasSubstr("message must have the same descriptor")));
  }
}

}  // namespace
}  // namespace arolla
