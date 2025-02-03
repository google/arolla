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
#include "arolla/io/proto_types/proto_qtype_map.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "google/protobuf/descriptor.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"

namespace arolla::proto {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;

TEST(ProtoFieldTypeToQTypeTest, Get) {
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_SFIXED32),
              IsOkAndHolds(arolla::GetQType<int32_t>()));
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_FIXED32),
              IsOkAndHolds(arolla::GetQType<int64_t>()));
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_SFIXED64),
              IsOkAndHolds(arolla::GetQType<int64_t>()));
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_FIXED64),
              IsOkAndHolds(arolla::GetQType<uint64_t>()));
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_FLOAT),
              IsOkAndHolds(arolla::GetQType<float>()));
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_DOUBLE),
              IsOkAndHolds(arolla::GetQType<double>()));
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_BOOL),
              IsOkAndHolds(arolla::GetQType<bool>()));
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_ENUM),
              IsOkAndHolds(arolla::GetQType<int32_t>()));
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_STRING),
              IsOkAndHolds(arolla::GetQType<arolla::Bytes>()));
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_BYTES),
              IsOkAndHolds(arolla::GetQType<arolla::Bytes>()));
  EXPECT_THAT(ProtoFieldTypeToQType(google::protobuf::FieldDescriptor::TYPE_MESSAGE),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace arolla::proto
