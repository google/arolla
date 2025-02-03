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
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "google/protobuf/descriptor.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/proto/reflection/reader.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::proto {
namespace {

using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

using ProtoRoot = ::testing_namespace::Root;
const auto* kRootDescr = ProtoRoot::descriptor();

TEST(ProtoTypeReader, CreateTopLevelOptionalReaderCreationErrors) {
  EXPECT_THAT(ProtoTypeReader::CreateOptionalReader(
                  {kRootDescr->FindFieldByName("ys")}, {{}}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("incorrect access to the repeated field")));
  EXPECT_THAT(ProtoTypeReader::CreateOptionalReader(
                  {kRootDescr->FindFieldByName("x")}, {}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("fields and access_info must be same size")));
  EXPECT_THAT(ProtoTypeReader::CreateOptionalReader(
                  {kRootDescr->FindFieldByName("x")}, {{}, {}}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("fields and access_info must be same size")));
  EXPECT_THAT(
      ProtoTypeReader::CreateOptionalReader({kRootDescr->FindFieldByName("x")},
                                            {RepeatedFieldSizeAccess()}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("incorrect access to the regular field")));
  EXPECT_THAT(
      ProtoTypeReader::CreateOptionalReader({kRootDescr->FindFieldByName("x")},
                                            {RepeatedFieldIndexAccess(0)}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("incorrect access to the regular field")));
  EXPECT_THAT(ProtoTypeReader::CreateOptionalReader({nullptr}, {{}}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("field is nullptr")));
}

TEST(ProtoTypeReader, CreateTopLevelOptionalReaderBindErrors) {
  ASSERT_OK_AND_ASSIGN(auto reader,
                       ProtoTypeReader::CreateOptionalReader(
                           {kRootDescr->FindFieldByName("x")}, {{}}));
  EXPECT_EQ(reader->qtype(), GetOptionalQType<int32_t>());

  {
    FrameLayout::Builder layout_builder;
    auto slot = layout_builder.AddSlot<OptionalValue<int64_t>>();
    EXPECT_THAT(reader->BindReadFn(TypedSlot::FromSlot(slot)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("slot type does not match")));
  }
}

}  // namespace
}  // namespace arolla::proto
