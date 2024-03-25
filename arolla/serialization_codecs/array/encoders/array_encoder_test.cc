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
// The main test set is located here:
//
//   py/arolla/s11n/testing/array_codec_test.py
//
// This file contains only tests for properties that cannot be easily
// implemented using the python api. In particular:
//
//   * handling of array.id_filter().ids_offset != 0
//

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/qtype/types.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization_codecs/array/array_codec.pb.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::serialization_base::ValueProto;
using ::arolla::serialization_codecs::ArrayV1Proto;

template <typename T>
absl::StatusOr<ValueProto> GenValueProto(const T& value) {
  ASSIGN_OR_RETURN(auto container_proto,
                   serialization::Encode({TypedValue::FromValue(value)}, {}));
  CHECK(!container_proto.decoding_steps().empty());
  CHECK(container_proto.decoding_steps().rbegin()->has_value());
  return container_proto.decoding_steps().rbegin()->value();
}

class EncodeArrayTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_OK(InitArolla()); }
};

TEST_F(EncodeArrayTest, IdsOffset) {
  auto array = CreateArray<float>({5, std::nullopt, 3, std::nullopt, 1})
                   .ToSparseForm()
                   .Slice(1, 3);
  ASSERT_EQ(array.id_filter().ids_offset(), 1);
  ASSERT_OK_AND_ASSIGN(auto value_proto, GenValueProto(array));
  EXPECT_THAT(value_proto.input_value_indices(), testing::ElementsAre(0, 1));
  ASSERT_TRUE(value_proto.HasExtension(ArrayV1Proto::extension));
  const auto& array_proto = value_proto.GetExtension(ArrayV1Proto::extension);
  ASSERT_EQ(array_proto.value_case(), ArrayV1Proto::kArrayFloat32Value);
  const auto& array_float32_proto = array_proto.array_float32_value();
  EXPECT_EQ(array_float32_proto.size(), 3);
  EXPECT_THAT(array_float32_proto.ids(), testing::ElementsAre(1));
}

TEST_F(EncodeArrayTest, EdgeRoundTrips) {
  const auto splits = CreateArray<int64_t>({0, 2, 5});
  ASSERT_OK_AND_ASSIGN(auto array_edge, ArrayEdge::FromSplitPoints(splits));
  ASSERT_OK_AND_ASSIGN(auto value_proto, GenValueProto(array_edge));
  ASSERT_TRUE(value_proto.HasExtension(ArrayV1Proto::extension));
  const auto& array_proto = value_proto.GetExtension(ArrayV1Proto::extension);
  ASSERT_EQ(array_proto.value_case(), ArrayV1Proto::kArrayEdgeValue);
  const auto& array_edge_proto = array_proto.array_edge_value();
  EXPECT_EQ(array_edge_proto.parent_size(), 0);
  EXPECT_THAT(array_edge_proto.edge_type(),
              ArrayV1Proto::ArrayEdgeProto::SPLIT_POINTS);
}

}  // namespace
}  // namespace arolla::serialization_codecs
