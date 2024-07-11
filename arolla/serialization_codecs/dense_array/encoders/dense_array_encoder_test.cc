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
//   py/arolla/s11n/testing/dense_array_codec_test.py
//
// This file contains only tests for properties that cannot be easily
// implemented using the public api. In particular:
//
//   * handling of dense_array.bitmap_bit_offset != 0
//   * handling of string_buffer with base_offset != 0
//

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_codecs/dense_array/dense_array_codec.pb.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::serialization::Encode;
using ::arolla::serialization_base::ValueProto;

template <typename T>
absl::StatusOr<ValueProto> GenValueProto(const T& value) {
  ASSIGN_OR_RETURN(auto container_proto,
                   Encode({TypedValue::FromValue(value)}, {}));
  CHECK_GT(container_proto.decoding_steps_size(), 1);
  CHECK(container_proto.decoding_steps().rbegin()[1].has_value());
  return container_proto.decoding_steps().rbegin()[1].value();
}

class EncodeDenseArrayTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(EncodeDenseArrayTest, BitmapWithBitOffset) {
  DenseArray<float> arr;
  arr.values = CreateBuffer<float>({-1.0f, 1.0f, -1.0f, 3.0f, -1.0f});
  arr.bitmap = CreateBuffer<uint32_t>({0b1111111111111111010100});
  arr.bitmap_bit_offset = 1;
  ASSERT_OK_AND_ASSIGN(auto value_proto, GenValueProto(arr));
  ASSERT_TRUE(value_proto.HasExtension(DenseArrayV1Proto::extension));
  const auto& dense_array_proto =
      value_proto.GetExtension(DenseArrayV1Proto::extension);
  ASSERT_EQ(dense_array_proto.value_case(),
            DenseArrayV1Proto::kDenseArrayFloat32Value);
  const auto& dense_array_float32_proto =
      dense_array_proto.dense_array_float32_value();
  ASSERT_EQ(dense_array_float32_proto.size(), 5);
  ASSERT_THAT(dense_array_float32_proto.bitmap(),
              testing::ElementsAre(0b1010U));
  ASSERT_THAT(dense_array_float32_proto.values(),
              testing::ElementsAre(1.0f, 3.0f));
}

TEST_F(EncodeDenseArrayTest, StringBufferBaseOffset) {
  constexpr absl::string_view characters = "abracadabra";
  DenseArray<Text> arr;
  arr.values = StringsBuffer(
      CreateBuffer<StringsBuffer::Offsets>({{1, 3}, {4, 5}, {8, 10}, {8, 11}}),
      Buffer<char>::Create(characters.begin(), characters.end()), 1);
  arr.bitmap = CreateBuffer<uint32_t>({0b0101});
  ASSERT_THAT(arr,
              testing::ElementsAre("ab", std::nullopt, "ab", std::nullopt));
  ASSERT_OK_AND_ASSIGN(auto value_proto, GenValueProto(arr));
  ASSERT_TRUE(value_proto.HasExtension(DenseArrayV1Proto::extension));
  const auto& dense_array_proto =
      value_proto.GetExtension(DenseArrayV1Proto::extension);
  ASSERT_EQ(dense_array_proto.value_case(),
            DenseArrayV1Proto::kDenseArrayTextValue);
  const auto& dense_array_string_proto =
      dense_array_proto.dense_array_text_value();
  ASSERT_EQ(dense_array_string_proto.size(), 4);
  ASSERT_THAT(dense_array_string_proto.bitmap(), testing::ElementsAre(0b101U));
  ASSERT_EQ(dense_array_string_proto.characters(), characters);
  ASSERT_THAT(dense_array_string_proto.value_offset_starts(),
              testing::ElementsAre(0, 7));
  ASSERT_THAT(dense_array_string_proto.value_offset_ends(),
              testing::ElementsAre(2, 9));
}

}  // namespace
}  // namespace arolla::serialization_codecs
