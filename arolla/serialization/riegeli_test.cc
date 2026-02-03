// Copyright 2025 Google LLC
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
// A basic test for serialization/deserialization facility.
//
// NOTE: Extensive tests can be found in: py/arolla/s11n/testing

#include "arolla/serialization/riegeli.h"

#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization_base/decoder.h"

namespace arolla::serialization {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::Leaf;
using ::arolla::serialization_base::ValueDecoder;
using ::arolla::serialization_base::ValueProto;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::QValueWith;
using ::testing::ElementsAre;

TEST(RiegeliSerializationTest, EncodeDecodeRiegeliData) {
  ASSERT_OK_AND_ASSIGN(std::string riegeli_data,
                       EncodeAsRiegeliData({TypedValue::FromValue(1.0f),
                                            TypedValue::FromValue(2.0f)},
                                           {Leaf("x")}, "snappy"));
  ASSERT_OK_AND_ASSIGN(auto decode_result, DecodeFromRiegeliData(riegeli_data));
  EXPECT_THAT(decode_result.values,
              ElementsAre(QValueWith<float>(1.0f), QValueWith<float>(2.0f)));
  EXPECT_THAT(decode_result.exprs, ElementsAre(EqualsExpr(Leaf("x"))));
}

TEST(RiegeliSerializationTest, DecodeWithCustomValueDecoderProvider) {
  auto value_decoder_provider =
      [](absl::string_view codec_name) -> ValueDecoder {
    EXPECT_EQ(codec_name,
              "arolla.serialization_codecs.ScalarV1Proto.extension");
    return [](const ValueProto& /*value_proto*/,
              absl::Span<const TypedValue> input_values,
              absl::Span<const ExprNodePtr> input_exprs) -> TypedValue {
      EXPECT_THAT(input_values, ElementsAre());
      EXPECT_THAT(input_exprs, ElementsAre());
      return TypedValue::FromValue(3.14159f);
    };
  };
  ASSERT_OK_AND_ASSIGN(
      auto riegeli_data,
      EncodeAsRiegeliData({TypedValue::FromValue(2.71828f)}, {}, ""));
  DecodingOptions decoding_options;
  decoding_options.value_decoder_provider = value_decoder_provider;
  ASSERT_OK_AND_ASSIGN(
      auto decode_result,
      DecodeFromRiegeliData(riegeli_data, std::move(decoding_options)));
  EXPECT_THAT(decode_result.values, ElementsAre(QValueWith<float>(3.14159f)));
  EXPECT_THAT(decode_result.exprs, ElementsAre());
}

}  // namespace
}  // namespace arolla::serialization
