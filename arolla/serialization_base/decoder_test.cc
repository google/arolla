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
#include "arolla/serialization_base/decoder.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"

namespace arolla::serialization_base {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::LambdaOperator;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::TypedValueWith;
using ::testing::ElementsAre;
using ::testing::InSequence;
using ::testing::IsEmpty;
using ::testing::MockFunction;
using ::testing::Ref;
using ::testing::Return;

using MockValueDecoderProvider =
    MockFunction<absl::StatusOr<ValueDecoder>(absl::string_view codec_name)>;

using MockValueDecoder = MockFunction<absl::StatusOr<ValueDecoderResult>(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs)>;

TEST(DecodeTest, EmptyMessage) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  auto output = std::move(decoder).Finish();
  EXPECT_THAT(output.values, IsEmpty());
  EXPECT_THAT(output.exprs, IsEmpty());
}

TEST(DecodeTest, LiteralNode) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value();
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(1.0f)));
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_literal_node()->set_literal_value_index(1);
    EXPECT_OK(decoder.OnDecodingStep(2, decoding_step_proto));
  }
  {
    decoding_step_proto.set_output_expr_index(2);
    EXPECT_OK(decoder.OnDecodingStep(3, decoding_step_proto));
  }
  auto output = std::move(decoder).Finish();
  EXPECT_THAT(output.values, IsEmpty());
  EXPECT_THAT(output.exprs, ElementsAre(EqualsExpr(Literal(1.0f))));
}

TEST(DecodeTest, LeafNode) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_leaf_node()->set_leaf_key("leaf_key");
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.set_output_expr_index(0);
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  auto output = std::move(decoder).Finish();
  EXPECT_THAT(output.values, IsEmpty());
  EXPECT_THAT(output.exprs, ElementsAre(EqualsExpr(Leaf("leaf_key"))));
}

TEST(DecodeTest, PlaceholderNode) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_placeholder_node()->set_placeholder_key(
        "placeholder_key");
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.set_output_expr_index(0);
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  auto output = std::move(decoder).Finish();
  EXPECT_THAT(output.values, IsEmpty());
  EXPECT_THAT(output.exprs,
              ElementsAre(EqualsExpr(Placeholder("placeholder_key"))));
}

TEST(DecodeTest, OperatorNode) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr dummy_op,
                       LambdaOperator::Make("dummy_op", Placeholder("x")));
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(dummy_op)));
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(1.0f)));
    EXPECT_OK(decoder.OnDecodingStep(2, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_literal_node()->set_literal_value_index(2);
    EXPECT_OK(decoder.OnDecodingStep(3, decoding_step_proto));
  }
  {
    auto* operator_node_proto = decoding_step_proto.mutable_operator_node();
    operator_node_proto->set_operator_value_index(1);
    operator_node_proto->add_input_expr_indices(3);
    EXPECT_OK(decoder.OnDecodingStep(4, decoding_step_proto));
  }
  {
    decoding_step_proto.set_output_expr_index(4);
    EXPECT_OK(decoder.OnDecodingStep(5, decoding_step_proto));
  }
  auto output = std::move(decoder).Finish();
  EXPECT_THAT(output.values, IsEmpty());
  EXPECT_THAT(output.exprs,
              ElementsAre(EqualsExpr(ExprNode::UnsafeMakeOperatorNode(
                  ExprOperatorPtr(dummy_op), {Literal(1.0f)},
                  ExprAttributes{TypedValue::FromValue(1.0f)}))));
}

TEST(DecodeTest, OperatorNode_NoMetadata) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(),
                  {.infer_attributes_for_operator_nodes = false});
  DecodingStepProto decoding_step_proto;
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr dummy_op,
                       LambdaOperator::Make("dummy_op", Placeholder("x")));
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(dummy_op)));
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(1.0f)));
    EXPECT_OK(decoder.OnDecodingStep(2, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_literal_node()->set_literal_value_index(2);
    EXPECT_OK(decoder.OnDecodingStep(3, decoding_step_proto));
  }
  {
    auto* operator_node_proto = decoding_step_proto.mutable_operator_node();
    operator_node_proto->set_operator_value_index(1);
    operator_node_proto->add_input_expr_indices(3);
    EXPECT_OK(decoder.OnDecodingStep(4, decoding_step_proto));
  }
  {
    decoding_step_proto.set_output_expr_index(4);
    EXPECT_OK(decoder.OnDecodingStep(5, decoding_step_proto));
  }
  auto output = std::move(decoder).Finish();
  EXPECT_THAT(output.values, IsEmpty());
  EXPECT_THAT(
      output.exprs,
      ElementsAre(EqualsExpr(ExprNode::UnsafeMakeOperatorNode(
          ExprOperatorPtr(dummy_op), {Literal(1.0f)}, ExprAttributes{}))));
}

TEST(DecodeTest, Value) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder_1;
  MockValueDecoder mock_value_decoder_2;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec_1");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec_1"))
        .WillOnce(Return(mock_value_decoder_1.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec_2");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec_2"))
        .WillOnce(Return(mock_value_decoder_2.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder_1,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(1.0f)));
    EXPECT_OK(decoder.OnDecodingStep(2, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder_1,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(2.0f)));
    EXPECT_OK(decoder.OnDecodingStep(3, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_leaf_node()->set_leaf_key("leaf_key");
    EXPECT_OK(decoder.OnDecodingStep(4, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_placeholder_node()->set_placeholder_key(
        "placeholder_key");
    EXPECT_OK(decoder.OnDecodingStep(5, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->add_input_value_indices(2);
    decoding_step_proto.mutable_value()->add_input_value_indices(3);
    decoding_step_proto.mutable_value()->add_input_value_indices(2);
    decoding_step_proto.mutable_value()->add_input_expr_indices(5);
    decoding_step_proto.mutable_value()->add_input_expr_indices(4);
    decoding_step_proto.mutable_value()->add_input_expr_indices(5);
    decoding_step_proto.mutable_value()->add_input_expr_indices(4);
    decoding_step_proto.mutable_value()->set_codec_index(1);
    EXPECT_CALL(mock_value_decoder_2,
                Call(Ref(decoding_step_proto.value()),
                     ElementsAre(TypedValueWith<float>(1.0f),
                                 TypedValueWith<float>(2.0f),
                                 TypedValueWith<float>(1.0f)),
                     ElementsAre(EqualsExpr(Placeholder("placeholder_key")),
                                 EqualsExpr(Leaf("leaf_key")),
                                 EqualsExpr(Placeholder("placeholder_key")),
                                 EqualsExpr(Leaf("leaf_key")))))
        .WillOnce(Return(TypedValue::FromValue(3.0f)));
    EXPECT_OK(decoder.OnDecodingStep(6, decoding_step_proto));
  }
  {
    decoding_step_proto.set_output_value_index(6);
    EXPECT_OK(decoder.OnDecodingStep(7, decoding_step_proto));
  }
  auto output = std::move(decoder).Finish();
  EXPECT_THAT(output.values, ElementsAre(TypedValueWith<float>(3.0f)));
  EXPECT_THAT(output.exprs, IsEmpty());
}

TEST(DecodeTest, ValueWithUnknownCodec) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder_1;
  MockValueDecoder mock_value_decoder_2;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec_1");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec_1"))
        .WillOnce(Return(mock_value_decoder_1.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec_2");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec_2"))
        .WillOnce(Return(mock_value_decoder_2.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  {
    InSequence seq;
    decoding_step_proto.mutable_value();
    EXPECT_CALL(mock_value_decoder_1,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(NoExtensionFound{}));
    EXPECT_CALL(mock_value_decoder_2,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(1.0f)));
    EXPECT_OK(decoder.OnDecodingStep(2, decoding_step_proto));
  }
  {
    decoding_step_proto.set_output_value_index(2);
    EXPECT_OK(decoder.OnDecodingStep(3, decoding_step_proto));
  }
  auto output = std::move(decoder).Finish();
  EXPECT_THAT(output.values, ElementsAre(TypedValueWith<float>(1.0f)));
  EXPECT_THAT(output.exprs, IsEmpty());
}

TEST(DecodeTest, Error_UnexpectedDecodingStepIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  EXPECT_THAT(decoder.OnDecodingStep(2, DecodingStepProto()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "encountered unexpected decoding_step_index=2, "
                       "indicating missing step 0"));
}

TEST(DecodeTest, Error_EmptyDecodingStep) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  EXPECT_THAT(decoder.OnDecodingStep(0, DecodingStepProto()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "missing decoding_step.type"));
}

TEST(DecodeTest, Error_Codec_CodecNotFound) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("foo");
    EXPECT_CALL(mock_value_decoder_provider, Call("foo"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_codec()->set_name("foo");
    EXPECT_CALL(mock_value_decoder_provider, Call("foo"))
        .WillOnce(Return(absl::InvalidArgumentError("unknown codec: bar")));
    EXPECT_THAT(decoder.OnDecodingStep(1, decoding_step_proto),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "unknown codec: bar; decoding_step.type=CODEC"));
  }
}

TEST(DecodeTest, Error_CodecIndexCollision) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("foo");
    EXPECT_CALL(mock_value_decoder_provider, Call("foo"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_codec()->set_name("bar");
    EXPECT_CALL(mock_value_decoder_provider, Call("bar"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_THAT(
        decoder.OnDecodingStep(0, decoding_step_proto),
        StatusIs(absl::StatusCode::kInvalidArgument, "codec_index collision"));
  }
}

TEST(DecodeTest, Error_ExprIndexCollision) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_leaf_node()->set_leaf_key("leaf_key");
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_leaf_node()->set_leaf_key("leaf_key");
    EXPECT_THAT(
        decoder.OnDecodingStep(0, decoding_step_proto),
        StatusIs(absl::StatusCode::kInvalidArgument, "expr_index collision"));
  }
}

TEST(DecodeTest, Error_ValueIndexCollision) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value();
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(1.0f)));
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value();
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(1.0f)));
    EXPECT_THAT(
        decoder.OnDecodingStep(1, decoding_step_proto),
        StatusIs(absl::StatusCode::kInvalidArgument, "value_index collision"));
  }
}

TEST(DecodeTest, Error_LiteralNode_MissingLiteralValueIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_literal_node();
  EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "missing literal_node.literal_value_index; "
                       "decoding_step.type=LITERAL_NODE"));
}

TEST(DecodeTest, Error_LiteralNode_IllegalLiteralValueIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_literal_node()->set_literal_value_index(100);
  EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "value_index is out of range: 100; "
                       "decoding_step.type=LITERAL_NODE"));
}

TEST(DecodeTest, Error_LiteralNode_LiteralValueIndexPointsToExpr) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_leaf_node()->set_leaf_key("leaf_key");
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_literal_node()->set_literal_value_index(1);
    EXPECT_THAT(decoder.OnDecodingStep(1, decoding_step_proto),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "found no value in decoding_step_results[1]; "
                         "decoding_step.type=LITERAL_NODE"));
  }
}

TEST(DecodeTest, Error_LeafNode_MissingLeafKey) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_leaf_node();
  EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "missing leaf_node.leaf_key; "
                       "decoding_step.type=LEAF_NODE"));
}

TEST(DecodeTest, Error_PlaceholderNode_MissingPlaceholderKey) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_placeholder_node();
  EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "missing placeholder_node.placeholder_key; "
                       "decoding_step.type=PLACEHOLDER_NODE"));
}

TEST(DecodeTest, Error_OperatorNode_MissingOperatorValueIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_operator_node();
  EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "missing operator_node.operator_value_index; "
                       "decoding_step.type=OPERATOR_NODE"));
}

TEST(DecodeTest, Error_OperatorNode_IllegalOperatorValueIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_operator_node()->set_operator_value_index(100);
  EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "value_index is out of range: 100; "
                       "decoding_step.type=OPERATOR_NODE"));
}

TEST(DecodeTest, Error_OperatorNode_OperatorValueIndexPointsToFloat32) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(1.0f)));
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_operator_node()->set_operator_value_index(1);
    EXPECT_THAT(
        decoder.OnDecodingStep(2, decoding_step_proto),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            "expected an operator in decoding_step_results[1], got FLOAT32; "
            "decoding_step.type=OPERATOR_NODE"));
  }
}

TEST(DecodeTest, Error_OperatorNode_IllegalInputExprIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr dummy_op,
                       LambdaOperator::Make("dummy_op", Placeholder("x")));
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(dummy_op)));
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  {
    auto* operator_node_proto = decoding_step_proto.mutable_operator_node();
    operator_node_proto->set_operator_value_index(1);
    operator_node_proto->add_input_expr_indices(100);
    EXPECT_THAT(decoder.OnDecodingStep(2, decoding_step_proto),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expr_index is out of range: 100; "
                         "decoding_step.type=OPERATOR_NODE"));
  }
}

TEST(DecodeTest, Error_OperatorNode_InvalidDepCount) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  ASSERT_OK_AND_ASSIGN(ExprOperatorPtr dummy_op,
                       LambdaOperator::Make("dummy_op", Placeholder("x")));
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(TypedValue::FromValue(dummy_op)));
    EXPECT_OK(decoder.OnDecodingStep(1, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_leaf_node()->set_leaf_key("leaf_key");
    EXPECT_OK(decoder.OnDecodingStep(2, decoding_step_proto));
  }
  {
    auto* operator_node_proto = decoding_step_proto.mutable_operator_node();
    operator_node_proto->set_operator_value_index(1);
    operator_node_proto->add_input_expr_indices(2);
    operator_node_proto->add_input_expr_indices(2);
    EXPECT_THAT(
        decoder.OnDecodingStep(2, decoding_step_proto),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "incorrect number of dependencies passed to an "
                 "operator node: expected 1 but got 2; "
                 "while calling dummy_op with args {L.leaf_key, L.leaf_key}; "
                 "decoding_step.type=OPERATOR_NODE"));
  }
}

TEST(DecodeTest, Error_Value_IllegalInputValueIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    auto* value_proto = decoding_step_proto.mutable_value();
    value_proto->set_codec_index(0);
    value_proto->add_input_value_indices(100);
    EXPECT_THAT(decoder.OnDecodingStep(1, decoding_step_proto),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "value_index is out of range: 100; "
                         "decoding_step.type=VALUE"));
  }
}

TEST(DecodeTest, Error_Value_IllegalInputExprIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    auto* value_proto = decoding_step_proto.mutable_value();
    value_proto->set_codec_index(0);
    value_proto->add_input_expr_indices(100);
    EXPECT_THAT(decoder.OnDecodingStep(1, decoding_step_proto),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expr_index is out of range: 100; "
                         "decoding_step.type=VALUE"));
  }
}

TEST(DecodeTest, Error_Value_IllegalCodecIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_value()->set_codec_index(100);
  EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "codec_index is out of range: 100; "
                       "decoding_step.type=VALUE"));
}

TEST(DecodeTest, Error_Value_InvalidCodecIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_leaf_node()->set_leaf_key("leaf_key");
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "found no codec in decoding_step_results[0]; "
                         "decoding_step.type=VALUE"));
  }
}

TEST(DecodeTest, Error_Value_CodecFailed) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(absl::UnimplementedError("codec error")));
    EXPECT_THAT(decoder.OnDecodingStep(1, decoding_step_proto),
                StatusIs(absl::StatusCode::kUnimplemented,
                         "codec error; codecs[0]=mock_codec; "
                         "decoding_step.type=VALUE"));
  }
}

TEST(DecodeTest, Error_Value_NoExtensionFound) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value()->set_codec_index(0);
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(NoExtensionFound{}));
    EXPECT_THAT(decoder.OnDecodingStep(1, decoding_step_proto),
                StatusIs(absl::StatusCode::kNotFound,
                         "no extension found; codecs[0]=mock_codec; "
                         "decoding_step.type=VALUE"));
  }
}

TEST(DecodeTest, Error_ValueWithUnknownCodec_CodecFailed) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value();
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(absl::UnimplementedError("codec error")));
    EXPECT_THAT(decoder.OnDecodingStep(1, decoding_step_proto),
                StatusIs(absl::StatusCode::kUnimplemented,
                         "codec error; detected_codec=mock_codec; "
                         "decoding_step.type=VALUE"));
  }
}

TEST(DecodeTest, Error_ValueWithUnknownCodec_CodecUndetected) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.mutable_value();
    EXPECT_CALL(mock_value_decoder,
                Call(Ref(decoding_step_proto.value()), IsEmpty(), IsEmpty()))
        .WillOnce(Return(NoExtensionFound{}));
    EXPECT_THAT(decoder.OnDecodingStep(1, decoding_step_proto),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "unable to detect codec; decoding_step.type=VALUE"));
  }
}

TEST(DecodeTest, Error_Output_IllegalOutputValueIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.set_output_value_index(100);
  EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "value_index is out of range: 100; "
                       "decoding_step.type=OUTPUT_VALUE_INDEX"));
}

TEST(DecodeTest, Error_Output_InvalidOutputValueIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.set_output_value_index(0);
    EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "found no value in decoding_step_results[0]; "
                         "decoding_step.type=OUTPUT_VALUE_INDEX"));
  }
}

TEST(DecodeTest, Error_Output_IllegalOutputExprIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.set_output_expr_index(100);
  EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expr_index is out of range: 100; "
                       "decoding_step.type=OUTPUT_EXPR_INDEX"));
}

TEST(DecodeTest, Error_Output_InvalidOutputExprIndex) {
  MockValueDecoderProvider mock_value_decoder_provider;
  MockValueDecoder mock_value_decoder;
  Decoder decoder(mock_value_decoder_provider.AsStdFunction(), {});
  DecodingStepProto decoding_step_proto;
  {
    decoding_step_proto.mutable_codec()->set_name("mock_codec");
    EXPECT_CALL(mock_value_decoder_provider, Call("mock_codec"))
        .WillOnce(Return(mock_value_decoder.AsStdFunction()));
    EXPECT_OK(decoder.OnDecodingStep(0, decoding_step_proto));
  }
  {
    decoding_step_proto.set_output_expr_index(0);
    EXPECT_THAT(decoder.OnDecodingStep(0, decoding_step_proto),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "found no expression in decoding_step_results[0]; "
                         "decoding_step.type=OUTPUT_EXPR_INDEX"));
  }
}

}  // namespace
}  // namespace arolla::serialization_base
