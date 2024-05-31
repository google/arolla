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
#include "arolla/serialization_base/decode.h"

#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace arolla::serialization_base {
namespace {

using ::arolla::serialization_base::ContainerProto;
using ::arolla::serialization_base::ValueProto;
using ::arolla::testing::EqualsExpr;
using ::arolla::testing::StatusIs;
using ::arolla::testing::TypedValueWith;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::MockFunction;
using ::testing::Ref;
using ::testing::Return;

using MockValueDecoder = MockFunction<absl::StatusOr<ValueDecoderResult>(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const expr::ExprNodePtr> input_exprs)>;

class DecodeTest : public ::testing::Test {
 protected:
  DecodeTest() { container_proto_.set_version(1); }

  ValueDecoderProvider codecs() {
    return
        [this](absl::string_view codec_name) -> absl::StatusOr<ValueDecoder> {
          auto it = codecs_.find(codec_name);
          if (it == codecs_.end()) {
            return absl::InvalidArgumentError(
                absl::StrFormat("unknown codec: %s", codec_name));
          }
          return it->second;
        };
  }

  // Dummy operator.
  expr::ExprOperatorPtr dummy_op_ = std::make_shared<expr::testing::DummyOp>(
      "dummy_op", expr::ExprOperatorSignature{{"x"}, {"y"}});

  // Mock value decoder.
  MockValueDecoder mock_value_decoder_;
  absl::flat_hash_map<std::string, ValueDecoder> codecs_ = {
      {"mock_codec", mock_value_decoder_.AsStdFunction()}};
  ContainerProto container_proto_;
};

TEST_F(DecodeTest, EmptyMessage) {
  ASSERT_OK_AND_ASSIGN(auto output, Decode(container_proto_, codecs()));
  EXPECT_THAT(output.values, IsEmpty());
  EXPECT_THAT(output.exprs, IsEmpty());
}

TEST_F(DecodeTest, LiteralNode) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  container_proto_.add_decoding_steps()
      ->mutable_literal_node()
      ->set_literal_value_index(0);
  container_proto_.add_output_expr_indices(1);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(TypedValue::FromValue(1.0f)));
  ASSERT_OK_AND_ASSIGN(auto output, Decode(container_proto_, codecs()));
  EXPECT_THAT(output.values, IsEmpty());
  auto expected_output = expr::Literal(1.0f);
  EXPECT_THAT(output.exprs, ElementsAre(EqualsExpr(expected_output)));
}

TEST_F(DecodeTest, LeafNode) {
  container_proto_.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      "leaf_key");
  container_proto_.add_output_expr_indices(0);
  ASSERT_OK_AND_ASSIGN(auto output, Decode(container_proto_, codecs()));
  EXPECT_THAT(output.values, IsEmpty());
  auto expected_output = expr::Leaf("leaf_key");
  EXPECT_THAT(output.exprs, ElementsAre(EqualsExpr(expected_output)));
}

TEST_F(DecodeTest, PlaceholderNode) {
  container_proto_.add_decoding_steps()
      ->mutable_placeholder_node()
      ->set_placeholder_key("placeholder_key");
  container_proto_.add_output_expr_indices(0);
  ASSERT_OK_AND_ASSIGN(auto output, Decode(container_proto_, codecs()));
  EXPECT_THAT(output.values, IsEmpty());
  auto expected_output = expr::Placeholder("placeholder_key");
  EXPECT_THAT(output.exprs, ElementsAre(EqualsExpr(expected_output)));
}

TEST_F(DecodeTest, OperatorNode) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  container_proto_.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      "leaf_key");
  auto* operator_node_proto =
      container_proto_.add_decoding_steps()->mutable_operator_node();
  operator_node_proto->set_operator_value_index(0);
  operator_node_proto->add_input_expr_indices(1);
  operator_node_proto->add_input_expr_indices(1);
  container_proto_.add_output_expr_indices(2);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(TypedValue::FromValue(dummy_op_)));
  ASSERT_OK_AND_ASSIGN(auto output, Decode(container_proto_, codecs()));
  EXPECT_THAT(output.values, IsEmpty());
  auto leaf = expr::Leaf("leaf_key");
  auto expected_output = expr::ExprNode::UnsafeMakeOperatorNode(
      expr::ExprOperatorPtr(dummy_op_), {leaf, leaf}, expr::ExprAttributes{});
  EXPECT_THAT(output.exprs, ElementsAre(EqualsExpr(expected_output)));
}

TEST_F(DecodeTest, OperatorNode_NoMetadata) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  container_proto_.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      "leaf_key");
  auto* operator_node_proto =
      container_proto_.add_decoding_steps()->mutable_operator_node();
  operator_node_proto->set_operator_value_index(0);
  operator_node_proto->add_input_expr_indices(1);
  container_proto_.add_output_expr_indices(2);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(TypedValue::FromValue(dummy_op_)));
  ASSERT_OK_AND_ASSIGN(
      auto output,
      Decode(container_proto_, codecs(),
             DecodingOptions{.generate_metadata_for_operator_nodes = false}));
  EXPECT_THAT(output.values, IsEmpty());
  auto leaf = expr::Leaf("leaf_key");
  auto expected_output = expr::ExprNode::UnsafeMakeOperatorNode(
      expr::ExprOperatorPtr(dummy_op_), {leaf}, expr::ExprAttributes{});
  EXPECT_THAT(output.exprs, ElementsAre(EqualsExpr(expected_output)));
}

TEST_F(DecodeTest, ValueWithKnownCodec) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  container_proto_.add_output_value_indices(0);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(TypedValue::FromValue(1.0f)));
  ASSERT_OK_AND_ASSIGN(auto output, Decode(container_proto_, codecs()));
  EXPECT_THAT(output.values, ElementsAre(TypedValueWith<float>(1.0f)));
  EXPECT_THAT(output.exprs, IsEmpty());
}

TEST_F(DecodeTest, ValueWithUnknownCodec) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value();
  container_proto_.add_output_value_indices(0);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(NoExtensionFound{}))
      .WillOnce(Return(TypedValue::FromValue(1.0f)));
  ASSERT_OK_AND_ASSIGN(auto output, Decode(container_proto_, codecs()));
  EXPECT_THAT(output.values, ElementsAre(TypedValueWith<float>(1.0f)));
  EXPECT_THAT(output.exprs, IsEmpty());
}

TEST_F(DecodeTest, Error_MissingContainerVersion) {
  EXPECT_THAT(Decode(ContainerProto(), codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing container.version")));
}

TEST_F(DecodeTest, Error_WrongContainerVersion) {
  container_proto_.set_version(-1);
  EXPECT_THAT(
      Decode(container_proto_, codecs()),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected container.version to be 1, got -1")));
}

TEST_F(DecodeTest, Error_UnknownCodecs) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_codecs()->set_name("foo");
  container_proto_.add_codecs()->set_name("bar");
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unknown codec: foo")));
}

TEST_F(DecodeTest, Error_EmptyDecodingStep) {
  container_proto_.add_decoding_steps();
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing decoding_step.type; while handling "
                                 "decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_LiteralNode_MissingLiteralValueIndex) {
  container_proto_.add_decoding_steps()->mutable_literal_node();
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing literal_node.literal_value_index; "
                                 "decoding_step.type=LITERAL_NODE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_LiteralNode_InvalidLiteralValueIndex) {
  container_proto_.add_decoding_steps()
      ->mutable_literal_node()
      ->set_literal_value_index(-1);
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("value index is out of range: -1; "
                                 "decoding_step.type=LITERAL_NODE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_LiteralNode_LiteralValueIndexPointsToExpr) {
  container_proto_.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      "leaf_key");
  container_proto_.add_decoding_steps()
      ->mutable_literal_node()
      ->set_literal_value_index(0);
  EXPECT_THAT(
      Decode(container_proto_, codecs()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected a value in decoding_steps[0], got an expression; "
                    "decoding_step.type=LITERAL_NODE; "
                    "while handling decoding_steps[1]")));
}

TEST_F(DecodeTest, Error_LeafNode_MissingLeafKey) {
  container_proto_.add_decoding_steps()->mutable_leaf_node();
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing leaf_node.leaf_key; "
                                 "decoding_step.type=LEAF_NODE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_PlaceholderNode_MissingPlaceholderKey) {
  container_proto_.add_decoding_steps()->mutable_placeholder_node();
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing placeholder_node.placeholder_key; "
                                 "decoding_step.type=PLACEHOLDER_NODE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_OperatorNode_MissingOperatorValueIndex) {
  container_proto_.add_decoding_steps()->mutable_operator_node();
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing operator_node.operator_value_index; "
                                 "decoding_step.type=OPERATOR_NODE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_OperatorNode_InvalidOperatorValueIndex) {
  container_proto_.add_decoding_steps()
      ->mutable_operator_node()
      ->set_operator_value_index(-1);
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("value index is out of range: -1; "
                                 "decoding_step.type=OPERATOR_NODE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_OperatorNode_OperatorValueIndexPointsToExpr) {
  container_proto_.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      "leaf_key");
  container_proto_.add_decoding_steps()
      ->mutable_operator_node()
      ->set_operator_value_index(0);
  EXPECT_THAT(
      Decode(container_proto_, codecs()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected a value in decoding_steps[0], got an expression; "
                    "decoding_step.type=OPERATOR_NODE; "
                    "while handling decoding_steps[1]")));
}

TEST_F(DecodeTest, Error_OperatorNode_OperatorValueIndexPointsToFloat32) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  container_proto_.add_decoding_steps()
      ->mutable_operator_node()
      ->set_operator_value_index(0);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(TypedValue::FromValue(1.0f)));
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected a value of EXPR_OPERATOR type in "
                                 "decoding_steps[0], got FLOAT32; "
                                 "decoding_step.type=OPERATOR_NODE; "
                                 "while handling decoding_steps[1]")));
}

TEST_F(DecodeTest, Error_OperatorNode_InvalidInputExprIndex) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  auto* operator_node_proto =
      container_proto_.add_decoding_steps()->mutable_operator_node();
  operator_node_proto->set_operator_value_index(0);
  operator_node_proto->add_input_expr_indices(-1);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(TypedValue::FromValue(dummy_op_)));
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expr index is out of range: -1; "
                                 "decoding_step.type=OPERATOR_NODE; "
                                 "while handling decoding_steps[1]")));
}

TEST_F(DecodeTest, Error_OperatorNode_InputExprIndexPointsToValue) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  auto* operator_node_proto =
      container_proto_.add_decoding_steps()->mutable_operator_node();
  operator_node_proto->set_operator_value_index(0);
  operator_node_proto->add_input_expr_indices(0);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(TypedValue::FromValue(dummy_op_)));
  EXPECT_THAT(
      Decode(container_proto_, codecs()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected an expression in decoding_steps[0], got a value; "
                    "decoding_step.type=OPERATOR_NODE; "
                    "while handling decoding_steps[1]")));
}

TEST_F(DecodeTest, Error_OperatorNode_InvalidDepCount) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  auto* operator_node_proto =
      container_proto_.add_decoding_steps()->mutable_operator_node();
  operator_node_proto->set_operator_value_index(0);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(TypedValue::FromValue(dummy_op_)));
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("incorrect number of dependencies passed to "
                                 "an operator node: expected 2 but got 0; "
                                 "while calling dummy_op with args {}; "
                                 "decoding_step.type=OPERATOR_NODE; "
                                 "while handling decoding_steps[1]")));
}

TEST_F(DecodeTest, Error_Value_InvalidInputValueIndex) {
  container_proto_.add_decoding_steps()
      ->mutable_value()
      ->add_input_value_indices(-1);
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("value index is out of range: -1; "
                                 "decoding_step.type=VALUE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_Value_InputValueIndexPointsToExpr) {
  container_proto_.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      "leaf_key");
  container_proto_.add_decoding_steps()
      ->mutable_value()
      ->add_input_value_indices(0);
  EXPECT_THAT(
      Decode(container_proto_, codecs()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected a value in decoding_steps[0], got an expression; "
                    "decoding_step.type=VALUE; "
                    "while handling decoding_steps[1]")));
}

TEST_F(DecodeTest, Error_Value_InvalidInputExprIndex) {
  container_proto_.add_decoding_steps()
      ->mutable_value()
      ->add_input_expr_indices(-1);
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expr index is out of range: -1; "
                                 "decoding_step.type=VALUE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_Value_InputExprIndexPointsToValue) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value();
  container_proto_.add_decoding_steps()
      ->mutable_value()
      ->add_input_expr_indices(0);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(TypedValue::FromValue(1.0f)));
  EXPECT_THAT(
      Decode(container_proto_, codecs()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected an expression in decoding_steps[0], got a value; "
                    "decoding_step.type=VALUE; "
                    "while handling decoding_steps[1]")));
}

TEST_F(DecodeTest, Error_ValueWithKnownCodec_InvalidCodecIndex) {
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(-1);
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("codec index is out of range: -1; "
                                 "decoding_step.type=VALUE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_ValueWithKnownCodec_CodecFailed) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(absl::UnimplementedError("codec error")));
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("codec error; "
                                 "codecs[0]=mock_codec; "
                                 "decoding_step.type=VALUE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_ValueWithKnownCodec_NoExtensionFound) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(NoExtensionFound{}));
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("no extension found; "
                                 "codecs[0]=mock_codec; "
                                 "decoding_step.type=VALUE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_ValueWithUnknownCodec_CodecFailed) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value();
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(absl::UnimplementedError("codec error")));
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("codec error; "
                                 "codecs[0]=mock_codec; "
                                 "decoding_step.type=VALUE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_ValueWithUnknownCodec_NoExtensionFound) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value();
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .Times(2)
      .WillRepeatedly(Return(NoExtensionFound{}));
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unable to detect codec; "
                                 "decoding_step.type=VALUE; "
                                 "while handling decoding_steps[0]")));
}

TEST_F(DecodeTest, Error_Output_InvalidOutputValueIndex) {
  container_proto_.add_output_value_indices(-1);
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("value index is out of range: -1; "
                                 "while loading output values")));
}

TEST_F(DecodeTest, Error_Output_OutputValueIndexPointsToExpr) {
  container_proto_.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      "leaf_key");
  container_proto_.add_output_value_indices(0);
  EXPECT_THAT(
      Decode(container_proto_, codecs()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected a value in decoding_steps[0], got an expression; "
                    "while loading output values")));
}

TEST_F(DecodeTest, Error_Output_InvalidOutputExprIndex) {
  container_proto_.add_output_expr_indices(-1);
  EXPECT_THAT(Decode(container_proto_, codecs()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expr index is out of range: -1; "
                                 "while loading output expressions")));
}

TEST_F(DecodeTest, Error_Output_OutputExprIndexPointsToValue) {
  container_proto_.add_codecs()->set_name("mock_codec");
  container_proto_.add_decoding_steps()->mutable_value()->set_codec_index(0);
  container_proto_.add_output_value_indices(0);
  EXPECT_CALL(mock_value_decoder_,
              Call(Ref(container_proto_.decoding_steps(0).value()), IsEmpty(),
                   IsEmpty()))
      .WillOnce(Return(TypedValue::FromValue(1.0f)));
  container_proto_.add_output_expr_indices(0);
  EXPECT_THAT(
      Decode(container_proto_, codecs()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected an expression in decoding_steps[0], got a value; "
                    "while loading output expressions")));
}

}  // namespace
}  // namespace arolla::serialization_base
