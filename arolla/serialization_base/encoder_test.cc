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
#include "arolla/serialization_base/encoder.h"

#include <cstdint>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container.h"
#include "arolla/util/testing/equals_proto.h"

namespace arolla::serialization_base {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::expr::testing::DummyOp;
using ::arolla::testing::EqualsProto;
using ::testing::InSequence;
using ::testing::MockFunction;
using ::testing::Ref;
using ::testing::Return;
using ::testing::Truly;

auto EqualsTypedRef(TypedRef expected_value) {
  return Truly([expected_value](TypedRef actual_value) {
    return expected_value.GetRawPointer() == actual_value.GetRawPointer() &&
           expected_value.GetType() == actual_value.GetType();
  });
}

template <typename T>
auto EqualsTypedRefAsT(const T& expected_value) {
  return Truly([expected_value](TypedRef actual_value) {
    auto status_or_value = actual_value.As<T>();
    return status_or_value.ok() &&
           status_or_value.value().get() == expected_value;
  });
}

class MockContainerBuilder : public ContainerBuilder {
 public:
  MOCK_METHOD(absl::StatusOr<uint64_t>, Add,
              (DecodingStepProto && decoding_step_proto), (override));
};

class EncoderTest : public ::testing::Test {
 protected:
  MockContainerBuilder mock_container_builder_;
  MockFunction<absl::StatusOr<ValueProto>(TypedRef value, Encoder& encoder)>
      mock_value_encoder_;
  Encoder encoder_{mock_value_encoder_.AsStdFunction(),
                   mock_container_builder_};
};

TEST_F(EncoderTest, EncodeExpr_nullptr) {
  EXPECT_THAT(encoder_.EncodeExpr(nullptr),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(EncoderTest, Codecs) {
  {
    EXPECT_CALL(mock_container_builder_,
                Add(EqualsProto(R"pb(codec: { name: "a" })pb")))
        .WillOnce(Return(0));
    EXPECT_THAT(encoder_.EncodeCodec("a"), IsOkAndHolds(0));
    EXPECT_THAT(encoder_.EncodeCodec("a"), IsOkAndHolds(0));
  }
  {
    EXPECT_CALL(mock_container_builder_,
                Add(EqualsProto(R"pb(codec: { name: "b" })pb")))
        .WillOnce(Return(1));
    EXPECT_THAT(encoder_.EncodeCodec("b"), IsOkAndHolds(1));
    EXPECT_THAT(encoder_.EncodeCodec("a"), IsOkAndHolds(0));
  }
}

TEST_F(EncoderTest, EncodeExpr_LiteralNode) {
  auto literal = Literal(1.0f);
  InSequence seq;
  EXPECT_CALL(mock_value_encoder_,
              Call(EqualsTypedRef(literal->qvalue()->AsRef()), Ref(encoder_)))
      .WillOnce(Return(ValueProto()));
  EXPECT_CALL(mock_container_builder_, Add(EqualsProto(R"pb(value: {})pb")))
      .WillOnce(Return(0));
  EXPECT_CALL(
      mock_container_builder_,
      Add(EqualsProto(R"pb(literal_node: { literal_value_index: 0 })pb")))
      .WillOnce(Return(1));
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(output_expr_index: 1)pb")))
      .WillOnce(Return(2));
  EXPECT_THAT(encoder_.EncodeExpr(literal), IsOkAndHolds(1));
}

TEST_F(EncoderTest, EncodeExpr_LeafNode) {
  auto leaf = Leaf("key");
  InSequence seq;
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(leaf_node: { leaf_key: "key" })pb")))
      .WillOnce(Return(0));
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(output_expr_index: 0)pb")))
      .WillOnce(Return(1));
  EXPECT_THAT(encoder_.EncodeExpr(leaf), IsOkAndHolds(0));
}

TEST_F(EncoderTest, EncodeExpr_PlaceholderNode) {
  auto placeholder = Placeholder("key");
  InSequence seq;
  EXPECT_CALL(
      mock_container_builder_,
      Add(EqualsProto(R"pb(placeholder_node: { placeholder_key: "key" })pb")))
      .WillOnce(Return(0));
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(output_expr_index: 0)pb")))
      .WillOnce(Return(1));
  EXPECT_THAT(encoder_.EncodeExpr(placeholder), IsOkAndHolds(0));
}

TEST_F(EncoderTest, EncodeExpr_OperatorNode) {
  ExprOperatorPtr dummy_op = std::make_shared<DummyOp>(
      "dummy_op", ExprOperatorSignature::MakeVariadicArgs());
  auto leaf = Leaf("leaf");
  auto literal = Literal(1.0f);
  ASSERT_OK_AND_ASSIGN(auto expr,
                       BindOp(dummy_op, {leaf, literal, leaf, literal}, {}));
  InSequence seq;
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(leaf_node: { leaf_key: "leaf" })pb")))
      .WillOnce(Return(0));
  EXPECT_CALL(mock_value_encoder_,
              Call(EqualsTypedRef(literal->qvalue()->AsRef()), Ref(encoder_)))
      .WillOnce(Return(ValueProto()));
  EXPECT_CALL(mock_container_builder_, Add(EqualsProto(R"pb(value: {})pb")))
      .WillOnce(Return(1));
  EXPECT_CALL(
      mock_container_builder_,
      Add(EqualsProto(R"pb(literal_node: { literal_value_index: 1 })pb")))
      .WillOnce(Return(2));
  EXPECT_CALL(mock_value_encoder_,
              Call(EqualsTypedRefAsT(dummy_op), Ref(encoder_)))
      .WillOnce(Return(ValueProto()));
  EXPECT_CALL(mock_container_builder_, Add(EqualsProto(R"pb(value: {})pb")))
      .WillOnce(Return(3));
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(operator_node: {
                                     operator_value_index: 3
                                     input_expr_indices: [ 0, 2, 0, 2 ]
                                   })pb")))
      .WillOnce(Return(4));
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(output_expr_index: 4)pb")))
      .WillOnce(Return(5));
  EXPECT_THAT(encoder_.EncodeExpr(expr), IsOkAndHolds(4));
}

TEST_F(EncoderTest, EncodeExpr_SubExprDeduplication) {
  ExprOperatorPtr dummy_op = std::make_shared<DummyOp>(
      "dummy_op", ExprOperatorSignature::MakeVariadicArgs());
  auto leaf = Leaf("leaf");
  ASSERT_OK_AND_ASSIGN(auto expr, BindOp(dummy_op, {leaf}, {}));
  InSequence seq;
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(leaf_node { leaf_key: "leaf" })pb")))
      .WillOnce(Return(0));
  EXPECT_CALL(mock_value_encoder_,
              Call(EqualsTypedRefAsT(dummy_op), Ref(encoder_)))
      .WillOnce(Return(ValueProto()));
  EXPECT_CALL(mock_container_builder_, Add(EqualsProto(R"pb(value {})pb")))
      .WillOnce(Return(1));
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(operator_node {
                                     operator_value_index: 1
                                     input_expr_indices: [ 0 ]
                                   })pb")))
      .WillOnce(Return(2));
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(output_expr_index: 2)pb")))
      .WillOnce(Return(3));
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(output_expr_index: 0)pb")))
      .WillOnce(Return(4));
  EXPECT_THAT(encoder_.EncodeExpr(expr), IsOkAndHolds(2));
  EXPECT_THAT(encoder_.EncodeExpr(leaf),
              IsOkAndHolds(0));  // leaf node is serialized only once
}

TEST_F(EncoderTest, EncodeValue) {
  auto value = TypedValue::FromValue(1.0f);
  auto value_proto = ValueProto();
  value_proto.add_input_value_indices(1);
  value_proto.add_input_value_indices(2);
  value_proto.add_input_expr_indices(3);
  value_proto.add_input_expr_indices(4);
  value_proto.add_input_expr_indices(5);
  value_proto.set_codec_index(123);
  InSequence seq;
  EXPECT_CALL(mock_value_encoder_,
              Call(EqualsTypedRef(value.AsRef()), Ref(encoder_)))
      .WillOnce(Return(value_proto));
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(value {
                                     input_value_indices: [ 1, 2 ]
                                     input_expr_indices: [ 3, 4, 5 ]
                                     codec_index: 123
                                   })pb")))
      .WillOnce(Return(0));
  EXPECT_CALL(mock_container_builder_, Add(EqualsProto(R"pb(output_value_index:
                                                                0)pb")))
      .WillOnce(Return(1));
  EXPECT_THAT(encoder_.EncodeValue(value), IsOkAndHolds(0));
  EXPECT_CALL(
      mock_container_builder_,
      Add(EqualsProto(R"pb(literal_node: { literal_value_index: 0 })pb")))
      .WillOnce(Return(2));
  EXPECT_CALL(mock_container_builder_,
              Add(EqualsProto(R"pb(output_expr_index: 2)pb")))
      .WillOnce(Return(3));
  EXPECT_THAT(encoder_.EncodeExpr(Literal(value)),
              IsOkAndHolds(2));  // value is serialized only once
}

}  // namespace
}  // namespace arolla::serialization_base
