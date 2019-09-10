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
#include "arolla/serialization_base/encode.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/util/testing/equals_proto.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace arolla::serialization_base {
namespace {

using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::expr::testing::DummyOp;
using ::arolla::testing::EqualsProto;
using ::arolla::testing::IsOkAndHolds;
using ::arolla::testing::StatusIs;
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

class EncoderTest : public ::testing::Test {
 protected:
  MockFunction<absl::StatusOr<ValueProto>(TypedRef value, Encoder& encoder)>
      value_encoder_;
  ContainerProto container_proto_;
  Encoder encoder_{value_encoder_.AsStdFunction(), container_proto_};
};

TEST_F(EncoderTest, EncodeExpr_nullptr) {
  EXPECT_THAT(encoder_.EncodeExpr(nullptr),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(EncoderTest, Codecs) {
  EXPECT_EQ(encoder_.EncodeCodec("a"), 0);
  EXPECT_EQ(encoder_.EncodeCodec("a"), 0);
  EXPECT_EQ(encoder_.EncodeCodec("b"), 1);
  EXPECT_TRUE(EqualsProto(container_proto_,
                          R"pb(
                            version: 1
                            codecs { name: "a" }
                            codecs { name: "b" }
                          )pb"));
}

TEST_F(EncoderTest, EncodeExpr_LiteralNode) {
  auto literal = Literal(1.0f);

  EXPECT_CALL(value_encoder_,
              Call(EqualsTypedRef(literal->qvalue()->AsRef()), Ref(encoder_)))
      .WillOnce(Return(ValueProto()));
  EXPECT_THAT(encoder_.EncodeExpr(literal), IsOkAndHolds(1));
  EXPECT_TRUE(
      EqualsProto(container_proto_,
                  R"pb(
                    version: 1
                    decoding_steps { value {} }
                    decoding_steps { literal_node { literal_value_index: 0 } }
                  )pb"));
}

TEST_F(EncoderTest, EncodeExpr_LeafNode) {
  auto leaf = Leaf("key");
  EXPECT_THAT(encoder_.EncodeExpr(leaf), IsOkAndHolds(0));
  EXPECT_TRUE(EqualsProto(container_proto_,
                          R"pb(
                            version: 1
                            decoding_steps { leaf_node { leaf_key: "key" } }
                          )pb"));
}

TEST_F(EncoderTest, EncodeExpr_PlaceholderNode) {
  auto placeholder = Placeholder("key");
  EXPECT_THAT(encoder_.EncodeExpr(placeholder), IsOkAndHolds(0));
  EXPECT_TRUE(EqualsProto(
      container_proto_,
      R"pb(
        version: 1
        decoding_steps { placeholder_node { placeholder_key: "key" } }
      )pb"));
}

TEST_F(EncoderTest, EncodeExpr_OperatorNode) {
  ExprOperatorPtr dummy_op = std::make_shared<DummyOp>(
      "dummy_op", ExprOperatorSignature::MakeVariadicArgs());
  auto leaf_x = Leaf("leaf");
  auto literal = Literal(1.0f);
  ASSERT_OK_AND_ASSIGN(
      auto expr, BindOp(dummy_op, {leaf_x, literal, leaf_x, literal}, {}));
  EXPECT_CALL(value_encoder_,
              Call(EqualsTypedRef(literal->qvalue()->AsRef()), Ref(encoder_)))
      .WillOnce(Return(ValueProto()));
  EXPECT_CALL(value_encoder_, Call(EqualsTypedRefAsT(dummy_op), Ref(encoder_)))
      .WillOnce(Return(ValueProto()));
  EXPECT_THAT(encoder_.EncodeExpr(expr), IsOkAndHolds(4));
  EXPECT_TRUE(
      EqualsProto(container_proto_,
                  R"pb(
                    version: 1
                    decoding_steps { leaf_node { leaf_key: "leaf" } }
                    decoding_steps { value {} }  # literal value
                    decoding_steps { literal_node { literal_value_index: 1 } }
                    decoding_steps { value {} }  # opertor
                    decoding_steps {
                      operator_node {
                        operator_value_index: 3
                        input_expr_indices: [ 0, 2, 0, 2 ]
                      }
                    }
                  )pb"));
}

TEST_F(EncoderTest, EncodeExpr_SubExprDeduplication) {
  ExprOperatorPtr dummy_op = std::make_shared<DummyOp>(
      "dummy_op", ExprOperatorSignature::MakeVariadicArgs());
  auto leaf_x = Leaf("leaf");
  ASSERT_OK_AND_ASSIGN(auto expr, BindOp(dummy_op, {leaf_x}, {}));
  EXPECT_CALL(value_encoder_, Call(EqualsTypedRefAsT(dummy_op), Ref(encoder_)))
      .WillOnce(Return(ValueProto()));
  EXPECT_THAT(encoder_.EncodeExpr(leaf_x), IsOkAndHolds(0));
  EXPECT_THAT(encoder_.EncodeExpr(expr), IsOkAndHolds(2));
  EXPECT_TRUE(  // leaf node is serialized only once
      EqualsProto(container_proto_,
                  R"pb(
                    version: 1
                    decoding_steps { leaf_node { leaf_key: "leaf" } }
                    decoding_steps { value {} }  # opertor
                    decoding_steps {
                      operator_node {
                        operator_value_index: 1
                        input_expr_indices: [ 0 ]
                      }
                    }
                  )pb"));
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
  EXPECT_CALL(value_encoder_,
              Call(EqualsTypedRef(value.AsRef()), Ref(encoder_)))
      .WillOnce(Return(value_proto));
  EXPECT_THAT(encoder_.EncodeValue(value), IsOkAndHolds(0));
  EXPECT_TRUE(EqualsProto(container_proto_,
                          R"pb(
                            version: 1
                            decoding_steps {
                              value {
                                input_value_indices: [ 1, 2 ]
                                input_expr_indices: [ 3, 4, 5 ]
                                codec_index: 123
                              }
                            }
                          )pb"));
}

}  // namespace
}  // namespace arolla::serialization_base
