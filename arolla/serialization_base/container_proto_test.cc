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
#include "arolla/serialization_base/container_proto.h"

#include <cstdint>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container.h"
#include "arolla/util/testing/equals_proto.h"
#include "arolla/util/status_macros_backport.h"  // IWYU pragma: keep

namespace arolla::serialization_base {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsProto;
using ::testing::HasSubstr;
using ::testing::InSequence;
using ::testing::Return;

TEST(ContainerProtoBuilderTest, TrivialBehaviour) {
  ContainerProtoBuilder container_builder;
  {
    DecodingStepProto decoding_step_proto;
    decoding_step_proto.mutable_codec()->set_name("codec1");
    ASSERT_THAT(container_builder.Add(std::move(decoding_step_proto)),
                IsOkAndHolds(0));
  }
  {
    DecodingStepProto decoding_step_proto;
    decoding_step_proto.mutable_leaf_node()->set_leaf_key("key1");
    ASSERT_THAT(container_builder.Add(std::move(decoding_step_proto)),
                IsOkAndHolds(1));
  }
  {
    DecodingStepProto decoding_step_proto;
    decoding_step_proto.set_output_expr_index(1);
    ASSERT_THAT(container_builder.Add(std::move(decoding_step_proto)),
                IsOkAndHolds(2));
  }
  {
    DecodingStepProto decoding_step_proto;
    decoding_step_proto.mutable_codec()->set_name("codec2");
    ASSERT_THAT(container_builder.Add(std::move(decoding_step_proto)),
                IsOkAndHolds(3));
  }
  {
    DecodingStepProto decoding_step_proto;
    decoding_step_proto.mutable_placeholder_node()->set_placeholder_key("key2");
    ASSERT_THAT(container_builder.Add(std::move(decoding_step_proto)),
                IsOkAndHolds(4));
  }
  {
    DecodingStepProto decoding_step_proto;
    decoding_step_proto.set_output_expr_index(4);
    ASSERT_THAT(container_builder.Add(std::move(decoding_step_proto)),
                IsOkAndHolds(5));
  }
  {
    DecodingStepProto decoding_step_proto;
    decoding_step_proto.mutable_value();
    ASSERT_THAT(container_builder.Add(std::move(decoding_step_proto)),
                IsOkAndHolds(6));
  }
  {
    DecodingStepProto decoding_step_proto;
    decoding_step_proto.set_output_value_index(6);
    ASSERT_THAT(container_builder.Add(std::move(decoding_step_proto)),
                IsOkAndHolds(7));
  }
  EXPECT_TRUE(EqualsProto(
      std::move(container_builder).Finish(),
      R"pb(
        version: 2
        decoding_steps { codec { name: "codec1" } }
        decoding_steps { leaf_node { leaf_key: "key1" } }
        decoding_steps { output_expr_index: 1 }
        decoding_steps { codec { name: "codec2" } }
        decoding_steps { placeholder_node { placeholder_key: "key2" } }
        decoding_steps { output_expr_index: 4 }
        decoding_steps { value {} }
        decoding_steps { output_value_index: 6 }
      )pb"));
}

class MockContainerProcessor : public ContainerProcessor {
 public:
  MOCK_METHOD(absl::Status, OnDecodingStep,
              (uint64_t, const DecodingStepProto& decoding_step_proto),
              (override));
};

TEST(ProcessContainerProto, V1Behaviour) {
  ContainerProto container_proto;
  container_proto.set_version(1);
  container_proto.add_codecs()->set_name("codec1");
  container_proto.add_codecs()->set_name("codec2");
  container_proto.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      "key1");
  container_proto.add_decoding_steps()
      ->mutable_placeholder_node()
      ->set_placeholder_key("key2");
  container_proto.add_decoding_steps()->mutable_value();
  container_proto.add_output_value_indices(2);
  container_proto.add_output_expr_indices(0);
  container_proto.add_output_expr_indices(1);
  MockContainerProcessor mock_container_processor;
  {
    InSequence seq;
    EXPECT_CALL(
        mock_container_processor,
        OnDecodingStep(0, EqualsProto(R"pb(codec: { name: "codec1" })pb")));
    EXPECT_CALL(
        mock_container_processor,
        OnDecodingStep(1, EqualsProto(R"pb(codec: { name: "codec2" })pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(
                    0, EqualsProto(R"pb(leaf_node: { leaf_key: "key1" })pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(1, EqualsProto(R"pb(placeholder_node: {
                                                     placeholder_key: "key2"
                                                   })pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(2, EqualsProto(R"pb(value: {})pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(0, EqualsProto(R"pb(output_value_index: 2)pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(0, EqualsProto(R"pb(output_expr_index: 0)pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(0, EqualsProto(R"pb(output_expr_index: 1)pb")));
  }
  EXPECT_OK(ProcessContainerProto(container_proto, mock_container_processor));
}

TEST(ProcessContainerProto, V2Behaviour) {
  ContainerProto container_proto;
  container_proto.set_version(2);
  container_proto.add_decoding_steps()->mutable_codec()->set_name("codec1");
  container_proto.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      "key1");
  container_proto.add_decoding_steps()->set_output_expr_index(1);
  container_proto.add_decoding_steps()->mutable_codec()->set_name("codec2");
  container_proto.add_decoding_steps()
      ->mutable_placeholder_node()
      ->set_placeholder_key("key2");
  container_proto.add_decoding_steps()->set_output_expr_index(4);
  container_proto.add_decoding_steps()->mutable_value();
  container_proto.add_decoding_steps()->set_output_value_index(6);
  MockContainerProcessor mock_container_processor;
  {
    InSequence seq;
    EXPECT_CALL(
        mock_container_processor,
        OnDecodingStep(0, EqualsProto(R"pb(codec: { name: "codec1" })pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(
                    1, EqualsProto(R"pb(leaf_node: { leaf_key: "key1" })pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(2, EqualsProto(R"pb(output_expr_index: 1)pb")));
    EXPECT_CALL(
        mock_container_processor,
        OnDecodingStep(3, EqualsProto(R"pb(codec: { name: "codec2" })pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(4, EqualsProto(R"pb(placeholder_node: {
                                                     placeholder_key: "key2"
                                                   })pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(5, EqualsProto(R"pb(output_expr_index: 4)pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(6, EqualsProto(R"pb(value: {})pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(7, EqualsProto(R"pb(output_value_index: 6)pb")));
  }
  EXPECT_OK(ProcessContainerProto(container_proto, mock_container_processor));
}

TEST(ProcessContainerProto, MissingContainerVersion) {
  ContainerProto container_proto;
  MockContainerProcessor mock_container_processor;
  EXPECT_THAT(ProcessContainerProto(container_proto, mock_container_processor),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing container.version")));
}

TEST(ProcessContainerProto, WrongContainerVersion) {
  ContainerProto container_proto;
  container_proto.set_version(100);
  MockContainerProcessor mock_container_processor;
  EXPECT_THAT(
      ProcessContainerProto(container_proto, mock_container_processor),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected container.version to be 1 or 2, got 100")));
}

TEST(ProcessContainerProto, ProcessorFailureOnCodec) {
  ContainerProto container_proto;
  container_proto.set_version(1);
  container_proto.add_codecs()->set_name("codec1");
  container_proto.add_codecs()->set_name("codec2");
  MockContainerProcessor mock_container_processor;
  {
    InSequence seq;
    EXPECT_CALL(
        mock_container_processor,
        OnDecodingStep(0, EqualsProto(R"pb(codec: { name: "codec1" })pb")));
    EXPECT_CALL(
        mock_container_processor,
        OnDecodingStep(1, EqualsProto(R"pb(codec: { name: "codec2" })pb")))
        .WillOnce(Return(absl::FailedPreconditionError("stop")));
  }
  EXPECT_THAT(ProcessContainerProto(container_proto, mock_container_processor),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("stop; while handling codecs[1]")));
}

TEST(ProcessContainerProto, ProcessorFailureOnDecodingStep) {
  ContainerProto container_proto;
  container_proto.set_version(1);
  container_proto.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      "key1");
  container_proto.add_decoding_steps()->mutable_value();
  MockContainerProcessor mock_container_processor;
  {
    InSequence seq;
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(
                    0, EqualsProto(R"pb(leaf_node: { leaf_key: "key1" })pb")));
    EXPECT_CALL(mock_container_processor,
                OnDecodingStep(1, EqualsProto(R"pb(value {})pb")))
        .WillOnce(Return(absl::FailedPreconditionError("stop")));
  }
  EXPECT_THAT(ProcessContainerProto(container_proto, mock_container_processor),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("stop; while handling decoding_steps[1]")));
}

TEST(ProcessContainerProto, ProcessorFailureOnOutputValueIndex) {
  ContainerProto container_proto;
  container_proto.set_version(1);
  container_proto.add_output_value_indices(1);
  MockContainerProcessor mock_container_processor;
  EXPECT_CALL(mock_container_processor,
              OnDecodingStep(0, EqualsProto(R"pb(output_value_index: 1)pb")))
      .WillOnce(Return(absl::FailedPreconditionError("stop")));
  EXPECT_THAT(
      ProcessContainerProto(container_proto, mock_container_processor),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("stop; while handling output_value_indices[0]")));
}

TEST(ProcessContainerProto, ProcessorFailureOnOutputExprIndex) {
  ContainerProto container_proto;
  container_proto.set_version(1);
  container_proto.add_output_expr_indices(2);
  MockContainerProcessor mock_container_processor;
  EXPECT_CALL(mock_container_processor,
              OnDecodingStep(0, EqualsProto(R"pb(output_expr_index: 2)pb")))
      .WillOnce(Return(absl::FailedPreconditionError("stop")));
  EXPECT_THAT(
      ProcessContainerProto(container_proto, mock_container_processor),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("stop; while handling output_expr_indices[0]")));
}

}  // namespace
}  // namespace arolla::serialization_base
