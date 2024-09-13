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
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/text_format.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/util/testing/equals_proto.h"

namespace arolla::testing {
namespace {

using ::absl_testing::StatusIs;

constexpr float kInf = std::numeric_limits<float>::infinity();
constexpr auto S = DecisionTreeNodeId::SplitNodeId;
constexpr auto A = DecisionTreeNodeId::AdjustmentId;

DecisionForestPtr CreateForest() {
  std::vector<DecisionTree> trees(2);
  trees[0].adjustments = {0.5, 1.5, 2.5, 3.5};
  trees[0].split_nodes = {
      {S(1), S(2), IntervalSplit(0, 1.5, kInf)},
      {A(0), A(1), SetOfValuesSplit<int64_t>(1, {5}, false)},
      {A(2), A(3), IntervalSplit(0, -kInf, 10)}};
  trees[1].adjustments = {-0.5};
  trees[1].weight = 1.5;
  trees[1].tag.step = 1;
  trees[1].tag.submodel_id = 2;

  return DecisionForest::FromTrees(std::move(trees)).value();
}

constexpr absl::string_view kExpectedProtoStr =
    R"pb(version: 2
         decoding_steps {
           codec {
             name: "arolla.serialization_codecs.DecisionForestV1Proto.extension"
           }
         }
         decoding_steps {
           value {
             codec_index: 0
             [arolla.serialization_codecs.DecisionForestV1Proto.extension] {
               forest {
                 trees {
                   split_nodes {
                     child_if_false { split_node_id: 1 }
                     child_if_true { split_node_id: 2 }
                     interval_condition { input_id: 0 left: 1.5 right: inf }
                   }
                   split_nodes {
                     child_if_false { adjustment_id: 0 }
                     child_if_true { adjustment_id: 1 }
                     set_of_values_int64_condition {
                       input_id: 1
                       values: 5
                       result_if_missed: false
                     }
                   }
                   split_nodes {
                     child_if_false { adjustment_id: 2 }
                     child_if_true { adjustment_id: 3 }
                     interval_condition { input_id: 0 left: -inf right: 10 }
                   }
                   adjustments: 0.5
                   adjustments: 1.5
                   adjustments: 2.5
                   adjustments: 3.5
                   weight: 1
                   step: 0
                   submodel_id: 0
                 }
                 trees { adjustments: -0.5 weight: 1.5 step: 1 submodel_id: 2 }
               }
             }
           }
         }
         decoding_steps { output_value_index: 1 }
    )pb";

constexpr absl::string_view kInvalidProtoStr =
    R"pb(version: 2
         decoding_steps {
           codec {
             name: "arolla.serialization_codecs.DecisionForestV1Proto.extension"
           }
         }
         decoding_steps {
           value {
             codec_index: 0
             [arolla.serialization_codecs.DecisionForestV1Proto.extension] {
               forest {
                 trees {
                   split_nodes {
                     child_if_false { split_node_id: 1 }
                     child_if_true { split_node_id: 2 }
                     interval_condition { input_id: 0 left: 1.5 right: inf }
                   }
                   adjustments: 0.5
                   adjustments: 1.5
                   adjustments: 2.5
                   adjustments: 3.5
                   weight: 1
                   step: 0
                   submodel_id: 0
                 }
               }
             }
           }
         }
         decoding_steps { output_value_index: 1 }
    )pb";

constexpr absl::string_view kQTypeProtoStr =
    R"pb(version: 2
         decoding_steps {
           codec {
             name: "arolla.serialization_codecs.DecisionForestV1Proto.extension"
           }
         }
         decoding_steps {
           value {
             codec_index: 0
             [arolla.serialization_codecs.DecisionForestV1Proto.extension] {
               forest_qtype: true
             }
           }
         }
         decoding_steps { output_value_index: 1 })pb";

TEST(DecisionForestCodec, DecisionForestQValue) {
  DecisionForestPtr forest = CreateForest();
  ASSERT_OK_AND_ASSIGN(
      arolla::serialization_base::ContainerProto proto,
      serialization::Encode({TypedValue::FromValue(forest)}, {}));
  EXPECT_TRUE(EqualsProto(proto, kExpectedProtoStr));
  ASSERT_OK_AND_ASSIGN(serialization::DecodeResult res,
                       serialization::Decode(proto));
  EXPECT_TRUE(res.exprs.empty());
  ASSERT_EQ(res.values.size(), 1);
  ASSERT_OK_AND_ASSIGN(DecisionForestPtr res_forest,
                       res.values[0].As<DecisionForestPtr>());

  EXPECT_EQ(forest->fingerprint(), res_forest->fingerprint())
      << ToDebugString(*forest) << "\nvs\n"
      << ToDebugString(*res_forest);
}

TEST(DecisionForestCodec, DecodeInvalidProto) {
  arolla::serialization_base::ContainerProto proto;
  google::protobuf::TextFormat::ParseFromString(
      // Converted to std::string because OSS version of ParseFromString doesn't
      // work with string_view.
      std::string(kInvalidProtoStr), &proto);  // NOLINT
  EXPECT_THAT(serialization::Decode(proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr("incorrect number of regions")));
}

TEST(DecisionForestCodec, DecisionForestQType) {
  ASSERT_OK_AND_ASSIGN(
      arolla::serialization_base::ContainerProto proto,
      serialization::Encode(
          {TypedValue::FromValue(GetQType<DecisionForestPtr>())}, {}));
  EXPECT_TRUE(EqualsProto(proto, kQTypeProtoStr));
  ASSERT_OK_AND_ASSIGN(serialization::DecodeResult res,
                       serialization::Decode(proto));
  EXPECT_TRUE(res.exprs.empty());
  ASSERT_EQ(res.values.size(), 1);
  EXPECT_THAT(res.values[0],
              TypedValueWith<QTypePtr>(GetQType<DecisionForestPtr>()));
}

}  // namespace
}  // namespace arolla::testing
