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

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/expr_operator/forest_model.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/decision_forest/codec_name.h"
#include "arolla/serialization_codecs/decision_forest/decision_forest_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::expr::ExprOperatorPtr;
using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;

void NodeIdToProto(const DecisionTreeNodeId node_id,
                   DecisionForestV1Proto::NodeId& proto) {
  if (node_id.is_leaf()) {
    proto.set_adjustment_id(node_id.adjustment_index());
  } else {
    proto.set_split_node_id(node_id.split_node_index());
  }
}

absl::Status SplitConditionToProto(
    const SplitCondition& condition,
    DecisionForestV1Proto::SplitNode& node_proto) {
  if (const auto* as_interval =
          fast_dynamic_downcast_final<const IntervalSplitCondition*>(
              &condition)) {
    auto* cond = node_proto.mutable_interval_condition();
    cond->set_input_id(as_interval->input_id());
    cond->set_left(as_interval->left());
    cond->set_right(as_interval->right());
  } else if (const auto* as_set_int64 = fast_dynamic_downcast_final<
                 const SetOfValuesSplitCondition<int64_t>*>(&condition)) {
    auto* cond = node_proto.mutable_set_of_values_int64_condition();
    cond->set_input_id(as_set_int64->input_id());
    for (int64_t v : as_set_int64->ValuesAsVector()) {
      cond->add_values(v);
    }
    cond->set_result_if_missed(as_set_int64->GetDefaultResultForMissedInput());
  } else {
    return absl::InvalidArgumentError(
        absl::StrCat("unknown split condition: ", condition.ToString()));
  }
  return absl::OkStatus();
}

absl::Status DecisionForestToProto(
    const DecisionForest& forest,
    DecisionForestV1Proto::DecisionForest& proto) {
  for (const auto& tree : forest.GetTrees()) {
    auto* tree_proto = proto.add_trees();
    tree_proto->set_weight(tree.weight);
    tree_proto->set_step(tree.tag.step);
    tree_proto->set_submodel_id(tree.tag.submodel_id);
    for (float v : tree.adjustments) tree_proto->add_adjustments(v);
    for (const auto& node : tree.split_nodes) {
      auto* node_proto = tree_proto->add_split_nodes();
      NodeIdToProto(node.child_if_false, *node_proto->mutable_child_if_false());
      NodeIdToProto(node.child_if_true, *node_proto->mutable_child_if_true());
      RETURN_IF_ERROR(SplitConditionToProto(*node.condition, *node_proto));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index,
                   encoder.EncodeCodec(kDecisionForestV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeForestModel(const ForestModel& op,
                                             Encoder& encoder) {
  if (op.oob_filters().has_value()) {
    return absl::UnimplementedError(
        "serialization of ForestModel with oob_filters is not supported yet");
  }
  if (op.truncation_step().has_value()) {
    return absl::UnimplementedError(
        "serialization of truncated ForestModel is not supported yet");
  }
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* forest_model_proto =
      value_proto.MutableExtension(DecisionForestV1Proto::extension)
          ->mutable_forest_model();
  ASSIGN_OR_RETURN(auto forest_index,
                   encoder.EncodeValue(TypedValue::FromValue(op.forest())));
  value_proto.add_input_value_indices(forest_index);
  ASSIGN_OR_RETURN(auto posprocessing_expr_index,
                   encoder.EncodeExpr(op.expression()));
  value_proto.add_input_expr_indices(posprocessing_expr_index);
  for (const ForestModel::Parameter& p : op.inputs()) {
    auto* arg = forest_model_proto->add_args();
    arg->set_name(p.name);
    if (p.preprocessing != nullptr) {
      ASSIGN_OR_RETURN(auto preprocessing_expr_index,
                       encoder.EncodeExpr(p.preprocessing));
      value_proto.add_input_expr_indices(preprocessing_expr_index);
      arg->set_has_preprocessing(true);
    }
  }
  for (const auto& [group_name, submodel_ids] : op.submodel_ids()) {
    auto* submodel_group = forest_model_proto->add_submodel_groups();
    submodel_group->set_name(group_name);
    for (int id : submodel_ids) {
      submodel_group->add_submodel_ids(id);
    }
  }
  return value_proto;
}

}  // namespace

absl::StatusOr<ValueProto> EncodeDecisionForest(TypedRef value,
                                                Encoder& encoder) {
  if (value.GetType() == GetQType<ExprOperatorPtr>()) {
    const ForestModel* forest_model = dynamic_cast<const ForestModel*>(
        value.UnsafeAs<ExprOperatorPtr>().get());
    if (forest_model == nullptr) {
      return absl::InvalidArgumentError("expected ForestModel operator");
    }
    return EncodeForestModel(*forest_model, encoder);
  }
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  if (value.GetType() == GetQType<QTypePtr>() &&
      value.UnsafeAs<QTypePtr>() == GetQType<DecisionForestPtr>()) {
    value_proto.MutableExtension(DecisionForestV1Proto::extension)
        ->set_forest_qtype(true);
  } else {
    ASSIGN_OR_RETURN(const DecisionForestPtr& forest,
                     value.As<DecisionForestPtr>());
    auto forest_proto =
        value_proto.MutableExtension(DecisionForestV1Proto::extension);
    RETURN_IF_ERROR(
        DecisionForestToProto(*forest, *forest_proto->mutable_forest()));
  }
  return value_proto;
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(RegisterValueEncoderByQValueSpecialisationKey(
              kForestModelQValueSpecializationKey, &EncodeDecisionForest));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              GetQType<DecisionForestPtr>(), &EncodeDecisionForest));
          return absl::OkStatus();
        })

}  // namespace arolla::serialization_codecs
