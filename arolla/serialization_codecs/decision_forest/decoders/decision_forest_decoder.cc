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
#include <memory>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/expr_operator/forest_model.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/decision_forest/codec_name.h"
#include "arolla/serialization_codecs/decision_forest/decision_forest_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;

DecisionTreeNodeId NodeIdFromProto(const DecisionForestV1Proto::NodeId id) {
  if (id.node_id_case() == id.kAdjustmentId) {
    return DecisionTreeNodeId::AdjustmentId(id.adjustment_id());
  } else {
    return DecisionTreeNodeId::SplitNodeId(id.split_node_id());
  }
}

absl::StatusOr<std::shared_ptr<const SplitCondition>> SplitConditionFromProto(
    const DecisionForestV1Proto::SplitNode& node_proto) {
  switch (node_proto.condition_case()) {
    case DecisionForestV1Proto::SplitNode::kIntervalCondition: {
      const auto& cond = node_proto.interval_condition();
      return IntervalSplit(cond.input_id(), cond.left(), cond.right());
    }
    case DecisionForestV1Proto::SplitNode::kSetOfValuesInt64Condition: {
      const auto& cond = node_proto.set_of_values_int64_condition();
      return SetOfValuesSplit(cond.input_id(),
                              absl::flat_hash_set<int64_t>(
                                  cond.values().begin(), cond.values().end()),
                              cond.result_if_missed());
    }
    case DecisionForestV1Proto::SplitNode::CONDITION_NOT_SET:
      break;
  }
  return absl::InvalidArgumentError("incorrect split condition");
}

absl::StatusOr<DecisionForestPtr> DecisionForestFromProto(
    const DecisionForestV1Proto::DecisionForest& proto) {
  std::vector<DecisionTree> trees;
  trees.reserve(proto.trees_size());
  for (const auto& tree_proto : proto.trees()) {
    DecisionTree tree;
    tree.weight = tree_proto.weight();
    tree.tag.step = tree_proto.step();
    tree.tag.submodel_id = tree_proto.submodel_id();
    tree.adjustments = {tree_proto.adjustments().begin(),
                        tree_proto.adjustments().end()};
    tree.split_nodes.reserve(tree_proto.split_nodes_size());
    for (const auto& node_proto : tree_proto.split_nodes()) {
      auto child_if_false = NodeIdFromProto(node_proto.child_if_false());
      auto child_if_true = NodeIdFromProto(node_proto.child_if_true());
      ASSIGN_OR_RETURN(auto condition, SplitConditionFromProto(node_proto));
      tree.split_nodes.push_back(
          {child_if_false, child_if_true, std::move(condition)});
    }
    trees.push_back(std::move(tree));
  }
  // `FromTrees` validates the data.
  return DecisionForest::FromTrees(std::move(trees));
}

absl::StatusOr<TypedValue> DecodeForestModel(
    const DecisionForestV1Proto::ForestModel& proto,
    absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 1 input value for ForestModel, got %d", input_values.size()));
  }
  int expected_expr_count = 1;
  for (const auto& arg : proto.args()) {
    if (arg.has_preprocessing()) expected_expr_count++;
  }
  if (input_exprs.size() != expected_expr_count) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %d input exprs for ForestModel, got %d",
                        expected_expr_count, input_values.size()));
  }

  ASSIGN_OR_RETURN(DecisionForestPtr forest,
                   input_values[0].As<DecisionForestPtr>());

  ForestModel::SubmodelIds submodel_ids;
  for (const auto& submodel_group : proto.submodel_groups()) {
    if (!submodel_group.has_name()) {
      return absl::InvalidArgumentError("submodel_groups[idx].name is missing");
    }
    std::vector<int> ids(submodel_group.submodel_ids().begin(),
                         submodel_group.submodel_ids().end());
    auto [_, inserted] =
        submodel_ids.emplace(submodel_group.name(), std::move(ids));
    if (!inserted) {
      return absl::InvalidArgumentError(absl::StrCat(
          "duplicated submodel_groups[idx].name: ", submodel_group.name()));
    }
  }

  std::vector<ForestModel::Parameter> inputs;
  int expr_index = 1;
  inputs.reserve(proto.args_size());
  for (const auto& arg : proto.args()) {
    if (!arg.has_name()) {
      return absl::InvalidArgumentError("ForestModel input name is missing");
    }
    ExprNodePtr preprocessing = nullptr;
    if (arg.has_preprocessing()) {
      preprocessing = input_exprs[expr_index++];
    }
    inputs.push_back({arg.name(), preprocessing});
  }

  ASSIGN_OR_RETURN(ExprOperatorPtr op,
                   ForestModel::Create({.forest = std::move(forest),
                                        .submodel_ids = std::move(submodel_ids),
                                        .inputs = std::move(inputs),
                                        .expression = input_exprs[0]}));
  return TypedValue::FromValue(op);
}

}  // namespace

absl::StatusOr<ValueDecoderResult> DecodeDecisionForest(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!value_proto.HasExtension(DecisionForestV1Proto::extension)) {
    return NoExtensionFound();
  }
  const DecisionForestV1Proto& forest_proto =
      value_proto.GetExtension(DecisionForestV1Proto::extension);
  switch (forest_proto.value_case()) {
    case DecisionForestV1Proto::kForest: {
      ASSIGN_OR_RETURN(DecisionForestPtr forest,
                       DecisionForestFromProto(forest_proto.forest()));
      return TypedValue::FromValue(std::move(forest));
    }
    case DecisionForestV1Proto::kForestModel: {
      return DecodeForestModel(forest_proto.forest_model(), input_values,
                               input_exprs);
    }
    case DecisionForestV1Proto::kForestQtype:
      return TypedValue::FromValue(GetQType<DecisionForestPtr>());
    case DecisionForestV1Proto::VALUE_NOT_SET:
      break;
  }
  return absl::InvalidArgumentError("invalid DecisionForestV1Proto");
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          return RegisterValueDecoder(kDecisionForestV1Codec,
                                      &DecodeDecisionForest);
        })

}  // namespace arolla::serialization_codecs
