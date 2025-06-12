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
#include "arolla/decision_forest/expr_operator/forest_model.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/expr_operator/decision_forest_operator.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace {

absl::Status ValidateExpression(
    const expr::ExprNodePtr& expression,
    const ForestModel::SubmodelIds& submodel_ids,
    const absl::flat_hash_set<std::string>& input_names) {
  absl::flat_hash_set<std::string> unused_submodels;
  for (const auto& [k, _] : submodel_ids) unused_submodels.insert(k);
  for (const auto& node : expr::VisitorOrder(expression)) {
    if (node->is_leaf()) {
      return absl::InvalidArgumentError(
          "leaves are not allowed in an expression");
    }
    if (node->is_placeholder()) {
      if (submodel_ids.count(node->placeholder_key()) > 0) {
        unused_submodels.erase(node->placeholder_key());
      } else if (!input_names.contains(node->placeholder_key())) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "P.%s doesn't correspond to any input and it is not "
            "found in submodel_ids",
            node->placeholder_key()));
      }
    }
  }
  if (!unused_submodels.empty()) {
    std::vector<std::string> unused_vec(unused_submodels.begin(),
                                        unused_submodels.end());
    absl::c_sort(unused_vec);
    return absl::InvalidArgumentError(
        absl::StrFormat("submodels [%s] are not used in the expression, but "
                        "are mentioned in submodel_ids",
                        absl::StrJoin(unused_vec, ", ")));
  }
  return absl::OkStatus();
}

absl::Status ValidateInputs(const DecisionForestPtr& forest,
                            const ForestModel::SubmodelIds& submodel_ids,
                            const std::vector<ForestModel::Parameter>& inputs) {
  for (const auto& input : inputs) {
    if (submodel_ids.count(input.name) > 0) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "name collision of an input and a submodel: '%s'", input.name));
    }
  }
  for (const auto& [key, unused] : forest->GetRequiredQTypes()) {
    if (key >= inputs.size()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "not enough args: used_input_index=%d  size=%d", key, inputs.size()));
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateOOBFilters(
    const std::vector<expr::ExprNodePtr>& oob_filters,
    const DecisionForestPtr& forest,
    const absl::flat_hash_set<std::string>& input_names) {
  for (const expr::ExprNodePtr& filter : oob_filters) {
    if (filter == nullptr) {
      return absl::InvalidArgumentError("OOB filter can't be nullptr");
    }
    for (const auto& node : expr::VisitorOrder(filter)) {
      if (node->is_leaf()) {
        return absl::InvalidArgumentError(
            "leaves are not allowed in an OOB filter expressing");
      }
      if (node->is_placeholder() &&
          !input_names.contains(node->placeholder_key())) {
        return absl::InvalidArgumentError(
            absl::StrCat("no input matches P.", node->placeholder_key(),
                         " in OOB filter ", expr::ToDebugString(node)));
      }
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<expr::ExprNodePtr> AddAll(
    const expr::ExprNodePtr& first, absl::Span<const expr::ExprNodePtr> nodes) {
  auto res = first;
  for (const auto& node : nodes) {
    ASSIGN_OR_RETURN(res, expr::CallOp("math.add", {res, node}));
  }
  return res;
}

using NodeCountMap = absl::flat_hash_map<Fingerprint, int>;

NodeCountMap GetNodeCountMap(const expr::ExprNodePtr& expr) {
  return PostOrderTraverse(expr,
                           [&](const expr::ExprNodePtr& node,
                               absl::Span<const NodeCountMap* const> visits) {
                             NodeCountMap res{{node->fingerprint(), 1}};
                             for (const NodeCountMap* visit : visits) {
                               for (const auto& [k, v] : *visit) {
                                 if (res.contains(k)) {
                                   res[k] += v;
                                 } else {
                                   res[k] = v;
                                 }
                               }
                             }
                             return res;
                           });
}

}  // namespace

absl::StatusOr<ForestModelPtr> ForestModel::Create(
    ForestModel::ConstructorArgs args) {
  // Construct operator signature.
  expr::ExprOperatorSignature signature;
  signature.parameters.reserve(args.inputs.size());
  for (const Parameter& param : args.inputs) {
    signature.parameters.push_back({param.name});
  }

  RETURN_IF_ERROR(expr::ValidateSignature(signature));
  RETURN_IF_ERROR(ValidateInputs(args.forest, args.submodel_ids, args.inputs));

  absl::flat_hash_set<std::string> input_names;
  input_names.reserve(args.inputs.size());
  for (const auto& input : args.inputs) {
    input_names.insert(input.name);
  }
  RETURN_IF_ERROR(
      ValidateExpression(args.expression, args.submodel_ids, input_names));
  if (args.oob_filters.has_value()) {
    RETURN_IF_ERROR(
        ValidateOOBFilters(*args.oob_filters, args.forest, input_names));
  }

  // Fingerprint calculation.
  FingerprintHasher hasher("d18261c6a5414ee8e5b0af80dc480ea8");
  hasher.Combine(args.forest->fingerprint(), args.expression->fingerprint(),
                 signature);
  hasher.Combine(args.submodel_ids.size());
  for (const auto& [k, v] : args.submodel_ids) {
    hasher.Combine(k).CombineSpan(v);
  }
  hasher.Combine(args.inputs.size());
  for (const auto& input : args.inputs) {
    if (input.preprocessing != nullptr) {
      hasher.Combine(input.preprocessing->fingerprint());
    } else {
      hasher.Combine(Fingerprint{});
    }
  }
  if (args.oob_filters.has_value()) {
    for (const auto& oob_filter : *args.oob_filters) {
      hasher.Combine(oob_filter->fingerprint());
    }
  } else {
    hasher.Combine(Fingerprint{});
  }
  if (args.truncation_step.has_value()) {
    hasher.Combine(*args.truncation_step);
  } else {
    hasher.Combine(Fingerprint{});
  }

  std::shared_ptr<ForestModel> model(new ForestModel(
      std::move(signature), std::move(hasher).Finish(), std::move(args)));
  RETURN_IF_ERROR(model->Initialize());
  return model;
}

absl::StatusOr<std::vector<expr::ExprNodePtr>> ForestModel::PreprocessInputs(
    const expr::ExprNodePtr& node) const {
  RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
  std::vector<expr::ExprNodePtr> args(inputs_.size());
  for (int i = 0; i < inputs_.size(); ++i) {
    expr::ExprNodePtr arg = node->node_deps()[i];
    if (inputs_[i].preprocessing != nullptr) {
      ASSIGN_OR_RETURN(auto lambda,
                       expr::LambdaOperator::Make(inputs_[i].preprocessing));
      ASSIGN_OR_RETURN(arg, expr::CallOp(lambda, {arg}));
      ASSIGN_OR_RETURN(arg,
                       expr::ToLowerNode(arg));  // expand the lambda operator
    }
    if (arg->qtype() == nullptr) {
      return absl::InternalError(
          absl::StrFormat("invalid preprocessing for input #%d: QType metadata "
                          "can not be propagated",
                          i));
    }
    ASSIGN_OR_RETURN(args[i], CastAndValidateArgType(i, std::move(arg)));
  }
  return args;
}

absl::StatusOr<expr::ExprNodePtr> ForestModel::ApplyPostprocessing(
    const expr::ExprNodePtr& node, const expr::ExprNodePtr& raw_result) const {
  RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
  absl::flat_hash_map<std::string, expr::ExprNodePtr> expression_params;
  expression_params.reserve(inputs_.size() + 1);
  for (int i = 0; i < inputs_.size(); ++i) {
    expression_params[inputs_[i].name] = node->node_deps()[i];
  }
  if (res_tuple_key_) {
    if (raw_result == nullptr) {
      return absl::InvalidArgumentError(
          "raw_result can be nullptr only if expression doesn't use decision "
          "forest");
    }
    expression_params[*res_tuple_key_] = raw_result;
  }
  ASSIGN_OR_RETURN(auto result, SubstitutePlaceholders(
                                    processed_expression_, expression_params,
                                    /*must_substitute_all=*/true));
  if (IsNameAnnotation(node)) {
    return expr::CallOp(
        "annotation.name",
        {result, expr::Literal(Text(expr::ReadNameAnnotation(node)))});
  }
  return result;
}

absl::StatusOr<expr::ExprNodePtr> ForestModel::ToLowerLevel(
    const expr::ExprNodePtr& node) const {
  RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
  for (size_t i = 0; i < inputs_.size(); ++i) {
    if (node->node_deps()[i]->qtype() == nullptr) {
      // Type information is incomplete, so ForestModel can not be expanded to
      // a lower level. It is not an error, so return the original node with
      // substituted default values.
      return node;
    }
  }

  if (!res_tuple_key_) {  // corner case if forest is not used in the model.
    return ApplyPostprocessing(node, nullptr);
  }

  ASSIGN_OR_RETURN(std::vector<expr::ExprNodePtr> args, PreprocessInputs(node));
  ASSIGN_OR_RETURN(auto op, CreateDecisionForestOperator(tree_filters_));
  ASSIGN_OR_RETURN(auto res_tuple, expr::MakeOpNode(op, std::move(args)));
  return ApplyPostprocessing(node, res_tuple);
}

absl::StatusOr<expr::ExprNodePtr> ForestModel::CreatePartialEvaluator(
    absl::Span<const std::pair<int, int>> step_ranges,
    absl::Span<const expr::ExprNodePtr> preprocessed_inputs) const {
  std::vector<TreeFilter> filters;
  filters.reserve(step_ranges.size() * tree_filters_.size());
  for (auto& [from, to] : step_ranges) {
    for (const TreeFilter& filter : tree_filters_) {
      if ((filter.step_range_from > from) ||
          (filter.step_range_to >= 0 && filter.step_range_to < to)) {
        return absl::InvalidArgumentError("requested range is not available");
      }
      filters.push_back({from, to, filter.submodels});
    }
  }
  ASSIGN_OR_RETURN(auto op, CreateDecisionForestOperator(std::move(filters)));
  return expr::MakeOpNode(
      op, std::vector(preprocessed_inputs.begin(), preprocessed_inputs.end()));
}

absl::StatusOr<QTypePtr>
ForestModel::InferTypeOfFirstForestInputAfterPreprocessing(
    absl::Span<const QTypePtr> input_qtypes) const {
  if (!first_forest_input_id_.has_value()) {
    return absl::FailedPreconditionError("forest has no inputs");
  }
  QTypePtr in_type = input_qtypes[*first_forest_input_id_];
  if (inputs_[*first_forest_input_id_].preprocessing != nullptr) {
    ASSIGN_OR_RETURN(auto lambda,
                     expr::LambdaOperator::Make(
                         inputs_[*first_forest_input_id_].preprocessing));
    ASSIGN_OR_RETURN(expr::ExprAttributes attr,
                     lambda->InferAttributes({expr::ExprAttributes(in_type)}));
    if (attr.qtype() == nullptr) {
      return absl::InternalError("can't infer preprocessed input type");
    }
    return attr.qtype();
  } else {
    return in_type;
  }
}

absl::StatusOr<QTypePtr> ForestModel::GetOutputQType(
    absl::Span<const QTypePtr> input_qtypes) const {
  // Note: size of input_qtypes is validated in
  // BasicExprOperator::InferAttributes.
  QTypePtr out_type = GetQType<float>();
  if (first_forest_input_id_.has_value()) {
    ASSIGN_OR_RETURN(
        QTypePtr in_type,
        InferTypeOfFirstForestInputAfterPreprocessing(input_qtypes));
    if (IsArrayLikeQType(in_type)) {
      ASSIGN_OR_RETURN(const ArrayLikeQType* array_qtype,
                       ToArrayLikeQType(in_type));
      ASSIGN_OR_RETURN(out_type,
                       array_qtype->WithValueQType(GetQType<float>()));
    }
  }
  ASSIGN_OR_RETURN(auto fake_res,
                   expr::CallOp("annotation.qtype", {expr::Leaf("fake_res"),
                                                     expr::Literal(out_type)}));
  ASSIGN_OR_RETURN(
      auto fake_res_tuple,
      expr::BindOp(
          "core.make_tuple",
          std::vector<expr::ExprNodePtr>(tree_filters_.size(), fake_res), {}));
  absl::flat_hash_map<std::string, expr::ExprNodePtr> expression_params;
  if (res_tuple_key_) {
    expression_params[*res_tuple_key_] = fake_res_tuple;
  }
  for (int i = 0; i < inputs_.size(); ++i) {
    ASSIGN_OR_RETURN(
        expression_params[inputs_[i].name],
        expr::CallOp("annotation.qtype", {expr::Leaf("fake_input"),
                                          expr::Literal(input_qtypes[i])}));
  }
  ASSIGN_OR_RETURN(auto expr, SubstitutePlaceholders(
                                  processed_expression_, expression_params,
                                  /*must_substitute_all=*/true));
  const auto result = expr->qtype();
  if (result == nullptr) {
    return absl::FailedPreconditionError("unable to deduce output qtype");
  }
  return result;
}

absl::StatusOr<expr::ExprNodePtr> ForestModel::CastAndValidateArgType(
    int input_id, expr::ExprNodePtr arg) const {
  const auto& required_qtypes = forest_->GetRequiredQTypes();
  auto required_qtype_iter = required_qtypes.find(input_id);
  if (required_qtype_iter == required_qtypes.end()) {
    // The input is not used by decision forest.
    return arg;
  }
  QTypePtr required_qtype = required_qtype_iter->second;
  QTypePtr required_scalar_qtype = DecayOptionalQType(required_qtype);
  ASSIGN_OR_RETURN(QTypePtr actual_scalar_qtype, GetScalarQType(arg->qtype()));

  if (required_scalar_qtype == GetQType<float>() &&
      actual_scalar_qtype != GetQType<float>() &&
      IsNumericScalarQType(actual_scalar_qtype)) {
    ASSIGN_OR_RETURN(arg,
                     expr::BindOp("core.to_float32", {std::move(arg)}, {}));
  } else if (required_scalar_qtype != actual_scalar_qtype) {
    return absl::InvalidArgumentError(
        absl::StrFormat("value type of input #%d (%s) doesn't match: "
                        "expected to be compatible with %s, got %s",
                        input_id, expr::GetDebugSnippet(arg),
                        required_qtype->name(), arg->qtype()->name()));
  }

  if (IsScalarQType(arg->qtype()) && IsOptionalQType(required_qtype)) {
    ASSIGN_OR_RETURN(arg,
                     expr::BindOp("core.to_optional", {std::move(arg)}, {}));
  }
  return arg;
}

absl::StatusOr<ForestModel::ExpressionAnalysisResult>
ForestModel::AnalyzeExpression() const {
  ExpressionAnalysisResult res;
  ASSIGN_OR_RETURN(auto expression, expr::ToLowest(expression_));
  for (const auto& node : expr::VisitorOrder(expression)) {
    if (node->is_op()) {
      ASSIGN_OR_RETURN(auto op, expr::DecayRegisteredOperator(node->op()));
      res.plain_sum = res.plain_sum && expr::IsBackendOperator(op, "math.add");
    } else if (node->is_placeholder() &&
               submodel_ids_.count(node->placeholder_key()) > 0) {
      res.submodel_nodes.push_back(node);
      const auto& submodels = submodel_ids_.at(node->placeholder_key());
      if (submodels.empty()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "submodel_ids[%s] is empty", node->placeholder_key()));
      }
      if (res.bag_count != 0 && res.bag_count != submodels.size()) {
        return absl::InvalidArgumentError(
            "all submodels should have the same number of bags");
      }
      res.bag_count = submodels.size();
    } else {
      res.plain_sum_nodes.push_back(node);
    }
  }
  res.bag_count = std::max(res.bag_count, 1);
  return res;
}

absl::Status ForestModel::HandlePlainSumExpression(
    const std::vector<expr::ExprNodePtr>& submodel_nodes,
    std::vector<expr::ExprNodePtr>&& plain_sum_nodes) {
  // Expression is a plain sum, so all used submodels can be combined
  // in order to achieve better performance. Division by bag_count is encoded
  // in tree.weight, so we don't need to divide after summation.
  // Note: linear expressions like 2*(P.x+P.y) - 3*P.z also can be combined
  // (it would require modification of tree.weight). Linear parts in
  // non-linear expressions can be collapsed as well. But currently
  // there is no need in further optimization.
  ASSIGN_OR_RETURN(
      processed_expression_,
      expr::CallOp("core.get_first", {expr::Placeholder(*res_tuple_key_)}));
  auto count_map = GetNodeCountMap(expression_);
  for (auto& node : plain_sum_nodes) {
    int count = count_map[node->fingerprint()];
    if (count > 1) {
      ASSIGN_OR_RETURN(node, expr::CallOp("math.multiply",
                                          {node, expr::Literal<float>(count)}));
    }
  }
  ASSIGN_OR_RETURN(processed_expression_,
                   AddAll(processed_expression_, plain_sum_nodes));
  TreeFilter used_trees;
  for (const auto& node : submodel_nodes) {
    int count = count_map[node->fingerprint()];
    for (int submodel_id : submodel_ids_[node->placeholder_key()]) {
      used_trees.submodels.insert(submodel_id);
      if (count > 1) submodel_weight_multipliers_[submodel_id] = count;
    }
  }
  tree_filters_.push_back(used_trees);
  return absl::OkStatus();
}

absl::Status ForestModel::HandleExpressionWithoutBags() {
  absl::flat_hash_map<std::string, expr::ExprNodePtr> params;
  for (const auto& [key, submodels] : submodel_ids_) {
    ASSIGN_OR_RETURN(
        params[key],
        expr::CallOp("core.get_nth",
                     {expr::Placeholder(*res_tuple_key_),
                      expr::Literal<int64_t>(tree_filters_.size())}));
    TreeFilter filter;
    filter.submodels.insert(submodels.begin(), submodels.end());
    tree_filters_.push_back(std::move(filter));
  }
  ASSIGN_OR_RETURN(processed_expression_,
                   SubstitutePlaceholders(expression_, params));
  return absl::OkStatus();
}

absl::StatusOr<expr::ExprNodePtr> ForestModel::UsedBagCountExpr() const {
  DCHECK_GT(bag_count_, 0);
  if (!oob_filters_.has_value()) {
    return expr::Literal<float>(bag_count_);
  }
  expr::ExprNodePtr used_bag_count = nullptr;
  for (int bag_id = 0; bag_id < bag_count_; ++bag_id) {
    ASSIGN_OR_RETURN(expr::ExprNodePtr used,
                     expr::CallOp("core.where", {(*oob_filters_)[bag_id],
                                                 expr::Literal<float>(1),
                                                 expr::Literal<float>(0)}));
    if (used_bag_count != nullptr) {
      ASSIGN_OR_RETURN(used_bag_count,
                       expr::CallOp("math.add", {used_bag_count, used}));
    } else {
      used_bag_count = used;
    }
  }
  // Use missing if no bags are used. Otherwise division by bag count would be
  // able to produce NaN.
  ASSIGN_OR_RETURN(
      used_bag_count,
      expr::CallOp(
          "core.where",
          {expr::CallOp("core.greater",
                        {used_bag_count, expr::Literal<float>(0)}),
           used_bag_count, expr::Literal<OptionalValue<float>>(std::nullopt)}));
  return used_bag_count;
}

absl::Status ForestModel::HandleExpressionWithBags() {
  // In case of non-trivial expression with bags, we need to push
  // the expression down and average bags on the top level.
  std::vector<expr::ExprNodePtr> bags(bag_count_);
  for (int bag_id = 0; bag_id < bag_count_; ++bag_id) {
    absl::flat_hash_map<std::string, expr::ExprNodePtr> params;
    for (const auto& [key, submodels] : submodel_ids_) {
      // `param` is a replacement for placeholder P(key) in the expression.
      expr::ExprNodePtr& param = params[key];
      ASSIGN_OR_RETURN(
          param, expr::CallOp("core.get_nth",
                              {expr::Placeholder(*res_tuple_key_),
                               expr::Literal<int64_t>(tree_filters_.size())}));
      TreeFilter filter;
      if (submodels.size() <= bag_id) {
        // Can never happen. Validated in AnalyzeExpression.
        return absl::InternalError("invalid submodel_ids");
      }
      filter.submodels.insert(submodels[bag_id]);
      tree_filters_.push_back(std::move(filter));
      // Division by bag_count is encoded in tree.weight. In this case we
      // manually divide after summation, so we have to modify tree weights.
      submodel_weight_multipliers_[submodels[bag_id]] = bag_count_;
    }
    ASSIGN_OR_RETURN(bags[bag_id], SubstitutePlaceholders(expression_, params));
    if (oob_filters_.has_value()) {
      // We evaluate all bags for all inputs, but ignore the result of a bag
      // if oob_filter for this bag returns nullopt.
      ASSIGN_OR_RETURN(
          bags[bag_id],
          expr::CallOp("core.where", {(*oob_filters_)[bag_id], bags[bag_id],
                                      expr::Literal<float>(0)}));
    }
  }
  ASSIGN_OR_RETURN(
      auto sum, AddAll(bags[0], absl::Span<expr::ExprNodePtr>(bags.data() + 1,
                                                              bag_count_ - 1)));
  ASSIGN_OR_RETURN(processed_expression_,
                   expr::CallOp("math.divide", {sum, UsedBagCountExpr()}));
  return absl::OkStatus();
}

absl::Status ForestModel::Initialize() {
  if (submodel_ids_.empty()) {
    // Corner case if forest is not used in the expression.
    res_tuple_key_ = std::nullopt;
    processed_expression_ = expression_;
    bag_count_ = 1;
    return absl::OkStatus();
  } else {
    // We use here the first key from submodel_ids_, because it is guaranteed
    // that there is no forest model input with the same name.
    res_tuple_key_ = submodel_ids_.begin()->first;
  }

  ASSIGN_OR_RETURN(auto info, AnalyzeExpression());
  is_plain_sum_ = info.plain_sum;
  bag_count_ = info.bag_count;
  if (oob_filters_.has_value() && oob_filters_->size() != bag_count_) {
    return absl::FailedPreconditionError(
        "if oob_filters is present, its size must be equal to bag count");
  }
  if (info.plain_sum && !oob_filters_) {
    RETURN_IF_ERROR(HandlePlainSumExpression(info.submodel_nodes,
                                             std::move(info.plain_sum_nodes)));
  } else if (bag_count_ == 1 && !oob_filters_) {
    RETURN_IF_ERROR(HandleExpressionWithoutBags());
  } else {
    RETURN_IF_ERROR(HandleExpressionWithBags());
  }
  if (truncation_step_.has_value()) {
    for (TreeFilter& filter : tree_filters_) {
      filter.step_range_to = *truncation_step_;
    }
  }
  for (const auto& [id, _] : forest_->GetRequiredQTypes()) {
    if (first_forest_input_id_.has_value()) {
      first_forest_input_id_ = std::min(*first_forest_input_id_, id);
    } else {
      first_forest_input_id_ = id;
    }
  }
  return absl::OkStatus();
}

namespace {

// Filters out trees with tag.step below step_range_from or above step_range_to
// of any filter.
std::vector<DecisionTree> GetMaybeUsedTrees(
    absl::Span<const DecisionTree> trees,
    absl::Span<const TreeFilter> tree_filters) {
  if (tree_filters.empty()) {
    return {};
  }
  TreeFilter combined_step_filter{
      .step_range_from = tree_filters.front().step_range_from,
      .step_range_to = tree_filters.front().step_range_to};
  for (int i = 1; i < tree_filters.size(); ++i) {
    combined_step_filter.step_range_from = std::min(
        combined_step_filter.step_range_from, tree_filters[i].step_range_from);
    if (tree_filters[i].step_range_to == -1 ||
        combined_step_filter.step_range_to == -1) {
      combined_step_filter.step_range_to = -1;
    } else {
      combined_step_filter.step_range_to = std::max(
          combined_step_filter.step_range_to, tree_filters[i].step_range_to);
    }
  }
  std::vector<DecisionTree> res;
  for (const DecisionTree& tree : trees) {
    if (combined_step_filter(tree.tag)) {
      res.push_back(tree);
    }
  }
  return res;
}

}  // namespace

absl::StatusOr<expr::ExprOperatorPtr> ForestModel::CreateDecisionForestOperator(
    std::vector<TreeFilter> tree_filters) const {
  DecisionForestPtr forest = forest_;
  auto required_types = forest->GetRequiredQTypes();
  if (!submodel_weight_multipliers_.empty()) {
    std::vector<DecisionTree> trees =
        GetMaybeUsedTrees(forest->GetTrees(), tree_filters);
    for (DecisionTree& tree : trees) {
      auto mult_iter = submodel_weight_multipliers_.find(tree.tag.submodel_id);
      if (mult_iter != submodel_weight_multipliers_.end()) {
        tree.weight *= mult_iter->second;
      }
    }
    ASSIGN_OR_RETURN(forest, DecisionForest::FromTrees(std::move(trees)));
  }
  return std::make_shared<DecisionForestOperator>(
      std::move(forest), std::move(tree_filters), required_types);
}

ForestModel::ForestModel(expr::ExprOperatorSignature&& signature,
                         Fingerprint&& fingerprint, ConstructorArgs&& args)
    : expr::BasicExprOperator("core.forest_model", signature,
                              "DecisionForest with pre- and post-processing",
                              fingerprint),
      forest_(std::move(args.forest)),
      submodel_ids_(std::move(args.submodel_ids)),
      oob_filters_(std::move(args.oob_filters)),
      truncation_step_(args.truncation_step),
      inputs_(std::move(args.inputs)),
      expression_(std::move(args.expression)) {}

absl::string_view ForestModel::py_qvalue_specialization_key() const {
  return kForestModelQValueSpecializationKey;
}

}  // namespace arolla
