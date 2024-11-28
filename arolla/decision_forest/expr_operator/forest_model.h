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
#ifndef AROLLA_DECISION_FOREST_EXPR_OPERATOR_FOREST_MODEL_H_
#define AROLLA_DECISION_FOREST_EXPR_OPERATOR_FOREST_MODEL_H_

#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl//container/flat_hash_map.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

constexpr absl::string_view kForestModelQValueSpecializationKey =
    "::arolla::ForestModel";

class ForestModel;

// ForestModel is always used with std::shared_ptr to simplify integration with
// python (like any other ExprOperator; see ExprOperatorPtr). It is safe because
// it is an immutable object.
using ForestModelPtr = std::shared_ptr<const ForestModel>;

class ForestModel : public expr::BasicExprOperator {
 public:
  // submodel_ids['x'][3] refers to bag #3 of submodel 'x' (P.x) in the model
  // expression. Value means the submodel_id in DecisionTree::Tag. All submodels
  // should have the same number of bags.
  // Note: absl::flat_hash_map is not used here because the traversing order
  // should be stable. absl::btree_map can not be used because it is not
  // supported by CLIF.
  // Examples:
  //  1) expression=P.x, 3 bags
  //       submodel_ids = {'x' : [0, 1, 2]}
  //  2) expression=P.x+P.y, 2 bags
  //       submodel_ids = {'x' : [0, 1], 'y' : [2, 3]}
  using SubmodelIds = std::map<std::string, std::vector<int>>;

  struct Parameter {
    // Data to construct ExprOperatorSignature::Parameter.
    std::string name;

    // Expression with a single placeholder to preprocess the input.
    expr::ExprNodePtr preprocessing = nullptr;
  };

  struct ConstructorArgs {
    // Low level decision forest data.
    DecisionForestPtr forest;

    // Mapping from expression params to submodels in forest.
    // See the comment about SubmodelIds above.
    SubmodelIds submodel_ids;

    // Inputs specification. For each input it contains a name and
    // a preprocessing formula.
    std::vector<Parameter> inputs;

    // Postprocessing of the outputs of the low level decision forest. Each
    // placeholder key should either be present in submodel_ids or be the name
    // of one of the inputs. If placeholder key is the name of an input,
    // the input is used in expression without preprocessing.
    expr::ExprNodePtr expression;

    // Out of bag filters: either an empty vector or expressions
    // `inputs` -> OptionalValue<Unit> that enables (if the result of the
    // filter is present) or disables specific bags during evaluation.
    // The size of oob_filters (if present) must be equal to the number of
    // bags (i.e. size of each list in submodel_ids).
    std::optional<std::vector<expr::ExprNodePtr>> oob_filters;

    // If present then use only trees where
    // tag.step is in range [0, truncation_step).
    std::optional<size_t> truncation_step;
  };

  // Creates ForestModel
  static absl::StatusOr<ForestModelPtr> Create(ConstructorArgs args);

  // ToLowerLevel constructs an expression that creates forest evaluator and
  // attaches preprocessing and postprocessing to it:
  // lowered_expr: ApplyPostprocessing <- [forest evaluator] <- PreprocessInputs
  absl::StatusOr<expr::ExprNodePtr> ToLowerLevel(
      const expr::ExprNodePtr& node) const override;

  // Applies preprocessing and type conversions to decision forest inputs.
  // It takes inputs from node->node_deps() and returns a modified expression
  // for every input.
  absl::StatusOr<std::vector<expr::ExprNodePtr>> PreprocessInputs(
      const expr::ExprNodePtr& node) const;

  // Applies postprocessing `expression`.
  // `raw_result` is direct output of decision forest evaluator; it is a tuple
  // of either floats or float arrays. `raw_result` can be nullptr if forest
  // is not used in the expression.
  // `node` is the same as in `ToLowerLevel`: this ForestModel with attached
  // inputs. Needed because some inputs can be used in `expression` directly.
  absl::StatusOr<expr::ExprNodePtr> ApplyPostprocessing(
      const expr::ExprNodePtr& node, const expr::ExprNodePtr& raw_result) const;

  // Creates partial (i.e. only some step ranges) evaluator for the decision
  // forest. Applies neither preprocessing nor postprocessing.
  // The produced expression returns a tuple concatenated from `raw_result`
  // tuples (see ApplyPostprocessing comment above) for each requested step
  // range.
  // For exampe if step_ranges.size() == 1 then the output of the evaluator can
  // be passed to ApplyPostprocessing as `raw_result`.
  // If step_ranges.size() == 2 then the output is
  // tuple(A1, ..., An, B1, ..., Bn) and `raw_result` is
  // tuple(A1, ..., An) for the first step range and tuple(B1, ..., Bn) for
  // the second range.
  absl::StatusOr<expr::ExprNodePtr> CreatePartialEvaluator(
      absl::Span<const std::pair<int, int>> step_ranges,
      absl::Span<const expr::ExprNodePtr> preprocessed_inputs) const;

  // See comments to ForestModel::ConstructorArgs.
  DecisionForestPtr forest() const { return forest_; }
  const SubmodelIds& submodel_ids() const { return submodel_ids_; }
  const std::optional<std::vector<expr::ExprNodePtr>>& oob_filters() const {
    return oob_filters_;
  }
  size_t bag_count() const { return bag_count_; }
  std::optional<size_t> truncation_step() const { return truncation_step_; }
  const std::vector<Parameter>& inputs() const { return inputs_; }
  expr::ExprNodePtr expression() const { return expression_; }

  absl::string_view py_qvalue_specialization_key() const override;

 private:
  ForestModel(expr::ExprOperatorSignature&& signature,
              Fingerprint&& fingerprint, ConstructorArgs&& args);

  absl::Status Initialize();

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override;

  struct ExpressionAnalysisResult {
    bool plain_sum = true;
    int bag_count = 0;
    std::vector<expr::ExprNodePtr> submodel_nodes;
    std::vector<expr::ExprNodePtr> plain_sum_nodes;
  };
  absl::StatusOr<ExpressionAnalysisResult> AnalyzeExpression() const;

  absl::Status HandlePlainSumExpression(
      const std::vector<expr::ExprNodePtr>& submodel_nodes,
      std::vector<expr::ExprNodePtr>&& plain_sum_nodes);
  absl::Status HandleExpressionWithoutBags();
  absl::Status HandleExpressionWithBags();

  // Used by `HandleExpressionWithBags`. Returns an expression that calculates
  // the number of non-filtered-out bags. If there are no oob_filters it is
  // Literal(bag_count_).
  absl::StatusOr<expr::ExprNodePtr> UsedBagCountExpr() const;

  absl::StatusOr<expr::ExprOperatorPtr> CreateDecisionForestOperator(
      std::vector<TreeFilter> tree_filters) const;

  absl::StatusOr<expr::ExprNodePtr> ApplyEvaluator(
      expr::ExprNodePtr forest_evaluator,
      absl::Span<const expr::ExprNodePtr> args) const;

  // Validates that the qtype of arg is compatible with
  // forest_->GetRequiredQTypes() and converts if necessary.
  absl::StatusOr<expr::ExprNodePtr> CastAndValidateArgType(
      int input_id, expr::ExprNodePtr arg) const;

  absl::StatusOr<QTypePtr> InferTypeOfFirstForestInputAfterPreprocessing(
      absl::Span<const QTypePtr> input_qtypes) const;

  // Model data
  DecisionForestPtr forest_;
  SubmodelIds submodel_ids_;
  std::optional<std::vector<expr::ExprNodePtr>> oob_filters_;
  std::optional<size_t> truncation_step_;
  std::vector<Parameter> inputs_;
  expr::ExprNodePtr expression_;

  // Calculated from model data by ForestModel::Initialize

  // Key of the placeholder in processed_expression_, that should be replaced
  // with the result of the decision forest.
  std::optional<std::string> res_tuple_key_;
  std::vector<TreeFilter> tree_filters_;
  // expression_ with optimizations and bag-related preprocessing.
  expr::ExprNodePtr processed_expression_;
  // True if expression is a plain sum of submodels.
  bool is_plain_sum_ = false;
  absl::flat_hash_map<int, float> submodel_weight_multipliers_;
  size_t bag_count_;
  // first_forest_input_id_ = min(id for id, _ in forest_.GetRequiredQTypes())
  std::optional<int> first_forest_input_id_;
};

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_EXPR_OPERATOR_FOREST_MODEL_H_
