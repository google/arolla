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
#ifndef AROLLA_DECISION_FOREST_EXPR_OPERATOR_DECISION_FOREST_OPERATOR_H_
#define AROLLA_DECISION_FOREST_EXPR_OPERATOR_DECISION_FOREST_OPERATOR_H_

#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/class_info.h"

namespace arolla {

inline constexpr absl::string_view
    kDecisionForestOperatorQValueSpecializationKey =
        "::arolla::DecisionForestOperator";

// Stateful operator computing a decision forest using the given tree filters.
//
// Inputs are validated against forest->GetRequiredQTypes(). Inputs with
// compatible, but not exactly matching qtypes (e.g. INT32 instead of
// OPTIONAL_FLOAT32) are converted in ToLowerLevel.
class DecisionForestOperator : public expr::BasicExprOperator {
 public:
  // `required_input_ids` are inputs used for the scalar/batched evaluation
  // dispatch. If not specified, it is deduced from forest->GetRequiredQTypes().
  // These inputs must either all be scalars (then DecisionForestOperator
  // returns a tuple of floats), or all be Arrays/DenseArrays (then the operator
  // returns a tuple of float DenseArrays).
  // Note1: we don't use all inputs for the scalar/batch detection because
  // ForestModel propagates all its inputs to DecisionForestOperator (otherwise
  // we would need to remap input ids in DecisionForest), and some of them are
  // not actually forest inputs.
  // Note2: we can't just always use forest->GetRequiredQTypes(), because
  // there is a corner case with a forest without inputs (i.e. a constant) where
  // we still need to distinguish scalar and batch cases.
  DecisionForestOperator(DecisionForestPtr forest,
                         std::vector<TreeFilter> tree_filters);
  DecisionForestOperator(DecisionForestPtr forest,
                         std::vector<TreeFilter> tree_filters,
                         std::vector<int> required_input_ids);

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final;

  // Inserts type conversions (core.to_float32, core.to_optional) for the
  // inputs which qtypes are known, but don't exactly match the required ones.
  absl::StatusOr<expr::ExprNodePtr> ToLowerLevel(
      const expr::ExprNodePtr& node) const final;

  DecisionForestPtr forest() const { return forest_; }
  const std::vector<TreeFilter>& tree_filters() const { return tree_filters_; }
  // Sorted list of required input ids (see the constructor comment).

  const std::vector<int>& required_input_ids() const {
    return required_input_ids_;
  }

  absl::string_view py_qvalue_specialization_key() const final {
    return kDecisionForestOperatorQValueSpecializationKey;
  }

 private:
  // `required_input_ids` must be sorted, unique, and include all inputs used
  // by `forest`.
  DecisionForestOperator(std::vector<int> required_input_ids,
                         DecisionForestPtr forest,
                         std::vector<TreeFilter> tree_filters);

  DecisionForestPtr forest_;
  std::vector<TreeFilter> tree_filters_;
  // Sorted list of required input ids.
  std::vector<int> required_input_ids_;

  AROLLA_DECLARE_SUBCLASS_INFO(DecisionForestOperator, ExprOperator);
};

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_EXPR_OPERATOR_DECISION_FOREST_OPERATOR_H_
