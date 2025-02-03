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
#ifndef AROLLA_DECISION_FOREST_EXPR_OPERATOR_DECISION_FOREST_OPERATOR_H_
#define AROLLA_DECISION_FOREST_EXPR_OPERATOR_DECISION_FOREST_OPERATOR_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// Stateful operator computing a decision forest using the given tree filters.
class DecisionForestOperator : public expr::BuiltinExprOperatorTag,
                               public expr::BasicExprOperator {
 public:
  DecisionForestOperator(DecisionForestPtr forest,
                         std::vector<TreeFilter> tree_filters);

  // Creates ForestOperator with potentially extended list of required
  // inputs. This is useful when Operator created on subset of trees, but
  // need to have the same limitations as original with respect to inputs.
  DecisionForestOperator(
      DecisionForestPtr forest, std::vector<TreeFilter> tree_filters,
      const absl::flat_hash_map<int, QTypePtr>& required_types);

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final;

  DecisionForestPtr forest() const { return forest_; }
  const std::vector<TreeFilter>& tree_filters() const { return tree_filters_; }

  absl::string_view py_qvalue_specialization_key() const final {
    return "::arolla::DecisionForestOperator";
  }

 private:
  DecisionForestOperator(std::vector<int> required_input_ids,
                         DecisionForestPtr forest,
                         std::vector<TreeFilter> tree_filters);

  DecisionForestPtr forest_;
  std::vector<TreeFilter> tree_filters_;
  // Sorted list of required input ids.
  std::vector<int> required_input_ids_;
};

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_EXPR_OPERATOR_DECISION_FOREST_OPERATOR_H_
