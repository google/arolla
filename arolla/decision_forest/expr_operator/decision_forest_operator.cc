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
#include "arolla/decision_forest/expr_operator/decision_forest_operator.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace {

std::vector<int> GetRequiredInputIds(
    const absl::flat_hash_map<int, QTypePtr>& required_types) {
  std::vector<int> result;
  result.reserve(required_types.size());
  for (const auto& [id, _] : required_types) {
    result.push_back(id);
  }
  return result;
}

}  // namespace

DecisionForestOperator::DecisionForestOperator(
    DecisionForestPtr forest, std::vector<TreeFilter> tree_filters)
    : DecisionForestOperator(GetRequiredInputIds(forest->GetRequiredQTypes()),
                             forest, std::move(tree_filters)) {}

DecisionForestOperator::DecisionForestOperator(
    DecisionForestPtr forest, std::vector<TreeFilter> tree_filters,
    const absl::flat_hash_map<int, QTypePtr>& required_types)
    : DecisionForestOperator(GetRequiredInputIds(required_types),
                             std::move(forest), std::move(tree_filters)) {}

DecisionForestOperator::DecisionForestOperator(
    std::vector<int> required_input_ids, DecisionForestPtr forest,
    std::vector<TreeFilter> tree_filters)
    : BasicExprOperator(
          "anonymous.decision_forest_operator",
          expr::ExprOperatorSignature::MakeVariadicArgs(),
          "Evaluates decision forest stored in the operator state.",
          FingerprintHasher("::arolla::DecisionForestOperator")
              .Combine(forest->fingerprint())
              .CombineSpan(tree_filters)
              .Finish()),
      forest_(std::move(forest)),
      tree_filters_(std::move(tree_filters)),
      required_input_ids_(std::move(required_input_ids)) {
  std::sort(required_input_ids_.begin(), required_input_ids_.end());
}

absl::StatusOr<QTypePtr> DecisionForestOperator::GetOutputQType(
    absl::Span<const QTypePtr> input_qtypes) const {
  int last_forest_input_id =
      required_input_ids_.empty() ? -1 : required_input_ids_.back();
  if (last_forest_input_id >= static_cast<int>(input_qtypes.size())) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "not enough arguments for the decision forest: expected at least %d, "
        "got %d",
        last_forest_input_id + 1, input_qtypes.size()));
  }
  bool batched = !input_qtypes.empty() && !required_input_ids_.empty() &&
                 IsArrayLikeQType(input_qtypes[required_input_ids_[0]]);
  for (int id : required_input_ids_) {
    if (IsArrayLikeQType(input_qtypes[id]) != batched) {
      DCHECK(!required_input_ids_.empty());
      return absl::InvalidArgumentError(absl::StrFormat(
          "either all forest inputs must be scalars or all forest inputs "
          "must be arrays, but arg[%d] is %s and arg[%d] is %s",
          required_input_ids_[0], input_qtypes[required_input_ids_[0]]->name(),
          id, input_qtypes[id]->name()));
    }
  }

  QTypePtr output_type;
  if (batched) {
    DCHECK(!required_input_ids_.empty());
    ASSIGN_OR_RETURN(const ArrayLikeQType* array_type,
                     ToArrayLikeQType(input_qtypes[required_input_ids_[0]]));
    ASSIGN_OR_RETURN(output_type,
                     array_type->WithValueQType(GetQType<float>()));
  } else {
    output_type = GetQType<float>();
  }

  return MakeTupleQType(
      std::vector<QTypePtr>(tree_filters_.size(), output_type));
}

}  // namespace arolla
