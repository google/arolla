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
#include "arolla/decision_forest/expr_operator/decision_forest_operator.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/class_info.h"
#include "arolla/util/fingerprint.h"

namespace arolla {
namespace {

// Returns sorted unique ids of inputs used by `forest` together with
// `extra_input_ids`.
std::vector<int> GetRequiredInputIds(const DecisionForest& forest,
                                     std::vector<int> extra_input_ids) {
  std::vector<int> result = std::move(extra_input_ids);
  result.reserve(result.size() + forest.GetRequiredQTypes().size());
  for (const auto& [id, _] : forest.GetRequiredQTypes()) {
    result.push_back(id);
  }
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  DCHECK(result.empty() || result.front() >= 0);
  return result;
}

// Casting that should be applied to a decision forest input in order to
// make its qtype match the required one.
struct CastingMode {
  bool to_float32 = false;
  bool to_optional = false;
};

// Validates that `actual_qtype` is compatible with `required_qtype` and returns
// the conversions needed to make them match.
absl::StatusOr<CastingMode> GetCastingMode(int input_id,
                                           QTypePtr required_qtype,
                                           QTypePtr actual_qtype) {
  auto mismatch_error = [&] {
    return absl::InvalidArgumentError(absl::StrFormat(
        "value type of input #%d doesn't match: expected to be "
        "compatible with %s, got %s",
        input_id, required_qtype->name(), actual_qtype->name()));
  };
  absl::StatusOr<QTypePtr> actual_scalar_qtype = GetScalarQType(actual_qtype);
  if (!actual_scalar_qtype.ok()) {
    return mismatch_error();
  }
  QTypePtr required_scalar_qtype = DecayOptionalQType(required_qtype);

  CastingMode result;
  if (required_scalar_qtype == GetQType<float>() &&
      *actual_scalar_qtype != GetQType<float>() &&
      IsNumericScalarQType(*actual_scalar_qtype)) {
    result.to_float32 = true;
  } else if (required_scalar_qtype != *actual_scalar_qtype) {
    return mismatch_error();
  }
  result.to_optional =
      IsScalarQType(actual_qtype) && IsOptionalQType(required_qtype);
  return result;
}

}  // namespace

DecisionForestOperator::DecisionForestOperator(
    DecisionForestPtr forest, std::vector<TreeFilter> tree_filters)
    : DecisionForestOperator(GetRequiredInputIds(*forest, {}), forest,
                             std::move(tree_filters)) {}

DecisionForestOperator::DecisionForestOperator(
    DecisionForestPtr forest, std::vector<TreeFilter> tree_filters,
    std::vector<int> required_input_ids)
    : DecisionForestOperator(
          GetRequiredInputIds(*forest, std::move(required_input_ids)), forest,
          std::move(tree_filters)) {}

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
              .CombineSpan(required_input_ids)
              .Finish(),
          expr::ExprOperatorTags::kBuiltin,
          GetClassInfo<DecisionForestOperator>()),
      forest_(std::move(forest)),
      tree_filters_(std::move(tree_filters)),
      required_input_ids_(std::move(required_input_ids)) {}

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
  const auto& required_qtypes = forest_->GetRequiredQTypes();
  for (int id : required_input_ids_) {
    auto it = required_qtypes.find(id);
    if (it == required_qtypes.end()) {
      continue;  // The input is not used by `forest_`.
    }
    RETURN_IF_ERROR(
        GetCastingMode(id, it->second, input_qtypes[id]).status());
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

absl::StatusOr<expr::ExprNodePtr> DecisionForestOperator::ToLowerLevel(
    const expr::ExprNodePtr& node) const {
  RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
  std::vector<expr::ExprNodePtr> deps = node->node_deps();
  bool modified = false;
  const auto& required_qtypes = forest_->GetRequiredQTypes();
  for (int id : required_input_ids_) {
    if (id >= static_cast<int>(deps.size())) {
      break;  // Not enough arguments; reported by GetOutputQType.
    }
    auto it = required_qtypes.find(id);
    if (it == required_qtypes.end()) {
      continue;  // The input is not used by `forest_`.
    }
    expr::ExprNodePtr& dep = deps[id];
    if (dep->qtype() == nullptr) {
      continue;  // Conversion will be added when the qtype is known.
    }
    ASSIGN_OR_RETURN(CastingMode cast,
                     GetCastingMode(id, it->second, dep->qtype()));
    if (cast.to_float32) {
      ASSIGN_OR_RETURN(dep, expr::CallOp("core.to_float32", {dep}));
      modified = true;
    }
    if (cast.to_optional) {
      ASSIGN_OR_RETURN(dep, expr::CallOp("core.to_optional", {dep}));
      modified = true;
    }
  }
  if (!modified) {
    return node;
  }
  return expr::WithNewDependencies(node, std::move(deps));
}

}  // namespace arolla
