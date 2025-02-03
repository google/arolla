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
#include "arolla/expr/operator_loader/restricted_lambda_operator.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operator_loader/parameter_qtypes.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::GetPlaceholderKeys;
using ::arolla::expr::LambdaOperator;
using ::arolla::expr::ValidateDepsCount;

absl::StatusOr<ExprOperatorPtr> RestrictedLambdaOperator::Make(
    std::shared_ptr<const LambdaOperator> base_lambda_operator,
    std::vector<QTypeConstraint> qtype_constraints) {
  // Collect parameters used by qtype constraints.
  absl::flat_hash_set<std::string> qtype_constraint_parameters;
  for (const auto& qtype_constraint : qtype_constraints) {
    const auto& placeholder_keys =
        GetPlaceholderKeys(qtype_constraint.predicate_expr);
    qtype_constraint_parameters.insert(placeholder_keys.begin(),
                                       placeholder_keys.end());
  }
  // Check that all parameters keys are present in the operator signature.
  auto undefined_parameters = qtype_constraint_parameters;
  for (const auto& param : base_lambda_operator->signature().parameters) {
    undefined_parameters.erase(param.name);
  }
  if (!undefined_parameters.empty()) {
    std::vector<std::string> undefined_parameters_sorted(
        undefined_parameters.begin(), undefined_parameters.end());
    std::sort(undefined_parameters_sorted.begin(),
              undefined_parameters_sorted.end());
    return absl::InvalidArgumentError(
        "unexpected parameters: P." +
        absl::StrJoin(undefined_parameters_sorted, ", P."));
  }

  // Compile qtype constraints
  ASSIGN_OR_RETURN(auto qtype_constraint_fn,
                   MakeQTypeConstraintFn(qtype_constraints));

  // Generate fingerprint.
  FingerprintHasher hasher(
      "::arolla::operator_loader::RestrictedLambdaOperator");
  hasher.Combine(base_lambda_operator->fingerprint(), qtype_constraints.size());
  for (const auto& qtype_constraint : qtype_constraints) {
    hasher.Combine(qtype_constraint.predicate_expr->fingerprint(),
                   qtype_constraint.error_message);
  }
  return std::make_shared<RestrictedLambdaOperator>(
      PrivateConstructorTag{}, std::move(base_lambda_operator),
      std::move(hasher).Finish(), std::move(qtype_constraint_fn),
      std::move(qtype_constraints));
}

RestrictedLambdaOperator::RestrictedLambdaOperator(
    PrivateConstructorTag,
    std::shared_ptr<const LambdaOperator> base_lambda_operator,
    Fingerprint fingerprint, QTypeConstraintFn qtype_constraint_fn,
    std::vector<QTypeConstraint> qtype_constraints)
    : ExprOperator(base_lambda_operator->display_name(), fingerprint),
      base_lambda_operator_(std::move(base_lambda_operator)),
      qtype_constraint_fn_(std::move(qtype_constraint_fn)),
      qtype_constraints_(std::move(qtype_constraints)) {}

absl::StatusOr<ExprAttributes> RestrictedLambdaOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateDepsCount(signature(), inputs.size(),
                                    absl::StatusCode::kInvalidArgument));
  ASSIGN_OR_RETURN(auto parameter_qtypes,
                   ExtractParameterQTypes(signature(), inputs));
  // Check the constraints.
  ASSIGN_OR_RETURN(bool args_present, qtype_constraint_fn_(parameter_qtypes));
  if (!args_present) {
    // If a required parameter is missing, returns empty attributes.
    return ExprAttributes{};
  }
  return base_lambda_operator_->InferAttributes(inputs);
}

absl::StatusOr<ExprNodePtr> RestrictedLambdaOperator::ToLowerLevel(
    const ExprNodePtr& node) const {
  if (!node->qtype()) {
    return node;
  }
  return base_lambda_operator_->ToLowerLevel(node);
}

absl::string_view RestrictedLambdaOperator::py_qvalue_specialization_key()
    const {
  return "::arolla::operator_loader::RestrictedLambdaOperator";
}

}  // namespace arolla::operator_loader
