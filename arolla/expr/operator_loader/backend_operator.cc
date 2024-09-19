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
#include "arolla/expr/operator_loader/backend_operator.h"

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/parameter_qtypes.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/expr/operator_loader/qtype_inference.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::GetPlaceholderKeys;

absl::StatusOr<ExprOperatorPtr> BackendOperator::Make(
    absl::string_view name, ExprOperatorSignature signature,
    absl::string_view doc, std::vector<QTypeConstraint> qtype_constraints,
    ExprNodePtr qtype_inference_expr) {
  RETURN_IF_ERROR(ValidateSignature(signature));
  // Check that all placeholder keys are present in the operator signature.
  absl::flat_hash_set<absl::string_view> parameter_names;
  for (const auto& param : signature.parameters) {
    parameter_names.insert(param.name);
  }
  std::set<std::string> undefined_parameter_names;
  for (const auto& qtype_constraint : qtype_constraints) {
    for (auto&& placeholder_key :
         GetPlaceholderKeys(qtype_constraint.predicate_expr)) {
      if (!parameter_names.contains(placeholder_key)) {
        undefined_parameter_names.insert(std::move(placeholder_key));
      }
    }
  }
  for (auto&& placeholder_key : GetPlaceholderKeys(qtype_inference_expr)) {
    if (!parameter_names.contains(placeholder_key)) {
      undefined_parameter_names.insert(std::move(placeholder_key));
    }
  }
  if (!undefined_parameter_names.empty()) {
    return absl::InvalidArgumentError(
        "unexpected parameters: P." +
        absl::StrJoin(undefined_parameter_names, ", P."));
  }

  // Compile expression
  ASSIGN_OR_RETURN(
      auto qtype_inference_fn,
      MakeQTypeInferenceFn(qtype_constraints, qtype_inference_expr));

  // Generate fingerprint.
  FingerprintHasher hasher("::arolla::operator_loader::BackendOperator");
  hasher.Combine(name, signature, doc, qtype_inference_expr->fingerprint(),
                 qtype_constraints.size());
  for (const auto& qtype_constraint : qtype_constraints) {
    hasher.Combine(qtype_constraint.predicate_expr->fingerprint(),
                   qtype_constraint.error_message);
  }
  return std::make_shared<BackendOperator>(
      PrivateConstructorTag{}, name, std::move(signature), doc,
      std::move(hasher).Finish(), std::move(qtype_constraints),
      std::move(qtype_inference_expr), std::move(qtype_inference_fn));
}

BackendOperator::BackendOperator(PrivateConstructorTag, absl::string_view name,
                                 ExprOperatorSignature signature,
                                 absl::string_view doc, Fingerprint fingerprint,
                                 std::vector<QTypeConstraint> qtype_constraints,
                                 ExprNodePtr qtype_inference_expr,
                                 QTypeInferenceFn qtype_inference_fn)
    : ExprOperatorWithFixedSignature(name, std::move(signature), doc,
                                     fingerprint),
      qtype_constraints_(std::move(qtype_constraints)),
      qtype_inference_expr_(std::move(qtype_inference_expr)),
      qtype_inference_fn_(std::move(qtype_inference_fn)) {}

absl::StatusOr<ExprAttributes> BackendOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  ASSIGN_OR_RETURN(auto parameter_qtypes,
                   ExtractParameterQTypes(signature(), inputs));
  ASSIGN_OR_RETURN(auto* output_qtype, qtype_inference_fn_(parameter_qtypes));
  return ExprAttributes(output_qtype);
}

absl::string_view BackendOperator::py_qvalue_specialization_key() const {
  return "::arolla::operator_loader::BackendOperator";
}

}  // namespace arolla::operator_loader
