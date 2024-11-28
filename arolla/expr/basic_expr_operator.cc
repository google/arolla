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
#include "arolla/expr/basic_expr_operator.h"

#include <string>

#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_format.h"
#include "absl//types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

absl::StatusOr<ExprNodePtr> ExprOperatorWithFixedSignature::ToLowerLevel(
    const ExprNodePtr& node) const {
  RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
  return node;
}

absl::StatusOr<ExprOperatorSignature>
ExprOperatorWithFixedSignature::GetSignature() const {
  return signature_;
}

absl::StatusOr<std::string> ExprOperatorWithFixedSignature::GetDoc() const {
  return doc_;
}

absl::Status ExprOperatorWithFixedSignature::ValidateNodeDepsCount(
    const ExprNode& expr) const {
  return ValidateDepsCount(signature_, expr.node_deps().size(),
                           absl::StatusCode::kFailedPrecondition);
}

absl::Status ExprOperatorWithFixedSignature::ValidateOpInputsCount(
    absl::Span<const ExprAttributes> inputs) const {
  return ValidateDepsCount(signature_, inputs.size(),
                           absl::StatusCode::kInvalidArgument);
}

absl::StatusOr<ExprAttributes> BasicExprOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  // This method can be called if some input qtypes are nullptr, so we need to
  // check for that before falling back to GetOutputQType(input_qtypes), that
  // expects all inputs to be not null.
  if (!HasAllAttrQTypes(inputs)) {
    return ExprAttributes{};
  }
  ASSIGN_OR_RETURN(auto* output_qtype, GetOutputQType(GetAttrQTypes(inputs)));
  if (output_qtype == nullptr) {
    return absl::InternalError(absl::StrFormat(
        "GetOutputQType() for operator %s returned nullptr", display_name()));
  }
  return ExprAttributes(output_qtype);
}

}  // namespace arolla::expr
