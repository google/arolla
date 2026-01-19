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
#include "arolla/expr/basic_expr_operator.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

ExprOperatorWithFixedSignature::ExprOperatorWithFixedSignature(
    absl::string_view name, ExprOperatorSignature signature,
    absl::string_view doc, Fingerprint fingerprint)
    : ExprOperator(name, fingerprint),
      signature_(
          std::make_shared<const ExprOperatorSignature>(std::move(signature))),
      doc_(doc) {}

absl::StatusOr<ExprOperatorSignaturePtr absl_nonnull>
ExprOperatorWithFixedSignature::GetSignature() const {
  return signature_;
}

absl::StatusOr<std::string> ExprOperatorWithFixedSignature::GetDoc() const {
  return doc_;
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

UnnamedExprOperator::UnnamedExprOperator(ExprOperatorSignature signature,
                                         Fingerprint fingerprint)
    : BasicExprOperator("unnamed_operator", std::move(signature), "",
                        fingerprint) {}

}  // namespace arolla::expr
