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
#include "arolla/expr/derived_qtype_cast_operator.h"

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

absl::StatusOr<QTypePtr absl_nonnull>
DerivedQTypeUpcastOperator::GetOutputQType(QTypePtr absl_nonnull derived_qtype,
                                           QTypePtr absl_nonnull value_qtype) {
  if (value_qtype == derived_qtype) {
    return DecayDerivedQType(derived_qtype);
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("expected %s, got value: %s", derived_qtype->name(),
                      value_qtype->name()));
}

DerivedQTypeUpcastOperator::DerivedQTypeUpcastOperator(
    QTypePtr absl_nonnull derived_qtype)
    : ExprOperatorWithFixedSignature(
          absl::StrFormat("derived_qtype.upcast[%s]", derived_qtype->name()),
          ExprOperatorSignature{{"value"}},
          "Casts a derived value to the base type.",
          FingerprintHasher("arolla::expr::DerivedQTypeUpcastOperator")
              .Combine(derived_qtype)
              .Finish(),
          ExprOperatorTags::kBuiltin),
      derived_qtype_(derived_qtype) {}

absl::StatusOr<ExprAttributes> DerivedQTypeUpcastOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (inputs[0].qtype() == nullptr) {
    return ExprAttributes{};
  }
  ASSIGN_OR_RETURN(auto output_qtype,
                   DerivedQTypeUpcastOperator::GetOutputQType(
                       derived_qtype_, inputs[0].qtype()));
  return ExprAttributes(output_qtype);
}

QTypePtr absl_nonnull DerivedQTypeUpcastOperator::derived_qtype() const {
  return derived_qtype_;
}

absl::StatusOr<QTypePtr absl_nonnull>
DerivedQTypeDowncastOperator::GetOutputQType(  // clang-format hint
    QTypePtr absl_nonnull derived_qtype, QTypePtr absl_nonnull value_qtype) {
  const auto* base_qtype = DecayDerivedQType(derived_qtype);
  if (value_qtype == base_qtype) {
    return derived_qtype;
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "expected %s, got value: %s", base_qtype->name(), value_qtype->name()));
}

DerivedQTypeDowncastOperator::DerivedQTypeDowncastOperator(
    QTypePtr absl_nonnull derived_qtype)
    : ExprOperatorWithFixedSignature(
          absl::StrFormat("derived_qtype.downcast[%s]", derived_qtype->name()),
          ExprOperatorSignature{{"value"}},
          "Casts a base qtype value to the derived qtype.",
          FingerprintHasher("arolla::expr::DerivedQTypeDowncastOperator")
              .Combine(derived_qtype)
              .Finish(),
          ExprOperatorTags::kBuiltin),
      derived_qtype_(derived_qtype) {}

absl::StatusOr<ExprAttributes> DerivedQTypeDowncastOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (inputs[0].qtype() == nullptr) {
    return ExprAttributes{};
  }
  ASSIGN_OR_RETURN(auto output_qtype,
                   DerivedQTypeDowncastOperator::GetOutputQType(
                       derived_qtype_, inputs[0].qtype()));
  return ExprAttributes(output_qtype);
}

QTypePtr absl_nonnull DerivedQTypeDowncastOperator::derived_qtype() const {
  return derived_qtype_;
}

}  // namespace arolla::expr
