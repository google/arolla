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
#include "arolla/expr/annotation_expr_operators.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

ExprOperatorPtr QTypeAnnotation::Make() {
  static const absl::NoDestructor<ExprOperatorPtr> result(
      std::make_shared<QTypeAnnotation>(""));
  return *result;
}

QTypeAnnotation::QTypeAnnotation(std::string aux_policy)
    : ExprOperatorWithFixedSignature(
          "annotation.qtype",
          ExprOperatorSignature(
              /* parameters=*/{{"expr"}, {"qtype"}},
              /* aux_policy=*/std::move(aux_policy)),
          "QType annotation.",
          FingerprintHasher("::arolla::expr::QTypeAnnotation").Finish()) {}

absl::StatusOr<ExprAttributes> QTypeAnnotation::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (!inputs[1].qtype()) {
    return inputs[0];
  }
  if (inputs[1].qtype() != GetQTypeQType()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected QTYPE, got qtype: %s", inputs[1].qtype()->name()));
  }
  if (!inputs[1].qvalue()) {
    return absl::InvalidArgumentError("`qtype` must be a literal");
  }
  const QTypePtr output_qtype = inputs[1].qvalue()->UnsafeAs<QTypePtr>();
  if (inputs[0].qtype() && inputs[0].qtype() != output_qtype) {
    return absl::InvalidArgumentError(
        absl::StrFormat("inconsistent annotation.qtype(expr: %s, qtype=%s)",
                        inputs[0].qtype()->name(), output_qtype->name()));
  }
  return ExprAttributes(output_qtype, inputs[0].qvalue());
}

ExprOperatorPtr NameAnnotation::Make() {
  static const absl::NoDestructor result(std::make_shared<NameAnnotation>(""));
  return *result;
}

NameAnnotation::NameAnnotation(std::string aux_policy)
    : ExprOperatorWithFixedSignature(
          "annotation.name",
          ExprOperatorSignature(
              /*parameters=*/{{"expr"}, {"name"}},
              /*aux_policy=*/std::move(aux_policy)),
          "Name annotation.",
          FingerprintHasher("::arolla::expr::NameAnnotation").Finish()) {}

absl::StatusOr<ExprAttributes> NameAnnotation::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (inputs[1].qtype() && inputs[1].qtype() != GetQType<Text>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a TEXT literal, got name: %s", inputs[1].qtype()->name()));
  }
  if (!inputs[1].qvalue()) {
    return absl::InvalidArgumentError("`name` must be a TEXT literal");
  }
  return inputs[0];
}

ExprOperatorPtr ExportAnnotation::Make() {
  static const absl::NoDestructor result(std::make_shared<ExportAnnotation>());
  return *result;
}

ExportAnnotation::ExportAnnotation()
    : ExprOperatorWithFixedSignature(
          "annotation.export", ExprOperatorSignature{{"expr"}, {"export_tag"}},
          "Side-channel output annotation.",
          FingerprintHasher("::arolla::expr::ExportAnnotation").Finish()) {}

absl::StatusOr<ExprAttributes> ExportAnnotation::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (inputs[1].qtype() && inputs[1].qtype() != GetQType<Text>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected TEXT, got export_tag: %s", inputs[1].qtype()->name()));
  }
  if (!inputs[1].qvalue()) {
    return absl::InvalidArgumentError("`export_tag` must be a TEXT literal");
  }
  if (inputs[1].qvalue()->UnsafeAs<Text>().view().empty()) {
    return absl::InvalidArgumentError("`export_tag` must be non-empty");
  }
  return inputs[0];
}

ExprOperatorPtr ExportValueAnnotation::Make() {
  static const absl::NoDestructor result(
      std::make_shared<ExportValueAnnotation>());
  return *result;
}

ExportValueAnnotation::ExportValueAnnotation()
    : ExprOperatorWithFixedSignature(
          "annotation.export_value",
          ExprOperatorSignature{{"expr"}, {"export_tag"}, {"value"}},
          "Side-channel output annotation.",
          FingerprintHasher("::arolla::expr::ExportValueAnnotation").Finish()) {
}

absl::StatusOr<ExprAttributes> ExportValueAnnotation::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (inputs[1].qtype() && inputs[1].qtype() != GetQType<Text>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected TEXT, got export_tag: %s", inputs[1].qtype()->name()));
  }
  if (!inputs[1].qvalue()) {
    return absl::InvalidArgumentError("`export_tag` must be a TEXT literal");
  }
  if (inputs[1].qvalue()->UnsafeAs<Text>().view().empty()) {
    return absl::InvalidArgumentError("`export_tag` must be non-empty");
  }
  return inputs[0];
}

}  // namespace arolla::expr
