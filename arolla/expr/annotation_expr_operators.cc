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
#include "arolla/expr/annotation_expr_operators.h"

#include <cstdint>
#include <memory>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
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
namespace {

absl::Status ExpectLiteral(absl::string_view param_name,
                           const ExprAttributes& attr,
                           QTypePtr absl_nonnull expected_qtype) {
  if (attr.qtype() && attr.qtype() != expected_qtype) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a %s literal, got %s: %s", expected_qtype->name(), param_name,
        attr.qtype()->name()));
  }
  if (!attr.qvalue()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "`%s` must be a %s literal", param_name, expected_qtype->name()));
  }
  return absl::OkStatus();
}

}  // namespace

const ExprOperatorPtr absl_nonnull& QTypeAnnotation::Make() {
  static const absl::NoDestructor<ExprOperatorPtr> result(
      std::make_shared<QTypeAnnotation>(""));
  return *result;
}

QTypeAnnotation::QTypeAnnotation(absl::string_view aux_policy)
    : ExprOperatorWithFixedSignature(
          "annotation.qtype",
          ExprOperatorSignature(
              /* parameters=*/{{"expr"}, {"qtype"}},
              /* aux_policy=*/aux_policy),
          "QType annotation.",
          FingerprintHasher("::arolla::expr::QTypeAnnotation")
              .Combine(aux_policy)
              .Finish()) {}

absl::StatusOr<ExprAttributes> QTypeAnnotation::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (!inputs[1].qtype()) {
    return inputs[0];
  }
  RETURN_IF_ERROR(ExpectLiteral("qtype", inputs[1], GetQTypeQType()));
  const QTypePtr output_qtype = inputs[1].qvalue()->UnsafeAs<QTypePtr>();
  if (inputs[0].qtype() && inputs[0].qtype() != output_qtype) {
    return absl::InvalidArgumentError(
        absl::StrFormat("inconsistent annotation.qtype(expr: %s, qtype=%s)",
                        inputs[0].qtype()->name(), output_qtype->name()));
  }
  return ExprAttributes(output_qtype, inputs[0].qvalue());
}

const ExprOperatorPtr absl_nonnull& NameAnnotation::Make() {
  static const absl::NoDestructor<ExprOperatorPtr> result(
      std::make_shared<NameAnnotation>(""));
  return *result;
}

NameAnnotation::NameAnnotation(absl::string_view aux_policy)
    : ExprOperatorWithFixedSignature(
          "annotation.name",
          ExprOperatorSignature(
              /*parameters=*/{{"expr"}, {"name"}},
              /*aux_policy=*/aux_policy),
          "Name annotation.",
          FingerprintHasher("::arolla::expr::NameAnnotation")
              .Combine(aux_policy)
              .Finish()) {}

absl::StatusOr<ExprAttributes> NameAnnotation::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  RETURN_IF_ERROR(ExpectLiteral("name", inputs[1], GetQType<Text>()));
  return inputs[0];
}

const ExprOperatorPtr absl_nonnull& ExportAnnotation::Make() {
  static const absl::NoDestructor<ExprOperatorPtr> result(
      std::make_shared<ExportAnnotation>());
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
  RETURN_IF_ERROR(ExpectLiteral("export_tag", inputs[1], GetQType<Text>()));
  if (inputs[1].qvalue()->UnsafeAs<Text>().view().empty()) {
    return absl::InvalidArgumentError("`export_tag` must be non-empty");
  }
  return inputs[0];
}

const ExprOperatorPtr absl_nonnull& ExportValueAnnotation::Make() {
  static const absl::NoDestructor<ExprOperatorPtr> result(
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
  RETURN_IF_ERROR(ExpectLiteral("export_tag", inputs[1], GetQType<Text>()));
  if (inputs[1].qvalue()->UnsafeAs<Text>().view().empty()) {
    return absl::InvalidArgumentError("`export_tag` must be non-empty");
  }
  return inputs[0];
}

const ExprOperatorPtr absl_nonnull& SourceLocationAnnotation::Make() {
  static const absl::NoDestructor<ExprOperatorPtr> result(
      std::make_shared<SourceLocationAnnotation>());
  return *result;
}

SourceLocationAnnotation::SourceLocationAnnotation()
    : ExprOperatorWithFixedSignature(
          "annotation.source_location",
          ExprOperatorSignature{{"expr"},
                                {"function_name"},
                                {"file_name"},
                                {"line"},
                                {"column"},
                                {"line_text"}},
          "Annotation for source location where the expr node was created.\n"
          "\n"
          "The annotation is considered as \"best effort\" so any of the\n"
          "arguments may be missing.\n"
          "\n"
          "Args:\n"
          "  function_name: name of the function where the expr node was\n"
          "    created\n"
          "  file_name: name of the file where the expr node was created\n"
          "  line: line number where the expr node was created. 0 indicates\n"
          "    an unknown line number.\n"
          "  column: column number where the expr node was created. 0\n"
          "    indicates an unknown line number.\n"
          " line_text: text of the line where the expr node was created\n",
          FingerprintHasher("::arolla::expr::SourceLocationAnnotation")
              .Finish()) {}

absl::StatusOr<ExprAttributes> SourceLocationAnnotation::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  RETURN_IF_ERROR(ExpectLiteral("function_name", inputs[1], GetQType<Text>()));
  RETURN_IF_ERROR(ExpectLiteral("file_name", inputs[2], GetQType<Text>()));
  RETURN_IF_ERROR(ExpectLiteral("line", inputs[3], GetQType<int32_t>()));
  RETURN_IF_ERROR(ExpectLiteral("column", inputs[4], GetQType<int32_t>()));
  RETURN_IF_ERROR(ExpectLiteral("line_text", inputs[5], GetQType<Text>()));
  return inputs[0];
}

}  // namespace arolla::expr
