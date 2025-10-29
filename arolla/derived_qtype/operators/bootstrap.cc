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
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/derived_qtype/labeled_qtype.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qexpr/operator_factory.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

class DerivedQTypeGetLabeledQTypeOp final
    : public BackendExprOperatorTag,
      public ExprOperatorWithFixedSignature {
 public:
  DerivedQTypeGetLabeledQTypeOp()
      : ExprOperatorWithFixedSignature(
            "derived_qtype.get_labeled_qtype",
            ExprOperatorSignature{{"qtype"}, {"label"}},
            "Returns a derived type with an embedded label.\n\n"
            "Note: If the label is empty, the decayed qtype is returned.\n"
            "The label should preferably be unique, at least between\n"
            "projects. Use e.g. 'project::module::type' to achieve this.",
            FingerprintHasher("::arolla::LabeledRelabelQTypeOp").Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> input_attrs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(input_attrs));
    const auto& qtype_attr = input_attrs[0];
    const auto& label_attr = input_attrs[1];
    if (qtype_attr.qtype() != nullptr &&
        qtype_attr.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected qtype: QTYPE, got %s", qtype_attr.qtype()->name()));
    }
    if (label_attr.qtype() != nullptr &&
        label_attr.qtype() != GetQType<Text>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a text scalar, got label: %s", label_attr.qtype()->name()));
    }
    if (!HasAllAttrQTypes(input_attrs)) {
      return ExprAttributes{};
    }
    if (!qtype_attr.qvalue() || !label_attr.qvalue()) {
      return ExprAttributes(GetQTypeQType());
    }
    auto result = GetLabeledQType(qtype_attr.qvalue()->UnsafeAs<QTypePtr>(),
                                  label_attr.qvalue()->UnsafeAs<Text>());
    return ExprAttributes(TypedValue::FromValue(std::move(result)));
  }
};

class DerivedQTypeGetQTypeLabelOp final
    : public BackendExprOperatorTag,
      public ExprOperatorWithFixedSignature {
 public:
  DerivedQTypeGetQTypeLabelOp()
      : ExprOperatorWithFixedSignature(
            "derived_qtype.get_qtype_label", ExprOperatorSignature{{"qtype"}},
            "Returns the qtype's label, or an empty string if none exists.",
            FingerprintHasher("::arolla::DerivedQTypeGetQTypeLabelOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> input_attrs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(input_attrs));
    const auto& qtype_attr = input_attrs[0];
    if (qtype_attr.qtype() != nullptr &&
        qtype_attr.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected qtype: QTYPE, got %s", qtype_attr.qtype()->name()));
    }
    if (!HasAllAttrQTypes(input_attrs)) {
      return ExprAttributes{};
    }
    if (!qtype_attr.qvalue()) {
      return ExprAttributes(GetQType<Text>());
    }
    auto result = GetQTypeLabel(qtype_attr.qvalue()->UnsafeAs<QTypePtr>());
    return ExprAttributes(TypedValue::FromValue(Text(result)));
  }
};

AROLLA_INITIALIZER(
        .name = "arolla_operators/derived_qtype:bootstrap",
        .reverse_deps =
            {
                arolla::initializer_dep::kOperators,
                arolla::initializer_dep::kQExprOperators,
            },
        .init_fn = []() -> absl::Status {
          {  // derived_qtype.get_labeled_qtype
            ASSIGN_OR_RETURN(auto qexpr_op,
                             QExprOperatorFromFunction(
                                 [](const QTypePtr& qtype, const Text& text) {
                                   return GetLabeledQType(qtype, text);
                                 }));
            RETURN_IF_ERROR(OperatorRegistry::GetInstance()->RegisterOperator(
                "derived_qtype.get_labeled_qtype", std::move(qexpr_op)));
            RETURN_IF_ERROR(RegisterOperator<DerivedQTypeGetLabeledQTypeOp>(
                                "derived_qtype.get_labeled_qtype")
                                .status());
          }
          {  // derived_qtype.get_qtype_label
            ASSIGN_OR_RETURN(
                auto qexpr_op,
                QExprOperatorFromFunction([](const QTypePtr& qtype) {
                  return Text(GetQTypeLabel(qtype));
                }));
            RETURN_IF_ERROR(OperatorRegistry::GetInstance()->RegisterOperator(
                "derived_qtype.get_qtype_label", std::move(qexpr_op)));
            RETURN_IF_ERROR(RegisterOperator<DerivedQTypeGetQTypeLabelOp>(
                                "derived_qtype.get_qtype_label")
                                .status());
          }
          return absl::OkStatus();
        })

}  // namespace
}  // namespace arolla::expr
