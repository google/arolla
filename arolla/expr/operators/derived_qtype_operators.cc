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
#include "arolla/expr/operators/derived_qtype_operators.h"

#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/derived_qtype_cast_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::BindOp;
using ::arolla::expr::DerivedQTypeDowncastOperator;
using ::arolla::expr::DerivedQTypeUpcastOperator;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::ExprOperatorWithFixedSignature;

class DerivedQTypeUpcastOp final : public ExprOperatorWithFixedSignature {
 public:
  DerivedQTypeUpcastOp()
      : ExprOperatorWithFixedSignature(
            "derived_qtype.upcast",
            ExprOperatorSignature{{"derived_qtype"}, {"value"}},
            "Upcasts the given value to the base type.",
            FingerprintHasher("::arolla::expr_operators::DerivedQTypeUpcastOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& derived_qtype = inputs[0];
    const auto& value_qtype = inputs[1];
    if (derived_qtype.qtype() && derived_qtype.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected derived_qtype: QTYPE, got %s",
                          derived_qtype.qtype()->name()));
    }
    if (derived_qtype.qtype() && !derived_qtype.qvalue()) {
      return absl::InvalidArgumentError("`derived_qtype` must be a literal");
    }
    if (!derived_qtype.qvalue() || !value_qtype.qtype()) {
      return ExprAttributes{};
    }
    ASSIGN_OR_RETURN(
        auto* output_qtype,
        DerivedQTypeUpcastOperator::GetOutputQType(
            derived_qtype.qvalue()->UnsafeAs<QTypePtr>(), value_qtype.qtype()));
    return ExprAttributes(output_qtype);
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (!node->qtype()) {
      return node;
    }
    ASSIGN_OR_RETURN(auto derived_qtype,
                     node->node_deps()[0]->qvalue()->As<QTypePtr>());
    return BindOp(std::make_shared<DerivedQTypeUpcastOperator>(derived_qtype),
                  {node->node_deps()[1]}, {});
  }
};

class DerivedQTypeDowncastOp final : public ExprOperatorWithFixedSignature {
 public:
  DerivedQTypeDowncastOp()
      : ExprOperatorWithFixedSignature(
            "derived_qtype.downcast",
            ExprOperatorSignature{{"derived_qtype"}, {"value"}},
            "Downcasts the given value to the derived type.",
            FingerprintHasher(
                "::arolla::expr_operators::DerivedQTypeDowncastOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& derived_qtype = inputs[0];
    const auto& value_qtype = inputs[1];
    if (derived_qtype.qtype() && derived_qtype.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected derived_qtype: QTYPE, got %s",
                          derived_qtype.qtype()->name()));
    }
    if (derived_qtype.qtype() && !derived_qtype.qvalue()) {
      return absl::InvalidArgumentError("`derived_qtype` must be a literal");
    }
    if (!derived_qtype.qvalue() || !value_qtype.qtype()) {
      return ExprAttributes{};
    }
    ASSIGN_OR_RETURN(
        auto* output_qtype,
        DerivedQTypeDowncastOperator::GetOutputQType(
            derived_qtype.qvalue()->UnsafeAs<QTypePtr>(), value_qtype.qtype()));
    return ExprAttributes(output_qtype);
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (!node->qtype()) {
      return node;
    }
    ASSIGN_OR_RETURN(auto derived_qtype,
                     node->node_deps()[0]->qvalue()->As<QTypePtr>());
    return BindOp(std::make_shared<DerivedQTypeDowncastOperator>(derived_qtype),
                  {node->node_deps()[1]}, {});
  }
};

}  // namespace

expr::ExprOperatorPtr MakeDerivedQTypeUpcastOp() {
  return std::make_shared<DerivedQTypeUpcastOp>();
}

expr::ExprOperatorPtr MakeDerivedQTypeDowncastOp() {
  return std::make_shared<DerivedQTypeDowncastOp>();
}

}  // namespace arolla::expr_operators
