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
#include "arolla/expr/operators/restricted_operator.h"

#include <memory>
#include <utility>

#include "absl//status/statusor.h"
#include "absl//types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperator;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::GetAttrQTypes;
using ::arolla::expr::HasAllAttrQTypes;
using ::arolla::expr::WithNewOperator;

class RestrictedOp final : public ExprOperator {
 public:
  RestrictedOp(ExprOperatorPtr wrapped_op, type_meta::Strategy restriction)
      : ExprOperator(wrapped_op->display_name(),
                     // NOTE: The fingerprint does not take `restriction` into
                     // account, so there will be a collision if an operator is
                     // wrapped with different restrictions.
                     // TODO: Fix it.
                     FingerprintHasher("::arolla::expr_operators::RestrictedOp")
                         .Combine(wrapped_op)
                         .Finish()),
        wrapped_op_(std::move(wrapped_op)),
        restriction_(std::move(restriction)) {}

  absl::StatusOr<ExprOperatorSignature> GetSignature() const final {
    return wrapped_op_->GetSignature();
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    if (!node->qtype()) {
      // If the resulting qtype is not ready, the wrapped_op_ may not be ready
      // for lowering.
      return node;
    }
    ASSIGN_OR_RETURN(auto unwrapped_node, WithNewOperator(node, wrapped_op_));
    return wrapped_op_->ToLowerLevel(unwrapped_node);
  }

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    // restriction_ strategy may not work correctly if not all the types are
    // available
    if (!HasAllAttrQTypes(inputs)) {
      return ExprAttributes{};
    }
    RETURN_IF_ERROR(restriction_(GetAttrQTypes(inputs)).status())
        << "in restriction for " << display_name() << " operator";
    return wrapped_op_->InferAttributes(inputs);
  }

 private:
  ExprOperatorPtr wrapped_op_;
  type_meta::Strategy restriction_;
};

}  // namespace

ExprOperatorPtr RestrictOperator(ExprOperatorPtr wrapped_op,
                                 type_meta::Strategy restriction) {
  return std::make_shared<RestrictedOp>(std::move(wrapped_op),
                                        std::move(restriction));
}

absl::StatusOr<ExprOperatorPtr> RestrictOperator(
    absl::StatusOr<ExprOperatorPtr> wrapped_op,
    absl::StatusOr<type_meta::Strategy> restriction) {
  RETURN_IF_ERROR(wrapped_op.status());
  RETURN_IF_ERROR(restriction.status());
  return RestrictOperator(*wrapped_op, *restriction);
}

}  // namespace arolla::expr_operators
