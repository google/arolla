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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_RESTRICTED_LAMBDA_OPERATOR_H_
#define AROLLA_EXPR_OPERATOR_LOADER_RESTRICTED_LAMBDA_OPERATOR_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/util/fingerprint.h"

namespace arolla::operator_loader {

// A restricted lambda operator.
class RestrictedLambdaOperator final : public expr::ExprOperator {
  struct PrivateConstructorTag {};

 public:
  // Factory function for a restricted lambda operator.
  static absl::StatusOr<expr::ExprOperatorPtr> Make(
      std::shared_ptr<const expr::LambdaOperator> base_lambda_operator,
      std::vector<QTypeConstraint> qtype_constraints);

  // Private constructor.
  RestrictedLambdaOperator(
      PrivateConstructorTag,
      std::shared_ptr<const expr::LambdaOperator> base_lambda_operator,
      Fingerprint fingerprint, QTypeConstraintFn qtype_constraint_fn,
      std::vector<QTypeConstraint> qtype_constraints);

  // Returns a copy of the stored signature.
  absl::StatusOr<expr::ExprOperatorSignature> GetSignature() const final {
    return base_lambda_operator_->GetSignature();
  }

  // Returns a copy of the stored doc-string.
  absl::StatusOr<std::string> GetDoc() const final {
    return base_lambda_operator_->GetDoc();
  }

  // Returns a reference to the stored signature.
  const expr::ExprOperatorSignature& signature() const {
    return base_lambda_operator_->signature();
  }

  // Returns a reference to the stored doc-string.
  const std::string doc() const { return base_lambda_operator_->doc(); }

  // Returns the qtype constraint definitions.
  const std::vector<QTypeConstraint>& qtype_constraints() const {
    return qtype_constraints_;
  }

  // Returns the lambda body expression
  const std::shared_ptr<const expr::LambdaOperator>& base_lambda_operator()
      const {
    return base_lambda_operator_;
  }

  absl::StatusOr<expr::ExprAttributes> InferAttributes(
      absl::Span<const expr::ExprAttributes> inputs) const final;

  absl::StatusOr<expr::ExprNodePtr> ToLowerLevel(
      const expr::ExprNodePtr& node) const final;

  absl::string_view py_qvalue_specialization_key() const override;

 private:
  std::shared_ptr<const expr::LambdaOperator> base_lambda_operator_;
  QTypeConstraintFn qtype_constraint_fn_;
  std::vector<QTypeConstraint> qtype_constraints_;
};

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_RESTRICTED_LAMBDA_OPERATOR_H_
