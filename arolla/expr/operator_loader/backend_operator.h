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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_BACKEND_OPERATOR_H_
#define AROLLA_EXPR_OPERATOR_LOADER_BACKEND_OPERATOR_H_

#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/expr/operator_loader/qtype_inference.h"
#include "arolla/util/fingerprint.h"

namespace arolla::operator_loader {

// A backend operator with qtype inference algorithm defined via an expression.
//
// Important properties:
//  * serializable
//  * the fingerprint of the operator instance depends on qtype inference
//    expression
//
class BackendOperator final : public expr::BackendExprOperatorTag,
                              public expr::ExprOperatorWithFixedSignature {
  struct PrivateConstructorTag {};

 public:
  // Returns a new backend operator instance.
  static absl::StatusOr<expr::ExprOperatorPtr> Make(
      absl::string_view name, expr::ExprOperatorSignature signature,
      absl::string_view doc, std::vector<QTypeConstraint> qtype_constraints,
      expr::ExprNodePtr qtype_inference_expr);

  BackendOperator(PrivateConstructorTag, absl::string_view name,
                  expr::ExprOperatorSignature signature, absl::string_view doc,
                  Fingerprint fingerprint,
                  std::vector<QTypeConstraint> qtype_constraints,
                  expr::ExprNodePtr qtype_inference_expr,
                  QTypeInferenceFn qtype_inference_fn);

  absl::StatusOr<expr::ExprAttributes> InferAttributes(
      absl::Span<const expr::ExprAttributes> inputs) const override;

  // Returns the qtype constraint definitions.
  const std::vector<QTypeConstraint>& qtype_constraints() const {
    return qtype_constraints_;
  }

  // Returns the qtype inference expression.
  const expr::ExprNodePtr& qtype_inference_expr() const {
    return qtype_inference_expr_;
  }

  absl::string_view py_qvalue_specialization_key() const override;

 private:
  std::vector<QTypeConstraint> qtype_constraints_;
  expr::ExprNodePtr qtype_inference_expr_;
  QTypeInferenceFn qtype_inference_fn_;
};

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_BACKEND_OPERATOR_H_
