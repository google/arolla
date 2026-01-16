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
#ifndef AROLLA_EXPR_LAMBDA_EXPR_OPERATOR_H_
#define AROLLA_EXPR_LAMBDA_EXPR_OPERATOR_H_

#include <cstddef>
#include <memory>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

// Forward declaration.
class LambdaOperator;
using LambdaOperatorPtr = std::shared_ptr<const LambdaOperator>;

// Class for Lambda expr operators.
class LambdaOperator final : public ExprOperatorWithFixedSignature {
  struct PrivateConstrutorTag {};

 public:
  // Factory function for a lambda expr operator with a single parameter and
  // the default name.
  static absl::StatusOr<LambdaOperatorPtr absl_nonnull> Make(
      ExprNodePtr absl_nonnull lambda_body);

  // Factory function for a named lambda expr operator with a single parameter.
  static absl::StatusOr<LambdaOperatorPtr absl_nonnull> Make(
      absl::string_view operator_name, ExprNodePtr absl_nonnull lambda_body);

  // Factory function for a lambda expr operator with the default name.
  static absl::StatusOr<LambdaOperatorPtr absl_nonnull> Make(
      const ExprOperatorSignature& lambda_signature,
      ExprNodePtr absl_nonnull lambda_body);

  // Factory function for a lambda expr operator.
  static absl::StatusOr<LambdaOperatorPtr absl_nonnull> Make(
      absl::string_view operator_name,
      const ExprOperatorSignature& lambda_signature,
      ExprNodePtr absl_nonnull lambda_body);

  // Factory function for a lambda expr operator.
  static absl::StatusOr<LambdaOperatorPtr absl_nonnull> Make(
      absl::string_view operator_name,
      const ExprOperatorSignature& lambda_signature,
      ExprNodePtr absl_nonnull lambda_body, absl::string_view doc);

  // Private constructor.
  LambdaOperator(PrivateConstrutorTag, absl::string_view name,
                 const ExprOperatorSignature& signature,
                 PostOrder lambda_body_post_order, absl::string_view doc,
                 Fingerprint fingerprint);

  const ExprNodePtr absl_nonnull& lambda_body() const {
    return lambda_body_post_order_.nodes().back();
  }

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;

  absl::StatusOr<ExprNodePtr absl_nonnull> ToLowerLevel(
      const ExprNodePtr absl_nonnull& node) const final;

  absl::string_view py_qvalue_specialization_key() const final;

  ReprToken GenReprToken() const final;

 private:
  PostOrder lambda_body_post_order_;

  // Indices of the nodes in lambda_body_post_order that correspond to
  // the operator parameters.
  std::vector<size_t> lambda_param_indices_;
};

// Helper factory, which unwrap absl::StatusOr for any argument and transfer
// parameters to the Make function.
template <class... Args>
absl::StatusOr<LambdaOperatorPtr absl_nonnull> MakeLambdaOperator(
    Args&&... args) {
  RETURN_IF_ERROR(CheckInputStatus(args...));
  return LambdaOperator::Make(UnStatus(std::forward<Args>(args))...);
}

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_LAMBDA_EXPR_OPERATOR_H_
