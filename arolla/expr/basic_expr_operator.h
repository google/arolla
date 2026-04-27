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
#ifndef AROLLA_EXPR_BASIC_EXPR_OPERATOR_H_
#define AROLLA_EXPR_BASIC_EXPR_OPERATOR_H_

#include <string>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {

// Base class for a simple ExprOperator with fixed param-signature.
class ExprOperatorWithFixedSignature : public ExprOperator {
 public:
  // Returns the operator's signature.
  absl::StatusOr<ExprOperatorSignaturePtr absl_nonnull> GetSignature()
      const final;

  // Returns a copy of the stored doc-string.
  absl::StatusOr<std::string> GetDoc() const final;

  // Given operator inputs, return an expression representing this operator's
  // translation to a lower level.
  //
  // NOTE: Default implementation that only checks the number of dependencies.
  absl::StatusOr<ExprNodePtr absl_nonnull> ToLowerLevel(
      const ExprNodePtr absl_nonnull& node) const override;

  // Returns a reference to the stored signature.
  const ExprOperatorSignature& signature() const { return *signature_; }

  // Returns a reference to the stored doc-string.
  const std::string& doc() const { return doc_; }

 protected:
  ExprOperatorWithFixedSignature(
      absl::string_view name, ExprOperatorSignature signature,
      absl::string_view doc, Fingerprint fingerprint,
      ExprOperatorTags tags = ExprOperatorTags::kNone);

  // Validates the number of input dependencies for the operator node.
  // An incorrect number of dependencies indicates a broken expression DAG;
  // reports the error as a FailedPreconditionError.
  //
  // ValidateNodeDepsCount() is intended for use in the ToLowerLevel() method.
  absl::Status ValidateNodeDepsCount(const ExprNode& expr) const;

  // Validates the number of inputs provided to the operator.
  // An incompatible number of inputs is reported as an InvalidArgumentError;
  // this does not necessarily imply that the expression DAG is broken; e.g.,
  // OverloadedOperator handles this error by attempting the next candidate
  // operator.
  //
  // ValidateOpInputsCount() is intended for use in InferAttributes() and
  // GetOutputQType() methods.
  absl::Status ValidateOpInputsCount(
      absl::Span<const ExprAttributes> inputs) const;

 private:
  ExprOperatorSignaturePtr absl_nonnull signature_;
  std::string doc_;
};

// Base class for an operator with a GetOutputQType() strategy.
class BasicExprOperator : public ExprOperatorWithFixedSignature {
 public:
  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;

  // An extension point for InferAttributes().
  virtual absl::StatusOr<QTypePtr absl_nonnull> GetOutputQType(
      absl::Span<const QTypePtr absl_nonnull> input_qtypes) const = 0;

 protected:
  using ExprOperatorWithFixedSignature::ExprOperatorWithFixedSignature;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_BASIC_EXPR_OPERATOR_H_
