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
#ifndef AROLLA_EXPR_BASIC_EXPR_OPERATOR_H_
#define AROLLA_EXPR_BASIC_EXPR_OPERATOR_H_

#include <string>
#include <utility>

#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {

class ExprNode;

// Base class for a simple ExprOperator with fixed param-signature.
class ExprOperatorWithFixedSignature : public ExprOperator {
 public:
  // Returns a copy of the stored signature.
  absl::StatusOr<ExprOperatorSignature> GetSignature() const final;

  // Returns a copy of the stored doc-string.
  absl::StatusOr<std::string> GetDoc() const final;

  // Returns a reference to the stored signature.
  const ExprOperatorSignature& signature() const { return signature_; }

  // Returns a reference to the stored doc-string.
  const std::string& doc() const { return doc_; }

 protected:
  ExprOperatorWithFixedSignature(absl::string_view name,
                                 ExprOperatorSignature signature,
                                 absl::string_view doc, Fingerprint fingerprint)
      : ExprOperator(name, fingerprint),
        signature_(std::move(signature)),
        doc_(doc) {}

  // Validates the number of dependencies passed to the operator against the
  // operator signature.
  //
  // ValidateNodeDepsCount() is intended for ToLowerLevel() method.
  //
  // A wrong number of dependencies indicates that the expression dag is broken.
  // Reports the dag error as a FailedPrecondition.
  absl::Status ValidateNodeDepsCount(const ExprNode& expr) const;

  // Validates the number of inputs of an operator.
  //
  // ValidateOpInputsCount() is intended for GetOutputQType() or
  // ComputeOutputQType() methods.
  //
  // An incompatible number of inputs gets reported as InvalidArgumentError.
  // It does not automatically imply that the expression dag is broken,
  // e.g. OverloadedOperator handles such an error by trying the next base
  // operator.
  absl::Status ValidateOpInputsCount(
      absl::Span<const ExprAttributes> inputs) const;

  // Default implementation that only checks the number of dependencies.
  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const override;

 private:
  ExprOperatorSignature signature_;
  std::string doc_;
};

// Base class for an operator with a GetOutputQType() strategy.
class BasicExprOperator : public ExprOperatorWithFixedSignature {
 protected:
  using ExprOperatorWithFixedSignature::ExprOperatorWithFixedSignature;

 private:
  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;

  // An extension point for InferAttributes().
  virtual absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const = 0;
};

// Base class for a simple ExprOperator without name and with fixed
// param-signature.
//
// Note: The primary application of this base class is for operators that
// available only from the operator registry, in that case, the registry
// provides a name. Please do not use it outside of that scope.
class UnnamedExprOperator : public BasicExprOperator {
 protected:
  UnnamedExprOperator(ExprOperatorSignature signature, Fingerprint fingerprint)
      : BasicExprOperator("unnamed_operator", std::move(signature), "",
                          fingerprint) {}
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_BASIC_EXPR_OPERATOR_H_
