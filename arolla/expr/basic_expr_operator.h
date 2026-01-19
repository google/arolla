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
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
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

  // Returns a reference to the stored signature.
  const ExprOperatorSignature& signature() const { return *signature_; }

  // Returns a reference to the stored doc-string.
  const std::string& doc() const { return doc_; }

 protected:
  ExprOperatorWithFixedSignature(absl::string_view name,
                                 ExprOperatorSignature signature,
                                 absl::string_view doc,
                                 Fingerprint fingerprint);

 private:
  ExprOperatorSignaturePtr absl_nonnull signature_;
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
  virtual absl::StatusOr<QTypePtr absl_nonnull> GetOutputQType(
      absl::Span<const QTypePtr absl_nonnull> input_qtypes) const = 0;
};

// Base class for a simple ExprOperator without name and with fixed
// param-signature.
//
// Note: The primary application of this base class is for operators that
// available only from the operator registry, in that case, the registry
// provides a name. Please do not use it outside of that scope.
class UnnamedExprOperator : public BasicExprOperator {
 protected:
  UnnamedExprOperator(ExprOperatorSignature signature, Fingerprint fingerprint);
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_BASIC_EXPR_OPERATOR_H_
