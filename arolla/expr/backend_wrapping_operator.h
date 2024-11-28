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
#ifndef AROLLA_EXPR_BACKEND_WRAPPING_OPERATOR_H_
#define AROLLA_EXPR_BACKEND_WRAPPING_OPERATOR_H_

#include <functional>

#include "absl//status/statusor.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"

namespace arolla::expr {

// BackendWrappingOperator is a default wrapper for an operator implemented in
// an evaluation backend, wrapping it into an ExprOperator with the same
// name. The created ExprOperator is using the provided TypeMetaEvalStrategy
// to implement type propagation, and is lowered into itself.
//
// A BackendWrappingOperator should be created and registered for each
// evaluation backend operator used in Expr. After that, it can be used in
// expressions in the same way as any other expression operator.
class BackendWrappingOperator final : public BackendExprOperatorTag,
                                      public BasicExprOperator {
 public:
  // Function that verifies input types and computes the output type for given
  // input types. See operators/type_meta_eval_strategies.h for commonly
  // used strategies.
  using TypeMetaEvalStrategy = std::function<absl::StatusOr<QTypePtr>(
      absl::Span<const QTypePtr> input_qtypes)>;

  // Constructs an instance with the given name, signature and type evaluation
  // strategy.
  BackendWrappingOperator(absl::string_view name,
                          const ExprOperatorSignature& signature,
                          TypeMetaEvalStrategy strategy,
                          absl::string_view doc = "");

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override;

  const TypeMetaEvalStrategy& type_meta_eval_strategy() const {
    return type_meta_eval_strategy_;
  }

 private:
  TypeMetaEvalStrategy type_meta_eval_strategy_;
};

// The following functions register a backend wrapping operator in the
// ExprOperatorRegistry.
//
// For example,
// AROLLA_DEFINE_EXPR_OPERATOR(
//   CoreHas, RegisterBackendOperator("core.has", Chain(Unary, PresenceType)));
absl::StatusOr<ExprOperatorPtr> RegisterBackendOperator(
    absl::string_view name,
    BackendWrappingOperator::TypeMetaEvalStrategy strategy,
    absl::string_view doc = "");
absl::StatusOr<ExprOperatorPtr> RegisterBackendOperator(
    absl::string_view name, const ExprOperatorSignature& signature,
    BackendWrappingOperator::TypeMetaEvalStrategy strategy,
    absl::string_view doc = "");

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_BACKEND_WRAPPING_OPERATOR_H_
