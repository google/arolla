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
#ifndef AROLLA_EXPR_OVERLOADED_EXPR_OPERATOR_H_
#define AROLLA_EXPR_OVERLOADED_EXPR_OPERATOR_H_

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

// Overloaded operator.
//
// Overloaded operator is an adapter for a list of base operators. For each
// inputs it takes the first operator in the list that fits the case and apply
// it.
//
// Whether or not an operator fits is decided based on the result of
// GetOutputQType(input_qtype):
//   * qtype (must be non-null)  -- operator fits
//   * InvalidArgumentError -- operator doesn't fit
//   * other error  -- an unexpected error, should be escalated
//
class OverloadedOperator final : public ExprOperator {
 public:
  // Constructs a overloaded operator from a given list of operators.
  OverloadedOperator(absl::string_view name,
                     std::vector<ExprOperatorPtr> base_ops);

  // Returns signature of the first operator.
  absl::StatusOr<ExprOperatorSignature> GetSignature() const final;

  // Returns doc-string of the first operator.
  absl::StatusOr<std::string> GetDoc() const final;

  // Returns a list of base operators.
  absl::Span<const ExprOperatorPtr> base_ops() const;

  // Returns the first base operator that supports the given inputs qtypes.
  absl::StatusOr<ExprOperatorPtr> LookupOp(
      absl::Span<const ExprAttributes> inputs) const;

  // Forwards the call to the first operator that fits the input qtypes.
  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;

  // Forwards the call to the first operator that fits the input qtypes.
  absl::StatusOr<ExprNodePtr> ToLowerLevel(const ExprNodePtr& node) const final;

  absl::string_view py_qvalue_specialization_key() const final;

 private:
  // Returns the first operator that fits the input qtypes, and
  // the corresponding output qtype.
  absl::StatusOr<std::tuple<ExprOperatorPtr, ExprAttributes>> LookupImpl(
      absl::Span<const ExprAttributes> inputs) const;

  std::vector<ExprOperatorPtr> base_ops_;
};

// Helper factory, which unwrap absl::StatusOr for any argument and transfer
// parameters to the constructor of OverloadedOperator.
template <class... Args>
absl::StatusOr<ExprOperatorPtr> MakeOverloadedOperator(absl::string_view name,
                                                       Args&&... args) {
  RETURN_IF_ERROR(CheckInputStatus(args...));
  std::vector<ExprOperatorPtr> base_ops(
      {UnStatus(std::forward<Args>(args))...});
  return std::make_shared<OverloadedOperator>(name, std::move(base_ops));
}

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_OVERLOADED_EXPR_OPERATOR_H_
