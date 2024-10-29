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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_DISPATCH_OPERATOR_H_
#define AROLLA_EXPR_OPERATOR_LOADER_DISPATCH_OPERATOR_H_

#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/generic_operator_overload_condition.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla::operator_loader {

// A dispatch operator.
//
// Dispatch operator is an adapter for a list of overloads.
// Dispatch operator has an explicit signature, list of overloads where each
// operator comes with the explicit qtype constraint and a default operator.
// For each set of inputs, dispatch operator would return:
//   - the only overload with passing constraint,
//   - default operator if none of the constraints passes,
//   - an Error if more than one constraint passes.
class DispatchOperator final : public expr::ExprOperatorWithFixedSignature {
  struct PrivateConstructorTag {};

 public:
  struct Overload {
    std::string name;
    expr::ExprOperatorPtr op;
    // Overload condition. It can use L.input_tuple_qtype where DispatchOperator
    // will pass TupleQType for operator *args. It must return
    // OptionalUnit{present} if the overload should be selected.
    expr::ExprNodePtr condition;
  };

  // Factory function for a dispatch operator.
  static absl::StatusOr<expr::ExprOperatorPtr> Make(
      absl::string_view name, expr::ExprOperatorSignature signature,
      std::vector<Overload> overloads,
      expr::ExprNodePtr dispatch_readiness_condition);

  // Private constructor.
  DispatchOperator(PrivateConstructorTag, absl::string_view name,
                   expr::ExprOperatorSignature signature,
                   std::vector<Overload> overloads,
                   GenericOperatorOverloadConditionFn overloads_condition_fn,
                   expr::ExprNodePtr dispatch_readiness_condition,
                   Fingerprint fingerprint);

  absl::StatusOr<expr::ExprAttributes> InferAttributes(
      absl::Span<const expr::ExprAttributes> inputs) const final;

  absl::StatusOr<expr::ExprNodePtr> ToLowerLevel(
      const expr::ExprNodePtr& node) const final;

  absl::string_view py_qvalue_specialization_key() const override;

  // Returns expression to check that dispatching is possible.
  const expr::ExprNodePtr& dispatch_readiness_condition() const {
    return dispatch_readiness_condition_;
  }

  // Returns overloads
  const std::vector<Overload>& overloads() const { return overloads_; }

  ReprToken GenReprToken() const final;

 private:
  // Returns the overload that fits the input QTypes.
  absl::StatusOr<absl::Nullable<const Overload*>> LookupImpl(
      absl::Span<const expr::ExprAttributes> inputs) const;

  std::vector<Overload> overloads_;
  GenericOperatorOverloadConditionFn overloads_condition_fn_;
  expr::ExprNodePtr dispatch_readiness_condition_;
};

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_DISPATCH_OPERATOR_H_
