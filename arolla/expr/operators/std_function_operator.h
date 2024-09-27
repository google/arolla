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
#ifndef AROLLA_EXPR_OPERATORS_STD_FUNCTION_OPERATOR_H_
#define AROLLA_EXPR_OPERATORS_STD_FUNCTION_OPERATOR_H_

#include <functional>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::expr_operators {

// Operator for evaluating std::functions.
//
// Important properties:
//  * the fingerprint of the operator instance depends on both the
//    output_qtype_fn and eval_fn.
//  * _Not_ serializable.
class StdFunctionOperator : public expr::BuiltinExprOperatorTag,
                            public expr::BasicExprOperator {
 public:
  // Function that verifies input types and computes the output type for given
  // input types.
  using OutputQTypeFn =
      std::function<absl::StatusOr<QTypePtr>(absl::Span<const QTypePtr>)>;
  // Function that is called during evaluation.
  using EvalFn =
      std::function<absl::StatusOr<TypedValue>(absl::Span<const TypedRef>)>;

  // NOTE: Consider allowing a fingerprint to be passed here.
  StdFunctionOperator(absl::string_view name,
                      expr::ExprOperatorSignature signature,
                      absl::string_view doc, OutputQTypeFn output_qtype_fn,
                      EvalFn eval_fn);

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final;

  const OutputQTypeFn& GetOutputQTypeFn() const;

  const EvalFn& GetEvalFn() const;

 private:
  OutputQTypeFn output_qtype_fn_;
  EvalFn eval_fn_;
};

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_STD_FUNCTION_OPERATOR_H_
