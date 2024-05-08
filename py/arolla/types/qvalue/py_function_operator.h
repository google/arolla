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
#ifndef THIRD_PARTY_PY_AROLLA_TYPES_QVALUE_PY_FUNCTION_OPERATOR_H_
#define THIRD_PARTY_PY_AROLLA_TYPES_QVALUE_PY_FUNCTION_OPERATOR_H_

#include <Python.h>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/std_function_operator.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

// Operator for evaluating python functions.
//
// Important properties:
//  * Points to the original py_eval_fn.
//  * Not serializable.
class PyFunctionOperator final
    : public ::arolla::expr_operators::StdFunctionOperator {
  struct PrivateConstructorTag {};

 public:
  static absl::StatusOr<expr::ExprOperatorPtr> Make(
      absl::string_view name, expr::ExprOperatorSignature signature,
      absl::string_view doc, expr::ExprNodePtr qtype_inference_expr,
      TypedValue py_eval_fn);

  absl::string_view py_qvalue_specialization_key() const override;

  const expr::ExprNodePtr& GetQTypeInferenceExpr() const;

  const TypedValue& GetPyEvalFn() const;

  PyFunctionOperator(PrivateConstructorTag, absl::string_view name,
                     expr::ExprOperatorSignature signature,
                     absl::string_view doc, OutputQTypeFn output_qtype_fn,
                     EvalFn eval_fn, expr::ExprNodePtr qtype_inference_expr,
                     TypedValue py_eval_fn);

 private:
  expr::ExprNodePtr qtype_inference_expr_;
  TypedValue py_eval_fn_;
};

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_TYPES_QVALUE_PY_FUNCTION_OPERATOR_H_
