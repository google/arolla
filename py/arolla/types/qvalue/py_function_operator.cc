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
#include "py/arolla/types/qvalue/py_function_operator.h"

#include <Python.h>

#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "py/arolla/abc/py_object_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/parameter_qtypes.h"
#include "arolla/expr/operator_loader/qtype_inference.h"
#include "arolla/expr/operators/std_function_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

using OutputQTypeFn = expr_operators::StdFunctionOperator::OutputQTypeFn;
using EvalFn = expr_operators::StdFunctionOperator::EvalFn;

absl::StatusOr<OutputQTypeFn> MakeOutputQTypeStdFn(
    expr::ExprNodePtr qtype_inference_expr,
    expr::ExprOperatorSignature signature) {
  // Check that all placeholder keys are present in the operator signature.
  absl::flat_hash_set<absl::string_view> parameter_names;
  for (const auto& param : signature.parameters) {
    parameter_names.insert(param.name);
  }
  std::set<std::string> undefined_parameter_names;
  for (auto&& placeholder_key :
       expr::GetPlaceholderKeys(qtype_inference_expr)) {
    if (!parameter_names.contains(placeholder_key)) {
      undefined_parameter_names.insert(std::move(placeholder_key));
    }
  }
  if (!undefined_parameter_names.empty()) {
    return absl::InvalidArgumentError(
        "unexpected parameters: P." +
        absl::StrJoin(undefined_parameter_names, ", P."));
  }
  // Compile expression
  ASSIGN_OR_RETURN(
      auto qtype_inference_fn,
      operator_loader::MakeQTypeInferenceFn({}, qtype_inference_expr));
  return
      [signature = std::move(signature),
       qtype_inference_fn = std::move(qtype_inference_fn)](
          absl::Span<const QTypePtr> qtype_inputs) -> absl::StatusOr<QTypePtr> {
        return qtype_inference_fn(operator_loader::ExtractParameterQTypes(
            signature, std::vector<expr::ExprAttributes>(qtype_inputs.begin(),
                                                         qtype_inputs.end())));
      };
}

EvalFn MakeEvalStdFn(PyObjectGILSafePtr py_eval_fn, absl::string_view name) {
  return [fn = std::move(py_eval_fn), name = std::string(name)](
             absl::Span<const TypedRef> inputs) -> absl::StatusOr<TypedValue> {
    AcquirePyGIL gil_acquire;
    auto py_args = PyObjectPtr::Own(PyTuple_New(inputs.size()));
    for (size_t i = 0; i < inputs.size(); ++i) {
      PyTuple_SET_ITEM(py_args.get(), i, WrapAsPyQValue(TypedValue(inputs[i])));
    }

    auto res = PyObjectPtr::Own(PyObject_CallObject(fn.get(), py_args.get()));
    // NOTE: If the error is re-raised using SetPyErrFromStatus,
    // only the python exception will be included and the appended error message
    // will be discarded.
    RETURN_IF_ERROR(StatusWithRawPyErr(absl::StatusCode::kInvalidArgument, ""))
        << "error during evaluation of PyFunctionOperator[" << name << "]";
    auto* typed_value = UnwrapPyQValue(res.get());
    RETURN_IF_ERROR(StatusCausedByPyErr(absl::StatusCode::kInvalidArgument, ""))
        << "error when unpacking the evaluation result of PyFunctionOperator["
        << name << "]";
    return *typed_value;
  };
}

};  // namespace

absl::StatusOr<expr::ExprOperatorPtr> PyFunctionOperator::Make(
    absl::string_view name, expr::ExprOperatorSignature signature,
    absl::string_view doc, expr::ExprNodePtr qtype_inference_expr,
    TypedValue py_eval_fn) {
  ASSIGN_OR_RETURN(const PyObjectGILSafePtr& py_object_eval_fn,
                   GetPyObjectValue(py_eval_fn.AsRef()));
  ASSIGN_OR_RETURN(auto qtype_inference_fn,
                   MakeOutputQTypeStdFn(qtype_inference_expr, signature));
  return std::make_shared<PyFunctionOperator>(
      PrivateConstructorTag{}, name, std::move(signature), doc,
      std::move(qtype_inference_fn), MakeEvalStdFn(py_object_eval_fn, name),
      std::move(qtype_inference_expr), std::move(py_eval_fn));
}

PyFunctionOperator::PyFunctionOperator(
    PrivateConstructorTag, absl::string_view name,
    expr::ExprOperatorSignature signature, absl::string_view doc,
    OutputQTypeFn output_qtype_fn, EvalFn eval_fn,
    expr::ExprNodePtr qtype_inference_expr, TypedValue py_eval_fn)
    : expr_operators::StdFunctionOperator(name, std::move(signature), doc,
                                          std::move(output_qtype_fn),
                                          std::move(eval_fn)),
      qtype_inference_expr_(std::move(qtype_inference_expr)),
      py_eval_fn_(std::move(py_eval_fn)) {}

absl::string_view PyFunctionOperator::py_qvalue_specialization_key() const {
  return "::arolla::python::PyFunctionOperator";
}

const expr::ExprNodePtr& PyFunctionOperator::GetQTypeInferenceExpr() const {
  return qtype_inference_expr_;
}

const TypedValue& PyFunctionOperator::GetPyEvalFn() const {
  return py_eval_fn_;
}

}  // namespace arolla::python
