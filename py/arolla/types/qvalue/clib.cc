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
// Python extension module with C++ primitives for arolla.types.qvalue.*.
//
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/types/qvalue/py_function_operator.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operator_loader/backend_operator.h"
#include "arolla/expr/operator_loader/dispatch_operator.h"
#include "arolla/expr/operator_loader/dummy_operator.h"
#include "arolla/expr/operator_loader/generic_operator.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/expr/operator_loader/restricted_lambda_operator.h"
#include "arolla/expr/overloaded_expr_operator.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::GetNthOperator;
using ::arolla::expr::LambdaOperator;
using ::arolla::expr::OverloadedOperator;
using ::arolla::operator_loader::BackendOperator;
using ::arolla::operator_loader::DispatchOperator;
using ::arolla::operator_loader::DummyOperator;
using ::arolla::operator_loader::GenericOperator;
using ::arolla::operator_loader::GenericOperatorOverload;
using ::arolla::operator_loader::QTypeConstraint;
using ::arolla::operator_loader::RestrictedLambdaOperator;

PYBIND11_MODULE(clib, m) {
  // go/keep-sorted start block=yes newline_separated=yes
  m.def(
      "get_lambda_body",
      [](const ExprOperatorPtr& op) {
        if (auto* lambda_op = dynamic_cast<const LambdaOperator*>(op.get())) {
          return lambda_op->lambda_body();
        }
        if (auto* lambda_op =
                dynamic_cast<const operator_loader::RestrictedLambdaOperator*>(
                    op.get())) {
          return lambda_op->base_lambda_operator()->lambda_body();
        }
        throw py::type_error("expected a lambda operator, got " +
                             op->GenReprToken().str);
      },
      py::arg("op"), py::doc("Returns lambda body expression."));

  m.def(
      "get_nth_operator_index",
      [](const ExprOperatorPtr& op) {
        if (auto* get_nth_op = dynamic_cast<const GetNthOperator*>(op.get())) {
          return get_nth_op->index();
        }
        throw py::type_error("expected get_nth[*] operator, got " +
                             op->GenReprToken().str);
      },
      py::arg("op"),
      py::doc("Returns the index value from a get_nth[index] operator."));

  m.def(
      "get_py_function_operator_py_eval_fn",
      [](const ExprOperatorPtr& op) {
        if (auto* py_fn_op =
                dynamic_cast<const PyFunctionOperator*>(op.get())) {
          return py_fn_op->GetPyEvalFn();
        }
        throw py::type_error("expected PyFunctionOperator, got " +
                             op->GenReprToken().str);
      },
      py::arg("op"),
      py::doc("Returns py_eval_fn of a PyFunctionOperator instance."));

  m.def(
      "get_py_function_operator_qtype_inference_expr",
      [](const ExprOperatorPtr& op) {
        if (auto* py_fn_op =
                dynamic_cast<const PyFunctionOperator*>(op.get())) {
          return py_fn_op->GetQTypeInferenceExpr();
        }
        throw py::type_error("expected PyFunctionOperator, got " +
                             op->GenReprToken().str);
      },
      py::arg("op"),
      py::doc(
          "Returns qtype_inference_expr of a PyFunctionOperator instance."));

  m.def(
      "make_backend_operator",
      [](absl::string_view name, ExprOperatorSignature signature,
         absl::string_view doc,
         std::vector<std::pair<ExprNodePtr, std::string>> qtype_constraints,
         ExprNodePtr qtype_inference_expr) {
        absl::StatusOr<ExprOperatorPtr> result;
        {
          // Note: We release the GIL because constructing this operator is
          // time-consuming, as it involves the compilation of expressions.
          py::gil_scoped_release guard;
          std::vector<QTypeConstraint> constraints(qtype_constraints.size());
          for (size_t i = 0; i < qtype_constraints.size(); ++i) {
            std::tie(constraints[i].predicate_expr,
                     constraints[i].error_message) =
                std::move(qtype_constraints[i]);
          }
          result = BackendOperator::Make(name, std::move(signature), doc,
                                         std::move(constraints),
                                         std::move(qtype_inference_expr));
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("name"), py::arg("signature"), py::arg("doc"),
      py::arg("qtype_constraints"), py::arg("qtype_inference_expr"),
      py::doc("Constructs a new BackendOperator instance."));

  m.def(
      "make_dispatch_operator",
      [](absl::string_view name, ExprOperatorSignature signature,
         std::vector<std::tuple<std::string, ExprOperatorPtr, ExprNodePtr>>
             overloads,
         ExprNodePtr dispatch_readiness_condition) {
        absl::StatusOr<ExprOperatorPtr> result;
        {
          // Note: We release the GIL because constructing this operator is
          // time-consuming, as it involves the compilation of expressions.
          py::gil_scoped_release guard;
          std::vector<DispatchOperator::Overload> processed_overloads(
              overloads.size());
          for (size_t i = 0; i < overloads.size(); ++i) {
            auto& [name, op, condition] = overloads[i];
            processed_overloads[i].name = std::move(name);
            processed_overloads[i].op = std::move(op);
            processed_overloads[i].condition = std::move(condition);
          }
          result = DispatchOperator::Make(
              name, std::move(signature), std::move(processed_overloads),
              std::move(dispatch_readiness_condition));
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("name"), py::arg("signature"), py::arg("overloads"),
      py::arg("dispatch_readiness_condition"),
      py::doc("Constructs a new DispatchOperator instance."));

  m.def(
      "make_dummy_operator",
      [](absl::string_view name, ExprOperatorSignature signature,
         absl::string_view doc, QTypePtr result_qtype) {
        return ExprOperatorPtr(std::make_shared<DummyOperator>(
            name, std::move(signature), doc, result_qtype));
      },
      py::arg("name"), py::arg("signature"), py::arg("doc"),
      py::arg("result_qtype"),
      py::doc("Constructs a new DummyOperator instance."));

  m.def(
      "make_generic_operator",
      [](absl::string_view name, ExprOperatorSignature signature,
         absl::string_view doc) {
        return ExprOperatorPtr(
            pybind11_unstatus_or(GenericOperator::Make(name, signature, doc)));
      },
      py::arg("name"), py::arg("signature"), py::arg("doc"),
      py::doc("Returns a new GenericOperator instance."));

  m.def(
      "make_generic_operator_overload",
      [](ExprOperatorPtr base_operator,
         ExprNodePtr prepared_overload_condition_expr) {
        return ExprOperatorPtr(
            pybind11_unstatus_or(GenericOperatorOverload::Make(
                std::move(base_operator),
                std::move(prepared_overload_condition_expr))));
      },
      py::arg("base_operator"), py::arg("prepared_overload_condition_expr"),
      py::doc("Returns a new GenericOperatorOverload instance."));

  m.def(
      "make_get_nth_operator",
      [](int64_t index) {
        return ExprOperatorPtr(
            pybind11_unstatus_or(GetNthOperator::Make(index)));
      },
      py::arg("index"), py::doc("Returns a new get_nth[index] operator."));

  m.def(
      "make_overloaded_operator",
      [](absl::string_view name, std::vector<ExprOperatorPtr> base_operators) {
        return ExprOperatorPtr(std::make_shared<OverloadedOperator>(
            name, std::move(base_operators)));
      },
      py::arg("name"), py::arg("base_operators"),
      py::doc("Returns a new OverloadedOperator instance."));

  m.def(
      "make_py_function_operator",
      [](absl::string_view name, ExprOperatorSignature signature,
         absl::string_view doc, ExprNodePtr qtype_inference_expr,
         TypedValue py_eval_fn) {
        absl::StatusOr<ExprOperatorPtr> result;
        {
          // Note: We release the GIL because constructing this operator is
          // time-consuming, as it involves the compilation of expressions.
          py::gil_scoped_release guard;
          result = PyFunctionOperator::Make(name, std::move(signature), doc,
                                            std::move(qtype_inference_expr),
                                            std::move(py_eval_fn));
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("name"), py::arg("signature"), py::arg("doc"),
      py::arg("qtype_inference_expr"), py::arg("py_eval_fn"),
      py::doc("Returns a new PyFunctionOperator instance."));

  m.def(
      "make_restricted_lambda_operator",
      [](absl::string_view name, ExprOperatorSignature signature,
         ExprNodePtr lambda_body_expr, absl::string_view doc,
         std::vector<std::pair<ExprNodePtr, std::string>> qtype_constraints) {
        absl::StatusOr<std::shared_ptr<const LambdaOperator>>
            base_lambda_operator;
        absl::StatusOr<ExprOperatorPtr> result;
        {
          // Note: We release the GIL because constructing this operator is
          // time-consuming, as it involves the compilation of expressions.
          py::gil_scoped_release guard;
          std::vector<QTypeConstraint> constraints(qtype_constraints.size());
          for (size_t i = 0; i < qtype_constraints.size(); ++i) {
            std::tie(constraints[i].predicate_expr,
                     constraints[i].error_message) =
                std::move(qtype_constraints[i]);
          }
          base_lambda_operator = LambdaOperator::Make(
              name, std::move(signature), std::move(lambda_body_expr), doc);
          if (base_lambda_operator.ok()) {
            result = RestrictedLambdaOperator::Make(
                *std::move(base_lambda_operator), std::move(constraints));
          }
        }
        pybind11_throw_if_error(base_lambda_operator.status());
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("name"), py::arg("signature"), py::arg("lambda_body_expr"),
      py::arg("doc"), py::arg("qtype_constraints"),
      py::doc("Returns a new RestrictedLambdaOperator instance."));
  // go/keep-sorted end
}

}  // namespace
}  // namespace arolla::python
