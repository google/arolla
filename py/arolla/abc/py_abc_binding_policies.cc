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
#include "py/arolla/abc/py_abc_binding_policies.h"

#include <Python.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_aux_binding_policy.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/abc/py_signature.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Literal;

using Param = ExprOperatorSignature::Parameter;

PyObject* ClassicAuxBindingPolicyWithCustomBoxing::MakePythonSignature(
    const ExprOperatorSignature& signature) const {
  DCHECK_OK(ValidateSignature(signature));
  return WrapAsPySignature(signature);
}

namespace {

// This function follows the behaviour of `::arolla::expr::BindArguments()`.
//
// We cannot directly use `::arolla::expr::BindArguments()` here because it only
// supports `Expr` arguments, and we need `QValue|Expr`.
bool ClassicBindArguments(
    const ExprOperatorSignature& signature, std::vector<QValueOrExpr>&& args,
    absl::flat_hash_map<absl::string_view, QValueOrExpr>&& kwargs,
    std::vector<QValueOrExpr>* result) {
  result->clear();
  result->reserve(args.size() + kwargs.size());

  size_t param_idx = 0;

  // Parse positional arguments.
  size_t arg_idx = 0;
  for (; param_idx < signature.parameters.size() && arg_idx < args.size();
       ++param_idx) {
    const auto& param = signature.parameters[param_idx];
    switch (param.kind) {
      case Param::Kind::kPositionalOrKeyword:
        if (kwargs.contains(param.name)) {
          PyErr_Format(PyExc_TypeError,
                       "got multiple values for parameter: '%s'",
                       param.name.c_str());
          return false;
        }
        result->push_back(std::move(args[arg_idx++]));
        break;

      case Param::Kind::kVariadicPositional:
        while (arg_idx < args.size()) {
          result->push_back(std::move(args[arg_idx++]));
        }
        break;
    }
  }

  if (arg_idx < args.size()) {  // too many positional arguments
    size_t min_arg_count = 0;
    size_t max_arg_count = 0;
    for (const auto& param : signature.parameters) {
      if (param.kind == Param::Kind::kPositionalOrKeyword) {
        min_arg_count += !param.default_value.has_value();
        max_arg_count += 1;
      }
    }
    if (min_arg_count == 1 && min_arg_count == max_arg_count) {
      PyErr_Format(PyExc_TypeError, "expected 1 positional argument, got %zu",
                   args.size());
    } else if (min_arg_count == max_arg_count) {
      PyErr_Format(PyExc_TypeError,
                   "expected %zu positional arguments, got %zu", min_arg_count,
                   args.size());
    } else {
      PyErr_Format(PyExc_TypeError,
                   "expected from %zu to %zu positional arguments, got %zu",
                   min_arg_count, max_arg_count, args.size());
    }
    return false;
  }

  // Parse keyword arguments and defaults.
  std::vector<std::string> missing_arguments;
  for (; param_idx < signature.parameters.size(); ++param_idx) {
    const auto& param = signature.parameters[param_idx];
    if (param.kind == Param::Kind::kPositionalOrKeyword) {
      auto it = kwargs.find(param.name);
      if (it != kwargs.end()) {
        result->push_back(std::move(it->second));
        kwargs.erase(it);
      } else if (param.default_value.has_value()) {
        result->push_back(*param.default_value);
      } else {
        missing_arguments.push_back(param.name);
      }
    }
  }

  if (!kwargs.empty()) {  // unknown keyword arguments.
    std::vector<std::string> unexpected_kwnames;
    for (const auto& [k, _] : kwargs) {
      unexpected_kwnames.emplace_back(k);
    }
    std::sort(unexpected_kwnames.begin(), unexpected_kwnames.end());
    if (unexpected_kwnames.size() == 1) {
      PyErr_Format(PyExc_TypeError, "unexpected keyword argument: '%s'",
                   unexpected_kwnames[0].c_str());
    } else {
      PyErr_Format(PyExc_TypeError, "unexpected keyword arguments: '%s'",
                   absl::StrJoin(unexpected_kwnames, "', '").c_str());
    }
    return false;
  }

  if (!missing_arguments.empty()) {  // missing arguments.
    if (missing_arguments.size() == 1) {
      PyErr_Format(PyExc_TypeError, "missing 1 required argument: '%s'",
                   missing_arguments[0].c_str());
    } else if (missing_arguments.size() > 1) {
      PyErr_Format(PyExc_TypeError, "missing %zu required arguments: '%s'",
                   missing_arguments.size(),
                   absl::StrJoin(missing_arguments, "', '").c_str());
    }
    return false;
  }

  return true;
}

}  // namespace

bool ClassicAuxBindingPolicyWithCustomBoxing::BindArguments(
    const ExprOperatorSignature& signature, PyObject** py_args,
    Py_ssize_t nargsf, PyObject* py_tuple_kwnames,
    std::vector<QValueOrExpr>* result) const {
  DCHECK_OK(ValidateSignature(signature));
  DCheckPyGIL();
  const size_t args_count = PyVectorcall_NARGS(nargsf);
  const size_t kwargs_count =
      (py_tuple_kwnames != nullptr ? PyTuple_GET_SIZE(py_tuple_kwnames) : 0);

  // Parse `args`.
  std::vector<QValueOrExpr> args;
  args.reserve(args_count);
  for (size_t i = 0; i < args_count; ++i) {
    auto arg = AsQValueOrExpr(py_args[i]);
    if (!arg.has_value()) {
      // Add context for TypeError and ValueError, and forward any other
      // exceptions.
      auto* py_exc = PyErr_Occurred();
      if (PyErr_GivenExceptionMatches(py_exc, PyExc_TypeError) ||
          PyErr_GivenExceptionMatches(py_exc, PyExc_ValueError)) {
        PyErr_FormatFromCause(
            py_exc, "unable to represent as QValue or Expr: args[%zu]: %s", i,
            Py_TYPE(py_args[i])->tp_name);
      }
      return false;
    }
    args.push_back(*std::move(arg));
  }

  // Parse `kwargs`.
  absl::flat_hash_map<absl::string_view, QValueOrExpr> kwargs;
  kwargs.reserve(kwargs_count);
  for (size_t i = 0; i < kwargs_count; ++i) {
    PyObject* const py_kwname = PyTuple_GET_ITEM(py_tuple_kwnames, i);
    PyObject* const py_arg = py_args[args_count + i];
    auto arg = AsQValueOrExpr(py_arg);
    if (!arg.has_value()) {
      // Add context for TypeError and ValueError, and forward any other
      // exceptions.
      auto* py_exc = PyErr_Occurred();
      if (PyErr_GivenExceptionMatches(py_exc, PyExc_TypeError) ||
          PyErr_GivenExceptionMatches(py_exc, PyExc_ValueError)) {
        PyErr_FormatFromCause(
            py_exc, "unable to represent as QValue or Expr: kwargs[%R]: %s",
            py_kwname, Py_TYPE(py_arg)->tp_name);
      }
      return false;
    }
    Py_ssize_t kwname_size = 0;
    const char* kwname_data = PyUnicode_AsUTF8AndSize(py_kwname, &kwname_size);
    if (kwname_data == nullptr) {
      return false;
    }
    kwargs.emplace(absl::string_view(kwname_data, kwname_size),
                   *std::move(arg));
  }
  return ClassicBindArguments(signature, std::move(args), std::move(kwargs),
                              result);
}

namespace {

class PyClassicAuxBindingPolicyWithCustomBoxing final
    : public ClassicAuxBindingPolicyWithCustomBoxing {
 public:
  explicit PyClassicAuxBindingPolicyWithCustomBoxing(
      PyObjectPtr py_callable_as_qvalue_or_expr,
      PyObjectPtr py_callable_make_literal)
      : py_callable_as_qvalue_or_expr_(
            std::move(py_callable_as_qvalue_or_expr)),
        py_callable_make_literal_(std::move(py_callable_make_literal)) {}

  std::optional<QValueOrExpr> AsQValueOrExpr(PyObject* py_arg) const final {
    DCheckPyGIL();

    // Forward QValues and Exprs unchanged.
    if (IsPyExprInstance(py_arg)) {
      return UnsafeUnwrapPyExpr(py_arg);
    } else if (IsPyQValueInstance(py_arg)) {
      return UnsafeUnwrapPyQValue(py_arg);
    }
    // Calling: py_callable_as_qvalue_or_expr_(py_arg)
    auto py_result = PyObjectPtr::Own(
        PyObject_CallOneArg(py_callable_as_qvalue_or_expr_.get(), py_arg));
    if (py_result == nullptr) {
      return std::nullopt;
    }
    if (IsPyExprInstance(py_result.get())) {
      return UnsafeUnwrapPyExpr(py_result.get());
    } else if (IsPyQValueInstance(py_result.get())) {
      return UnsafeUnwrapPyQValue(py_result.get());
    } else {
      PyErr_Format(
          PyExc_RuntimeError,
          "expected QValue or Expr, but as_qvalue_or_expr(arg: %s) returned %s",
          Py_TYPE(py_arg)->tp_name, Py_TYPE(py_result.get())->tp_name);
      return std::nullopt;
    }
  }

  absl::Nullable<ExprNodePtr> DoMakeLiteral(TypedValue&& value) const final {
    DCheckPyGIL();
    if (py_callable_make_literal_.get() == Py_None) {
      return Literal(std::move(value));
    }
    auto py_value = PyObjectPtr::Own(WrapAsPyQValue(std::move(value)));
    if (py_value == nullptr) {
      return nullptr;
    }
    auto res = PyObjectPtr::Own(
        PyObject_CallOneArg(py_callable_make_literal_.get(), py_value.get()));
    if (res == nullptr) {
      return nullptr;
    }
    return UnwrapPyExpr(res.get());
  }

 private:
  PyObjectPtr py_callable_as_qvalue_or_expr_;
  PyObjectPtr py_callable_make_literal_;
};

}  // namespace

void RegisterPyClassicAuxBindingPolicyWithCustomBoxing(
    absl::string_view aux_policy, PyObject* py_callable_as_qvalue_or_expr,
    PyObject* py_callable_make_literal) {
  DCHECK_NE(py_callable_as_qvalue_or_expr, nullptr);
  DCHECK_NE(py_callable_make_literal, nullptr);
  DCheckPyGIL();
  RegisterAuxBindingPolicy(
      aux_policy, std::make_shared<PyClassicAuxBindingPolicyWithCustomBoxing>(
                      PyObjectPtr::NewRef(py_callable_as_qvalue_or_expr),
                      PyObjectPtr::NewRef(py_callable_make_literal)));
}

}  // namespace arolla::python
