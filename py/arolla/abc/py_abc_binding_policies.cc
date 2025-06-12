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
#include "py/arolla/abc/py_abc_binding_policies.h"

#include <Python.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/typed_value.h"
#include "py/arolla/abc/py_aux_binding_policy.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/abc/py_signature.h"
#include "py/arolla/py_utils/py_utils.h"

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

// A sentinel entity indicating to use the default value.
static PyObject kSentinelDefaultValue;

// A sentinel entity indicating to use `py_var_args`.
static PyObject kSentinelVarArgs;

// Note: The order of keys in `PyVarKwargs` is defined by the memory
// addresses of the stored values, which are of type `PyObject**`.
using PyVarKwarg = absl::flat_hash_map<absl::string_view, PyObject**>;

void ReportMissingPositionalParameters(
    absl::Span<const absl::string_view> missing_positional_params);

void ReportUnprocessedPositionalArguments(
    const ExprOperatorSignature& signature, size_t py_args_size);

void ReportUnprocessedKeywordArguments(const PyVarKwarg& py_var_kwargs);

// A lower-level binding-arguments function without boxing python values.
// This function processes arguments and populates the output parameters
// (`result_py_*`).
//
// The main result is stored in `result_py_bound_args`, which has a one-to-one
// mapping with the signature parameters. If `result_py_bound_args[i]` points
// to `kSentinelDefaultValue`, it indicates that the argument's value should
// be taken from the signature's default (the function ensures that a default
// value exists). A pointer to `kSentinelVarArgs`  indicates that the value
// should be taken from `result_py_var_args` respectively.
//
// If the function is successful, it returns `true`. Otherwise, it returns
// `false` and sets the corresponding Python exception.
bool ClassicBindArguments(const ExprOperatorSignature& signature,
                          PyObject** py_args, Py_ssize_t nargsf,
                          PyObject* py_tuple_kwnames,
                          std::vector<PyObject*>& result_py_bound_args,
                          absl::Span<PyObject*>& result_py_var_args) {
  const auto& params = signature.parameters;
  const size_t py_args_size = PyVectorcall_NARGS(nargsf);

  // Load keyword arguments into a `py_var_kwargs` hashtable.
  PyVarKwarg py_var_kwargs;
  {
    absl::Span<PyObject*> py_kwnames;
    PyTuple_AsSpan(py_tuple_kwnames, &py_kwnames);
    py_var_kwargs.reserve(py_kwnames.size());
    for (size_t i = 0; i < py_kwnames.size(); ++i) {
      Py_ssize_t kwname_size = 0;
      const char* kwname_data =
          PyUnicode_AsUTF8AndSize(py_kwnames[i], &kwname_size);
      if (kwname_data == nullptr) {
        return false;
      }
      py_var_kwargs[absl::string_view(kwname_data, kwname_size)] =
          py_args + py_args_size + i;
    }
  }

  result_py_bound_args.reserve(params.size());
  size_t i = 0;

  // Process positional arguments.
  for (; i < params.size() && i < py_args_size; ++i) {
    DCHECK_EQ(result_py_bound_args.size(), i);
    const auto& param = params[i];
    if (param.kind == Param::Kind::kPositionalOrKeyword) {
      if (py_var_kwargs.contains(param.name)) {
        return PyErr_Format(PyExc_TypeError,
                            "multiple values for argument '%s'",
                            absl::Utf8SafeCHexEscape(param.name).c_str());
      }
      result_py_bound_args.push_back(py_args[i]);
    } else {
      break;
    }
  }
  bool has_unprocessed_positional_arguments = false;
  if (i < params.size() && params[i].kind == Param::Kind::kVariadicPositional) {
    result_py_bound_args.push_back(&kSentinelVarArgs);
    result_py_var_args = absl::Span<PyObject*>(py_args + i, py_args_size - i);
    i += 1;
  } else {
    has_unprocessed_positional_arguments = (i < py_args_size);
  }

  // Bind remaining parameters using keyword arguments and the default values.
  std::vector<absl::string_view> missing_positional_params;
  for (; i < params.size(); ++i) {
    const auto& param = params[i];
    if (param.kind == Param::Kind::kPositionalOrKeyword) {
      auto it = py_var_kwargs.find(param.name);
      if (it != py_var_kwargs.end()) {
        result_py_bound_args.push_back(*it->second);
        py_var_kwargs.erase(it);
      } else if (param.default_value.has_value()) {
        result_py_bound_args.push_back(&kSentinelDefaultValue);
      } else {
        missing_positional_params.push_back(param.name);
      }
    } else if (param.kind == Param::Kind::kVariadicPositional) {
      result_py_bound_args.push_back(&kSentinelVarArgs);
    } else {
      return PyErr_Format(PyExc_RuntimeError,
                          "unexpected parameter kind='%d', param='%s'",
                          static_cast<int>(param.kind),
                          absl::Utf8SafeCHexEscape(param.name).c_str());
    }
  }

  if (!missing_positional_params.empty()) {
    ReportMissingPositionalParameters(missing_positional_params);
    return false;
  } else if (has_unprocessed_positional_arguments) {
    ReportUnprocessedPositionalArguments(signature, py_args_size);
    return false;
  } else if (!py_var_kwargs.empty()) {
    ReportUnprocessedKeywordArguments(py_var_kwargs);
    return false;
  }
  DCHECK_EQ(result_py_bound_args.size(), params.size());
  return true;
}

void ReportMissingPositionalParameters(
    absl::Span<const absl::string_view> missing_positional_params) {
  DCHECK(!missing_positional_params.empty());
  if (missing_positional_params.size() == 1) {
    PyErr_Format(
        PyExc_TypeError, "missing 1 required positional argument: '%s'",
        absl::Utf8SafeCHexEscape(missing_positional_params[0]).c_str());
  } else if (missing_positional_params.size() > 1) {
    std::ostringstream message;
    message << "missing " << missing_positional_params.size()
            << " required positional arguments: ";
    for (size_t j = 0; j + 1 < missing_positional_params.size(); ++j) {
      if (j > 0) {
        message << ", ";
      }
      message << "'" << absl::Utf8SafeCHexEscape(missing_positional_params[j])
              << "'";
    }
    message << " and '"
            << absl::Utf8SafeCHexEscape(missing_positional_params.back())
            << "'";
    PyErr_SetString(PyExc_TypeError, std::move(message).str().c_str());
  }
}

void ReportUnprocessedPositionalArguments(
    const ExprOperatorSignature& signature, size_t py_args_size) {
  size_t count_positionals = 0;
  size_t count_required_positionals = 0;
  for (const auto& param : signature.parameters) {
    if (param.kind == Param::Kind::kPositionalOrKeyword) {
      count_positionals += 1;
      count_required_positionals += !param.default_value.has_value();
    }
  }
  if (count_positionals == count_required_positionals) {
    if (count_positionals == 1) {
      PyErr_Format(PyExc_TypeError,
                   "takes 1 positional argument but %zu were given",
                   py_args_size);
    } else {
      PyErr_Format(PyExc_TypeError,
                   "takes %zu positional arguments but %zu were given",
                   count_positionals, py_args_size);
    }
  } else {
    PyErr_Format(
        PyExc_TypeError,
        "takes from %zu to %zu positional arguments but %zu were given",
        count_required_positionals, count_positionals, py_args_size);
  }
}

void ReportUnprocessedKeywordArguments(const PyVarKwarg& py_var_kwargs) {
  auto it = std::min_element(
      py_var_kwargs.begin(), py_var_kwargs.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });
  PyErr_Format(PyExc_TypeError, "an unexpected keyword argument: '%s'",
               absl::Utf8SafeCHexEscape(it->first).c_str());
}

using AsQValueOrExprFn =
    absl::FunctionRef<std::optional<QValueOrExpr>(PyObject*)>;

// A lower-level boxing-arguments function that works with pre-bound python
// values.
//
// If the function is successful, it returns `true`. Otherwise, it returns
// `false` and sets the corresponding Python exception.
bool ClassicBoxBoundArguments(const ExprOperatorSignature& signature,
                              AsQValueOrExprFn as_qvalue_or_expr_fn,
                              absl::Span<PyObject* const> py_bound_args,
                              absl::Span<PyObject* const> py_var_args,
                              std::vector<QValueOrExpr>& result) {
  DCHECK_EQ(py_bound_args.size(), signature.parameters.size());
  result.clear();
  result.reserve(py_bound_args.size() + py_var_args.size());
  for (size_t i = 0; i < py_bound_args.size(); ++i) {
    auto& param = signature.parameters[i];
    auto* py_bound_arg = py_bound_args[i];
    if (py_bound_arg == &kSentinelDefaultValue) {
      DCHECK(param.default_value.has_value());
      result.push_back(*param.default_value);
    } else if (py_bound_arg == &kSentinelVarArgs) {
      for (size_t j = 0; j < py_var_args.size(); ++j) {
        if (auto arg = as_qvalue_or_expr_fn(py_var_args[j])) {
          result.push_back(*std::move(arg));
        } else {
          PyErr_AddNote(absl::StrFormat(
              "Error occurred while processing argument: `%s[%zu]`",
              absl::Utf8SafeCHexEscape(param.name), j));
          return false;
        }
      }
    } else {
      if (auto arg = as_qvalue_or_expr_fn(py_bound_arg)) {
        result.push_back(*std::move(arg));
      } else {
        PyErr_AddNote(
            absl::StrFormat("Error occurred while processing argument: `%s`",
                            absl::Utf8SafeCHexEscape(param.name)));
        return false;
      }
    }
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
  std::vector<PyObject*> py_bound_args;
  absl::Span<PyObject*> py_var_args;
  if (!ClassicBindArguments(signature, py_args, nargsf, py_tuple_kwnames,
                            py_bound_args, py_var_args)) {
    return false;
  }
  return ClassicBoxBoundArguments(
      signature, [this](PyObject* py_arg) { return AsQValueOrExpr(py_arg); },
      py_bound_args, py_var_args, *result);
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
      PyErr_Format(PyExc_RuntimeError,
                   "expected QValue or Expr, but as_qvalue_or_expr(arg: %s) "
                   "returned %s",
                   Py_TYPE(py_arg)->tp_name, Py_TYPE(py_result.get())->tp_name);
      return std::nullopt;
    }
  }

  absl_nullable ExprNodePtr MakeLiteral(TypedValue&& value) const final {
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

bool RegisterPyClassicAuxBindingPolicyWithCustomBoxing(
    absl::string_view aux_policy, PyObject* py_callable_as_qvalue_or_expr,
    PyObject* py_callable_make_literal) {
  DCHECK_NE(py_callable_as_qvalue_or_expr, nullptr);
  DCHECK_NE(py_callable_make_literal, nullptr);
  DCheckPyGIL();
  return RegisterAuxBindingPolicy(
      aux_policy, std::make_shared<PyClassicAuxBindingPolicyWithCustomBoxing>(
                      PyObjectPtr::NewRef(py_callable_as_qvalue_or_expr),
                      PyObjectPtr::NewRef(py_callable_make_literal)));
}

}  // namespace arolla::python
