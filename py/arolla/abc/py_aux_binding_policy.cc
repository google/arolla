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
#include "py/arolla/abc/py_aux_binding_policy.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/fixed_array.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/abc/py_signature.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Literal;
using ::arolla::expr::ValidateSignature;

using Param = ExprOperatorSignature::Parameter;

class AuxBindingPolicyRegistry {
 public:
  static AuxBindingPolicyRegistry& instance() {
    static absl::NoDestructor<AuxBindingPolicyRegistry> result;
    return *result;
  }

  // Registers an auxiliary binding policy.
  void Register(absl::string_view aux_policy,
                absl::Nonnull<AuxBindingPolicyPtr> policy_implementation) {
    registry_[aux_policy] = std::move(policy_implementation);
  }

  // Removes an auxiliary binding policy.
  void Remove(absl::string_view aux_policy) { registry_.erase(aux_policy); }

  // Returns the auxiliary policy with the given name, or nullptr and raises a
  // python exception.
  absl::Nullable<const AuxBindingPolicyPtr>& LookupOrNull(
      absl::string_view aux_policy) const {
    auto it = registry_.find(aux_policy);
    if (it != registry_.end()) {
      return it->second;
    }
    static const absl::NoDestructor<AuxBindingPolicyPtr> stub(nullptr);
    return *stub;
  }

  absl::flat_hash_map<std::string, absl::Nonnull<AuxBindingPolicyPtr>>
      registry_;
};

}  // namespace

PyObject* AuxMakePythonSignature(const ExprOperatorSignature& signature) {
  DCheckPyGIL();
  const auto& policy_implementation =
      AuxBindingPolicyRegistry::instance().LookupOrNull(signature.aux_policy);
  if (policy_implementation == nullptr) {
    PyErr_Format(PyExc_RuntimeError,
                 "arolla.abc.aux_make_python_signature() auxiliary binding "
                 "policy not found: '%s'",
                 absl::Utf8SafeCHexEscape(signature.aux_policy).c_str());
    return nullptr;
  }
  auto result =
      PyObjectPtr::Own(policy_implementation->MakePythonSignature(signature));
  if (result == nullptr) {
    return PyErr_FormatFromCause(
        PyExc_RuntimeError,
        "arolla.abc.aux_make_python_signature() auxiliary binding policy has "
        "failed: '%s'",
        absl::Utf8SafeCHexEscape(signature.aux_policy).c_str());
  }
  return result.release();
}

bool AuxBindArguments(const ::arolla::expr::ExprOperatorSignature& signature,
                      PyObject** args, Py_ssize_t nargsf, PyObject* kwnames,
                      std::vector<QValueOrExpr>* result,
                      absl::Nullable<AuxBindingPolicyPtr>* policy) {
  DCheckPyGIL();
  const auto& policy_implementation =
      AuxBindingPolicyRegistry::instance().LookupOrNull(signature.aux_policy);
  if (policy_implementation == nullptr) {
    PyErr_Format(
        PyExc_RuntimeError,
        "arolla.abc.aux_bind_arguments() auxiliary binding policy not found:"
        " '%s'",
        absl::Utf8SafeCHexEscape(signature.aux_policy).c_str());
    return false;
  }
  if (policy != nullptr) {
    *policy = policy_implementation;
  }
  if (!policy_implementation->BindArguments(signature, args, nargsf, kwnames,
                                            result)) {
    // Forward TypeError and ValueError to the caller unchanged, and treat any
    // other exceptions as a failure of the binding policy. (See note in
    // AuxBindingPolicy.bind_arguments()).
    if (PyErr_ExceptionMatches(PyExc_TypeError) ||
        PyErr_ExceptionMatches(PyExc_ValueError)) {
      return false;
    }
    PyErr_FormatFromCause(
        PyExc_RuntimeError,
        "arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:"
        " '%s'",
        absl::Utf8SafeCHexEscape(signature.aux_policy).c_str());
    return false;
  }
  if (auto status = ValidateDepsCount(signature, result->size(),
                                      absl::StatusCode::kFailedPrecondition);
      !status.ok()) {
    SetPyErrFromStatus(status);
    PyErr_FormatFromCause(
        PyExc_RuntimeError,
        "arolla.abc.aux_bind_arguments() auxiliary binding policy has failed:"
        " '%s'",
        absl::Utf8SafeCHexEscape(signature.aux_policy).c_str());
    return false;
  }
  return true;
}

void RegisterAuxBindingPolicy(
    absl::string_view aux_policy,
    absl::Nonnull<AuxBindingPolicyPtr> policy_implementation) {
  DCheckPyGIL();
  AuxBindingPolicyRegistry::instance().Register(
      aux_policy, std::move(policy_implementation));
}

void RemoveAuxBindingPolicy(absl::string_view aux_policy) {
  DCheckPyGIL();
  AuxBindingPolicyRegistry::instance().Remove(aux_policy);
}

namespace {

class PyAuxBindingPolicy final : public AuxBindingPolicy {
 public:
  PyAuxBindingPolicy(PyObjectPtr py_callable_make_python_signature,
                     PyObjectPtr py_callable_bind_arguments,
                     PyObjectPtr py_callable_make_literal)
      : py_callable_make_python_signature_(
            std::move(py_callable_make_python_signature)),
        py_callable_bind_arguments_(std::move(py_callable_bind_arguments)),
        py_callable_make_literal_(std::move(py_callable_make_literal)) {}

  PyObject* MakePythonSignature(
      const ExprOperatorSignature& signature) const final {
    DCHECK_OK(ValidateSignature(signature));
    DCheckPyGIL();
    auto py_signature = PyObjectPtr::Own(WrapAsPySignature(signature));
    if (py_signature == nullptr) {
      return nullptr;
    }
    return PyObject_CallOneArg(py_callable_make_python_signature_.get(),
                               py_signature.get());
  }

  bool BindArguments(const ExprOperatorSignature& signature, PyObject** py_args,
                     Py_ssize_t nargsf, PyObject* py_tuple_kwnames,
                     std::vector<QValueOrExpr>* result) const final {
    DCHECK_OK(ValidateSignature(signature));
    DCheckPyGIL();
    auto py_signature = PyObjectPtr::Own(WrapAsPySignature(signature));
    if (py_signature == nullptr) {
      return false;
    }
    const size_t args_count = PyVectorcall_NARGS(nargsf);
    const size_t kwargs_count =
        (py_tuple_kwnames != nullptr ? PyTuple_GET_SIZE(py_tuple_kwnames) : 0);
    PyObjectPtr py_result;
    // Calling: py_callable_bind_arguments_(py_signature, *args, **kwargs)
    if (nargsf & PY_VECTORCALL_ARGUMENTS_OFFSET) {
      // Apply the optimization:
      // https://docs.python.org/3/c-api/call.html#c.PY_VECTORCALL_ARGUMENTS_OFFSET
      auto* const tmp = py_args[-1];
      absl::Cleanup guard = [&] { py_args[-1] = tmp; };
      py_args[-1] = py_signature.get();
      py_result = PyObjectPtr::Own(
          PyObject_Vectorcall(py_callable_bind_arguments_.get(), py_args - 1,
                              args_count + 1, py_tuple_kwnames));
    } else {
      absl::FixedArray<PyObject*> new_py_args(2 + args_count + kwargs_count);
      new_py_args[1] = py_signature.get();
      std::copy(py_args, py_args + args_count + kwargs_count,
                new_py_args.data() + 2);
      py_result = PyObjectPtr::Own(PyObject_Vectorcall(
          py_callable_bind_arguments_.get(), &new_py_args[1],
          (args_count + 1) | PY_VECTORCALL_ARGUMENTS_OFFSET, py_tuple_kwnames));
    }
    if (py_result == nullptr) {
      return false;
    }
    if (!PyTuple_Check(py_result.get())) {
      PyErr_Format(PyExc_RuntimeError,
                   "expected tuple[QValue|Expr, ...], but .bind_arguments() "
                   "returned %s",
                   Py_TYPE(py_result.get())->tp_name);
      return false;
    }
    const size_t result_size = PyTuple_GET_SIZE(py_result.get());
    result->clear();
    result->reserve(result_size);
    for (size_t i = 0; i < result_size; ++i) {
      auto* const py_qvalue_or_expr = PyTuple_GET_ITEM(py_result.get(), i);
      if (IsPyExprInstance(py_qvalue_or_expr)) {
        result->push_back(UnsafeUnwrapPyExpr(py_qvalue_or_expr));
      } else if (IsPyQValueInstance(py_qvalue_or_expr)) {
        result->push_back(UnsafeUnwrapPyQValue(py_qvalue_or_expr));
      } else {
        PyErr_Format(PyExc_RuntimeError,
                     "expected tuple[QValue|Expr, ...], but .bind_arguments() "
                     "returned result[%zu]: %s",
                     i, Py_TYPE(py_qvalue_or_expr)->tp_name);
        return false;
      }
    }
    return true;
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
  PyObjectPtr py_callable_make_python_signature_;
  PyObjectPtr py_callable_bind_arguments_;
  PyObjectPtr py_callable_make_literal_;
};

}  // namespace

absl::Nullable<::arolla::expr::ExprNodePtr> AuxBindingPolicy::MakeLiteral(
    TypedValue&& value) const {
  DCheckPyGIL();
#ifndef NDEBUG  // Avoid expensive fingerprint checking unless debug mode.
  auto value_fingerprint = value.GetFingerprint();
  auto expr = DoMakeLiteral(std::move(value));
  if (expr == nullptr) {
    return nullptr;
  }
  if (!expr->qvalue() ||
      expr->qvalue()->GetFingerprint() != value_fingerprint) {
    PyErr_SetString(PyExc_AssertionError,
                    "make_literal(x).qvalue.fingerprint != x.fingerprint");
    return nullptr;
  }
  return expr;
#else
  return DoMakeLiteral(std::move(value));
#endif
}

void RegisterPyAuxBindingPolicy(absl::string_view aux_policy,
                                PyObject* py_callable_make_python_signature,
                                PyObject* py_callable_bind_arguments,
                                PyObject* py_callable_make_literal) {
  DCHECK_NE(py_callable_make_python_signature, nullptr);
  DCHECK_NE(py_callable_bind_arguments, nullptr);
  DCHECK_NE(py_callable_make_literal, nullptr);
  DCheckPyGIL();
  RegisterAuxBindingPolicy(
      aux_policy, std::make_shared<PyAuxBindingPolicy>(
                      PyObjectPtr::NewRef(py_callable_make_python_signature),
                      PyObjectPtr::NewRef(py_callable_bind_arguments),
                      PyObjectPtr::NewRef(py_callable_make_literal)));
}

}  // namespace arolla::python
