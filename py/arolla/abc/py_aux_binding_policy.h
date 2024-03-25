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
// IMPORTANT: All the following functions assume that the current thread is
// ready to call the Python C API. You can find extra information in
// documentation for PyGILState_Ensure() and PyGILState_Release().

#ifndef PY_AROLLA_ABC_PY_AUX_BINDING_POLICY_H_
#define PY_AROLLA_ABC_PY_AUX_BINDING_POLICY_H_

#include <Python.h>

#include <memory>
#include <variant>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

// Forward declaration.
class AuxBindingPolicy;
using AuxBindingPolicyPtr = std::shared_ptr<const AuxBindingPolicy>;

// A utility variant type combining a typed_value and an expr.
using QValueOrExpr =
    std::variant<::arolla::TypedValue, ::arolla::expr::ExprNodePtr>;

// Returns an `inspect.Signature` (or `arolla.abc.Signature`) corresponding to
// the given operator signature. If the function fails, it returns `nullptr` and
// sets a Python exception.
PyObject* AuxMakePythonSignature(
    const ::arolla::expr::ExprOperatorSignature& signature);

// Generates node dependencies for the given operator signature and `*args`,
// `**kwargs` and sets `policy` to the policy specified by the provided
// `signature`. If the method fails, it returns `false` and sets a Python
// exception.
//
// The semantic of `args`, `nargsf`, `kwnames` is same semantic as in
// `PyObject_Vectorcall()`.
//
// Note: Any exception that is not a TypeError or ValueError should be treated
// as a failure of the binding policy.
ABSL_MUST_USE_RESULT bool AuxBindArguments(
    const ::arolla::expr::ExprOperatorSignature& signature, PyObject** args,
    Py_ssize_t nargsf, PyObject* kwnames, std::vector<QValueOrExpr>* result,
    absl::Nullable<AuxBindingPolicyPtr>* policy = nullptr);

// Registers an auxiliary binding policy.
void RegisterAuxBindingPolicy(
    absl::string_view aux_policy,
    absl::Nonnull<AuxBindingPolicyPtr> policy_implementation);

// Removes an auxiliary binding policy.
void RemoveAuxBindingPolicy(absl::string_view aux_policy);

// Registers an auxiliary binding policy backed by Python callables.
//
//   def make_python_signature(
//       signature: arolla.abc.Signature
//   ) -> inspect.Signature|arolla.abc.Signature
//
//   def bind_arguments(
//       signature: arolla.abc.Signature,
//       *args: Any,
//       **kwargs: Any
//   ) -> tuple[QValue|Expr, ...]
//
//   def make_literal(value: QValue) -> Expr
//
// `make_literal` can also be None, causing `rl.literal(value)` to be used as
// default.
//
void RegisterPyAuxBindingPolicy(absl::string_view aux_policy,
                                PyObject* py_callable_make_python_signature,
                                PyObject* py_callable_bind_arguments,
                                PyObject* py_callable_make_literal);

// An auxiliary binding policy for Python environment.
class AuxBindingPolicy {
 public:
  // Returns an `inspect.Signature` (or `arolla.abc.Signature`) describing the
  // Python signature of the operator. If the method fails, it returns `nullptr`
  // and sets a Python exception.
  virtual PyObject* MakePythonSignature(
      const ::arolla::expr::ExprOperatorSignature& signature) const = 0;

  // Generates node dependencies for the given operator signature and `*args`,
  // `**kwargs`. If the method fails, it returns `false` and sets a Python
  // exception.
  //
  // The semantic of `args`, `nargsf`, `kwnames` is same semantic as in
  // `PyObject_Vectorcall()`.
  //
  // Note: Any exception that is not a TypeError or ValueError will be treated
  // as a failure of the binding policy.
  virtual bool BindArguments(
      const ::arolla::expr::ExprOperatorSignature& signature, PyObject** args,
      Py_ssize_t nargsf, PyObject* kwnames,
      std::vector<QValueOrExpr>* result) const = 0;

  // Returns `value` wrapped as a literal expr, or nullptr and raises a python
  // exception. Called with the QValue results of BindArguments. In debug mode,
  // it validates that `make_literal(x).qvalue == qvalue`.
  absl::Nullable<::arolla::expr::ExprNodePtr> MakeLiteral(
      TypedValue&& value) const;

  // Default constructible.
  AuxBindingPolicy() = default;
  virtual ~AuxBindingPolicy() = default;

  // Non-copyable.
  AuxBindingPolicy(const AuxBindingPolicy&) = delete;
  AuxBindingPolicy& operator=(const AuxBindingPolicy&) = delete;

 protected:
  // Returns `value` wrapped as a literal expr, or nullptr and raises a python
  // exception.
  virtual absl::Nullable<::arolla::expr::ExprNodePtr> DoMakeLiteral(
      TypedValue&& value) const = 0;
};

}  // namespace arolla::python

#endif  // PY_AROLLA_ABC_PY_AUX_BINDING_POLICY_H_
