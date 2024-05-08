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

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_ABC_BINDING_POLICIES_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_ABC_BINDING_POLICIES_H_

#include <Python.h>

#include <optional>
#include <vector>

#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_aux_binding_policy.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

// Registers a "classic" argument-binding policy backed by Python functions:
//
//   def as_qvalue_or_expr(arg: Any) -> QValue|Expr
//   def make_literal(arg: QValue) -> Expr
//
void RegisterPyClassicAuxBindingPolicyWithCustomBoxing(
    absl::string_view aux_policy, PyObject* py_callable_as_qvalue_or_expr,
    PyObject* py_callable_make_literal);

// A "classic" argument-binding policy.
//
// This argument-binding policy is compatible with arolla::expr::BindArguments.
// It only supports "positional-or-keyword" and "variadic-positional"
// parameters, with a behaviour similar to Python:
//
//   1) def unary_op(x): ...
//
//     unary_op()      -> error
//     unary_op(1)     -> (1, )
//     unary_op(x=1)   -> (1, )
//     unsary_op(1, 2) -> error
//
//   2) def binary_op(x, y): ...
//
//     binary_op()        -> error
//     binary_op(1)       -> error
//     binary_op(1, 2)    -> (1, 2)
//     binary_op(1, y=2)  -> (1, 2)
//     binary_op(1, 2, 3) -> error
//
//   3) def complex_op(x, y=unspecified, *args): ...
//
//     complex_op(1)        -> (1, unspecified)
//     complex_op(1, 2)     -> (1, 2)
//     complex_op(1, 2, 3)  -> (1, 2, 3)
//     complex_op(x=1, y=2) -> (1, 2)
//
class ClassicAuxBindingPolicyWithCustomBoxing : public AuxBindingPolicy {
 public:
  // This method provides an extension point for the "classic" argument-binding
  // policy.
  //
  // Returns QValue|Expr if successful; otherwise, it returns std::nullopt and
  // sets a Python exception.
  //
  // Note: `as_qvalue_or_expr_fn()` can raise TypeError or ValueError, with
  // the error messages intended to the client; the type of these errors will
  // generally be preserved. All other errors can be noticeably changed,
  // particularly replaced with RuntimeError.
  virtual std::optional<QValueOrExpr> AsQValueOrExpr(
      PyObject* py_arg) const = 0;

  // (See the base class.)
  PyObject* MakePythonSignature(
      const ::arolla::expr::ExprOperatorSignature& signature) const final;

  // (See the base class.)
  bool BindArguments(const ::arolla::expr::ExprOperatorSignature& signature,
                     PyObject** args, Py_ssize_t nargsf, PyObject* kwnames,
                     std::vector<QValueOrExpr>* result) const final;
};

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_ABC_BINDING_POLICIES_H_
