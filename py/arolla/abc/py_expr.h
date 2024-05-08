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

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_EXPR_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_EXPR_H_

#include <Python.h>

#include "arolla/expr/expr_node.h"

namespace arolla::python {

// Returns PyExpr type (or nullptr and sets a python exception).
PyTypeObject* PyExprType();

// Returns true if the argument is a subtype of PyExprType.
bool IsPyExprType(PyTypeObject* py_type);

// Returns true if the argument is a PyExpr instance.
bool IsPyExprInstance(PyObject* py_object);

// Returns PyExpr object (or nullptr and sets a python exception).
PyObject* WrapAsPyExpr(::arolla::expr::ExprNodePtr expr);

// Returns an expression stored in the given PyExpr instance (or nullptr and
// sets a python exception).
::arolla::expr::ExprNodePtr /*nullable*/ UnwrapPyExpr(PyObject* py_expr);

// Returns an expression stored in the given PyExpr instance. The argument must
// be a PyExpr instance.
const ::arolla::expr::ExprNodePtr& UnsafeUnwrapPyExpr(PyObject* py_expr);

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_EXPR_H_
