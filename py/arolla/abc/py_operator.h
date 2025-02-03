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

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_OPERATOR_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_OPERATOR_H_

#include <Python.h>

#include "absl/base/nullability.h"
#include "arolla/expr/expr_operator.h"

namespace arolla::python {

// Returns an operator stored in operator_qvalue (or nullptr and sets a python
// exception).
absl::Nullable<arolla::expr::ExprOperatorPtr> UnwrapPyOperator(
    PyObject* py_qvalue_operator);

// A helper for parsing a function argument. Returns an operator if the call is
// successful. Otherwise returns `nullptr` and sets a Python exception.
//
// `py_op` can be either an operator instance, or a registered operator name;
// `where` is used as an error message prefix containing a public function name.
//
// Example:
//
//   auto op = ParseArgPyOperator("arolla.abc.invoke_op()", py_args[0]);
//   if (op == nullptr) {
//     return /*error*/
//   }
//
absl::Nullable<arolla::expr::ExprOperatorPtr> ParseArgPyOperator(
    const char* fn_name, PyObject* py_op);

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_QTYPE_H_
