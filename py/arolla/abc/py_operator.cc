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
#include "py/arolla/abc/py_operator.h"

#include <Python.h>

#include <string>

#include "absl/base/nullability.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla::python {

using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorRegistry;

ExprOperatorPtr /*nullable*/ UnwrapPyOperator(PyObject* py_qvalue_operator) {
  DCheckPyGIL();
  if (!IsPyQValueInstance(py_qvalue_operator)) {
    PyErr_Format(PyExc_TypeError, "expected an operator, got %s",
                 Py_TYPE(py_qvalue_operator)->tp_name);
    return nullptr;
  }
  const auto& qvalue = UnsafeUnwrapPyQValue(py_qvalue_operator);
  if (qvalue.GetType() != GetQType<ExprOperatorPtr>()) {
    PyErr_Format(PyExc_TypeError, "expected an operator, got %s",
                 std::string(qvalue.GetType()->name()).c_str());
    return nullptr;
  }
  return qvalue.UnsafeAs<ExprOperatorPtr>();
}

absl::Nullable<arolla::expr::ExprOperatorPtr> ParseArgPyOperator(
    const char* fn_name, PyObject* py_op) {
  if (IsPyQValueInstance(py_op)) {
    auto& qvalue_op = UnsafeUnwrapPyQValue(py_op);
    if (qvalue_op.GetType() != GetQType<ExprOperatorPtr>()) {
      PyErr_Format(PyExc_TypeError, "%s() expected Operator|str, got op: %s",
                   fn_name, Py_TYPE(py_op)->tp_name);
      return nullptr;
    }
    return qvalue_op.UnsafeAs<ExprOperatorPtr>();
  }
  Py_ssize_t op_name_size = 0;
  const char* op_name_data = PyUnicode_AsUTF8AndSize(py_op, &op_name_size);
  if (op_name_data == nullptr) {
    PyErr_Clear();
    PyErr_Format(PyExc_TypeError, "%s() expected Operator|str, got op: %s",
                 fn_name, Py_TYPE(py_op)->tp_name);
    return nullptr;
  }
  if (auto result = ExprOperatorRegistry::GetInstance()->LookupOperatorOrNull(
          absl::string_view(op_name_data, op_name_size))) {
    return result;
  }
  PyErr_Format(PyExc_LookupError, "%s() operator not found: %R", fn_name,
               py_op);
  return nullptr;
}

}  // namespace arolla::python
