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
#include "py/arolla/abc/eval_options.h"

#include <Python.h>

#include "absl//base/nullability.h"

namespace arolla::python {

bool ParseExprCompilationOptions(absl::Nonnull<PyObject*> py_dict_options,
                                 ExprCompilationOptions& options) {
  if (!PyDict_Check(py_dict_options)) {
    return PyErr_Format(PyExc_TypeError, "expected a dict, got options: %s",
                        Py_TYPE(py_dict_options)->tp_name);
  }
  PyObject *py_option_name, *py_option_value;
  Py_ssize_t pos = 0;
  while (
      PyDict_Next(py_dict_options, &pos, &py_option_name, &py_option_value)) {
    if (!PyUnicode_CheckExact(py_option_name)) {
      return PyErr_Format(PyExc_TypeError,
                          "expected all options.keys() to be strings, got %s",
                          Py_TYPE(py_option_name)->tp_name);
    }
    if (PyUnicode_CompareWithASCIIString(py_option_name,
                                         "enable_expr_stack_trace") == 0) {
      if (!PyBool_Check(py_option_value)) {
        return PyErr_Format(PyExc_TypeError,
                            "expected value of `enable_expr_stack_trace` in "
                            "`options` to be boolean, got %s",
                            Py_TYPE(py_option_value)->tp_name);
      }
      options.verbose_runtime_errors = (py_option_value == Py_True);
    } else {
      return PyErr_Format(PyExc_ValueError,
                          "unexpected expr compiler option %R", py_option_name);
    }
  }
  return true;
}

}  // namespace arolla::python
