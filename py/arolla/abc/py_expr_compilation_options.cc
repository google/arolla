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
#include "py/arolla/abc/py_expr_compilation_options.h"

#include <Python.h>

#include "absl/base/nullability.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {

bool ParseExprCompilationOptions(PyObject* absl_nonnull py_dict_options,
                                 ExprCompilationOptions& options) {
  DCheckPyGIL();
  auto parse_bool_option = [&](absl::string_view option_name,
                               PyObject* absl_nonnull py_option_value,
                               bool* result) -> bool {
    if (Py_IsTrue(py_option_value)) {
      *result = true;
      return true;
    } else if (Py_IsFalse(py_option_value)) {
      *result = false;
      return true;
    }
    PyErr_SetString(
        PyExc_TypeError,
        absl::StrFormat("expected value of `%s` in `options` to be boolean, "
                        "got %s",
                        option_name, Py_TYPE(py_option_value)->tp_name)
            .c_str());
    return false;
  };
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
      if (!parse_bool_option("enable_expr_stack_trace", py_option_value,
                             &options.enable_expr_stack_trace)) {
        return false;
      }
    } else if (PyUnicode_CompareWithASCIIString(
                   py_option_name, "enable_literal_folding") == 0) {
      if (!parse_bool_option("enable_literal_folding", py_option_value,
                             &options.enable_literal_folding)) {
        return false;
      }
    } else if (PyUnicode_CompareWithASCIIString(
                   py_option_name, "enable_expr_optimization") == 0) {
      if (!parse_bool_option("enable_expr_optimization", py_option_value,
                             &options.enable_expr_optimization)) {
        return false;
      }
    } else {
      return PyErr_Format(
          PyExc_ValueError,
          "unexpected expr compiler option %R; supported options are:"
          " enable_expr_stack_trace (bool),"
          " enable_expr_optimization (bool),"
          " enable_literal_folding (bool)",
          py_option_name);
    }
  }
  return true;
}

}  // namespace arolla::python
