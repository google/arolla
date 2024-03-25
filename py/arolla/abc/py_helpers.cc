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
#include "py/arolla/abc/py_helpers.h"

#include <Python.h>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/optimization/default/default_optimizer.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {

using ::arolla::expr::DefaultOptimizer;
using ::arolla::expr::DynamicEvaluationEngineOptions;

absl::StatusOr<DynamicEvaluationEngineOptions>
ParseDynamicEvaluationEngineOptions(PyObject* /*nullable*/ py_dict_options) {
  DynamicEvaluationEngineOptions options = {};
  ASSIGN_OR_RETURN(options.optimizer, DefaultOptimizer());
  if (py_dict_options == nullptr) {
    return options;
  }
  if (!PyDict_Check(py_dict_options)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a dict, got options: %s", Py_TYPE(py_dict_options)->tp_name));
  }
  PyObject *py_option_name, *py_option_value;
  Py_ssize_t pos = 0;
  while (
      PyDict_Next(py_dict_options, &pos, &py_option_name, &py_option_value)) {
    if (!PyUnicode_Check(py_option_name)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected all options.keys() to be strings, got %s",
                          Py_TYPE(py_option_name)->tp_name));
    }
    Py_ssize_t option_name_size = 0;
    const char* option_name_data =
        PyUnicode_AsUTF8AndSize(py_option_name, &option_name_size);
    if (option_name_data == nullptr) {
      return StatusCausedByPyErr(absl::StatusCode::kInvalidArgument,
                                 "invalid unicode object");
    }
    const absl::string_view option_name(option_name_data, option_name_size);
    if (option_name == "enable_expr_stack_trace") {
      if (!PyBool_Check(py_option_value)) {
        return absl::InvalidArgumentError(
            absl::StrFormat("expected value of `enable_expr_stack_trace` in "
                            "`options` to be boolean, got %s",
                            Py_TYPE(py_option_value)->tp_name));
      }
      options.enable_expr_stack_trace = (py_option_value == Py_True);
    } else {
      return absl::InvalidArgumentError(absl::StrFormat(
          "unexpected keyword argument `%s` in `options` dict", option_name));
    }
  }
  return options;
}

}  // namespace arolla::python
