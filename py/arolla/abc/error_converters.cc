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
#include <Python.h>

#include <string>

#include "absl/status/status.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/py_utils/status_payload_handler_registry.h"
#include "arolla/expr/errors.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

// Returns `arolla.abc.errors.VerboseRuntimeError` class if it was already
// imported, or nullptr otherwise.
PyObjectPtr GetPyVerboseRuntimeErrorOrNull() {
  static PyObject* module_name =
      PyUnicode_InternFromString("arolla.abc.errors");
  static PyObject* class_name =
      PyUnicode_InternFromString("VerboseRuntimeError");

  auto py_module = PyObjectPtr::Own(PyImport_GetModule(module_name));
  if (py_module == nullptr) {
    PyErr_Clear();
    return nullptr;
  }
  auto result = PyObjectPtr::Own(PyObject_GetAttr(py_module.get(), class_name));
  PyErr_Clear();
  return result;
}

bool HandleVerboseRuntimeError(const absl::Status& status) {
  const auto* runtime_error = GetPayload<expr::VerboseRuntimeError>(status);
  if (runtime_error == nullptr) {
    return false;
  }

  PyObjectPtr py_class = GetPyVerboseRuntimeErrorOrNull();
  if (py_class == nullptr) {
    // Fallback to the default handler if the errors module is not available.
    return false;
  }
  std::string status_str = StatusToString(status);
  PyObjectPtr py_message = PyObjectPtr::Own(
      PyUnicode_FromStringAndSize(status_str.data(), status_str.size()));
  if (py_message == nullptr) {
    return true;  // Error already set.
  }
  PyObjectPtr py_operator_name = PyObjectPtr::Own(
      PyUnicode_FromStringAndSize(runtime_error->operator_name.data(),
                                  runtime_error->operator_name.size()));
  if (py_operator_name == nullptr) {
    return true;  // Error already set.
  }
  PyObjectPtr py_exception = PyObjectPtr::Own(PyObject_CallFunctionObjArgs(
      py_class.get(), py_message.get(), py_operator_name.get(), nullptr));
  if (py_exception == nullptr) {
    return true;  // Error already set.
  }
  PyErr_SetObject(reinterpret_cast<PyObject*>(Py_TYPE(py_exception.get())),
                  py_exception.get());
  return true;
}

AROLLA_INITIALIZER(.init_fn = []() -> absl::Status {
  RETURN_IF_ERROR(RegisterStatusHandler(HandleVerboseRuntimeError));
  return absl::OkStatus();
})

}  // namespace
}  // namespace arolla::python
