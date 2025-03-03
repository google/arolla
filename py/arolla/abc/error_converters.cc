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

#include "absl/status/status.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/py_utils/status_payload_handler_registry.h"
#include "arolla/expr/errors.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status.h"

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
  PyObjectPtr py_class_exception = GetPyVerboseRuntimeErrorOrNull();
  if (py_class_exception == nullptr) {
    // Fallback to the default handler if the errors module is not available.
    return false;
  }
  auto py_exception = PyObjectPtr::Own(PyObject_CallFunction(
      py_class_exception.get(), "(ss)", StatusToString(status).c_str(),
      runtime_error->operator_name.c_str()));
  if (py_exception == nullptr) {
    return true;  // Error already set.
  }
  if (auto* cause = GetCause(status)) {
    SetPyErrFromStatus(*cause);
    PyException_SetCauseAndContext(py_exception.get(),
                                   PyErr_FetchRaisedException());
  }
  PyErr_SetObject(reinterpret_cast<PyObject*>(Py_TYPE(py_exception.get())),
                  py_exception.get());
  return true;
}

AROLLA_INITIALIZER(.init_fn = [] {
  return RegisterStatusHandler(HandleVerboseRuntimeError);
})

}  // namespace
}  // namespace arolla::python
