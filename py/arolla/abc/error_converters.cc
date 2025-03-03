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

#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/py_utils/status_payload_handler_registry.h"
#include "arolla/expr/eval/verbose_runtime_error.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status.h"

namespace arolla::python {
namespace {

using ::arolla::expr::VerboseRuntimeError;

bool HandleVerboseRuntimeError(const absl::Status& status) {
  const auto* runtime_error = GetPayload<VerboseRuntimeError>(status);
  if (runtime_error == nullptr) {
    return false;
  }
  auto* cause = GetCause(status);
  DCHECK(cause != nullptr);
  if (cause == nullptr) {
    return false;
  }
  SetPyErrFromStatus(*cause);
  auto py_exception = PyErr_FetchRaisedException();
  if (auto py_operator_name = PyObjectPtr::Own(
          PyUnicode_FromStringAndSize(runtime_error->operator_name.data(),
                                      runtime_error->operator_name.size()));
      py_operator_name != nullptr) {
    PyObject_SetAttrString(py_exception.get(), "operator_name",
                           py_operator_name.get());
  }
  PyErr_Clear();
  if (PyObjectPtr::Own(PyObject_CallMethod(
          py_exception.get(), "add_note", "(s)",
          absl::StrCat("operator_name: ", runtime_error->operator_name)
              .c_str())) == nullptr) {
    PyErr_Clear();
  }
  PyErr_RestoreRaisedException(std::move(py_exception));
  return true;
}

AROLLA_INITIALIZER(.init_fn = [] {
  return RegisterStatusHandler(HandleVerboseRuntimeError);
})

}  // namespace
}  // namespace arolla::python
