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
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "arolla/expr/eval/verbose_runtime_error.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status.h"
#include "py/arolla/py_utils/error_converter_registry.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {
namespace {

using ::arolla::expr::VerboseRuntimeError;

void ConvertVerboseRuntimeError(const absl::Status& status) {
  const auto* runtime_error = GetPayload<VerboseRuntimeError>(status);
  CHECK(runtime_error != nullptr);  // Only called when this payload is present.

  auto* cause = GetCause(status);
  if (cause == nullptr) {
    PyErr_Format(PyExc_ValueError,
                 "invalid VerboseRuntimeError(status.code=%d, "
                 "status.message='%s', operator_name='%s')",
                 status.code(), std::string(status.message()).c_str(),
                 runtime_error->operator_name.c_str());
    return;
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
  PyErr_RestoreRaisedException(std::move(py_exception));
  PyErr_AddNote(absl::StrCat("operator_name: ", runtime_error->operator_name));
}

AROLLA_INITIALIZER(.init_fn = [] {
  return RegisterErrorConverter<VerboseRuntimeError>(
      ConvertVerboseRuntimeError);
})

}  // namespace
}  // namespace arolla::python
