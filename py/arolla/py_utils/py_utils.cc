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
#include "py/arolla/py_utils/py_utils.h"

#include <Python.h>

#include <cstdarg>
#include <cstddef>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "py/arolla/py_utils/status_payload_handler_registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

// Payload for arolla::StructuredErrorPayload that represents a Python
// exception.
struct PythonExceptionPayload {
  // We need to use GIL-safe pointer because the Status can be destructed in C++
  // code that is not holding the GIL.
  PyObjectGILSafePtr py_exception;
};

std::string StatusToString(const absl::Status& status) {
  std::ostringstream message;

  // Include the status code, unless it's StatusCode::kInvalidArgument.
  if (status.code() != absl::StatusCode::kInvalidArgument) {
    message << "[" << absl::StatusCodeToString(status.code()) << "]";
  }
  if (auto status_message = absl::StripAsciiWhitespace(status.message());
      !status_message.empty()) {
    if (message.tellp() > 0) {
      message << ' ';
    }
    message << status_message;
  }

  return message.str();
}

bool HandlePyExceptionFromStatus(const absl::Status& status) {
  const auto* payload = GetPayload<PythonExceptionPayload>(status);
  if (payload == nullptr) {
    return false;
  }
  PyErr_RestoreRaisedException(
      PyObjectPtr::NewRef(payload->py_exception.get()));
  return true;
}

}  // namespace

void DefaultSetPyErrFromStatus(const absl::Status& status) {
  PyErr_SetString(PyExc_ValueError, StatusToString(status).c_str());
}

std::nullptr_t SetPyErrFromStatus(const absl::Status& status) {
  DCheckPyGIL();
  DCHECK(!status.ok());

  PyObjectPtr pcause;
  if (auto cause = GetCause(status); cause != nullptr) {
    SetPyErrFromStatus(*cause);
    pcause = PyErr_FetchRaisedException();
  }

  if (!arolla::python::CallStatusHandlers(status)) {
    DefaultSetPyErrFromStatus(status);
  }

  if (pcause != nullptr) {
    PyObjectPtr pexception = PyErr_FetchRaisedException();
    PyException_SetCause(pexception.get(), Py_NewRef(pcause.get()));
    PyException_SetContext(pexception.get(), pcause.release());
    PyErr_RestoreRaisedException(std::move(pexception));
  }

  return nullptr;
}

absl::Status StatusCausedByPyErr(absl::StatusCode code,
                                 absl::string_view message) {
  auto cause = StatusWithRawPyErr(absl::StatusCode::kInternal, "unused");
  if (cause.ok()) {
    return absl::OkStatus();
  }
  return WithCause(absl::Status(code, message), std::move(cause));
}

absl::Status StatusWithRawPyErr(absl::StatusCode code,
                                absl::string_view message) {
  DCheckPyGIL();

  // Fetch and normalize the python exception.
  PyObjectPtr py_exception = PyErr_FetchRaisedException();
  if (py_exception == nullptr) {
    return absl::OkStatus();
  }
  // TODO: Consider extracting exception __cause__ or __context__
  // into a nested absl::Status.
  return WithPayload(
      absl::Status(code, message),
      PythonExceptionPayload{
          .py_exception = PyObjectGILSafePtr::Own(py_exception.release())});
}

PyObjectPtr PyErr_FetchRaisedException() {
  DCheckPyGIL();
  PyObject *ptype, *pvalue, *ptraceback;
  PyErr_Fetch(&ptype, &pvalue, &ptraceback);
  if (ptype == nullptr) {
    return PyObjectPtr{};
  }
  PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
  if (ptraceback != nullptr) {
    PyException_SetTraceback(pvalue, ptraceback);
    Py_DECREF(ptraceback);
  }
  Py_DECREF(ptype);
  return PyObjectPtr::Own(pvalue);
}

std::nullptr_t PyErr_RestoreRaisedException(PyObjectPtr py_exception) {
  DCheckPyGIL();
  auto* py_type = Py_NewRef(Py_TYPE(py_exception.get()));
  auto* py_traceback = PyException_GetTraceback(py_exception.get());
  PyErr_Restore(py_type, py_exception.release(), py_traceback);
  return nullptr;
}

bool PyTuple_AsSpan(PyObject* /*nullable*/ py_obj,
                    absl::Span<PyObject*>* result) {
  if (py_obj != nullptr && (PyTuple_Check(py_obj) || PyList_Check(py_obj))) {
    *result = absl::Span<PyObject*>(PySequence_Fast_ITEMS(py_obj),
                                    PySequence_Fast_GET_SIZE(py_obj));
    return true;
  }
  return false;
}

PyObjectPtr PyType_LookupMemberOrNull(PyTypeObject* py_type,
                                      PyObject* py_str_attr) {
  DCheckPyGIL();
  // Note: We use the _PyType_Lookup() function for efficiency, even though it
  // is technically private. This function is used in multiple projects,
  // including PyBind11, so we consider it to be safe and stable.
  return PyObjectPtr::NewRef(_PyType_Lookup(py_type, py_str_attr));
}

PyObjectPtr PyObject_BindMember(PyObjectPtr&& py_member, PyObject* self) {
  DCheckPyGIL();
  // If the `member` object has a method `__get__`, we follow the Python
  // descriptor protocol. See:
  // https://docs.python.org/3/howto/descriptor.html#functions-and-methods
  PyTypeObject* py_type_member = Py_TYPE(py_member.get());
  if (py_type_member->tp_descr_get != nullptr) {
    return PyObjectPtr::Own(py_type_member->tp_descr_get(
        py_member.get(), self, reinterpret_cast<PyObject*>(Py_TYPE(self))));
  }
  return std::move(py_member);
}

PyObjectPtr PyObject_CallMember(PyObjectPtr&& py_member, PyObject* self,
                                PyObject* args, PyObject* kwargs) {
  DCheckPyGIL();
  auto py_attr = PyObject_BindMember(std::move(py_member), self);
  if (py_attr == nullptr) {
    return PyObjectPtr{};
  }
  return PyObjectPtr::Own(PyObject_Call(py_attr.get(), args, kwargs));
}

PyObjectPtr PyObject_VectorcallMember(PyObjectPtr&& py_member, PyObject** args,
                                      Py_ssize_t nargsf, PyObject* kwnames) {
  DCheckPyGIL();
  const auto nargs = PyVectorcall_NARGS(nargsf);
  if (nargs == 0) {
    PyErr_SetString(PyExc_TypeError, "no arguments provided");
    return PyObjectPtr{};
  }
  PyTypeObject* py_type_member = Py_TYPE(py_member.get());
  if (PyType_HasFeature(py_type_member, Py_TPFLAGS_METHOD_DESCRIPTOR)) {
    return PyObjectPtr::Own(
        PyObject_Vectorcall(py_member.get(), args, nargsf, kwnames));
  }
  auto py_attr = PyObject_BindMember(std::move(py_member), args[0]);
  if (py_attr == nullptr) {
    return PyObjectPtr{};
  }
  return PyObjectPtr::Own(PyObject_Vectorcall(
      py_attr.get(), args + 1, (nargs - 1) | PY_VECTORCALL_ARGUMENTS_OFFSET,
      kwnames));
}

std::nullptr_t PyErr_FormatFromCause(PyObject* py_exc, const char* format,
                                     ...) {
  DCheckPyGIL();
  PyObjectPtr py_exception_cause = PyErr_FetchRaisedException();
  DCHECK(py_exception_cause != nullptr);
  va_list vargs;
  va_start(vargs, format);
  PyErr_FormatV(py_exc, format, vargs);
  va_end(vargs);
  PyObjectPtr py_exception = PyErr_FetchRaisedException();
  DCHECK(py_exception != nullptr);
  PyException_SetCause(py_exception.get(), Py_NewRef(py_exception_cause.get()));
  PyException_SetContext(py_exception.get(), py_exception_cause.release());
  PyErr_RestoreRaisedException(std::move(py_exception));
  return nullptr;
}

AROLLA_INITIALIZER(.init_fn = []() -> absl::Status {
  RETURN_IF_ERROR(RegisterStatusHandler(HandlePyExceptionFromStatus));
  return absl::OkStatus();
})

}  // namespace arolla::python
