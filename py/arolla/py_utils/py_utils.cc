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
#include <frameobject.h>

#include <cstdarg>
#include <cstddef>
#include <string>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
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

bool HandlePyExceptionFromStatus(const absl::Status& status) {
  const auto* payload = GetPayload<PythonExceptionPayload>(status);
  if (payload == nullptr) {
    return false;
  }
  DCHECK(GetCause(status) == nullptr);
  PyErr_RestoreRaisedException(
      PyObjectPtr::NewRef(payload->py_exception.get()));
  return true;
}

void DefaultSetPyErrFromStatus(const absl::Status& status) {
  PyErr_SetString(PyExc_ValueError, StatusToString(status).c_str());
  if (auto* cause = GetCause(status)) {
    auto py_exception = PyErr_FetchRaisedException();
    SetPyErrFromStatus(*cause);
    PyException_SetCauseAndContext(py_exception.get(),
                                   PyErr_FetchRaisedException());
    PyErr_RestoreRaisedException(std::move(py_exception));
  }
}

}  // namespace

std::string StatusToString(const absl::Status& status) {
  // Include the status code, unless it's StatusCode::kInvalidArgument.
  auto status_message = absl::StripAsciiWhitespace(status.message());
  if (status.code() != absl::StatusCode::kInvalidArgument) {
    if (status_message.empty()) {
      return absl::StrCat("[", absl::StatusCodeToString(status.code()), "]");
    } else {
      return absl::StrCat("[", absl::StatusCodeToString(status.code()), "] ",
                          status_message);
    }
  } else {
    return std::string(status_message);
  }
}

std::nullptr_t SetPyErrFromStatus(const absl::Status& status) {
  DCheckPyGIL();
  DCHECK(!status.ok());
  if (!CallStatusHandlers(status)) {
    DefaultSetPyErrFromStatus(status);
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
    return nullptr;
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

void PyException_SetCauseAndContext(
    PyObject* py_exception, PyObjectPtr /*nullable*/ py_exception_cause) {
  DCheckPyGIL();
  DCHECK(py_exception != nullptr);
  PyException_SetCause(py_exception, Py_NewRef(py_exception_cause.get()));
  PyException_SetContext(py_exception, py_exception_cause.release());
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
    return nullptr;
  }
  return PyObjectPtr::Own(PyObject_Call(py_attr.get(), args, kwargs));
}

PyObjectPtr PyObject_VectorcallMember(PyObjectPtr&& py_member, PyObject** args,
                                      Py_ssize_t nargsf, PyObject* kwnames) {
  DCheckPyGIL();
  const auto nargs = PyVectorcall_NARGS(nargsf);
  if (nargs == 0) {
    PyErr_SetString(PyExc_TypeError, "no arguments provided");
    return nullptr;
  }
  PyTypeObject* py_type_member = Py_TYPE(py_member.get());
  if (PyType_HasFeature(py_type_member, Py_TPFLAGS_METHOD_DESCRIPTOR)) {
    return PyObjectPtr::Own(
        PyObject_Vectorcall(py_member.get(), args, nargsf, kwnames));
  }
  auto py_attr = PyObject_BindMember(std::move(py_member), args[0]);
  if (py_attr == nullptr) {
    return nullptr;
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
  auto py_exception = PyErr_FetchRaisedException();
  PyException_SetCauseAndContext(py_exception.get(),
                                 std::move(py_exception_cause));
  PyErr_RestoreRaisedException(std::move(py_exception));
  return nullptr;
}

AROLLA_INITIALIZER(.init_fn = []() -> absl::Status {
  RETURN_IF_ERROR(RegisterStatusHandler(HandlePyExceptionFromStatus));
  return absl::OkStatus();
})

bool PyTraceback_Add(const char* function_name, const char* file_name,
                     int line) {
  DCheckPyGIL();
  PyFrameObject* py_frame = nullptr;
  absl::Cleanup guard = [&] { Py_XDECREF(py_frame); };
  {
    // Temporarily retrieve the active error to allow free use of Python C API
    // during `py_frame` initialization; restore the error afterward.
    PyObjectPtr py_exception = PyErr_FetchRaisedException();
    if (py_exception == nullptr) {
      return false;
    }
    absl::Cleanup py_exception_guard = [&] {
      PyErr_Clear();  // Explicitly clear any existing error to avoid relying on
                      // the behavior of `PyErr_Restore` in such cases.
      PyErr_RestoreRaisedException(std::move(py_exception));
    };
    auto py_globals = PyObjectPtr::Own(PyDict_New());
    if (py_globals == nullptr) {
      return false;
    }
    PyCodeObject* py_code = PyCode_NewEmpty(file_name, function_name, line);
    absl::Cleanup py_code_guard = [&] { Py_XDECREF(py_code); };
    if (py_code == nullptr) {
      return false;
    }
    py_frame =
        PyFrame_New(PyThreadState_GET(), py_code, py_globals.get(), nullptr);
    if (py_frame == nullptr) {
      return false;
    }
  }
  return PyTraceBack_Here(py_frame) == 0;
}

}  // namespace arolla::python
