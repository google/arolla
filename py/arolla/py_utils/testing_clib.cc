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
// Python extension module exposing some endpoints for testing purposes.

#include <Python.h>

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "py/arolla/py_utils/py_object_as_status_payload.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "arolla/util/init_arolla.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(testing_clib, m) {
  // Note: Use a test local wrapper for absl::Status to mitigate an ODR
  // violation (https://github.com/pybind/pybind11_abseil/issues/20).
  struct AbslStatus {
    absl::Status status;
  };
  InitArolla();

  py::class_<AbslStatus> absl_status(m, "AbslStatus");
  absl_status
      .def(py::init([](int status_code, absl::string_view message) {
        return AbslStatus{
            absl::Status(static_cast<absl::StatusCode>(status_code), message)};
      }))
      .def("ok", [](const AbslStatus& self) { return self.status.ok(); })
      .def("code",
           [](const AbslStatus& self) {
             return static_cast<int>(self.status.code());
           })
      .def("message",
           [](const AbslStatus& self) -> py::str {
             return self.status.message();
           })
      .def("SetPayload",
           [](AbslStatus& self, absl::string_view type_url,
              absl::string_view payload) {
             self.status.SetPayload(type_url, absl::Cord(payload));
           })
      .def("AllPayloads", [](const AbslStatus& self) {
        py::dict result;
        self.status.ForEachPayload(
            [&](absl::string_view type_url, const absl::Cord& payload) {
              result[py::bytes(type_url)] = py::bytes(std::string(payload));
            });
        return result;
      });

  // go/keep-sorted start block=yes newline_separated=yes
  m.add_object("ABSL_STATUS_CODE_ABORTED",
               py::int_(static_cast<int>(absl::StatusCode::kAborted)));

  m.add_object(
      "ABSL_STATUS_CODE_FAILED_PRECONDITION",
      py::int_(static_cast<int>(absl::StatusCode::kFailedPrecondition)));

  m.add_object("ABSL_STATUS_CODE_INVALID_ARGUMENT",
               py::int_(static_cast<int>(absl::StatusCode::kInvalidArgument)));

  m.add_object("ABSL_STATUS_CODE_NOT_FOUND",
               py::int_(static_cast<int>(absl::StatusCode::kNotFound)));

  m.add_object("PY_EXCEPTION", py::bytes(kPyException));

  m.add_object("PY_EXCEPTION_CAUSE", py::bytes(kPyExceptionCause));

  m.def("bind_member", [](py::object member, py::object obj) {
    return py::reinterpret_steal<py::object>(
        PyObject_BindMember(PyObjectPtr::NewRef(member.ptr()), obj.ptr())
            .release());
  });

  m.def("call_format_from_cause", [] {
    PyErr_Format(PyExc_ValueError, "%s", "first error");
    PyErr_FormatFromCause(PyExc_TypeError, "%s", "second error");
    PyErr_FormatFromCause(PyExc_AssertionError, "%s", "third error");
    throw py::error_already_set();
  });

  m.def("call_member", [](py::object member, py::object self, py::object args,
                          py::object kwargs) {
    auto result = PyObject_CallMember(PyObjectPtr::NewRef(member.ptr()),
                                      self.ptr(), args.ptr(), kwargs.ptr());
    if (result == nullptr) {
      throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(result.release());
  });

  m.def("can_call_check_signal", [] { return PyErr_CanCallCheckSignal(); });

  m.def("default_raise_from_status", [](const AbslStatus& absl_status) {
    DefaultSetPyErrFromStatus(absl_status.status);
    throw py::error_already_set();
  });

  m.def("lookup_type_member", [](py::type type, py::str attr) -> py::object {
    auto result = PyType_LookupMemberOrNull(
        reinterpret_cast<PyTypeObject*>(type.ptr()), attr.ptr());
    if (result == nullptr) {
      return py::none();
    }
    return py::reinterpret_steal<py::object>(result.release());
  });

  m.def("pass_object_through_cord", [](py::object obj) -> py::object {
    auto status_or_cord =
        WrapPyObjectToCord(PyObjectGILSafePtr::NewRef(obj.ptr()));
    if (!status_or_cord.ok()) {
      SetPyErrFromStatus(status_or_cord.status());
      throw py::error_already_set();
    }
    auto status_or_py_object = UnwrapPyObjectFromCord(*status_or_cord);
    if (!status_or_py_object.ok()) {
      SetPyErrFromStatus(status_or_py_object.status());
      throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(status_or_py_object->release());
  });

  m.def("raise_from_status", [](const AbslStatus& absl_status) {
    SetPyErrFromStatus(absl_status.status);
    throw py::error_already_set();
  });

  m.def("read_object_to_status_from_status_payload",
        [](const AbslStatus& absl_status,
           absl::string_view payload_name) -> py::object {
          auto status_or_obj =
              ReadPyObjectFromStatusPayload(absl_status.status, payload_name);
          if (!status_or_obj.ok()) {
            SetPyErrFromStatus(status_or_obj.status());
            throw py::error_already_set();
          }
          if (status_or_obj.value() == nullptr) {
            return py::none();
          }
          return py::reinterpret_steal<py::object>(
              std::move(status_or_obj)->release());
        });

  m.def("status_caused_by_py_err",
        [](int status_code, absl::string_view message, py::object ex) {
          if (!ex.is_none()) {
            PyErr_SetObject((PyObject*)Py_TYPE(ex.ptr()), ex.ptr());
          }
          return AbslStatus{StatusCausedByPyErr(
              static_cast<absl::StatusCode>(status_code), message)};
        });

  m.def("status_with_raw_py_err",
        [](int status_code, absl::string_view message, py::object ex) {
          if (!ex.is_none()) {
            PyErr_SetObject((PyObject*)Py_TYPE(ex.ptr()), ex.ptr());
          }
          return AbslStatus{StatusWithRawPyErr(
              static_cast<absl::StatusCode>(status_code), message)};
        });

  m.def("vectorcall_member", [](py::object member, std::vector<py::object> args,
                                int n, py::object kwnames) {
    std::vector<PyObject*> py_args(args.size());
    for (size_t i = 0; i < args.size(); ++i) {
      py_args[i] = args[i].ptr();
    }
    auto result = PyObject_VectorcallMember(
        PyObjectPtr::NewRef(member.ptr()), py_args.data(), n,
        kwnames.is_none() ? nullptr : kwnames.ptr());
    if (result == nullptr) {
      throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(result.release());
  });

  m.def("write_object_to_status_payload", [](AbslStatus* absl_status,
                                             absl::string_view payload_name,
                                             py::object obj) {
    auto write_status =
        WritePyObjectToStatusPayload(&absl_status->status, payload_name,
                                     PyObjectGILSafePtr::NewRef(obj.ptr()));
    if (!write_status.ok()) {
      SetPyErrFromStatus(write_status);
      throw py::error_already_set();
    }
  });

  // go/keep-sorted end
}

}  // namespace
}  // namespace arolla::python
