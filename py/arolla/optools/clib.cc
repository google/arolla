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
#include <Python.h>

#include <stdexcept>
#include <vector>

#include "absl/strings/string_view.h"
#include "arolla/codegen/operator_package/operator_package.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11_abseil/absl_casters.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::operator_package::DumpOperatorPackageProto;

PYBIND11_MODULE(clib, m) {
  py::options options;
  options.disable_function_signatures();

  m.def(
      "dumps_operator_package",
      [](const std::vector<absl::string_view> op_names) {
        auto result = pybind11_unstatus_or(DumpOperatorPackageProto(op_names));
        return py::bytes(result.SerializeAsString());
      },
      py::arg("op_names"), py::pos_only(),
      py::doc(
          "dumps_operator_package(op_names, /)\n"
          "--\n\n"
          "Returns an operator package containing the specified operators.\n\n"
          "If an operator from the op_names list is used to declare other\n"
          "operators on the list, it must be mentioned before its first\n"
          "use, meaning operators should be listed in topological order.\n"
          "Operators used in implementations but not listed are considered\n"
          "prerequisites."));

  m.def(
      "internal_run_and_record_expr_source_locations",
      [](py::dict py_sink, py::function py_fn) -> py::object {
        PyExprSourceLocationMap sink;
        py::object result = pybind11_steal_or_throw<py::object>(
            CallAndRecordPyExprSourceLocations(py_fn.ptr(), sink));
        for (auto it = sink.begin(); it != sink.end(); ++it) {
          if (it->second.has_value()) {
            py_sink[py::cast(it->first)] = py::make_tuple(
                it->second->lasti,
                py::reinterpret_steal<py::object>(it->second->code.release()));
          }
        }
        return result;
      },
      py::arg("sink"), py::arg("fn"),
      py::doc("internal_run_and_record_expr_source_locations(sink, fn)\n"
              "--\n\n"
              "(internal) Invokes `fn` and populates `sink` with the source "
              "locations of all expressions created during the call."));

  m.def(
      "resolve_source_location",
      [](py::object code, int lasti) -> py::tuple {
        if (!PyCode_Check(code.ptr())) {
          throw py::type_error("expected a code object");
        }
        PyCodeObject* py_code = reinterpret_cast<PyCodeObject*>(code.ptr());
        PyObjectPtr py_code_bytes = PyObjectPtr::Own(PyCode_GetCode(py_code));
        if (py_code_bytes == nullptr) {
          throw py::error_already_set();
        }
        if (lasti < 0 || lasti >= PyBytes_GET_SIZE(py_code_bytes.get())) {
          throw py::value_error("lasti out of range");
        }
        int start_line = 0, start_col = 0, end_line = 0, end_col = 0;
        if (PyCode_Addr2Location(py_code, lasti, &start_line, &start_col,
                                 &end_line, &end_col)) {
          return py::make_tuple(start_line, start_col, end_line, end_col);
        }
        // NOTE: In the current (3.13) Python implementation Addr2Location never
        // returns false. Consequently, it is not documented whether it sets an
        // error in case of 0 returned, so we raise a new one just in case.
        throw std::runtime_error("PyCode_Addr2Location failed");
      },
      py::arg("code"), py::arg("lasti"),
      py::doc("resolve_source_location(code, lasti, /)\n"
              "--\n\n"
              "Resolves the source location (start_line, start_column, "
              "end_line, end_column) for the given code object and lasti."));
}

}  // namespace
}  // namespace arolla::python
