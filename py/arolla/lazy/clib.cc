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
// Python extension module with Arolla lazy type.
//

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/options.h"
#include "pybind11/pybind11.h"
#include "pybind11/pytypes.h"
#include "arolla/lazy/lazy.h"
#include "arolla/lazy/lazy_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(clib, m) {
  py::options options;
  options.disable_function_signatures();

  m.def(
      "make_lazy_from_callable",
      [](QTypePtr value_qtype, py::function callable) {
        return MakeLazyQValue(MakeLazyFromCallable(
            value_qtype,
            [callable = PyObjectGILSafePtr::Own(
                 callable.release().ptr())]() -> absl::StatusOr<TypedValue> {
              AcquirePyGIL guard;
              auto py_result =
                  PyObjectPtr::Own(PyObject_CallNoArgs(callable.get()));
              if (py_result == nullptr) {
                return StatusCausedByPyErr(
                    absl::StatusCode::kFailedPrecondition,
                    "a lazy callable has failed");
              }
              auto* result = UnwrapPyQValue(py_result.get());
              if (result == nullptr) {
                return StatusCausedByPyErr(
                    absl::StatusCode::kFailedPrecondition,
                    "a lazy callable returned unexpected python type");
              }
              return *result;
            }));
      },
      py::arg("value_qtype"), py::arg("callable"), py::pos_only(),
      py::doc("make_lazy_from_callable(value_qtype, callable, /)\n"
              "--\n\n"
              "Returns a lazy value constructed from a python callable."));
}

}  // namespace
}  // namespace arolla::python
