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

#include <thread>  // NOLINT
#include <utility>

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "py/arolla/py_utils/py_object_bridge.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/pybind11.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(testing_clib, m) {
  py_object_bridge::Init();
  // go/keep-sorted start block=yes newline_separated=yes
  m.def(
      "schedule_callback",
      [](py::function py_cb, double delay_seconds, bool do_call) {
        auto holder = PyObjectHolder(PyObjectPtr::NewRef(py_cb.ptr()));
        std::thread([holder = std::move(holder), delay_seconds,
                     do_call]() mutable {
          if (delay_seconds > 0) {
            absl::SleepFor(absl::Seconds(delay_seconds));
          }
          if (do_call) {
            std::move(holder).DispatchAction([](const PyObjectPtr& py_cb) {
              auto py_result =
                  PyObjectPtr::Own(PyObject_CallNoArgs(py_cb.get()));
              if (py_result == nullptr) {
                PyErr_Clear();
                PyErr_WarnEx(PyExc_RuntimeWarning,
                             "unhandled exception in callback", 0);
              }
            });
          }
        }).detach();
      },
      py::arg("callback"), py::arg("delay_seconds") = 0.0,
      py::arg("do_call") = true);
  // go/keep-sorted end
}

}  // namespace
}  // namespace arolla::python
