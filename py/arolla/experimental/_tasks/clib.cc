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
#include <utility>

#include "arolla/util/cancellation.h"
#include "py/arolla/abc/py_cancellation.h"
#include "py/arolla/experimental/_tasks/python_callback_bridge.h"
#include "py/arolla/py_utils/py_cancellation_controller.h"
#include "pybind11/pybind11.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

void SubscribeToCancellation(PythonCallbackBridge& bridge, py::function py_cb,
                             py::object py_cancellation_context) {
  CancellationContextPtr cancellation_context;
  if (py_cancellation_context.is_none()) {
    cancellation_context = CurrentCancellationContext();
    if (cancellation_context == nullptr) {
      // NOTE: Consider raising an exception without trying to acquire
      // a cancellation context from the py_cancellation_controller.
      cancellation_context =
          py_cancellation_controller::AcquirePyCancellationContext();
    }
    if (cancellation_context == nullptr) {
      throw std::runtime_error(
          "current thread has no active cancellation context");
    }
  } else {
    cancellation_context =
        UnwrapPyCancellationContext(py_cancellation_context.ptr());
    if (cancellation_context == nullptr) {
      throw py::error_already_set();
    }
  }
  DCHECK(cancellation_context != nullptr);
  cancellation_context->Subscribe(bridge.WrapPythonCallback(std::move(py_cb)))
      .Detach();
}

PYBIND11_MODULE(clib, m) {
  py::class_<PythonCallbackBridge>(m, "PythonCallbackBridge")
      .def(py::init<>())
      .def("close", &PythonCallbackBridge::Close);

  m.def("subscribe_to_cancellation", &SubscribeToCancellation);
}

}  // namespace
}  // namespace arolla::python
