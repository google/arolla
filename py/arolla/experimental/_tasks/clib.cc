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
#include <stdexcept>
#include <utility>

#include "absl/log/check.h"
#include "arolla/util/cancellation.h"
#include "py/arolla/abc/py_cancellation.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/experimental/_tasks/py_lock.h"
#include "py/arolla/experimental/_tasks/python_callback_bridge.h"
#include "py/arolla/py_utils/py_cancellation_controller.h"
#include "pybind11/pybind11.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

// To make the subscription mechanism more pythonic, we inverse the semantic of
// the subscription object, so that the subscription remains active unless
// the user explicitly unsubscribes.
class PyCancellationContextSubscription {
 public:
  explicit PyCancellationContextSubscription(
      CancellationContext::Subscription&& subscription)
      : subscription_(std::move(subscription)) {}

  PyCancellationContextSubscription(PyCancellationContextSubscription&&) =
      default;
  PyCancellationContextSubscription& operator=(
      PyCancellationContextSubscription&&) = default;

  ~PyCancellationContextSubscription() { std::move(subscription_).Detach(); }

  void Unsubscribe() { subscription_ = CancellationContext::Subscription(); };

 private:
  CancellationContext::Subscription subscription_;
};

PyCancellationContextSubscription SubscribeToCancellation(
    PythonCallbackBridge& bridge, py::function py_cb,
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
  return PyCancellationContextSubscription(cancellation_context->Subscribe(
      bridge.WrapPythonCallback(std::move(py_cb))));
}

PYBIND11_MODULE(clib, m) {
  py::class_<PythonCallbackBridge>(m, "PythonCallbackBridge")
      .def(py::init<>())
      .def("close", &PythonCallbackBridge::Close);

  py::class_<PyCancellationContextSubscription>(
      m, "CancellationContextSubscription")
      .def("unsubscribe", &PyCancellationContextSubscription::Unsubscribe)
      .def("__enter__", [](py::object& self) { return self; })
      .def("__exit__", [](PyCancellationContextSubscription& self,
                          py::handle /* exc_type */, py::handle /* exc_value */,
                          py::handle /* trace */) { self.Unsubscribe(); })
      .doc() = R"(Subscription to a cancellation context.

  Example:
    with arolla_tasks.subscribe_to_cancellation(callback):
      ...
  )";

  m.def("subscribe_to_cancellation", &SubscribeToCancellation,
        py::arg("bridge"), py::arg("callback"),
        py::arg("cancellation_context") = py::none());

  m.add_object("Lock", pybind11_steal_or_throw<py::type>(PyLockType()));
}

}  // namespace
}  // namespace arolla::python
