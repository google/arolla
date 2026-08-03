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
#ifndef PY_AROLLA_EXPERIMENTAL__TASKS_PYTHON_CALLBACK_BRIDGE_H_
#define PY_AROLLA_EXPERIMENTAL__TASKS_PYTHON_CALLBACK_BRIDGE_H_

#include <memory>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "pybind11/pybind11.h"

namespace arolla::python {

// This class solves the problem of triggering a Python callback from a
// non-Python thread. The problem is twofold:
//   * Owning a Python callback -- how to dispose of a callback without
//   interacting directly with the Python interpreter.
//   * Running a Python callback -- how to execute a Python callback without
//   interacting directly with the Python interpreter.
//
// IMPORTANT: All methods must be called with the GIL held. However,
// the callback produced by WrapPythonCallback() does not interact with
// the Python API directly and can be executed in a non-Python thread.
//
class PythonCallbackBridge {
 public:
  // It's expected that the caller holds the GIL.
  PythonCallbackBridge();
  ~PythonCallbackBridge();

  PythonCallbackBridge(PythonCallbackBridge&) = delete;
  PythonCallbackBridge& operator=(PythonCallbackBridge&) = delete;

  // Returns a C++ wrapper corresponding to a Python callback.
  // The resulting C++ wrapper does not interact with the Python API directly,
  // delegating all interactions to a private python thread owned by the bridge.
  absl::AnyInvocable<void() &&> WrapPythonCallback(pybind11::function py_cb);

  // This method shuts down the bridge and waits for the worker thread to stop.
  //
  // It is advised to call this method explicitly, since the destructor does
  // not wait for the worker thread to stop, which might lead to UB during
  // Python interpreter shutdown.
  void Close();

 private:
  class Impl;
  std::shared_ptr<Impl> absl_nullable impl_;
  pybind11::object worker_thread_;
};

}  // namespace arolla::python

#endif  // PY_AROLLA_EXPERIMENTAL__TASKS_PYTHON_CALLBACK_BRIDGE_H_
