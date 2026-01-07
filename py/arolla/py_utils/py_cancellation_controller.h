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
// IMPORTANT: All the following functions assume that the current thread is
// ready to call the Python C API. You can find extra information in
// documentation for PyGILState_Ensure() and PyGILState_Release().
//
#ifndef PY_AROLLA_PY_UTILS_PY_CANCELLATION_CONTROLLER_H_
#define PY_AROLLA_PY_UTILS_PY_CANCELLATION_CONTROLLER_H_

#include "absl/base/nullability.h"
#include "arolla/util/cancellation.h"

namespace arolla::python::py_cancellation_controller {

// Initializes the cancellation controller.
//
// The initialization of the cancellation controller might fail. If so,
// the remaining subsystems can be run safely; however, the controller may not
// provide a cancellation context.
void Init();

// Returns a cancellation context, if called from Python's main thread.
//
// If the current thread is not Python's main thread, or if the controller is
// non-operational (e.g., due to an initialization failure), returns `nullptr`.
//
// Note: This method never raises any python exceptions.
absl_nullable CancellationContextPtr AcquirePyCancellationContext();

// Simulate the effect of SIGINT. This function can be called from any thread
// without additional synchronisation.
void SimulateSIGINT();

// Overrides the signal handler for SIGINT.
//
// This function is unsafe because it replaces the existing SIGINT handler,
// potentially bypassing other signal handlers and directly forwarding
// the signal to the Python interpreter. However, it might be considered safe if
// the previous handler was set by Python.
//
// If this function fails, it returns `false` and sets a python exception.
bool UnsafeOverrideSignalHandler();

}  // namespace arolla::python::py_cancellation_controller

#endif  // PY_AROLLA_PY_UTILS_PY_CANCELLATION_CONTROLLER_H_
