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
// IMPORTANT: All the following functions assume that the current thread is
// ready to call the Python C API. You can find extra information in
// documentation for PyGILState_Ensure() and PyGILState_Release().

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_CANCELLATION_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_CANCELLATION_H_

#include <Python.h>

namespace arolla::python {

// Returns PyCancellationContext type (or nullptr and sets a python exception).
PyTypeObject* PyCancellationContextType();

// Definitions of the functions:
// go/keep-sorted start block=yes newline_separated=yes
// def cancelled(...)
extern const PyMethodDef kDefPyCancelled;

// def current_cancellation_context(...)
extern const PyMethodDef kDefPyCurrentCancellationContext;

// def raise_if_cancelled(...)
extern const PyMethodDef kDefPyRaiseIfCancelled;

// def run_in_cancellation_context(...)
extern const PyMethodDef kDefPyRunInCancellationContext;

// def run_in_default_cancellation_context(...)
extern const PyMethodDef kDefPyRunInDefaultCancellationContext;

// def simulate_SIGINT(...)
extern const PyMethodDef kDefPySimulateSIGINT;

// go/keep-sorted end

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_CANCELLATION_H_
