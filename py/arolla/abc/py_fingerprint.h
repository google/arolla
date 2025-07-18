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

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_FINGERPRINT_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_FINGERPRINT_H_

#include <Python.h>

#include "arolla/util/fingerprint.h"

namespace arolla::python {

// Returns PyFingerprint type (or nullptr and sets a python exception).
PyTypeObject* PyFingerprintType();

// Returns true if the argument is a PyFingerprint instance.
bool IsPyFingerprintInstance(PyObject* py_object);

// Returns PyBytes object (or nullptr and sets a python exception).
PyObject* WrapAsPyFingerprint(const Fingerprint& fingerprint);

// Returns a fingerprint (or nullptr and sets a python exception).
const Fingerprint* UnwrapPyFingerprint(PyObject* py_fingerprint);

// Returns the fingerprint stored in the given PyFingerprint instance. The
// argument must be a PyFingerprint instance.
const Fingerprint& UnsafeUnwrapPyFingerprint(PyObject* py_fingerprint);

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_FINGERPRINT_H_
