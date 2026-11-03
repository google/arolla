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
#ifndef PY_AROLLA_PY_UTILS_PY_HASH_H_
#define PY_AROLLA_PY_UTILS_PY_HASH_H_

#include <Python.h>

#include <cstdint>

#include "absl/strings/string_view.h"

namespace arolla::python {

// Computes the Python `int` hash (matching `PyObject_Hash` on a `PyLong`
// object) for an `int64_t` value without allocating a `PyLong` object.
//
// IMPORTANT: Requires holding the Python GIL. Never raises Python exceptions
// and never returns -1.
Py_hash_t PyHashInt64(int64_t value);

// Computes the Python `int` hash (matching `PyObject_Hash` on a `PyLong`
// object) for a `uint64_t` value without allocating a `PyLong` object.
//
// IMPORTANT: Requires holding the Python GIL. Never raises Python exceptions
// and never returns -1.
Py_hash_t PyHashUInt64(uint64_t value);

// Computes the Python `float` hash (matching `PyObject_Hash` on a `PyFloat`
// object) for a `double` value without allocating a `PyFloat` object.
// Note: For `NaN`, returns `0` (`_Py_HashDouble(nullptr, value)`).
//
// IMPORTANT: Requires holding the Python GIL. Never raises Python exceptions
// and never returns -1.
Py_hash_t PyHashDouble(double value);

// Computes the Python `bytes` hash (matching `PyObject_Hash` on a `PyBytes`
// object) for a byte buffer without allocating or copying byte data.
//
// IMPORTANT: Requires holding the Python GIL. Never raises Python exceptions
// and never returns -1.
Py_hash_t PyHashBytes(absl::string_view value);

// Computes the Python `str` hash (matching `PyObject_Hash` on a `PyUnicode`
// object) for a valid UTF-8 string without allocating or copying string data.
//
// IMPORTANT: Requires holding the Python GIL. Never raises Python exceptions
// and never returns -1.
Py_hash_t PyHashUtf8(absl::string_view value);

// Returns true if the fast zero-allocation implementations for `PyHashInt64`,
// `PyHashUInt64`, and `PyHashUtf8` match the current Python runtime's `hash()`
// implementation.
//
// IMPORTANT: Requires holding the Python GIL. Never raises Python exceptions.
bool IsFastPyHashCompatible();

}  // namespace arolla::python

#endif  // PY_AROLLA_PY_UTILS_PY_HASH_H_
