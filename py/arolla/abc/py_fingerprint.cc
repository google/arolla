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
#include "py/arolla/abc/py_fingerprint.h"

#include <Python.h>

#include <type_traits>

#include "absl/log/check.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/fingerprint.h"

namespace arolla::python {
namespace {

extern PyTypeObject PyFingerprint_Type;

struct PyFingerprintObject final {
  struct Fields {
    Fingerprint fingerprint;
  };
  PyObject_HEAD;
  Fields fields;
};

static_assert(std::is_trivially_constructible_v<PyFingerprintObject::Fields> &&
              std::is_trivially_destructible_v<PyFingerprintObject::Fields>);

PyFingerprintObject::Fields& PyFingerprint_fields(PyObject* self) {
  DCHECK_EQ(Py_TYPE(self), &PyFingerprint_Type);
  return reinterpret_cast<PyFingerprintObject*>(self)->fields;
}

PyObject* PyFingerprint_repr(PyObject* self) {
  auto buffer = PyFingerprint_fields(self).fingerprint.AsString();
  return PyUnicode_FromStringAndSize(buffer.data(), buffer.size());
}

Py_hash_t PyFingerprint_hash(PyObject* self) {
  return PyFingerprint_fields(self).fingerprint.PythonHash();
}

PyObject* PyFingerprint_richcompare(PyObject* self, PyObject* other, int op) {
  if (Py_TYPE(other) != &PyFingerprint_Type) {
    Py_RETURN_NOTIMPLEMENTED;
  }
  Py_RETURN_RICHCOMPARE(PyFingerprint_fields(self).fingerprint.value,
                        PyFingerprint_fields(other).fingerprint.value, op);
}

PyTypeObject PyFingerprint_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "arolla.abc.Fingerprint",
    .tp_basicsize = sizeof(PyFingerprintObject),
    .tp_dealloc = reinterpret_cast<destructor>(PyObject_Free),
    .tp_repr = PyFingerprint_repr,
    .tp_hash = PyFingerprint_hash,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE |
                Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_doc = "Fingerprint",
    .tp_richcompare = PyFingerprint_richcompare,
};

}  // namespace

PyTypeObject* PyFingerprintType() {
  DCheckPyGIL();
  if (PyType_Ready(&PyFingerprint_Type) < 0) {
    return nullptr;
  }
  Py_INCREF(&PyFingerprint_Type);
  return &PyFingerprint_Type;
}

bool IsPyFingerprintInstance(PyObject* py_object) {
  DCheckPyGIL();
  return Py_TYPE(py_object) == &PyFingerprint_Type;
}

PyObject* WrapAsPyFingerprint(const Fingerprint& fingerprint) {
  DCheckPyGIL();
  if (PyType_Ready(&PyFingerprint_Type) < 0) {
    return nullptr;
  }
  auto* self = _PyObject_New(&PyFingerprint_Type);
  if (self != nullptr) {
    PyFingerprint_fields(self).fingerprint = fingerprint;
  }
  return self;
}

const Fingerprint* UnwrapPyFingerprint(PyObject* py_fingerprint) {
  DCheckPyGIL();
  if (Py_TYPE(py_fingerprint) != &PyFingerprint_Type) {
    PyErr_Format(PyExc_TypeError, "expected fingerprint, got %s",
                 Py_TYPE(py_fingerprint)->tp_name);
    return nullptr;
  }
  return &PyFingerprint_fields(py_fingerprint).fingerprint;
}

const Fingerprint& UnsafeUnwrapPyFingerprint(PyObject* py_fingerprint) {
  DCheckPyGIL();
  DCHECK(IsPyFingerprintInstance(py_fingerprint));
  return PyFingerprint_fields(py_fingerprint).fingerprint;
}

}  // namespace arolla::python
