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

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_QVALUE_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_QVALUE_H_

#include <Python.h>

#include <memory>

#include "absl/log/check.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

// QValue representation for python.
struct PyQValueObject final {
  PyObject_HEAD;
  TypedValue typed_value;
  PyObject* weakrefs;
};

// Returns PyTypeObject corresponding to PyQValue type (or nullptr and
// sets a python exception).
PyTypeObject* PyQValueType();

// Returns true if the argument is a subtype of PyQValueType.
bool IsPyQValueSubtype(PyTypeObject* py_type);

// Returns true if the argument is a PyQValue instance.
bool IsPyQValueInstance(PyObject* py_object);

// Returns PyQValue object (or nullptr and sets a python exception).
//
// IMPORTANT: `py_type` must be a subtype of PyQValueType.
PyObject* MakePyQValue(PyTypeObject* py_type_qvalue, TypedValue&& typed_value);

// Returns a pointer to TypedValue stored in the given PyQValue instance
// (or nullptr and sets a python exception).
const TypedValue* UnwrapPyQValue(PyObject* py_qvalue);

// Returns a reference to TypedValue stored in the given PyQValue instance.
// The argument must be a PyQValue instance.
inline const TypedValue& UnsafeUnwrapPyQValue(PyObject* py_qvalue) {
  DCHECK(IsPyQValueInstance(py_qvalue));
  return reinterpret_cast<const PyQValueObject*>(py_qvalue)->typed_value;
}

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_QVALUE_H_
