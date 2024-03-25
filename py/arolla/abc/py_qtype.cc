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
#include "py/arolla/abc/py_qtype.h"

#include <Python.h>

#include "absl/log/check.h"
#include "absl/strings/str_format.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {
namespace {

using PyQTypeObject = PyQValueObject;

// Returns stored QTypePtr (or returns nullptr and sets a python exception).
const QType* ReadQType(PyQTypeObject* self_qtype) {
  if (self_qtype->typed_value.GetType() != GetQTypeQType()) {
    PyErr_SetString(PyExc_TypeError,
                    absl::StrFormat("expected QTYPE, got %s",
                                    self_qtype->typed_value.GetType()->name())
                        .c_str());
    return nullptr;
  }
  return self_qtype->typed_value.UnsafeAs<QTypePtr>();
}

PyObject* PyQType_arolla_init(PyObject* self, PyObject*) {
  auto* self_qtype = reinterpret_cast<PyQTypeObject*>(self);
  if (ReadQType(self_qtype) == nullptr) {
    return nullptr;
  }
  Py_RETURN_NONE;  // No need to call super()._arolla_init_() because
                   // we know that it's noop.
}

PyObject* PyQType_get_name(PyObject* self, void* /*closure*/) {
  auto* self_qtype = reinterpret_cast<PyQTypeObject*>(self);
  auto* qtype = ReadQType(self_qtype);
  DCHECK_NE(qtype, nullptr);
  return PyUnicode_FromStringAndSize(qtype->name().data(),
                                     qtype->name().size());
}

PyObject* PyQType_get_value_qtype(PyObject* self, void* /*closure*/) {
  auto* self_qtype = reinterpret_cast<PyQTypeObject*>(self);
  auto* qtype = ReadQType(self_qtype);
  DCHECK_NE(qtype, nullptr);
  auto* value_qtype = qtype->value_qtype();
  if (value_qtype == nullptr) {
    Py_RETURN_NONE;
  }
  static auto* py_qtype_type = PyQTypeType();
  // This is a method of PyQType, it means the class has been already
  // initialized.
  DCHECK_NE(py_qtype_type, nullptr);
  auto* result =
      MakePyQValue(py_qtype_type, TypedValue::FromValue(value_qtype));
  return result;
}

int PyQType_bool(PyObject* /*self*/) { return 1; }

Py_hash_t PyQType_hash(PyObject* self) {
  auto* self_qtype = reinterpret_cast<PyQTypeObject*>(self);
  return self_qtype->typed_value.GetFingerprint().PythonHash();
}

PyObject* PyQType_richcompare(PyObject* self, PyObject* other, int op) {
  if ((op != Py_EQ && op != Py_NE) || !IsPyQValueInstance(other)) {
    Py_RETURN_NOTIMPLEMENTED;
  }
  const auto& self_qvalue = UnsafeUnwrapPyQValue(self);
  const auto& other_qvalue = UnsafeUnwrapPyQValue(other);
  if (self_qvalue.GetType() != GetQTypeQType() ||
      other_qvalue.GetType() != GetQTypeQType()) {
    Py_RETURN_NOTIMPLEMENTED;
  }
  Py_RETURN_RICHCOMPARE(self_qvalue.UnsafeAs<QTypePtr>(),
                        other_qvalue.UnsafeAs<QTypePtr>(), op);
}

// LINT.IfChange

PyNumberMethods kPyQType_number_methods = {
    .nb_bool = PyQType_bool,
};

PyMethodDef kPyQType_methods[] = {
    {"_arolla_init_", &PyQType_arolla_init, METH_NOARGS,
     "Finishes a qtype initialization."},
    {nullptr} /* sentinel */
};

PyGetSetDef kPyQType_getset[] = {
    {
        .name = "name",
        .get = PyQType_get_name,
        .doc = "Type name.",
    },
    {
        .name = "value_qtype",
        .get = PyQType_get_value_qtype,
        .doc = "QType of values for a container type, otherwise None.",
    },
    {nullptr} /* sentinel */
};

// LINT.ThenChange(//depot/py/arolla/abc/qtype.py)

}  // namespace

PyTypeObject* PyQTypeType() {
  DCheckPyGIL();
  static PyTypeObject* py_qvalue_type = nullptr;
  if (py_qvalue_type == nullptr) {
    py_qvalue_type = PyQValueType();
    if (py_qvalue_type == nullptr) {
      return nullptr;
    }
  }
  static PyTypeObject result = [&] {
    PyTypeObject result{PyVarObject_HEAD_INIT(nullptr, 0)};
    result.tp_name = "arolla.abc.qtype.QType";
    result.tp_methods = kPyQType_methods;
    result.tp_getset = kPyQType_getset;
    result.tp_as_number = &kPyQType_number_methods;
    result.tp_hash = PyQType_hash;
    result.tp_richcompare = PyQType_richcompare;
    result.tp_flags =
        Py_TPFLAGS_DEFAULT;  // no inheritance (see a comment in py_qtype.h)
    result.tp_doc = "QType describes the memory layout of Arolla values.";
    result.tp_base = py_qvalue_type;
    return result;
  }();
  if (PyType_Ready(&result) < 0) {
    return nullptr;
  }
  Py_INCREF(&result);
  return &result;
}

const QType* UnwrapPyQType(PyObject* py_qvalue_qtype) {
  DCheckPyGIL();
  // Make this function work not only with QType but with any QValue that stores
  // QTYPE.
  if (!IsPyQValueInstance(py_qvalue_qtype)) {
    PyErr_Format(PyExc_TypeError, "expected QType, got %s",
                 Py_TYPE(py_qvalue_qtype)->tp_name);
    return nullptr;
  }
  const auto& qvalue = UnsafeUnwrapPyQValue(py_qvalue_qtype);
  if (qvalue.GetType() != GetQTypeQType()) {
    PyErr_Format(PyExc_TypeError, "expected QType, got %s",
                 Py_TYPE(py_qvalue_qtype)->tp_name);
    return nullptr;
  }
  return qvalue.UnsafeAs<QTypePtr>();
}

}  // namespace arolla::python
