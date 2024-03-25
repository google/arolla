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
#include "py/arolla/abc/py_qvalue.h"

#include <Python.h>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_fingerprint.h"
#include "py/arolla/abc/py_qtype.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

using ::arolla::serialization_base::ContainerProto;

void PyQValue_dealloc(PyObject* self) {
  auto* self_qvalue = reinterpret_cast<PyQValueObject*>(self);
  if (self_qvalue->weakrefs != nullptr) {
    PyObject_ClearWeakRefs(self);
  }
  self_qvalue->typed_value.~TypedValue();
  Py_TYPE(self)->tp_free(self);
}

PyObject* PyQValue_repr(PyObject* self) {
  auto* self_qvalue = reinterpret_cast<PyQValueObject*>(self);
  auto buffer = self_qvalue->typed_value.Repr();
  return PyUnicode_FromStringAndSize(buffer.data(), buffer.size());
}

int PyQValue_bool(PyObject* self) {
  PyErr_Format(PyExc_TypeError,
               "__bool__ disabled for %s",
               Py_TYPE(self)->tp_name);
  return -1;
}

PyObject* PyQValue_arolla_init(PyObject*, PyObject*) { Py_RETURN_NONE; }

// QValue.__reduce__ implementation.
PyObject* PyQValue_reduce(PyObject* self, PyObject*) {
  auto unreduce_func =
      PyObjectPtr::Own(PyObject_GetAttrString(self, "_arolla_unreduce"));
  if (unreduce_func == nullptr) {
    // PyObject_GetAttrString will raise AttributeError itself.
    return nullptr;
  }

  auto* self_qvalue = reinterpret_cast<PyQValueObject*>(self);
  // NOTE: Now we use the default set of codecs here. By default, PyObject
  // QValues without `codec` are not serializable. If it ever becomes a
  // limitation, we can replace the default PyObject codec with our own, that
  // just uses Pickle to serialize everything.
  ASSIGN_OR_RETURN(auto encoded,
                   serialization::Encode({self_qvalue->typed_value}, {}),
                   SetPyErrFromStatus(_));
  std::string serialized;
  if (!encoded.SerializeToString(&serialized)) {
    PyErr_Format(PyExc_ValueError, "ContainerProto.SerializeToString() failed");
    return nullptr;
  }
  auto serialized_bytes = PyObjectPtr::Own(
      PyBytes_FromStringAndSize(serialized.data(), serialized.size()));

  return PyTuple_Pack(2, unreduce_func.release(),
                      PyTuple_Pack(1, serialized_bytes.release()));
}

// QValue._unreduce implementation, used by QValue.__reduce__.
PyObject* PyQValue_arolla_unreduce(PyObject*, PyObject* arg) {
  char* buffer;
  Py_ssize_t length;
  if (PyBytes_AsStringAndSize(arg, &buffer, &length) == -1) {
    // PyBytes_AsStringAndSize will raise TypeError itself.
    return nullptr;
  }
  ContainerProto container;
  if (!container.ParseFromArray(buffer, length)) {
    PyErr_Format(PyExc_ValueError, "ContainerProto.ParseFromString() failed");
  }
  auto decode_result_or = serialization::Decode(container);
  if (!decode_result_or.ok()) {
    SetPyErrFromStatus(decode_result_or.status());
    return nullptr;
  }
  if (!decode_result_or->exprs.empty() ||
      decode_result_or->values.size() != 1) {
    PyErr_Format(PyExc_ValueError,
                 "unexpected sizes in the serialized container");
    return nullptr;
  }
  return WrapAsPyQValue(std::move(decode_result_or->values[0]));
}

PyObject* PyQValue_richcompare(PyObject* self, PyObject* other, int op) {
  if ((op != Py_EQ && op != Py_NE) || !IsPyQValueInstance(other)) {
    Py_RETURN_NOTIMPLEMENTED;
  }
  PyErr_Format(PyExc_TypeError,
               "__eq__ and __ne__ disabled for %s",
               Py_TYPE(self)->tp_name);
  return nullptr;
}

PyObject* PyQValue_get_fingerprint(PyObject* self, void* /*closure*/) {
  auto* self_qvalue = reinterpret_cast<PyQValueObject*>(self);
  return WrapAsPyFingerprint(self_qvalue->typed_value.GetFingerprint());
}

PyObject* PyQValue_get_fingerprint_hash(PyObject* self, void* /*closure*/) {
  auto* self_qvalue = reinterpret_cast<PyQValueObject*>(self);
  return PyLong_FromSsize_t(
      self_qvalue->typed_value.GetFingerprint().PythonHash());
}

PyObject* PyQValue_get_qtype(PyObject* self, void* /*closure*/) {
  auto* self_qvalue = reinterpret_cast<PyQValueObject*>(self);
  return MakePyQValue(
      PyQTypeType(), TypedValue::FromValue(self_qvalue->typed_value.GetType()));
}

PyObject* PyQValue_get_specialization_key(PyObject* self, void* /*closure*/) {
  auto* self_qvalue = reinterpret_cast<PyQValueObject*>(self);
  const auto result = self_qvalue->typed_value.PyQValueSpecializationKey();
  return PyUnicode_FromStringAndSize(result.data(), result.size());
}

// LINT.IfChange

PyNumberMethods kPyQValue_number_methods = {
    .nb_bool = PyQValue_bool,
};

PyMethodDef kPyQValue_methods[] = {
    {"_arolla_init_", &PyQValue_arolla_init, METH_NOARGS,
     "Finishes a qvalue object initialization."},
    {"__reduce__", &PyQValue_reduce, METH_NOARGS,
     "Serializes the object for pickle."},
    {"_arolla_unreduce", &PyQValue_arolla_unreduce, METH_O | METH_STATIC,
     "Unpickles the object."},
    {nullptr} /* sentinel */
};

PyGetSetDef kPyQValue_getset[] = {
    {
        .name = "fingerprint",
        .get = PyQValue_get_fingerprint,
        .doc = "Unique identifier of the value.",
    },
    {
        .name = "qtype",
        .get = PyQValue_get_qtype,
        .doc = "QType of the stored value.",
    },
    {
        .name = "_fingerprint_hash",
        .get = PyQValue_get_fingerprint_hash,
        .doc = "Hash of the fingerprint.",
    },
    {
        .name = "_specialization_key",
        .get = PyQValue_get_specialization_key,
        .doc = "QValue specialization key.",
    },
    {nullptr}, /* sentinel */
};

// LINT.ThenChange(//depot/py/arolla/abc/qtype.py)

PyTypeObject PyQValue_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "arolla.abc.qtype.QValue",
    .tp_basicsize = sizeof(PyQValueObject),
    .tp_dealloc = PyQValue_dealloc,
    .tp_repr = PyQValue_repr,
    .tp_as_number = &kPyQValue_number_methods,
    .tp_hash = PyObject_HashNotImplemented,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
                Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_doc = ("Base class of all Arolla values in Python.\n"
               "\n"
               "QValue is immutable. It provides only basic functionality.\n"
               "Subclasses of this class might have further specialization.\n"),
    .tp_richcompare = PyQValue_richcompare,
    .tp_weaklistoffset = offsetof(PyQValueObject, weakrefs),
    .tp_methods = kPyQValue_methods,
    .tp_getset = kPyQValue_getset,
};

}  // namespace

PyTypeObject* PyQValueType() {
  DCheckPyGIL();
  if (PyType_Ready(&PyQValue_Type) < 0) {
    return nullptr;
  }
  Py_INCREF(&PyQValue_Type);
  return &PyQValue_Type;
}

bool IsPyQValueSubtype(PyTypeObject* py_type) {
  DCheckPyGIL();
  if (PyType_Ready(&PyQValue_Type) < 0) {
    PyErr_Clear();  // If PyQValueType is not yet declared,
    return false;   // py_type cannot be a subtype of that type.
  }
  return py_type == &PyQValue_Type || PyType_IsSubtype(py_type, &PyQValue_Type);
}

bool IsPyQValueInstance(PyObject* py_object) {
  DCheckPyGIL();
  return IsPyQValueSubtype(Py_TYPE(py_object));
}

namespace {

// Calls the _arolla_init_ method. If an error occurs, returns False and sets
// the Python exception.
bool CallArollaInitMethod(PyTypeObject* py_type, PyObject* py_qvalue) {
  if (py_type == &PyQValue_Type) {
    return true;
  }
  static auto* py_str_method_name = PyUnicode_InternFromString("_arolla_init_");
  static auto* py_default_member =  // no-action implementation
      PyType_LookupMemberOrNull(&PyQValue_Type, py_str_method_name).release();
  auto py_member = PyType_LookupMemberOrNull(py_type, py_str_method_name);
  if (py_member == nullptr || py_member.get() == py_default_member) {
    return true;
  }
  return PyObject_VectorcallMember(std::move(py_member), &py_qvalue, 1,
                                   nullptr) != nullptr;
}

}  // namespace

PyObject* MakePyQValue(PyTypeObject* py_type, TypedValue&& typed_value) {
  DCheckPyGIL();
  if (!IsPyQValueSubtype(py_type)) {
    PyErr_Format(PyExc_TypeError, "expected a subclass of QValue, got %s",
                 py_type->tp_name);
    return nullptr;
  }
  auto self = PyObjectPtr::Own(py_type->tp_alloc(py_type, 0));
  if (self == nullptr) {
    return nullptr;
  }
  auto* self_qvalue = reinterpret_cast<PyQValueObject*>(self.get());
  self_qvalue->weakrefs = nullptr;
  new (&self_qvalue->typed_value) TypedValue(std::move(typed_value));
  // Call _arolla_init_() instead of __init__() according to the
  // convention.
  if (!CallArollaInitMethod(py_type, self.get())) {
    return nullptr;
  }
  return self.release();
}

const TypedValue* UnwrapPyQValue(PyObject* py_qvalue) {
  DCheckPyGIL();
  if (!IsPyQValueInstance(py_qvalue)) {
    PyErr_Format(PyExc_TypeError, "expected QValue, got %s",
                 Py_TYPE(py_qvalue)->tp_name);
    return nullptr;
  }
  return &(reinterpret_cast<PyQValueObject*>(py_qvalue)->typed_value);
}

}  // namespace arolla::python
