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
#include "py/arolla/abc/py_expr_quote.h"

#include <Python.h>

#include <cstddef>
#include <utility>

#include "absl/log/check.h"
#include "absl/strings/str_format.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"

namespace arolla::python {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprQuote;

// Forward declare.
extern PyTypeObject PyExprQuote_Type;

PyObject* PyExprQuote_vectorcall(PyObject* /*metatype*/, PyObject* const* args,
                                 size_t nargsf, PyObject* kwnames) {
  Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);
  if (kwnames != nullptr) {
    if (!PyTuple_CheckExact(kwnames)) {
      PyErr_BadInternalCall();
      return nullptr;
    }
    if (PyTuple_GET_SIZE(kwnames) != 0) {
      return PyErr_Format(PyExc_TypeError,
                          "arolla.abc.ExprQuote() takes no keyword arguments");
    }
  }
  if (nargs != 1) {
    return PyErr_Format(PyExc_TypeError,
                        "arolla.abc.ExprQuote() takes 1 positional argument "
                        "but %zd were given",
                        nargs);
  }
  auto expr = UnwrapPyExpr(args[0]);
  if (expr == nullptr) {
    PyErr_Clear();
    return PyErr_Format(
        PyExc_TypeError,
        "arolla.abc.ExprQuote() expected an expression, got expr: %s",
        Py_TYPE(args[0])->tp_name);
  }
  return MakePyQValue(&PyExprQuote_Type,
                      TypedValue::FromValue(ExprQuote(std::move(expr))));
}

Py_hash_t PyExprQuote_hash(PyObject* self) {
  const auto& typed_value = UnsafeUnwrapPyQValue(self);
  if (typed_value.GetType() != GetQType<ExprQuote>()) {
    PyErr_SetString(PyExc_RuntimeError,
                    absl::StrFormat("unexpected self.qtype=%s",
                                    typed_value.GetType()->name())
                        .c_str());
    return -1;
  }
  // Use `expr_fingerprint()` instead of typed_value.GetFingerprint() for
  // the performance reasons.
  return typed_value.UnsafeAs<ExprQuote>().expr_fingerprint().PythonHash();
}

PyObject* PyExprQuote_richcompare(PyObject* self, PyObject* other, int op) {
  if ((op != Py_EQ && op != Py_NE) || Py_TYPE(other) != &PyExprQuote_Type) {
    Py_RETURN_NOTIMPLEMENTED;
  }
  auto& self_typed_value = UnsafeUnwrapPyQValue(self);
  auto& other_typed_value = UnsafeUnwrapPyQValue(other);
  if (self_typed_value.GetType() != GetQType<ExprQuote>()) {
    PyErr_SetString(PyExc_RuntimeError,
                    absl::StrFormat("unexpected self.qtype=%s",
                                    self_typed_value.GetType()->name())
                        .c_str());
    return nullptr;
  }
  if (other_typed_value.GetType() != GetQType<ExprQuote>()) {
    PyErr_SetString(PyExc_RuntimeError,
                    absl::StrFormat("unexpected other.qtype=%s",
                                    other_typed_value.GetType()->name())
                        .c_str());
    return nullptr;
  }
  // Use `expr_fingerprint()` instead of typed_value.GetFingerprint() for
  // performance reasons.
  Py_RETURN_RICHCOMPARE(
      self_typed_value.UnsafeAs<ExprQuote>().expr_fingerprint().value,
      other_typed_value.UnsafeAs<ExprQuote>().expr_fingerprint().value, op);
}

PyObject* PyExprQuote_methods_unquote(PyObject* self) {
  const auto& typed_value = UnsafeUnwrapPyQValue(self);
  if (typed_value.GetType() != GetQType<ExprQuote>()) {
    PyErr_SetString(PyExc_RuntimeError,
                    absl::StrFormat("unexpected self.qtype=%s",
                                    typed_value.GetType()->name())
                        .c_str());
    return nullptr;
  }
  auto expr = typed_value.UnsafeAs<ExprQuote>().expr();
  if (!expr.ok()) {
    PyErr_SetString(PyExc_RuntimeError, expr.status().message().data());
    return nullptr;
  }
  return WrapAsPyExpr(*std::move(expr));
}

PyMethodDef kPyExprQuote_methods[] = {
    {
        "unquote",
        (PyCFunction)PyExprQuote_methods_unquote,
        METH_NOARGS,
        "Returns the stored expression.",
    },
    {nullptr} /* sentinel */
};

PyTypeObject PyExprQuote_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "arolla.abc.ExprQuote",
    .tp_hash = PyExprQuote_hash,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .tp_doc = ("ExprQuote represents an expression as a value.\n\nThis type is "
               "similar to Lisp \"quote\" and allows an expression to be "
               "embedded\nas a literal value in another expression without "
               "meaning to evaluate.\n\nThis type is hashable "
               "(https://docs.python.org/3/glossary.html#term-hashable)\nand "
               "allows the use of an expression as a dictionary key."),
    .tp_richcompare = PyExprQuote_richcompare,
    .tp_methods = kPyExprQuote_methods,
    .tp_vectorcall = PyExprQuote_vectorcall,
};

}  // namespace

PyTypeObject* PyExprQuoteType() {
  DCheckPyGIL();
  if (!PyType_HasFeature(&PyExprQuote_Type, Py_TPFLAGS_READY)) {
    PyExprQuote_Type.tp_base = PyQValueType();
    if (PyExprQuote_Type.tp_base == nullptr) {
      return nullptr;
    }
    if (PyType_Ready(&PyExprQuote_Type) < 0) {
      return nullptr;
    }
  }
  Py_INCREF(&PyExprQuote_Type);
  return &PyExprQuote_Type;
}

}  // namespace arolla::python
