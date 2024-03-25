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
#include "py/arolla/abc/py_expr.h"

#include <Python.h>

#include <cstddef>
#include <string>
#include <utility>

#include "absl/log/check.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_expr_view.h"
#include "py/arolla/abc/py_fingerprint.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ToDebugString;

// Forward declare.
extern PyTypeObject PyExpr_Type;

// Expr representation for python.
struct PyExprObject final {
  struct Fields {
    ExprNodePtr expr;
    ExprViewProxy expr_views;
  };
  PyObject_HEAD;
  Fields fields;
  PyObject* weakrefs;
};

PyExprObject::Fields& PyExpr_fields(PyObject* self) {
  DCHECK_EQ(Py_TYPE(self), &PyExpr_Type);
  return reinterpret_cast<PyExprObject*>(self)->fields;
}

void PyExpr_dealloc(PyObject* self) {
  auto* self_expr = reinterpret_cast<PyExprObject*>(self);
  if (self_expr->weakrefs != nullptr) {
    PyObject_ClearWeakRefs(self);
  }
  self_expr->fields.~Fields();
  Py_TYPE(self)->tp_free(self);
}

PyObject* PyExpr_repr(PyObject* self) {
  auto& self_fields = PyExpr_fields(self);
  auto buffer = ToDebugString(self_fields.expr);
  return PyUnicode_FromStringAndSize(buffer.data(), buffer.size());
}

Py_hash_t PyExpr_hash(PyObject* /*self*/) {
  PyErr_Format(PyExc_TypeError,
               "unhashable type: '%s'; please consider using `rl.quote(expr)`",
               PyExpr_Type.tp_name);
  return -1;
}

PyObject* PyExpr_call(PyObject* self, PyObject* args, PyObject* kwargs) {
  auto& self_fields = PyExpr_fields(self);
  self_fields.expr_views.Actualize(self_fields.expr);
  if (auto py_member = self_fields.expr_views.call_member_or_null();
      py_member != nullptr) {
    return PyObject_CallMember(std::move(py_member), self, args, kwargs)
        .release();
  }
  PyErr_Format(PyExc_TypeError, "'%s' object is not callable",
               PyExpr_Type.tp_name);
  return nullptr;
}

PyObject* PyExpr_getattro(PyObject* self, PyObject* py_str_attr) {
  auto& self_fields = PyExpr_fields(self);
  if (auto py_member = PyType_LookupMemberOrNull(&PyExpr_Type, py_str_attr);
      py_member != nullptr) {
    return PyObject_BindMember(std::move(py_member), self).release();
  }
  Py_ssize_t attr_size;
  const char* attr_data = PyUnicode_AsUTF8AndSize(py_str_attr, &attr_size);
  if (attr_data == nullptr) {
    return nullptr;
  }
  absl::string_view attr(attr_data, attr_size);
  self_fields.expr_views.Actualize(self_fields.expr);
  if (auto py_member = self_fields.expr_views.LookupMemberOrNull(attr);
      py_member != nullptr) {
    return PyObject_BindMember(std::move(py_member), self).release();
  }
  if (auto py_member_getattr = self_fields.expr_views.getattr_member_or_null();
      py_member_getattr != nullptr) {
    // Note: We expect `__getattr__` to return an "attribute", so that we don't
    // need to bind it to the instance.
    PyObject* args[2] = {self, py_str_attr};
    return PyObject_VectorcallMember(std::move(py_member_getattr), args, 2,
                                     nullptr)
        .release();
  }
  PyErr_Format(PyExc_AttributeError, "'%s' object has no attribute %R",
               Py_TYPE(self)->tp_name, py_str_attr);
  return nullptr;
}

PyObject* PyExpr_richcompare(PyObject* self, PyObject* other, int op) {
  auto& self_fields = PyExpr_fields(self);
  self_fields.expr_views.Actualize(self_fields.expr);
  PyObjectPtr method;
  switch (op) {
    case Py_LT:
      method = self_fields.expr_views.LookupMemberOrNull("__lt__");
      break;
    case Py_LE:
      method = self_fields.expr_views.LookupMemberOrNull("__le__");
      break;
    case Py_EQ:
      method = self_fields.expr_views.LookupMemberOrNull("__eq__");
      break;
    case Py_NE:
      method = self_fields.expr_views.LookupMemberOrNull("__ne__");
      break;
    case Py_GT:
      method = self_fields.expr_views.LookupMemberOrNull("__gt__");
      break;
    case Py_GE:
      method = self_fields.expr_views.LookupMemberOrNull("__ge__");
      break;
  }
  if (method == nullptr) {
    if (op != Py_EQ && op != Py_NE) {
      Py_RETURN_NOTIMPLEMENTED;
    }
    PyErr_Format(
        PyExc_TypeError,
        "__eq__ and __ne__ are disabled for %s; please use `expr.equals()`",
        Py_TYPE(self)->tp_name);
    return nullptr;
  }
  PyObject* args[2] = {self, other};
  return PyObject_Vectorcall(method.get(), args, 2, nullptr);
}

int PyExpr_as_number_bool(PyObject* self) {
  PyErr_Format(PyExc_TypeError, "__bool__ disabled for '%s'",
               Py_TYPE(self)->tp_name);
  return -1;
}

PyObject* PyExpr_as_number_power(PyObject* x, PyObject* y, PyObject* z) {
  if (IsPyExprInstance(x)) {
    auto& x_fields = PyExpr_fields(x);
    x_fields.expr_views.Actualize(x_fields.expr);
    auto method = x_fields.expr_views.LookupMemberOrNull("__pow__");
    if (method != nullptr) {
      PyObject* args[3] = {x, y, z};
      return PyObject_Vectorcall(method.get(), args, 2 + (z != Py_None),
                                 nullptr);
    }
  } else if (IsPyExprInstance(y)) {
    auto& y_fields = PyExpr_fields(y);
    y_fields.expr_views.Actualize(y_fields.expr);
    auto method = y_fields.expr_views.LookupMemberOrNull("__rpow__");
    if (method != nullptr) {
      PyObject* args[3] = {y, x, z};
      return PyObject_Vectorcall(method.get(), args, 2 + (z != Py_None),
                                 nullptr);
    }
  }
  Py_RETURN_NOTIMPLEMENTED;
}

PyObject* PyExpr_as_number_unary(PyObject* x,
                                 absl::string_view magic_method_name) {
  auto& x_fields = PyExpr_fields(x);
  x_fields.expr_views.Actualize(x_fields.expr);
  auto method = x_fields.expr_views.LookupMemberOrNull(magic_method_name);
  if (method != nullptr) {
    return PyObject_CallOneArg(method.get(), x);
  }
  PyErr_SetString(PyExc_TypeError,
                  absl::StrFormat("no expr-view provides '%s' implementation",
                                  magic_method_name)
                      .c_str());
  return nullptr;
}

PyObject* PyExpr_as_number_binary(PyObject* x, PyObject* y,
                                  absl::string_view magic_method_name,
                                  absl::string_view magic_method_rname) {
  if (IsPyExprInstance(x)) {
    auto& x_fields = PyExpr_fields(x);
    x_fields.expr_views.Actualize(x_fields.expr);
    auto method = x_fields.expr_views.LookupMemberOrNull(magic_method_name);
    if (method != nullptr) {
      PyObject* args[2] = {x, y};
      return PyObject_Vectorcall(method.get(), args, 2, nullptr);
    }
  } else {
    DCHECK(IsPyExprInstance(y));
    auto& y_fields = PyExpr_fields(y);
    y_fields.expr_views.Actualize(y_fields.expr);
    auto method = y_fields.expr_views.LookupMemberOrNull(magic_method_rname);
    if (method != nullptr) {
      PyObject* args[2] = {y, x};
      return PyObject_Vectorcall(method.get(), args, 2, nullptr);
    }
  }
  Py_RETURN_NOTIMPLEMENTED;
}

#define AS_NUMBER_UNARY_FN(name) \
  (+[](PyObject* x) -> PyObject* { return PyExpr_as_number_unary(x, (name)); })

#define AS_NUMBER_BINARY_FN(name, rname)                   \
  (+[](PyObject* x, PyObject* y) -> PyObject* {            \
    return PyExpr_as_number_binary(x, y, (name), (rname)); \
  })

PyNumberMethods kPyExpr_as_number = {
    .nb_add = AS_NUMBER_BINARY_FN("__add__", "__radd__"),
    .nb_subtract = AS_NUMBER_BINARY_FN("__sub__", "__rsub__"),
    .nb_multiply = AS_NUMBER_BINARY_FN("__mul__", "__rmul__"),
    .nb_remainder = AS_NUMBER_BINARY_FN("__mod__", "__rmod__"),
    .nb_power = PyExpr_as_number_power,
    .nb_negative = AS_NUMBER_UNARY_FN("__neg__"),
    .nb_positive = AS_NUMBER_UNARY_FN("__pos__"),
    .nb_bool = PyExpr_as_number_bool,
    .nb_invert = AS_NUMBER_UNARY_FN("__invert__"),
    .nb_lshift = AS_NUMBER_BINARY_FN("__lshift__", "__rlshift__"),
    .nb_rshift = AS_NUMBER_BINARY_FN("__rshift__", "__rrshift__"),
    .nb_and = AS_NUMBER_BINARY_FN("__and__", "__rand__"),
    .nb_xor = AS_NUMBER_BINARY_FN("__xor__", "__rxor__"),
    .nb_or = AS_NUMBER_BINARY_FN("__or__", "__ror__"),
    .nb_floor_divide = AS_NUMBER_BINARY_FN("__floordiv__", "__rfloordiv__"),
    .nb_true_divide = AS_NUMBER_BINARY_FN("__truediv__", "__rtruediv__"),
    .nb_matrix_multiply = AS_NUMBER_BINARY_FN("__matmul__", "__rmatmul__"),
};

#undef AS_NUMBER_BINARY_FN
#undef AS_NUMBER_UNARY_FN

PyObject* PyExpr_as_mapping_subscript(PyObject* self, PyObject* key) {
  auto& self_fields = PyExpr_fields(self);
  self_fields.expr_views.Actualize(self_fields.expr);
  if (auto py_member = self_fields.expr_views.getitem_member_or_null();
      py_member != nullptr) {
    PyObject* args[2] = {self, key};
    return PyObject_VectorcallMember(std::move(py_member), args, 2, nullptr)
        .release();
  }
  PyErr_Format(PyExc_TypeError, "'%s' object is not subscriptable",
               PyExpr_Type.tp_name);
  return nullptr;
}

PyMappingMethods kPyExpr_as_mapping = {
    .mp_subscript = PyExpr_as_mapping_subscript,
};

PyObject* PyExpr_methods_format(PyObject* self, PyObject* py_str_format) {
  const auto& self_fields = PyExpr_fields(self);
  Py_ssize_t format_size;
  const char* format_data =
      PyUnicode_AsUTF8AndSize(py_str_format, &format_size);
  if (format_data == nullptr) {
    return nullptr;
  }
  absl::string_view format(format_data, format_size);
  std::string buffer;
  if (format.empty()) {
    buffer = ToDebugString(self_fields.expr, /*verbose=*/false);
  } else if (format == "v") {
    buffer = ToDebugString(self_fields.expr, /*verbose=*/true);
  } else {
    PyErr_Format(PyExc_ValueError,
                 "expected format_spec='' or 'v', got format_spec=%R",
                 py_str_format);
    return nullptr;
  }
  return PyUnicode_FromStringAndSize(buffer.data(), buffer.size());
}

PyObject* PyExpr_methods_equals(PyObject* self, PyObject* py_expr_other) {
  if (!IsPyExprInstance(py_expr_other)) {
    PyErr_Format(PyExc_TypeError, "expected '%s', got '%s'",
                 Py_TYPE(self)->tp_name, Py_TYPE(py_expr_other)->tp_name);
    return nullptr;
  }
  const auto& self_fields = PyExpr_fields(self);
  const auto& other_fields = PyExpr_fields(py_expr_other);
  return PyBool_FromLong(self_fields.expr->fingerprint() ==
                         other_fields.expr->fingerprint());
}

PyObject* PyExpr_get_fingerprint(PyObject* self, void* /*closure*/) {
  return WrapAsPyFingerprint(PyExpr_fields(self).expr->fingerprint());
}

PyObject* PyExpr_get_is_literal(PyObject* self, void* /*closure*/) {
  return PyBool_FromLong(PyExpr_fields(self).expr->is_literal());
}

PyObject* PyExpr_get_is_leaf(PyObject* self, void* /*closure*/) {
  return PyBool_FromLong(PyExpr_fields(self).expr->is_leaf());
}

PyObject* PyExpr_get_is_placeholder(PyObject* self, void* /*closure*/) {
  return PyBool_FromLong(PyExpr_fields(self).expr->is_placeholder());
}

PyObject* PyExpr_get_is_operator(PyObject* self, void* /*closure*/) {
  return PyBool_FromLong(PyExpr_fields(self).expr->is_op());
}

PyObject* PyExpr_get_qtype(PyObject* self, void* /*closure*/) {
  if (auto* qtype = PyExpr_fields(self).expr->qtype()) {
    return WrapAsPyQValue(TypedValue::FromValue(qtype));
  }
  Py_RETURN_NONE;
}

PyObject* PyExpr_get_qvalue(PyObject* self, void* /*closure*/) {
  if (const auto& qvalue = PyExpr_fields(self).expr->qvalue()) {
    return WrapAsPyQValue(*qvalue);
  }
  Py_RETURN_NONE;
}

PyObject* PyExpr_get_leaf_key(PyObject* self, void* /*closure*/) {
  const auto& buffer = PyExpr_fields(self).expr->leaf_key();
  return PyUnicode_FromStringAndSize(buffer.data(), buffer.size());
}

PyObject* PyExpr_get_placeholder_key(PyObject* self, void* /*closure*/) {
  const auto& buffer = PyExpr_fields(self).expr->placeholder_key();
  return PyUnicode_FromStringAndSize(buffer.data(), buffer.size());
}

PyObject* PyExpr_get_op(PyObject* self, void* /*closure*/) {
  if (auto& op = PyExpr_fields(self).expr->op()) {
    return WrapAsPyQValue(TypedValue::FromValue(op));
  }
  Py_RETURN_NONE;
}

PyObject* PyExpr_get_node_deps(PyObject* self, void* /*closure*/) {
  const auto& node_deps = PyExpr_fields(self).expr->node_deps();
  auto result = PyObjectPtr::Own(PyTuple_New(node_deps.size()));
  if (result == nullptr) {
    return nullptr;
  }
  for (size_t i = 0; i < node_deps.size(); ++i) {
    PyTuple_SET_ITEM(result.get(), i, WrapAsPyExpr(node_deps[i]));
  }
  return result.release();
}

// LINT.IfChange

PyMethodDef kPyExpr_methods[] = {
    {
        "__format__",
        &PyExpr_methods_format,
        METH_O,
        "",
    },
    {
        "equals",
        &PyExpr_methods_equals,
        METH_O,
        "Returns true iff the fingerprints of the expressions are equal.",
    },
    {nullptr} /* sentinel */
};

PyGetSetDef kPyExpr_getset[] = {
    {
        .name = "fingerprint",
        .get = PyExpr_get_fingerprint,
        .doc = "Unique identifier of the value.",
    },
    {
        .name = "is_literal",
        .get = PyExpr_get_is_literal,
        .doc = "Indicates whether the node represents a literal.",
    },
    {
        .name = "is_leaf",
        .get = PyExpr_get_is_leaf,
        .doc = "Indicates whether the node represents a leaf.",
    },
    {
        .name = "is_placeholder",
        .get = PyExpr_get_is_placeholder,
        .doc = "Indicates whether the node represents a placeholder.",
    },
    {
        .name = "is_operator",
        .get = PyExpr_get_is_operator,
        .doc = "Indicates whether the node represents an operator.",
    },
    {
        .name = "qtype",
        .get = PyExpr_get_qtype,
        .doc = ("QType attribute.\n"
                "\n"
                "This property corresponds the qtype of the expression result, "
                "e.g. TEXT or\n"
                "ARRAY_FLOAT32. If no qtype attribute is set, the property "
                "returns None."),
    },
    {
        .name = "qvalue",
        .get = PyExpr_get_qvalue,
        .doc = ("QValue attribute.\n"
                "\n"
                "This property corresponds to the expression evalution result. "
                "It's always\n"
                "set for literal nodes, and conditionally available for other "
                "node kinds.\n"
                "If no qvalue attribute is set, the property returns None."),
    },
    {
        .name = "leaf_key",
        .get = &PyExpr_get_leaf_key,
        .doc = "The string key of a leaf node, or an empty string for a "
               "non-leaf.",
    },
    {
        .name = "placeholder_key",
        .get = &PyExpr_get_placeholder_key,
        .doc = "Placeholder's key, or empty string for non-placeholder nodes.",
    },
    {
        .name = "op",
        .get = &PyExpr_get_op,
        .doc = "The operator, or None for non-operator nodes.",
    },
    {
        .name = "node_deps",
        .get = &PyExpr_get_node_deps,
        .doc = "Node's dependencies.",
    },
    {nullptr}, /* sentinel */
};

// LINT.ThenChange(//depot/py/arolla/abc/expr.py)

PyTypeObject PyExpr_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "arolla.abc.Expr",
    .tp_basicsize = sizeof(PyExprObject),
    .tp_dealloc = PyExpr_dealloc,
    .tp_repr = PyExpr_repr,
    .tp_as_number = &kPyExpr_as_number,
    .tp_as_mapping = &kPyExpr_as_mapping,
    .tp_hash = PyExpr_hash,
    .tp_call = PyExpr_call,
    .tp_getattro = PyExpr_getattro,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION |
                Py_TPFLAGS_IMMUTABLETYPE,
    .tp_doc =
        ("A expression class.\n\nExpr is immutable. It provides only basic "
         "functionality, that can be extended\nwith ExprViews."),
    .tp_richcompare = PyExpr_richcompare,
    .tp_weaklistoffset = offsetof(PyExprObject, weakrefs),
    .tp_methods = kPyExpr_methods,
    .tp_getset = kPyExpr_getset,
};

}  // namespace

PyTypeObject* PyExprType() {
  DCheckPyGIL();
  if (PyType_Ready(&PyExpr_Type) < 0) {
    return nullptr;
  }
  Py_INCREF(&PyExpr_Type);
  return &PyExpr_Type;
}

bool IsPyExprType(PyTypeObject* py_type) { return py_type == &PyExpr_Type; }

bool IsPyExprInstance(PyObject* py_object) {
  DCheckPyGIL();
  return IsPyExprType(Py_TYPE(py_object));
}

PyObject* WrapAsPyExpr(ExprNodePtr expr) {
  DCheckPyGIL();
  if (expr == nullptr) {
    Py_RETURN_NONE;
  }
  if (PyType_Ready(&PyExpr_Type) < 0) {
    return nullptr;
  }
  auto self = PyObjectPtr::Own(PyExpr_Type.tp_alloc(&PyExpr_Type, 0));
  if (self == nullptr) {
    return nullptr;
  }
  auto& self_fields = PyExpr_fields(self.get());
  new (&self_fields) PyExprObject::Fields;
  self_fields.expr = std::move(expr);
  return self.release();
}

ExprNodePtr /*nullable*/ UnwrapPyExpr(PyObject* py_expr) {
  if (!IsPyExprInstance(py_expr)) {
    PyErr_Format(PyExc_TypeError, "expected %s, got %s", PyExpr_Type.tp_name,
                 Py_TYPE(py_expr)->tp_name);
    return nullptr;
  }
  return PyExpr_fields(py_expr).expr;
}

const ExprNodePtr& UnsafeUnwrapPyExpr(PyObject* py_expr) {
  DCHECK(IsPyExprInstance(py_expr));
  return PyExpr_fields(py_expr).expr;
}

}  // namespace arolla::python
