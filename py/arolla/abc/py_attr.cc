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
#include "py/arolla/abc/py_attr.h"

#include <array>
#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"
#include "py/arolla/abc/py_operator.h"
#include "py/arolla/abc/py_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprOperatorPtr;

// Forward declare.
extern PyTypeObject PyAttr_Type;

// ExprAttributes representation for python.
struct PyAttrObject final {
  PyObject_HEAD;
  struct Fields {
    const QType* /*nullable*/ qtype;
    std::optional<TypedValue> qvalue;
  };
  Fields fields;
};

PyObject* PyAttr_new(const QType* qtype, std::optional<TypedValue> qvalue) {
  DCheckPyGIL();
  if (qvalue.has_value()) {
    if (qtype == nullptr) {
      qtype = qvalue->GetType();
    } else if (qtype != qvalue->GetType()) {
      PyErr_SetString(
          PyExc_ValueError,
          absl::StrFormat("qtype mismatch: qtype=%s, qvalue.qtype=%s",
                          qtype->name(), qvalue->GetType()->name())
              .c_str());
      return nullptr;
    }
  }
  PyObject* self = PyAttr_Type.tp_alloc(&PyAttr_Type, 0);
  if (self == nullptr) {
    return nullptr;
  }
  auto* self_attr = reinterpret_cast<PyAttrObject*>(self);
  new (&self_attr->fields) PyAttrObject::Fields;
  self_attr->fields.qtype = qtype;
  self_attr->fields.qvalue = std::move(qvalue);
  return self;
}

PyObject* PyAttr_new(PyTypeObject* /*subtype*/, PyObject* args,
                     PyObject* kwargs) {
  // Parse the arguments: (*, qtype=None, qvalue=None)
  // https://docs.python.org/3/c-api/arg.html#parsing-arguments
  constexpr std::array<const char*, 3> kwlist = {"qtype", "qvalue", nullptr};
  PyObject *py_qtype = Py_None, *py_qvalue = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|$OO", (char**)kwlist.data(),
                                   &py_qtype, &py_qvalue)) {
    return nullptr;
  }
  const QType* qtype = nullptr;
  if (py_qtype != Py_None) {
    qtype = UnwrapPyQType(py_qtype);
    if (qtype == nullptr) {
      return nullptr;
    }
  }
  const TypedValue* qvalue = nullptr;
  if (py_qvalue != Py_None) {
    qvalue = UnwrapPyQValue(py_qvalue);
    if (qvalue == nullptr) {
      return nullptr;
    }
  }
  return PyAttr_new(
      qtype, (qvalue == nullptr ? std::nullopt : std::optional(*qvalue)));
}

void PyAttr_dealloc(PyObject* self) {
  auto* self_attr = reinterpret_cast<const PyAttrObject*>(self);
  self_attr->fields.~Fields();
  Py_TYPE(self)->tp_free(self);
}

PyObject* PyAttr_repr(PyObject* self) {
  const auto& attr = reinterpret_cast<const PyAttrObject*>(self)->fields;
  if (attr.qvalue.has_value()) {
    return PyUnicode_FromFormat("Attr(qvalue=%s)", attr.qvalue->Repr().c_str());
  }
  if (attr.qtype != nullptr) {
    return PyUnicode_FromString(
        absl::StrFormat("Attr(qtype=%s)", attr.qtype->name()).c_str());
  }
  return PyUnicode_FromString("Attr()");
}

int PyAttr_bool(PyObject* self) {
  const auto& attr = reinterpret_cast<const PyAttrObject*>(self)->fields;
  return attr.qtype ? 1 : 0;
}

PyObject* PyAttr_get_qtype(PyObject* self, void* /*closure*/) {
  const auto& attr = reinterpret_cast<const PyAttrObject*>(self)->fields;
  if (attr.qtype != nullptr) {
    return WrapAsPyQValue(TypedValue::FromValue(attr.qtype));
  }
  Py_RETURN_NONE;
}

PyObject* PyAttr_get_qvalue(PyObject* self, void* /*closure*/) {
  const auto& attr = reinterpret_cast<const PyAttrObject*>(self)->fields;
  if (attr.qvalue.has_value()) {
    return WrapAsPyQValue(*attr.qvalue);
  }
  Py_RETURN_NONE;
}

// LINT.IfChange

PyNumberMethods kPyAttr_as_number = {
    .nb_bool = PyAttr_bool,
};

PyGetSetDef kPyAttr_getset[] = {
    {
        .name = "qtype",
        .get = PyAttr_get_qtype,
        .doc = "QType attribute, or None if the attribute is not set.",
    },
    {
        .name = "qvalue",
        .get = PyAttr_get_qvalue,
        .doc = "QValue attribute, or None if the attribute is not set.",
    },
    {nullptr}, /* sentinel */
};

// LINT.ThenChange(//depot/py/arolla/abc/attr.py)

PyTypeObject PyAttr_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "arolla.abc.Attr",
    .tp_basicsize = sizeof(PyAttrObject),
    .tp_dealloc = PyAttr_dealloc,
    .tp_repr = PyAttr_repr,
    .tp_as_number = &kPyAttr_as_number,
    .tp_hash = PyObject_HashNotImplemented,
    .tp_flags = Py_TPFLAGS_DEFAULT,  // no inheritance
    .tp_doc = ("A helper class that stores attributes of an expression node."),
    .tp_getset = kPyAttr_getset,
    .tp_new = PyAttr_new,
};

// def infer_attr(
//     op: str|QValue, input_attrs: tuple[Attr|QType|None, ...] = (), /
// ) -> Expr
PyObject* PyInferAttr(PyObject* /*self*/, PyObject** py_args,
                      Py_ssize_t nargs) {
  DCheckPyGIL();
  if (PyType_Ready(&PyAttr_Type) < 0) {
    return nullptr;
  }
  if (nargs < 1) {
    PyErr_SetString(PyExc_TypeError,
                    "arolla.abc.infer_attr() missing 1 required "
                    "positional argument: 'op'");
    return nullptr;
  } else if (nargs > 2) {
    return PyErr_Format(PyExc_TypeError,
                        "arolla.abc.infer_attr() takes 2 positional "
                        "arguments but %zu were given",
                        nargs);
  }
  // Parse `op`.
  auto op = ParseArgPyOperator("arolla.abc.infer_attr", py_args[0]);
  if (op == nullptr) {
    return nullptr;
  }
  // Parse `input_attrs`.
  std::vector<ExprAttributes> input_attrs;
  if (nargs == 2) {
    PyObject* py_tuple_input_attrs = py_args[1];
    if (!PyTuple_Check(py_tuple_input_attrs)) {
      return PyErr_Format(PyExc_TypeError,
                          "arolla.abc.infer_attr() expected a "
                          "tuple[Attr|QType|None, ...], got input_attrs: %s",
                          Py_TYPE(py_tuple_input_attrs)->tp_name);
    }
    input_attrs.resize(PyTuple_GET_SIZE(py_tuple_input_attrs));
    for (size_t i = 0; i < input_attrs.size(); ++i) {
      auto* py_input_attr = PyTuple_GET_ITEM(py_tuple_input_attrs, i);
      if (py_input_attr == Py_None) {
        // pass
      } else if (Py_TYPE(py_input_attr) == &PyAttr_Type) {
        const auto& attr =
            reinterpret_cast<const PyAttrObject*>(py_input_attr)->fields;
        input_attrs[i] = ExprAttributes(attr.qtype, attr.qvalue);
      } else {  // Expect a qtype.
        auto* input_qtype = UnwrapPyQType(py_input_attr);
        if (input_qtype == nullptr) {
          PyErr_Clear();
          PyErr_Format(PyExc_TypeError,
                       "arolla.abc.infer_attr() expected Attr or QType, got "
                       "input_attrs[%d]: %s",
                       i, Py_TYPE(py_input_attr)->tp_name);
          return nullptr;
        }
        input_attrs[i] = ExprAttributes(input_qtype);
      }
    }
  }
  ASSIGN_OR_RETURN(auto output_attr, op->InferAttributes(input_attrs),
                   (SetPyErrFromStatus(_), nullptr));
  return PyAttr_new(output_attr.qtype(), output_attr.qvalue());
}

}  // namespace

PyTypeObject* PyAttrType() {
  DCheckPyGIL();
  if (PyType_Ready(&PyAttr_Type) < 0) {
    return nullptr;
  }
  Py_INCREF(&PyAttr_Type);
  return &PyAttr_Type;
}

const PyMethodDef kDefPyInferAttr = {
    "infer_attr",
    reinterpret_cast<PyCFunction>(PyInferAttr),
    METH_FASTCALL,
    ("infer_attr(op, input_attrs=(), /)\n"
     "--\n\n"
     "Infers the output attributes for the given inputs.\n\n"
     "Contract:\n"
     " * If there is not enough information in `input_attrs` to infer\n"
     "   the output attributes, which means that the result is inconclusive,\n"
     "   the method should return an empty Attr.\n"
     " * An operator is allowed to return an inconclusive result only if one\n"
     "   (or more) of the arguments has an unspecified qtype.\n\n"
     "Args:\n"
     "  op: An operator.\n"
     "  input_attrs: Tuple with input attributes.\n\n"
     "Returns:\n"
     "  Output attributes.\n\n"
     "Raises:\n"
     "  ValueError: If the operator doesn't support the given input\n"
     "    (the result is conclusive)."),
};

}  // namespace arolla::python
