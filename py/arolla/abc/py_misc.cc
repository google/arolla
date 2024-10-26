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
#include "py/arolla/abc/py_misc.h"

#include <Python.h>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_operator.h"
#include "py/arolla/abc/py_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/abc/py_signature.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

using ::arolla::expr::DecayRegisteredOperator;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorRegistry;
using ::arolla::expr::HasAnnotationExprOperatorTag;
using ::arolla::expr::IsNameAnnotation;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::expr::ReadNameAnnotation;
using ::arolla::expr::RegisteredOperator;
using ::arolla::expr::ToLowerNode;
using ::arolla::expr::ToLowest;

// go/keep-sorted start block=yes newline_separated=yes
// def check_registered_operator_presence(op_name: str, /) -> bool
PyObject* PyCheckRegisteredOperatorPresence(PyObject* /*self*/,
                                            PyObject* py_arg) {
  Py_ssize_t op_name_size = 0;
  const char* op_name_data = PyUnicode_AsUTF8AndSize(py_arg, &op_name_size);
  if (op_name_data == nullptr) {
    PyErr_Clear();
    return PyErr_Format(PyExc_TypeError, "expected an operator name, got %s",
                        Py_TYPE(py_arg)->tp_name);
  }
  return PyBool_FromLong(
      ExprOperatorRegistry::GetInstance()->AcquireOperatorImplementationFn(
          absl::string_view(op_name_data, op_name_size))() != nullptr);
}

// def decay_registered_operator(op: str|QValue, /) -> QValue
PyObject* PyDecayRegisteredOperator(PyObject* /*self*/, PyObject* py_arg) {
  auto op = ParseArgPyOperator("arolla.abc.decay_registered_operator", py_arg);
  if (op == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(op, DecayRegisteredOperator(op),
                   (SetPyErrFromStatus(_), nullptr));
  return WrapAsPyQValue(TypedValue::FromValue(std::move(op)));
}

// def get_field_qtypes(qtype, /) -> tuple[QType, ...]
PyObject* PyGetFieldQTypes(PyObject* /*self*/, PyObject* py_arg) {
  const QType* qtype = UnwrapPyQType(py_arg);
  if (qtype == nullptr) {
    return nullptr;
  }
  const auto& fields = qtype->type_fields();
  if (fields.empty() && !IsTupleQType(qtype)) {
    return PyErr_Format(PyExc_ValueError,
                        "expected a qtype with fields, got %s",
                        std::string(qtype->name()).c_str());
  }
  auto py_tuple = PyObjectPtr::Own(PyTuple_New(fields.size()));
  for (size_t i = 0; i < fields.size(); ++i) {
    auto* py_qvalue =
        WrapAsPyQValue(TypedValue::FromValue(fields[i].GetType()));
    if (py_qvalue == nullptr) {
      return nullptr;
    }
    PyTuple_SET_ITEM(py_tuple.get(), i, py_qvalue);
  }
  return std::move(py_tuple).release();
}

// def get_operator_doc(op: QValue, /) -> str
PyObject* PyGetOperatorDoc(PyObject* /*self*/, PyObject* py_arg) {
  auto op = UnwrapPyOperator(py_arg);
  if (op == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto result, op->GetDoc(), (SetPyErrFromStatus(_), nullptr));
  return PyUnicode_FromStringAndSize(result.data(), result.size());
}

// def get_operator_name(op: QValue, /) -> str
PyObject* PyGetOperatorName(PyObject* /*self*/, PyObject* py_arg) {
  auto op = UnwrapPyOperator(py_arg);
  if (op == nullptr) {
    return nullptr;
  }
  absl::string_view result = op->display_name();
  return PyUnicode_FromStringAndSize(result.data(), result.size());
}

// def get_operator_signature(op: QValue, /) -> Signature
PyObject* PyGetOperatorSignature(PyObject* /*self*/, PyObject* py_arg) {
  auto op = UnwrapPyOperator(py_arg);
  if (op == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto result, op->GetSignature(),
                   (SetPyErrFromStatus(_), nullptr));
  return WrapAsPySignature(result);
}

// def get_registry_revision_id() -> int
PyObject* PyGetRegistryRevisionId(PyObject* /*self*/, PyObject* /*py_args*/) {
  static const auto rev_id_fn =
      expr::ExprOperatorRegistry::GetInstance()->AcquireRevisionIdFn("");
  return PyLong_FromLongLong(rev_id_fn());
}

// def is_annotation_operator(op: str|QValue, /) -> bool
PyObject* PyIsAnnotationOperator(PyObject* /*self*/, PyObject* py_arg) {
  auto op = ParseArgPyOperator("arolla.abc.is_annotation_operator", py_arg);
  if (op == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(op, DecayRegisteredOperator(op),
                   (SetPyErrFromStatus(_), nullptr));
  return PyBool_FromLong(HasAnnotationExprOperatorTag(op));
}

// def leaf(leaf_key: str, /) -> Expr
PyObject* PyLeaf(PyObject* /*self*/, PyObject* py_arg) {
  Py_ssize_t key_size = 0;
  if (const char* key_data = PyUnicode_AsUTF8AndSize(py_arg, &key_size)) {
    return WrapAsPyExpr(Leaf(absl::string_view(key_data, key_size)));
  }
  PyErr_Clear();
  return PyErr_Format(PyExc_TypeError, "expected a leaf key, got %s",
                      Py_TYPE(py_arg)->tp_name);
}

// def literal(value: QValue, /) -> Expr
PyObject* PyLiteral(PyObject* /*self*/, PyObject* py_arg) {
  if (auto* qvalue = UnwrapPyQValue(py_arg)) {
    return WrapAsPyExpr(Literal(*qvalue));
  }
  return nullptr;
}

// def placeholder(placeholder_key: str, /) -> Expr
PyObject* PyPlaceholder(PyObject* /*self*/, PyObject* py_arg) {
  Py_ssize_t key_size = 0;
  if (const char* key_data = PyUnicode_AsUTF8AndSize(py_arg, &key_size)) {
    return WrapAsPyExpr(Placeholder(absl::string_view(key_data, key_size)));
  }
  return PyErr_Format(PyExc_TypeError, "expected a placeholder key, got %s",
                      Py_TYPE(py_arg)->tp_name);
}

// def read_name_annotation(node: Expr, /) -> str|None
PyObject* PyReadNameAnnotation(PyObject* /*self*/, PyObject* py_arg) {
  const auto& node = UnwrapPyExpr(py_arg);
  if (node == nullptr) {
    return nullptr;
  }
  if (!IsNameAnnotation(node)) {
    Py_RETURN_NONE;
  }
  const auto& name = ReadNameAnnotation(node);
  return PyUnicode_FromStringAndSize(name.data(), name.size());
}

// def to_lower_node(node: Expr, /) -> Expr
PyObject* PyToLowerNode(PyObject* /*self*/, PyObject* py_arg) {
  auto expr = UnwrapPyExpr(py_arg);
  if (expr == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto result, ToLowerNode(expr),
                   (SetPyErrFromStatus(_), nullptr));
  return WrapAsPyExpr(std::move(result));
}

// def to_lowest(expr: Expr, /) -> Expr
PyObject* PyToLowest(PyObject* /*self*/, PyObject* py_arg) {
  auto expr = UnwrapPyExpr(py_arg);
  if (expr == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto result, ToLowest(expr),
                   (SetPyErrFromStatus(_), nullptr));
  return WrapAsPyExpr(std::move(result));
}

// def unsafe_make_registered_operator(op_name: str, /) -> QValue
PyObject* PyUnsafeMakeRegisteredOperator(PyObject* /*self*/, PyObject* py_arg) {
  Py_ssize_t op_name_size = 0;
  const char* op_name_data = PyUnicode_AsUTF8AndSize(py_arg, &op_name_size);
  if (op_name_data == nullptr) {
    PyErr_Clear();
    return PyErr_Format(PyExc_TypeError, "expected an operator name, got %s",
                        Py_TYPE(py_arg)->tp_name);
  }
  return WrapAsPyQValue(TypedValue::FromValue<ExprOperatorPtr>(
      std::make_shared<RegisteredOperator>(
          absl::string_view(op_name_data, op_name_size))));
}

// def unspecified() -> QValue
PyObject* PyUnspecified(PyObject* /*self*/, PyObject* /*py_args*/) {
  return WrapAsPyQValue(GetUnspecifiedQValue());
}

// def vectorcall(
//     fn: Callable[..., Any], args: Any..., kw_names: tuple[str, ...], /
// ) -> Any
PyObject* PyVectorcall(PyObject* /*self*/, PyObject* const* args,
                       Py_ssize_t nargs) {
  if (nargs < 2) {
    return PyErr_Format(PyExc_TypeError,
                        "expected at least two positional arguments, got %zd",
                        nargs);
  }
  PyObject* py_callable = args[0];
  PyObject* py_tuple_kwnames = args[nargs - 1];
  if (!PyTuple_CheckExact(py_tuple_kwnames)) {
    return PyErr_Format(
        PyExc_TypeError,
        "expected the last argument to be tuple[str, ...], got %s",
        Py_TYPE(py_tuple_kwnames)->tp_name);
  }
  Py_ssize_t py_tuple_kwnames_size = PyTuple_GET_SIZE(py_tuple_kwnames);
  if (py_tuple_kwnames_size > nargs - 2) {
    return PyErr_Format(PyExc_TypeError,
                        "too few positional arguments (=%zd) for the given "
                        "number of keyword names (=%zd)",
                        nargs, py_tuple_kwnames_size);
  }
  return _PyObject_Vectorcall(
      py_callable, args + 1,
      (nargs - 2 - py_tuple_kwnames_size) | PY_VECTORCALL_ARGUMENTS_OFFSET,
      py_tuple_kwnames);
}

// def deep_transform(expr: Expr, transform_fn: Callable[[Expr], Expr]) -> Expr
// def transform(expr: Expr, transform_fn: Callable[[Expr], Expr]) -> Expr
namespace py_transform {

struct DeepTransformTraits {
  static constexpr const char* kFnName = "arolla.abc.deep_transform";
  static auto transform(const ExprNodePtr& expr, auto transform_fn) {
    return ::arolla::expr::DeepTransform(expr, std::move(transform_fn));
  }
};

struct TransformTraits {
  static constexpr const char* kFnName = "arolla.abc.transform";
  static auto transform(const ExprNodePtr& expr, auto transform_fn) {
    return ::arolla::expr::Transform(expr, std::move(transform_fn));
  }
};

template <typename Traits>
PyObject* Impl(PyObject* /*self*/, PyObject* py_args, PyObject* py_kwargs) {
  PyObject* py_expr = nullptr;
  PyObject* py_transform_fn = nullptr;
  static const absl::NoDestructor<std::string> format(std::string("OO:") +
                                                      Traits::kFnName);
  static constexpr const char* keywords[] = {"expr", "transform_fn", nullptr};
  if (!PyArg_ParseTupleAndKeywords(py_args, py_kwargs, format->c_str(),
                                   const_cast<char**>(keywords), &py_expr,
                                   &py_transform_fn)) {
    return nullptr;
  }
  // Parse `expr`.
  const auto expr = UnwrapPyExpr(py_expr);
  if (expr == nullptr) {
    PyErr_Clear();
    return PyErr_Format(PyExc_TypeError,
                        "%s() expected an expression, got expr: %s",
                        Traits::kFnName, Py_TYPE(py_expr)->tp_name);
  }
  // Parse `py_transform_fn`.
  if (!PyCallable_Check(py_transform_fn)) {
    return PyErr_Format(
        PyExc_TypeError,
        "%s() expected Callable[[Expr], Expr], got transform_fn: %s",
        Traits::kFnName, Py_TYPE(py_transform_fn)->tp_name);
  }
  auto transform_fn =
      [&](const ExprNodePtr& node) -> absl::StatusOr<ExprNodePtr> {
    auto py_node = PyObjectPtr::Own(WrapAsPyExpr(node));
    if (py_node == nullptr) {
      return StatusWithRawPyErr(absl::StatusCode::kInternal, "internal error");
    }
    auto py_ret_node =
        PyObjectPtr::Own(PyObject_CallOneArg(py_transform_fn, py_node.get()));
    if (py_ret_node == nullptr) {
      return StatusWithRawPyErr(absl::StatusCode::kFailedPrecondition,
                                "transform_fn() has failed");
    }
    if (!IsPyExprInstance(py_ret_node.get())) {
      PyErr_Format(PyExc_TypeError, "transform_fn() unexpected return type: %s",
                   Py_TYPE(py_ret_node.get())->tp_name);
      return StatusWithRawPyErr(absl::StatusCode::kFailedPrecondition,
                                "transform_fn() unexpected return type");
    }
    return UnsafeUnwrapPyExpr(py_ret_node.get());
  };
  // Generate the result.
  ASSIGN_OR_RETURN(auto result, Traits::transform(expr, transform_fn),
                   (SetPyErrFromStatus(_), nullptr));
  return WrapAsPyExpr(std::move(result));
}

}  // namespace py_transform

// go/keep-sorted end

}  // namespace

// go/keep-sorted start block=yes newline_separated=yes
const PyMethodDef kDefPyCheckRegisteredOperatorPresence = {
    "check_registered_operator_presence",
    &PyCheckRegisteredOperatorPresence,
    METH_O,
    ("check_registered_operator_presence(op_name, /)\n"
     "--\n\n"
     "Checks presence of an operator in the registry."),
};

const PyMethodDef kDefPyDecayRegisteredOperator = {
    "decay_registered_operator",
    &PyDecayRegisteredOperator,
    METH_O,
    ("decay_registered_operator(op, /)\n"
     "--\n\n"
     "Returns the implementation of the given registered operator.\n\n"
     "If the argument is a registered operator, the function retrieves its\n"
     "implementation from the registry. Otherwise, it returns the operator\n"
     "unchanged."),
};

const PyMethodDef kDefPyDeepTransform = {
    "deep_transform",
    reinterpret_cast<PyCFunction>(
        &py_transform::Impl<py_transform::DeepTransformTraits>),
    METH_VARARGS | METH_KEYWORDS,
    ("deep_transform(expr, transform_fn)\n"
     "--\n\n"
     "Transforms the `expr` by deeply applying `transform_fn`.\n\n"
     "The `transform_fn` is applied to each Expr node and to each node created "
     "by\n"
     "previous `transform_fn` calls.\n\n"
     "The nodes are processed in post order. For each call to "
     "`transform_fn(node)`\n"
     "it is guaranteed that all the node_deps are already processed and "
     "replaced by\n"
     "transformed versions.\n\n"
     "Note that `transform_fn` must not cause an infinite chain of "
     "transformations\n"
     "(e.g. a->b, b->c, c->a), otherwise an error will be returned. Applying "
     "no\n"
     "transformation (e.g. a->a) is permitted.\n\n"
     "Args:\n"
     "  expr: The expression to be transformed.\n"
     "  transform_fn: A function that applies a transformation to input "
     "nodes.\n\n"
     "Returns:\n"
     "  The transformed expression."),
};

const PyMethodDef kDefPyGetFieldQTypes = {
    "get_field_qtypes",
    &PyGetFieldQTypes,
    METH_O,
    ("get_field_qtypes(qtype, /)\n"
     "--\n\n"
     "Returns a tuple with field qtypes."),
};

const PyMethodDef kDefPyGetOperatorDoc = {
    "get_operator_doc",
    &PyGetOperatorDoc,
    METH_O,
    ("get_operator_doc(op, /)\n"
     "--\n\n"
     "Returns the operator's doc."),
};

const PyMethodDef kDefPyGetOperatorName = {
    "get_operator_name",
    &PyGetOperatorName,
    METH_O,
    ("get_operator_name(op, /)\n"
     "--\n\n"
     "Returns the operator's name."),
};

const PyMethodDef kDefPyGetOperatorSignature = {
    "get_operator_signature",
    &PyGetOperatorSignature,
    METH_O,
    ("get_operator_signature(op, /)\n"
     "--\n\n"
     "Returns the operator's signature."),
};

const PyMethodDef kDefPyGetRegistryRevisionId = {
    "get_registry_revision_id",
    &PyGetRegistryRevisionId,
    METH_NOARGS,
    ("get_registry_revision_id()\n"
     "--\n\n"
     "Returns the current revision of the operator registry.\n\n"
     "This function helps to detect changes in the registry. Any observable\n"
     "change in the registry alters the revision id."),
};

const PyMethodDef kDefPyIsAnnotationOperator = {
    "is_annotation_operator",
    &PyIsAnnotationOperator,
    METH_O,
    ("is_annotation_operator(op, /)\n"
     "--\n\n"
     "Returns True, iff the argument is an annotation operator or\n"
     "a registered operator that proxies to an annotation operator."),
};

const PyMethodDef kDefPyLeaf = {
    "leaf",
    &PyLeaf,
    METH_O,
    ("leaf(leaf_key, /)\n"
     "--\n\n"
     "Returns a leaf node with the given key."),
};

const PyMethodDef kDefPyLiteral = {
    "literal",
    &PyLiteral,
    METH_O,
    ("literal(value, /)\n"
     "--\n\n"
     "Returns a literal node with the given value."),
};

const PyMethodDef kDefPyPlaceholder = {
    "placeholder",
    &PyPlaceholder,
    METH_O,
    ("placeholder(placeholder_key, /)\n"
     "--\n\n"
     "Returns a placeholder node with the given key."),
};

const PyMethodDef kDefPyReadNameAnnotation = {
    "read_name_annotation",
    &PyReadNameAnnotation,
    METH_O,
    ("read_name_annotation(node, /)\n"
     "--\n\n"
     "Returns the name tag if the node is a name annotation; otherwise, None."),
};

const PyMethodDef kDefPyToLowerNode = {
    "to_lower_node",
    &PyToLowerNode,
    METH_O,
    ("to_lower_node(node, /)\n"
     "--\n\n"
     "Returns the expression after applying \"ToLowerLevel\" to the top node."),
};

const PyMethodDef kDefPyToLowest = {
    "to_lowest",
    &PyToLowest,
    METH_O,
    ("to_lowest(expr, /)\n"
     "--\n\n"
     "Returns the expression transitioned to the lowest accessible level."),
};

const PyMethodDef kDefPyTransform = {
    "transform",
    reinterpret_cast<PyCFunction>(
        &py_transform::Impl<py_transform::TransformTraits>),
    METH_VARARGS | METH_KEYWORDS,
    ("transform(expr, transform_fn)\n"
     "--\n\n"
     "Transforms the `expr` by applying `transform_fn`.\n\n"
     "The `transform_fn` is applied to each Expr node.\n\n"
     "The nodes are processed in post order. For each call to "
     "`transform_fn(node)`\n"
     "it is guaranteed that all the node_deps are already processed and "
     "replaced by\n"
     "transformed versions.\n\n"
     "Args:\n"
     "  expr: The expression to be transformed.\n"
     "  transform_fn: A function that applies a transformation to input "
     "nodes.\n\n"
     "Returns:\n"
     "  The transformed expression."),
};

const PyMethodDef kDefPyUnsafeMakeRegisteredOperator = {
    "unsafe_make_registered_operator",
    &PyUnsafeMakeRegisteredOperator,
    METH_O,
    ("unsafe_make_registered_operator(op_name, /)\n"
     "--\n\n"
     "Returns a proxy to an operator in the registry.\n\n"
     "This function does not perform a presence check on the registry.\n\n"
     "Args:\n"
     "  op_name: Operator name."),
};

const PyMethodDef kDefPyUnspecified = {
    "unspecified",
    &PyUnspecified,
    METH_NOARGS,
    ("unspecified()\n"
     "--\n\n"
     "Returns `unspecified` value.\n\n"
     "The main purpose of `unspecified` is to serve as a default value\n"
     "for a parameter in situations where the actual default value must\n"
     "be determined based on other parameters."),
};

const PyMethodDef kDefPyVectorcall = {
    "vectorcall",
    reinterpret_cast<PyCFunction>(&PyVectorcall),
    METH_FASTCALL,
    ("vectorcall(fn, /, *args)\n"
     "--\n\n"
     "vectorcall(fn: Callable, args: Any..., kw_names: tuple[str, ...]\n\n"
     "This is a proxy for PyObject_Vectorcall() in the Python C API. It "
     "provides\nan alternative for representing calls like:\n\n"
     "  fn(*args[:n], **dict(zip(kw_names, args [n:])))\n\n"
     "as\n\n  vectorcall(fn, *args, kw_names)\n\n"
     "which may be more efficient in certain situations."),
};

// go/keep-sorted end

}  // namespace arolla::python
