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
#include "py/arolla/abc/py_bind.h"

#include <Python.h>

#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_aux_binding_policy.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_operator.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

using ::arolla::expr::BindOp;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::RegisteredOperator;

// def make_operator_node(
//     op: str|QValue, inputs: tuple[QValue|Expr, ...] = (), /
// ) -> Expr
PyObject* PyMakeOperatorNode(PyObject* /*self*/, PyObject** py_args,
                             Py_ssize_t nargs) {
  if (nargs < 1) {
    PyErr_SetString(PyExc_TypeError,
                    "arolla.abc.make_operator_node() missing 1 required "
                    "positional argument: 'op'");
    return nullptr;
  } else if (nargs > 2) {
    return PyErr_Format(PyExc_TypeError,
                        "arolla.abc.make_operator_node() takes 2 positional "
                        "arguments but %zu were given",
                        nargs);
  }
  // Parse `op`.
  auto op = ParseArgPyOperator("arolla.abc.make_operator_node", py_args[0]);
  if (op == nullptr) {
    return nullptr;
  }
  // Parse `inputs`.
  std::vector<ExprNodePtr> inputs;
  if (nargs == 2) {
    PyObject* py_tuple_inputs = py_args[1];
    if (!PyTuple_Check(py_tuple_inputs)) {
      return PyErr_Format(PyExc_TypeError,
                          "arolla.abc.make_operator_node() expected a "
                          "tuple[Expr|QValue, ...], got inputs: %s",
                          Py_TYPE(py_tuple_inputs)->tp_name);
    }
    inputs.resize(PyTuple_GET_SIZE(py_tuple_inputs));
    for (size_t i = 0; i < inputs.size(); ++i) {
      PyObject* py_input = PyTuple_GET_ITEM(py_tuple_inputs, i);
      if (IsPyExprInstance(py_input)) {
        inputs[i] = UnsafeUnwrapPyExpr(py_input);
      } else if (IsPyQValueInstance(py_input)) {
        inputs[i] = Literal(UnsafeUnwrapPyQValue(py_input));
      } else {
        return PyErr_Format(PyExc_TypeError,
                            "arolla.abc.make_operator_node() expected "
                            "Expr|QValue, got inputs[%zu]: %s",
                            i, Py_TYPE(py_input)->tp_name);
      }
    }
  }
  ASSIGN_OR_RETURN(auto result, MakeOpNode(std::move(op), std::move(inputs)),
                   (SetPyErrFromStatus(_), nullptr));
  return WrapAsPyExpr(std::move(result));
}

// def unsafe_make_operator_node(
//     op: str|QValue, inputs: tuple[QValue|Expr, ...] = (), /
// ) -> Expr
PyObject* PyUnsafeMakeOperatorNode(PyObject* /*self*/, PyObject** py_args,
                                   Py_ssize_t nargs) {
  if (nargs < 1) {
    PyErr_SetString(
        PyExc_TypeError,
        "arolla.abc.unsafe_make_operator_node() missing 1 required "
        "positional argument: 'op'");
    return nullptr;
  } else if (nargs > 2) {
    return PyErr_Format(
        PyExc_TypeError,
        "arolla.abc.unsafe_make_operator_node() takes 2 positional "
        "arguments but %zu were given",
        nargs);
  }
  // Parse `op`.
  ExprOperatorPtr op;
  auto py_op = py_args[0];
  if (IsPyQValueInstance(py_op)) {
    auto& qvalue_op = UnsafeUnwrapPyQValue(py_op);
    if (qvalue_op.GetType() != GetQType<ExprOperatorPtr>()) {
      PyErr_Format(PyExc_TypeError,
                   "arolla.abc.unsafe_make_operator_node() expected "
                   "Operator|str, got op: %s",
                   Py_TYPE(py_op)->tp_name);
      return nullptr;
    }
    op = qvalue_op.UnsafeAs<ExprOperatorPtr>();
  } else {
    Py_ssize_t op_name_size = 0;
    const char* op_name_data = PyUnicode_AsUTF8AndSize(py_op, &op_name_size);
    if (op_name_data == nullptr) {
      PyErr_Clear();
      PyErr_Format(PyExc_TypeError,
                   "arolla.abc.unsafe_make_operator_node() expected "
                   "Operator|str, got op: %s",
                   Py_TYPE(py_op)->tp_name);
      return nullptr;
    }
    op = std::make_shared<RegisteredOperator>(
        absl::string_view(op_name_data, op_name_size));
  }
  // Parse `inputs`.
  std::vector<ExprNodePtr> inputs;
  if (nargs == 2) {
    PyObject* py_tuple_inputs = py_args[1];
    if (!PyTuple_Check(py_tuple_inputs)) {
      return PyErr_Format(PyExc_TypeError,
                          "arolla.abc.unsafe_make_operator_node() expected a "
                          "tuple[Expr|QValue, ...], got inputs: %s",
                          Py_TYPE(py_tuple_inputs)->tp_name);
    }
    inputs.resize(PyTuple_GET_SIZE(py_tuple_inputs));
    for (size_t i = 0; i < inputs.size(); ++i) {
      PyObject* py_input = PyTuple_GET_ITEM(py_tuple_inputs, i);
      if (IsPyExprInstance(py_input)) {
        inputs[i] = UnsafeUnwrapPyExpr(py_input);
      } else if (IsPyQValueInstance(py_input)) {
        inputs[i] = Literal(UnsafeUnwrapPyQValue(py_input));
      } else {
        return PyErr_Format(PyExc_TypeError,
                            "arolla.abc.unsafe_make_operator_node() expected "
                            "Expr|QValue, got inputs[%zu]: %s",
                            i, Py_TYPE(py_input)->tp_name);
      }
    }
  }
  return WrapAsPyExpr(ExprNode::UnsafeMakeOperatorNode(
      std::move(op), std::move(inputs), ExprAttributes{}));
}

// def bind_op(
//     op: str|QValue, /, *args: QValue|Expr, **kwargs: QValue|Expr
// ) -> Expr
PyObject* PyBindOp(PyObject* /*self*/, PyObject* const* py_args,
                   Py_ssize_t nargs, PyObject* py_tuple_kwnames) {
  if (nargs == 0) {
    PyErr_SetString(
        PyExc_TypeError,
        "arolla.abc.bind_op() missing 1 required positional argument: 'op'");
    return nullptr;
  }
  // Parse the operator.
  auto op = ParseArgPyOperator("arolla.abc.bind_op", py_args[0]);
  if (op == nullptr) {
    return nullptr;
  }
  // Parse args.
  const auto extract_expr = [](PyObject* py_arg) -> ExprNodePtr /*nullable*/ {
    if (IsPyExprInstance(py_arg)) {
      return UnsafeUnwrapPyExpr(py_arg);
    } else if (IsPyQValueInstance(py_arg)) {
      return Literal(UnsafeUnwrapPyQValue(py_arg));
    } else {
      return nullptr;
    }
  };
  std::vector<ExprNodePtr> args(nargs - 1);
  for (size_t i = 0; i < args.size(); ++i) {
    if (ExprNodePtr expr = extract_expr(py_args[1 + i]); expr == nullptr) {
      PyErr_Format(
          PyExc_TypeError,
          "arolla.abc.bind_op() expected Expr|QValue, got args[%zu]: %s", i,
          Py_TYPE(py_args[1 + i])->tp_name);
      return nullptr;
    } else {
      args[i] = std::move(expr);
    }
  }
  // Parse kwargs.
  absl::flat_hash_map<std::string, ExprNodePtr> kwargs;
  if (py_tuple_kwnames != nullptr) {
    Py_ssize_t kwargs_size = PyTuple_GET_SIZE(py_tuple_kwnames);
    kwargs.reserve(kwargs_size);
    for (Py_ssize_t i = 0; i < kwargs_size; ++i) {
      PyObject* const py_key = PyTuple_GET_ITEM(py_tuple_kwnames, i);
      PyObject* const py_arg = py_args[nargs + i];
      Py_ssize_t key_size = 0;
      const char* key_data = PyUnicode_AsUTF8AndSize(py_key, &key_size);
      if (key_data == nullptr) {
        return nullptr;
      }
      absl::string_view key(key_data, key_size);
      if (ExprNodePtr expr = extract_expr(py_arg); expr == nullptr) {
        PyErr_Format(
            PyExc_TypeError,
            "arolla.abc.bind_op() expected Expr|QValue, got kwargs[%R]: %s",
            py_key, Py_TYPE(py_arg)->tp_name);
        return nullptr;
      } else {
        kwargs[key] = std::move(expr);
      }
    }
  }
  // Bind
  absl::StatusOr<ExprNodePtr> status_or = BindOp(std::move(op), args, kwargs);
  if (!status_or.ok()) {
    SetPyErrFromStatus(status_or.status());
    return nullptr;
  }
  return WrapAsPyExpr(*std::move(status_or));
}

// def aux_bind_op(
//     op: str|QValue, /, *args: QValue|Expr, **kwargs: QValue|Expr
// ) -> QValue
PyObject* PyAuxBindOp(PyObject* /*self*/, PyObject** py_args, Py_ssize_t nargs,
                      PyObject* py_tuple_kwnames) {
  DCheckPyGIL();
  if (nargs == 0) {
    PyErr_SetString(PyExc_TypeError,
                    "arolla.abc.aux_bind_op() missing 1 required positional "
                    "argument: 'op'");
    return nullptr;
  }
  // Parse the operator.
  auto op = ParseArgPyOperator("arolla.abc.aux_bind_op", py_args[0]);
  if (op == nullptr) {
    return nullptr;
  }
  // Bind the arguments.
  auto signature = op->GetSignature();
  if (!signature.ok()) {
    SetPyErrFromStatus(signature.status());
    return nullptr;
  }
  std::vector<QValueOrExpr> bound_args;
  AuxBindingPolicyPtr policy_implementation;
  if (!AuxBindArguments(
          *signature, py_args + 1, (nargs - 1) | PY_VECTORCALL_ARGUMENTS_OFFSET,
          py_tuple_kwnames, &bound_args, &policy_implementation)) {
    return nullptr;
  }
  std::vector<ExprNodePtr> node_deps(bound_args.size());
  for (size_t i = 0; i < bound_args.size(); ++i) {
    node_deps[i] = std::visit(
        [&](auto&& bound_arg) {
          using T = std::decay_t<decltype(bound_arg)>;
          if constexpr (std::is_same_v<T, TypedValue>) {
            auto expr = policy_implementation->MakeLiteral(
                std::forward<decltype(bound_arg)>(bound_arg));
            if (expr == nullptr) {
              PyErr_FormatFromCause(
                  PyExc_RuntimeError,
                  "arolla.abc.aux_bind_op() call to make_literal() failed");
            }
            return expr;
          } else {
            return std::forward<decltype(bound_arg)>(bound_arg);
          }
        },
        std::move(bound_args[i]));
    if (node_deps[i] == nullptr) {
      return nullptr;
    }
  }
  // Create an operator node.
  absl::StatusOr<ExprNodePtr> result =
      MakeOpNode(std::move(op), std::move(node_deps));
  if (!result.ok()) {
    SetPyErrFromStatus(result.status());
    return nullptr;
  }
  return WrapAsPyExpr(*std::move(result));
}

// def aux_get_python_signature(
//     op: str|QValue, /
// ) -> PySignature|inspect.Signature
PyObject* PyAuxGetPythonSignature(PyObject* /*self*/, PyObject* py_op) {
  DCheckPyGIL();
  auto op = ParseArgPyOperator("arolla.abc.aux_get_python_signature", py_op);
  if (op == nullptr) {
    return nullptr;
  }
  auto signature = op->GetSignature();
  if (!signature.ok()) {
    SetPyErrFromStatus(signature.status());
    return nullptr;
  }
  return AuxMakePythonSignature(*signature);
}

}  // namespace

const PyMethodDef kDefPyMakeOperatorNode = {
    "make_operator_node",
    reinterpret_cast<PyCFunction>(PyMakeOperatorNode),
    METH_FASTCALL,
    ("make_operator_node(op, inputs=(), /)\n"
     "--\n\n"
     "Returns an operator node with the given operator and inputs.\n\n"
     "This function validates the dependencies and infers the node\n"
     "attributes.\n\n"
     "Args:\n"
     "  op: An operator.\n"
     "  inputs: Node inputs that will be attached as-is."),
};

const PyMethodDef kDefPyUnsafeMakeOperatorNode = {
    "unsafe_make_operator_node",
    reinterpret_cast<PyCFunction>(PyUnsafeMakeOperatorNode),
    METH_FASTCALL,
    ("unsafe_make_operator_node(op, inputs=(), /)\n"
     "--\n\n"
     "Returns an operator node with the given operator and inputs.\n\n"
     "NOTE: Only use this function if you know what you're doing. This\n"
     "function does not validate the input dependencies and does not\n"
     "perform attribute inference.\n\n"
     "Args:\n"
     "  op: An operator.\n"
     "  inputs: Node inputs that will be attached as-is. Must match\n"
     "    with the operator's signature."),
};

const PyMethodDef kDefPyBindOp = {
    "bind_op",
    reinterpret_cast<PyCFunction>(&PyBindOp),
    METH_FASTCALL | METH_KEYWORDS,
    ("bind_op(op, /, *args, **kwargs)\n"
     "--\n\n"
     "Returns an operator node with a specific operator and arguments."),
};

const PyMethodDef kDefPyAuxBindOp = {
    "aux_bind_op",
    reinterpret_cast<PyCFunction>(&PyAuxBindOp),
    METH_FASTCALL | METH_KEYWORDS,
    ("aux_bind_op(op, /, *args, **kwargs)\n"
     "--\n\n"
     "Returns an operator node with a specific operator and arguments.\n"
     "NOTE: The behaviour of this function depends on `signature.aux_policy`."),
};

const PyMethodDef kDefPyAuxGetPythonSignature = {
    "aux_get_python_signature",
    &PyAuxGetPythonSignature,
    METH_O,
    ("aux_get_python_signature(op, /)\n"
     "--\n\n"
     "Returns a \"python\" signature of the operator.\n"
     "NOTE: The behaviour of this function depends on `signature.aux_policy`."),
};

}  // namespace arolla::python
