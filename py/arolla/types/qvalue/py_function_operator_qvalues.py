# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""(Private) QValue specialisations for PyFunctionOperator."""

from __future__ import annotations

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing
from arolla.types.qvalue import clib
from arolla.types.qvalue import helpers


class PyFunctionOperator(arolla_abc.Operator):
  """QValue specialization for PyFunctionOperator."""

  __slots__ = ()

  def __new__(
      cls,
      name: str,
      signature: arolla_abc.MakeOperatorSignatureArg,
      eval_fn: arolla_abc.PyObject,
      *,
      qtype_inference_expr: arolla_abc.QType | arolla_abc.Expr,
      qtype_constraints: helpers.QTypeConstraints = (),
      doc: str = '',
  ) -> PyFunctionOperator:
    """Constructs an operator that evaluates the given function.

    Args:
      name: An operator name.
      signature: An operator signature.
      eval_fn: The function to be evaluated. All inputs will be passed as
        QValues and the result should be a QValue matching the provided
        `qtype_inference_expr`. Note that the callable should be pure and
        functionally const (i.e. it should not be mutated or depend on objects
        that are mutated between evaluations). No guarantees are made for the
        correctness of functions that rely on side-effects or on changing
        non-local data, as we may e.g. cache evaluation results.
      qtype_inference_expr: expression that computes operator's output qtype; an
        argument qtype can be referenced as P.arg_name.
      qtype_constraints: List of (predicate_expr, error_message) pairs.
        predicate_expr may refer to the argument QType as P.arg_name. If a qtype
        constraint is not fulfilled, the corresponding error_message is used.
        Placeholders, like {arg_name}, get replaced with the actual type names
        during the error message formatting.
      doc: An operator doc-string.

    Returns:
      Constructed operator.
    """
    if not callable(eval_fn.py_value()):
      raise TypeError(
          'expected `eval_fn.py_value()` to be a callable, '
          f'was {type(eval_fn.py_value())}'
      )
    signature = arolla_abc.make_operator_signature(
        signature, as_qvalue=boxing.as_qvalue
    )
    qtype_inference_expr = boxing.as_expr(qtype_inference_expr)
    prepared_qtype_constraints = helpers.prepare_qtype_constraints(
        qtype_constraints
    )
    return clib.make_py_function_operator(
        name,
        signature,
        doc,
        qtype_inference_expr,
        prepared_qtype_constraints,
        eval_fn,
    )

  def get_qtype_inference_expr(self) -> arolla_abc.Expr:
    """Returns the provided `qtype_inference_expr`."""
    return clib.get_py_function_operator_qtype_inference_expr(self)

  def get_eval_fn(self) -> arolla_abc.PyObject:
    """Returns the provided `eval_fn`."""
    return clib.get_py_function_operator_py_eval_fn(self)

  def get_qtype_constraints(self) -> list[helpers.QTypeConstraint]:
    """Returns the provided `qtype_constraints`."""
    return clib.get_py_function_operator_qtype_constraints(self)


arolla_abc.register_qvalue_specialization(
    '::arolla::python::PyFunctionOperator', PyFunctionOperator
)
