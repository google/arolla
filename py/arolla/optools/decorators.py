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

"""Module with helpers for operator declarations."""

import inspect
from typing import Any, Callable, Sequence

from arolla.abc import abc as arolla_abc
from arolla.types import types as arolla_types


def _build_operator_signanture_from_fn(
    fn: Callable[..., Any], experimental_aux_policy: str
) -> arolla_abc.Signature:
  signature = arolla_abc.make_operator_signature(
      inspect.signature(fn), as_qvalue=arolla_types.as_qvalue
  )
  return arolla_abc.Signature((signature.parameters, experimental_aux_policy))


def add_to_registry(
    name: str | None = None,
    *,
    unsafe_override: bool = False,
) -> Callable[[arolla_types.Operator], arolla_abc.RegisteredOperator]:
  """A decorator that adds an operator to the operator registry.

  Example:

    @arolla.optools.add_to_registry()
    @arolla.optools.as_lambda_operator('custom.identity')
    def identity(x):
      return x

  Args:
    name: Operator name for registration (by default it uses the operator's own
      name).
    unsafe_override: Allows to override an existing operator. Overriding is
      intrinsically unsafe, please use it with caution (for more information
      check arolla.abc.unsafe_override_registered_operator).

  Returns:
    A decorator for an arolla operator.
  """

  def impl(op: arolla_types.Operator) -> arolla_abc.RegisteredOperator:
    if name is None:
      registration_name = op.display_name
    else:
      registration_name = name
    if unsafe_override:
      return arolla_abc.unsafe_override_registered_operator(
          registration_name, op
      )
    return arolla_abc.register_operator(registration_name, op)

  return impl


def as_backend_operator(
    name: str,
    *,
    qtype_inference_expr: arolla_abc.Expr | arolla_abc.QType,
    qtype_constraints: Sequence[tuple[arolla_abc.Expr, str]] = (),
    experimental_aux_policy: str = '',
) -> Callable[[Callable[..., Any]], arolla_types.BackendOperator]:
  """A decorator for backend operator construction.

  Example:

    @arolla.optools.as_backend_operator(
        'math.add',
        qtype_inference_expr=M.qtype.common_qtype(P.x, P.y),
        qtype_constraints=[
            (M.qtype.is_numeric_qtype(P.x),
             '`x` expected a numeric type, got {x}'),
            (M.qtype.is_numeric_qtype(P.y),
             '`y` expected a numeric type, got {y}'),
            (M.qtype.common_qtype(P.x, P.y) != arolla.NOTHING,
             'no common type for `x:{x}` and `y:{y}`'),
    ])
    def math_add(x, y):
      raise NotImplementedError('implemented in backend')


  Args:
    name: Backend operator name.
    qtype_inference_expr: Expression that computes operator's output qtype.
      Argument qtype's can be referenced by P.arg_name.
    qtype_constraints: List of (predicate_expr, error_message) pairs.
      predicate_expr may refer to the argument QType as P.arg_name. If a qtype
      constraint is not fulfilled, the corresponding error_message is used.
      Placeholders, like {arg_name}, get replaced with the actual type names
      during the error message formatting.
    experimental_aux_policy: An auxiliary policy for the argument binding; it
      allows to customize operators call syntax.

  Returns:
    A decorator that constructs a backend operator after a python function
    (it's expected that the function raises NotImplementedError).
  """

  def impl(fn):
    signature = _build_operator_signanture_from_fn(fn, experimental_aux_policy)
    return arolla_types.BackendOperator(
        name,
        signature,
        doc=inspect.getdoc(fn) or '',
        qtype_inference_expr=qtype_inference_expr,
        qtype_constraints=qtype_constraints,
    )

  return impl


def _build_lambda_body_from_fn(
    signature: arolla_abc.Signature, fn: Callable[..., Any]
) -> arolla_abc.Expr:
  """Builds lambda body from the Python function that constructs it."""
  mangled_param_names = {
      f'_as_lambda_operator_argument_{param.name}': arolla_abc.placeholder(
          param.name
      )
      for param in signature.parameters
  }
  mangled_expr = arolla_types.as_expr(
      fn(*(arolla_abc.placeholder(name) for name in mangled_param_names))
  )
  unknown_placeholders = set(
      arolla_abc.get_placeholder_keys(mangled_expr)
  ) - set(mangled_param_names)
  if unknown_placeholders:
    raise ValueError(
        'unexpected placeholders in lambda operator definition: '
        + ', '.join(
            repr(arolla_abc.placeholder(key))
            for key in sorted(unknown_placeholders)
        )
    )
  return arolla_abc.sub_placeholders(mangled_expr, **mangled_param_names)


def as_lambda_operator(
    name: str,
    *,
    qtype_constraints: Sequence[tuple[arolla_abc.Expr, str]] = (),
    experimental_aux_policy: str = '',
) -> Callable[[Callable[..., Any]], arolla_types.RestrictedLambdaOperator]:
  """A decorator for a restricted lambda operator construction.

  Decorator needs to be applied to a python function that returns a valid
  expression for the lambda body.

  Example:

    @arolla.optools.as_lambda_operator(
        'array_size',
        qtype_constraints=[
            (M.qtype.is_array_qtype(P.size), 'expected an array, got x:{x}')
        ],
    )
    def array_size(x):
      return M.array.shape_size(M.core.shape_of(x))


  Args:
    name: Operator name.
    qtype_constraints: An optional list of (predicate_expr, error_message)
      pairs. If a qtype constraint is not fulfilled, the corresponding
      error_message is used. Placeholders, like {arg_name}, get replaced with
      the actual type names during the error message formatting.
    experimental_aux_policy: An auxiliary policy for the argument binding; it
      allows to customize operators call syntax.

  Returns:
    A decorator that constructs a lambda operator after a python function; the
    function shall return an expression for the lambda body.
  """

  def impl(fn):
    signature = _build_operator_signanture_from_fn(fn, experimental_aux_policy)
    lambda_body_expr = _build_lambda_body_from_fn(signature, fn)
    doc = inspect.getdoc(fn) or ''
    if not qtype_constraints:
      return arolla_types.LambdaOperator(
          signature,
          lambda_body_expr,
          name=name,
          doc=doc,
      )
    return arolla_types.RestrictedLambdaOperator(
        signature,
        lambda_body_expr,
        qtype_constraints=qtype_constraints,
        name=name,
        doc=doc,
    )

  return impl


def add_to_registry_as_overloadable(
    name: str,
    *,
    unsafe_override: bool = False,
    experimental_aux_policy: str = '',
) -> Callable[[Callable[..., Any]], arolla_abc.RegisteredOperator]:
  """A decorator that creates and registers a generic operator.

  A generic operator works as a frontend to a namespace (same as the operator's
  name) with overloads stored in the operator registry. The overloads have
  associated overload conditions (that must be mutually exclusive) based on
  which the overload selection happens.

  IMPORTANT: The generic operator doesn't provide a default behaviour, and
  the decorated function only provides its signature and docstring!

  Example:
    @arolla.optools.add_to_registry_as_overloadable('add_or_concat')
    def add_or_concat(x, y):
      '''Returns a sum for numbers, or a concatenation for strings.'''
      raise NotImplementedError('implemented in overloads')

    (See the continue in add_to_registry_as_overload.)

  Args:
    name: An operator name.
    unsafe_override: See arolla.optools.add_to_registry(..., unsafe_override).
    experimental_aux_policy: An auxiliary policy for the argument binding; it
      allows to customize operators call syntax.

  Returns:
    A decorator that constructs a generic operator after a python function
    (it's expected that the function raises NotImplementedError).
  """

  def impl(fn) -> arolla_abc.RegisteredOperator:
    signature = _build_operator_signanture_from_fn(fn, experimental_aux_policy)
    return add_to_registry(name, unsafe_override=unsafe_override)(
        arolla_types.GenericOperator(
            name,
            signature=signature,
            doc=inspect.getdoc(fn) or '',
        )
    )

  return impl


def add_to_registry_as_overload(
    name: str | None = None,
    *,
    overload_condition_expr: arolla_abc.Expr | Any,
    unsafe_override: bool = False,
) -> Callable[[arolla_types.Operator], arolla_abc.RegisteredOperator]:
  """A decorator that registers an operator as a generic operator overload.

  A generic operator must already exist before an overload can be registered for
  it. The overload should be inside the same namespace as the generic operator's
  name (e.g. the overloads for `core.getattr` must be registered in
  `core.getattr.*` namespace).

  The selection between the overloads happens based on the overload conditions,
  which must be mutually exclusive.

  The overload condition works similarly to the qtype constraint. It can
  reference the parameters' qtypes using placeholders, such as P.param_name, and
  must return present() or missing() to indicate whether the condition is met.

  The key distinction from qtype_constraints is that an unmet condition does not
  imply an error.

  Example:

    (See the beginning in add_to_registry_as_overloadable.)

    @arolla.optools.add_to_registry_as_overload(
        overload_condition_expr=M.qtype.is_numeric_qtype(P.x))
    @arolla.optools.as_lambda_operator('add_or_concat.for_numerics')
    def add_or_concat_for_numerics(x, y):
      return M.math.add(x, y)

    @arolla.optools.add_to_registry_as_overload(
        overload_condition_expr=(M.qtype.get_scalar_qtype(P.x) == arolla.TEXT))
    @arolla.optools.as_lambda_operator('add_or_concat.for_strings')
    def add_or_concat_for_strings(x, y):
      return M.strings.join(x, y)

  Args:
    name: Operator name for registration (by default it uses the operator's own
      name).
    overload_condition_expr: An overload condition (see GenericOperatorOverload
      for mor information).
    unsafe_override: See arolla.optools.add_to_registry(..., unsafe_override).

  Returns:
    A decorator for an arolla operator.
  """
  overload_condition_expr = arolla_types.as_expr(overload_condition_expr)

  def impl(op: arolla_types.Operator) -> arolla_abc.RegisteredOperator:
    if name is None:
      registration_name = op.display_name
    else:
      registration_name = name
    expected_generic_op_name, _, _ = registration_name.rpartition('.')
    if not arolla_abc.check_registered_operator_presence(
        expected_generic_op_name
    ):
      raise LookupError(
          'found no corresponding generic operator:'
          f' {expected_generic_op_name!r}'
      )
    expected_generic_op = arolla_abc.decay_registered_operator(
        expected_generic_op_name
    )
    if not isinstance(expected_generic_op, arolla_types.GenericOperator):
      raise ValueError(
          f'expected M.{expected_generic_op_name} to be a generic operator,'
          f' found {arolla_abc.get_type_name(type(expected_generic_op))}'
      )
    if expected_generic_op.display_name != expected_generic_op_name:
      raise ValueError(
          'expected a generic operator with name'
          f' {expected_generic_op_name!r}, found:'
          f' {expected_generic_op.display_name!r}'
      )
    # A possibly improvement: consider testing `signature.aux_policies` for
    # the generic operator and its overloads.
    return add_to_registry(registration_name, unsafe_override=unsafe_override)(
        arolla_types.GenericOperatorOverload(
            op, overload_condition_expr=overload_condition_expr
        )
    )

  return impl


def as_py_function_operator(
    name: str,
    *,
    qtype_inference_expr: arolla_abc.QType | arolla_abc.Expr,
    codec: bytes | None = None,
) -> Callable[
    [Callable[..., arolla_abc.QValue]], arolla_types.PyFunctionOperator
]:
  """A decorator for PyFunctionOperator construction.

  The wrapped function should take QValues as input and should return a single
  qvalue as output (multiple outputs can be supported through arolla.types.Tuple
  and similar).

  IMPORTANT: The callable should be pure and functionally const (i.e. it should
  not be mutated or depend on objects that are mutated between evaluations). No
  guarantees are made for the correctness of functions that rely on side-effects
  or on changing non-local data, as we may e.g. cache evaluation results.

  The resulting operator is serializable if it is added to the operator registry
  (arolla.optools.add_to_registry) or if a serialization `codec` is provided.

  Example:

    @arolla.optools.add_to_registry()
    @arolla.optools.as_py_function_operator(
        'py.add', qtype_inference_expr=M.qtype.common_qtype(P.x, P.y))
    def add(x, y):
      return x + y

  Args:
    name: operator name.
    qtype_inference_expr: expression that computes operator's output qtype; an
      argument qtype can be referenced as P.arg_name.
    codec: A PyObject serialization codec for the wrapped function, compatible
      with `arolla.types.encode_py_object`. See go/rlv2-py-object-codecs for
      details.

  Returns:
    A decorator for an arolla operator.
  """

  def impl(
      fn: Callable[..., arolla_abc.QValue],
  ) -> arolla_types.PyFunctionOperator:
    return arolla_types.PyFunctionOperator(
        name,
        arolla_types.PyObject(fn, codec=codec),
        qtype_inference_expr=qtype_inference_expr,
        doc=inspect.getdoc(fn) or '',
    )

  return impl


class _Dispatch:
  """A helper for overloaded operator construction.

  Example:

    @arolla.optools.as_lambda_operator('legacy.Add')
    def legacy_add(x, y):
      return arolla.optools.dispatch[M.math.add, M.strings.concat](x, y)
  """

  def __getitem__(
      self, ops: Sequence[arolla_types.Operator]
  ) -> arolla_types.OverloadedOperator:
    return arolla_types.OverloadedOperator(
        *ops, name='dispatch[' + ', '.join(op.display_name for op in ops) + ']'
    )


dispatch = _Dispatch()
