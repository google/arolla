# Copyright 2025 Google LLC
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
import types
from typing import Any, Callable, Iterable
import warnings

from arolla.abc import abc as arolla_abc
from arolla.optools import helpers
from arolla.types import types as arolla_types


class LambdaUnusedParameterWarning(UserWarning):
  """A warning for unused lambda parameters."""


def _build_operator_signature_from_fn(
    fn: types.FunctionType, experimental_aux_policy: str
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
    qtype_constraints: arolla_types.QTypeConstraints = (),
    experimental_aux_policy: str = '',
) -> Callable[[types.FunctionType], arolla_types.BackendOperator]:
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
    signature = _build_operator_signature_from_fn(fn, experimental_aux_policy)
    return arolla_types.BackendOperator(
        name,
        signature,
        doc=inspect.getdoc(fn) or '',
        qtype_inference_expr=qtype_inference_expr,
        qtype_constraints=qtype_constraints,
    )

  return impl


def _build_lambda_body_from_fn(fn: types.FunctionType) -> arolla_abc.Expr:
  """Builds lambda body from the Python function that constructs it."""
  unmangling = {}

  def gen_tracer(name: str) -> arolla_abc.Expr:
    mangled_name = f'_as_lambda_operator_argument_{name}'
    unmangling[mangled_name] = arolla_abc.placeholder(name)
    return arolla_abc.placeholder(mangled_name)

  mangled_result = arolla_types.as_expr(
      helpers.trace_function(fn, gen_tracer=gen_tracer)
  )
  unknown_placeholders = set(
      arolla_abc.get_placeholder_keys(mangled_result)
  ) - set(unmangling.keys())
  if unknown_placeholders:
    raise ValueError(
        'unexpected placeholders in lambda operator definition: '
        + ', '.join(
            sorted(map(repr, map(arolla_abc.placeholder, unknown_placeholders)))
        )
    )
  return arolla_abc.sub_placeholders(mangled_result, **unmangling)


def as_lambda_operator(
    name: str,
    *,
    qtype_constraints: arolla_types.QTypeConstraints = (),
    experimental_aux_policy: str = '',
    suppress_unused_parameter_warning: bool = False,
) -> Callable[[types.FunctionType], arolla_types.RestrictedLambdaOperator]:
  """A decorator for a lambda operator construction.

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
    suppress_unused_parameter_warning: If true, unused parameters will not cause
      a warning.

  Returns:
    A decorator that constructs a lambda operator after a python function; the
    function shall return an expression for the lambda body.
  """

  def impl(fn):
    signature = _build_operator_signature_from_fn(fn, experimental_aux_policy)
    lambda_body_expr = _build_lambda_body_from_fn(fn)
    doc = inspect.getdoc(fn) or ''
    if not suppress_unused_parameter_warning:
      unused_parameters = set(
          param.name
          for param in signature.parameters
          if not param.name.startswith('unused')
          and not param.name.startswith('_')
      )
      unused_parameters -= set(
          arolla_abc.get_placeholder_keys(lambda_body_expr)
      )
      if unused_parameters:
        warnings.warn(
            f'arolla.optools.as_lambda_operator({name!r}, ...) a lambda'
            ' operator not using some of its parameters: '
            + ', '.join(sorted(unused_parameters)),
            category=LambdaUnusedParameterWarning,
        )
    return helpers.make_lambda(
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
) -> Callable[[types.FunctionType], arolla_abc.RegisteredOperator]:
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
    signature = _build_operator_signature_from_fn(fn, experimental_aux_policy)
    doc = (
        fn.getdoc()
        if isinstance(fn, arolla_abc.Operator)
        else inspect.getdoc(fn) or ''
    )
    return add_to_registry(name, unsafe_override=unsafe_override)(
        arolla_types.GenericOperator(
            name,
            signature=signature,
            doc=doc,
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
    qtype_inference_expr: arolla_abc.Expr | arolla_abc.QType,
    qtype_constraints: arolla_types.QTypeConstraints = (),
    codec: bytes | None = None,
    experimental_aux_policy: str = '',
) -> Callable[[types.FunctionType], arolla_abc.Operator]:
  """Returns a decorator for defining py-function operators.

  The decorated function should accept QValues as input and return a single
  QValue.

  Importantly, it is recommended to use the decorator with pure functions --
  that is, deterministic and without side effects. The operator calls might be
  cached and/or deduplicated; if the function is non-pure, this might cause
  unpredictable behaviour.

  The resulting operator is serializable if it is added to the operator registry
  (arolla.optools.add_to_registry) or if a serialization `codec` is provided.

  Example:

    @arolla.optools.add_to_registry()
    @arolla.optools.as_py_function_operator(
        'py.add', qtype_inference_expr=M.qtype.common_qtype(P.x, P.y))
    def add(x, y):
      return x + y

  Args:
    name: The name of the operator.
    qtype_inference_expr: expression that computes operator's output qtype; an
      argument qtype can be referenced as P.arg_name.
    qtype_constraints: QType constraints for the operator.
    codec: A PyObject serialization codec for the wrapped function, compatible
      with `arolla.types.encode_py_object`. The resulting operator is
      serializable only if the codec is specified.
    experimental_aux_policy: An auxiliary policy for the argument binding; it
      allows to customize operators call syntax.
  """
  op_make_tuple = arolla_abc.lookup_operator('core.make_tuple')
  op_concat_tuples = arolla_abc.lookup_operator('core.concat_tuples')
  op_py_call = arolla_abc.lookup_operator('py.call')
  op_qtype_of = arolla_abc.lookup_operator('qtype.qtype_of')

  def impl(fn):
    sig = _build_operator_signature_from_fn(fn, experimental_aux_policy)
    all_params = [param.name for param in sig.parameters]
    all_param_placeholders = [*map(arolla_abc.placeholder, all_params)]

    # Prepare an expression for return_type. This expression must be computable
    # at compile time. To achieve this, we declare a backend operator using
    # the provided qtype_inference_expr (without actually defining it in
    # the backend), and we use `M.qtype.qtype_of(...)` to transform
    # the attribute into a value that is accessible at compile time.
    #
    # Importantly, we wrap both `M.qtype.qtype_of` and the backend operator in
    # a lambda to ensure compatibility, regardless of whether literal folding is
    # enabled:
    #  * if literal folding is enabled, the lambda will fold without lowering,
    #    as it infers the qvalue attribute;
    #  * if literal folding is disabled, the node M.qtype.qtype_of replaces
    #    itself with a literal during lowering.
    stub_sig = arolla_abc.make_operator_signature(','.join(all_params))
    return_type_expr = arolla_types.LambdaOperator(
        stub_sig,
        op_qtype_of(
            arolla_types.BackendOperator(
                'core._undefined_backend_op',
                stub_sig,
                qtype_inference_expr=qtype_inference_expr,
            )(*all_param_placeholders)
        ),
        name='return_type_stub_op',
    )(*all_param_placeholders)

    # Prepare an expression for `args`.
    has_variadic_positional = (
        sig.parameters and sig.parameters[-1].kind == 'variadic-positional'
    )
    if not has_variadic_positional:
      args_expr = op_make_tuple(*all_param_placeholders)
    elif len(all_param_placeholders) == 1:
      args_expr = all_param_placeholders[0]
    else:
      args_expr = op_concat_tuples(
          op_make_tuple(*all_param_placeholders[:-1]),
          all_param_placeholders[-1],
      )

    # Construct a lambda operator.
    expr = op_py_call(
        arolla_abc.PyObject(fn, codec=codec), return_type_expr, args_expr
    )
    return helpers.make_lambda(
        sig,
        expr,
        qtype_constraints=qtype_constraints,
        name=name,
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
      self, ops: Iterable[arolla_types.Operator]
  ) -> arolla_types.OverloadedOperator:
    return arolla_types.OverloadedOperator(
        *ops, name='dispatch[' + ', '.join(op.display_name for op in ops) + ']'
    )


dispatch = _Dispatch()
