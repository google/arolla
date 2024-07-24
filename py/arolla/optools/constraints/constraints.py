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

"""Helper functions that generate common qtype_constraint predicates."""

import functools
from typing import Collection, Iterable, Iterator

from arolla.abc import abc as arolla_abc
from arolla.expr import expr as arolla_expr
from arolla.types import types as arolla_types

M = arolla_expr.OperatorsContainer()

Placeholder = arolla_abc.Expr
QTypeConstraint = tuple[arolla_abc.Expr, str]


def name_type_msg(param: Placeholder) -> str:
  """Return 'param_name: {param_name}' string.

  {param_name} will automatically be replaced by the type name of param in the
  final output.

  Args:
    param: Placeholder with param_name as the key.

  Returns:
    'param_name: {param_name}' string.
  """
  assert param.is_placeholder
  return f'{param.placeholder_key}: {{{param.placeholder_key}}}'


def variadic_name_type_msg(variadic_param: Placeholder) -> str:
  """Return '*variadic_param_name: {*variadic_param_name}' string.

  {*variadic_param_name} will automatically be replaced by the field type names
  in variadic parameter in the final output.

  Args:
    variadic_param: Placeholder with param_name as the key.

  Returns:
    '*variadic_param_name: {*variadic_param_name}' string.
  """
  assert variadic_param.is_placeholder
  return '*{0}: {{*{0}}}'.format(variadic_param.placeholder_key)


def common_qtype_expr(*args: arolla_abc.Expr) -> arolla_abc.Expr:
  """Returns an expression that finds the common qtype of the given expressions.

  Args:
    *args: expressions that evaluate to a QType.

  Returns:
    An expression that returns a common qtype of *args.
  """
  return functools.reduce(M.qtype.common_qtype, args)


def common_float_qtype_expr(
    arg: arolla_abc.Expr, *args: arolla_abc.Expr
) -> arolla_abc.Expr:
  """Returns an expression that finds the common float qtype of the given expressions.

  See more details in `arolla.types.common_float_qtype` docstring.

  Args:
    arg: an expression that evaluates to a QType.
    *args: expressions that evaluate to a QType.

  Returns:
    An expression that returns a common float qtype of (args, *args).
  """
  return common_qtype_expr(arg, *args, arolla_types.WEAK_FLOAT)


def broadcast_qtype_expr(
    target_qtypes: Iterable[arolla_abc.QType | arolla_abc.Expr],
    qtype: arolla_abc.QType | arolla_abc.Expr,
) -> arolla_abc.Expr:
  """Returns an expression that broadcasts `qtype` after the `target_qtypes`.

  Args:
    target_qtypes: Target qtype expressions.
    qtype: A qtype expression.

  Returns:
    An expression that broadcasts `qtype` using the `target_qtypes`.
  """
  return M.qtype.broadcast_qtype_like(
      functools.reduce(M.qtype.broadcast_qtype_like, target_qtypes), qtype
  )


def expect_qtype(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a QTYPE."""
  return (
      param == arolla_abc.QTYPE,
      f'expected QTYPE, got {name_type_msg(param)}',
  )


def _expect_qtype_in_expr(
    x: arolla_abc.Expr, qtypes: Iterable[arolla_abc.QType]
) -> arolla_abc.Expr:
  eqs = (x == qtype for qtype in qtypes)
  return functools.reduce(M.core.presence_or, eqs)


def expect_qtype_in(
    param: Placeholder, qtypes: Collection[arolla_abc.QType]
) -> QTypeConstraint:
  """Returns a constraint that param equals any qtype in qtypes."""
  expr = _expect_qtype_in_expr(param, qtypes)
  return (
      expr,
      (
          f'expected one of {sorted(qtypes, key=lambda x: x.name)}, got'
          f' {name_type_msg(param)}'
      ),
  )


def expect_scalar_qtype_in(
    param: Placeholder, qtypes: Collection[arolla_abc.QType]
) -> QTypeConstraint:
  """Returns a constraint that qtypes contains get_scalar_qtype(param)."""
  expr = _expect_qtype_in_expr(M.qtype.get_scalar_qtype(param), qtypes)
  return (
      expr,
      (
          'expected the scalar qtype to be one of'
          f' {sorted(qtypes, key=lambda x: x.name)}, got {name_type_msg(param)}'
      ),
  )


def expect_shape(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a shape."""
  return (
      M.qtype.is_shape_qtype(param),
      f'expected a shape, got {name_type_msg(param)}',
  )


def has_presence_type(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument has an associated presence qtype."""
  return (
      M.qtype.get_presence_qtype(param) != arolla_abc.NOTHING,
      f'no presence type for {name_type_msg(param)}',
  )


def has_scalar_type(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is scalar, optional, or array."""
  return (
      M.qtype.get_scalar_qtype(param) != arolla_abc.NOTHING,
      f'expected a type storing scalar(s), got {name_type_msg(param)}',
  )


def expect_scalar(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a scalar."""
  return (
      M.qtype.is_scalar_qtype(param),
      f'expected a scalar type, got {name_type_msg(param)}',
  )


def expect_scalar_or_optional(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a scalar or optional scalar."""
  return (
      (M.qtype.is_scalar_qtype(param) | M.qtype.is_optional_qtype(param)),
      f'expected a scalar or optional scalar type, got {name_type_msg(param)}',
  )


def expect_array_shape(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is an array shape."""
  return (
      M.qtype.is_array_shape_qtype(param),
      f'expected an array shape, got {name_type_msg(param)}',
  )


def expect_array(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is an array."""
  return (
      M.qtype.is_array_qtype(param),
      f'expected an array type, got {name_type_msg(param)}',
  )


def expect_array_scalar_or_optional(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is an array, scalar or optional."""
  return (
      M.qtype.is_array_qtype(param)
      | M.qtype.is_scalar_qtype(param)
      | M.qtype.is_optional_qtype(param),
      f'expected an array, scalar or optional type, got {name_type_msg(param)}',
  )


def expect_array_or_unspecified(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is either an array or unspecified.

  Args:
    param: Placeholder corresponding to the array parameter.
  """
  return (
      M.qtype.is_array_qtype(param) | (param == arolla_abc.UNSPECIFIED),
      f'expected an array type, got {name_type_msg(param)}',
  )


def expect_dense_array(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a dense array."""
  return (
      M.qtype.is_dense_array_qtype(param),
      f'expected a dense_array type, got {name_type_msg(param)}',
  )


def expect_sequence(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a sequence."""
  return (
      M.qtype.is_sequence_qtype(param),
      f'expected a sequence type, got {name_type_msg(param)}',
  )


def expect_scalar_boolean(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a boolean scalar."""
  return (
      param == arolla_types.BOOLEAN,
      f'expected a boolean scalar, got {name_type_msg(param)}',
  )


def expect_scalar_bytes(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a bytes scalar."""
  return (
      param == arolla_types.BYTES,
      f'expected a bytes scalar, got {name_type_msg(param)}',
  )


def expect_scalar_text(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a text scalar."""
  return (
      param == arolla_types.TEXT,
      f'expected a text scalar, got {name_type_msg(param)}',
  )


def expect_scalar_integer(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is an integer scalar."""
  return (
      M.qtype.is_integral_scalar_qtype(param),
      f'expected an integer scalar, got {name_type_msg(param)}',
  )


def expect_scalar_float(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that a specific parameter is floating-point scalar."""
  return (
      M.qtype.is_floating_point_scalar_qtype(param),
      f'expected a floating-point scalar, got {name_type_msg(param)}',
  )


def expect_unit(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that a specific parameter is OPTIONAL_UNIT."""
  return (
      (param == arolla_types.UNIT) | (param == arolla_types.OPTIONAL_UNIT),
      f'expected a unit scalar or optional, got {name_type_msg(param)}',
  )


def expect_boolean(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a boolean scalar or optional."""
  return (
      (param == arolla_types.BOOLEAN)
      | (param == arolla_types.OPTIONAL_BOOLEAN),
      f'expected a boolean scalar or optional, got {name_type_msg(param)}',
  )


def expect_integer(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is an integer scalar or optional."""
  return (
      M.qtype.is_integral_qtype(param)
      & (M.qtype.is_scalar_qtype(param) | M.qtype.is_optional_qtype(param)),
      f'expected an integer scalar or optional, got {name_type_msg(param)}',
  )


def expect_units(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is unit scalar or array."""
  return (
      M.qtype.get_scalar_qtype(param) == arolla_types.UNIT,
      f'expected units, got {name_type_msg(param)}',
  )


def expect_booleans(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is boolean scalar or array."""
  return (
      M.qtype.get_scalar_qtype(param) == arolla_types.BOOLEAN,
      f'expected booleans, got {name_type_msg(param)}',
  )


def expect_byteses(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is bytes scalar or array."""
  return (
      M.qtype.get_scalar_qtype(param) == arolla_types.BYTES,
      f'expected bytes or array of bytes, got {name_type_msg(param)}',
  )


def expect_texts(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is text scalar or array."""
  return (
      M.qtype.get_scalar_qtype(param) == arolla_types.TEXT,
      f'expected texts or array of texts, got {name_type_msg(param)}',
  )


def expect_texts_or_byteses(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is text/bytes scalar or array."""
  scalar_qtype = M.qtype.get_scalar_qtype(param)
  return (
      (scalar_qtype == arolla_types.TEXT)
      | (scalar_qtype == arolla_types.BYTES),
      (
          'expected texts/byteses or corresponding array, '
          f'got {name_type_msg(param)}'
      ),
  )


def expect_integers(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is integer scalar or array."""
  return (
      M.qtype.is_integral_qtype(param),
      f'expected integers, got {name_type_msg(param)}',
  )


def expect_floats(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a float scalar or array."""
  return (
      M.qtype.is_floating_point_qtype(param),
      f'expected floating-points, got {name_type_msg(param)}',
  )


def expect_numerics(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that the argument is a numeric scalar or array."""
  return (
      M.qtype.is_numeric_qtype(param),
      f'expected numerics, got {name_type_msg(param)}',
  )


def expect_dict(
    dict_param: Placeholder,
    *,
    keys_compatible_with_param: Placeholder | None = None,
) -> Iterator[QTypeConstraint]:
  """Yields constraints that dict_param is a dict and keys are compatible.

  By default, this function emits only a constraint that dict_param holds to
  a dict. If the optional kwarg keys_compatible_with_param is set, it also emits
  constraints that keys_compatible_with_param holds to a type with a scalar type
  compatible with the keys of the given dict.

  Args:
    dict_param: Placeholder with param_name as the key.
    keys_compatible_with_param: Placeholder with param_name as the key.

  Yields:
    A sequence of constraints for the given parameters.
  """
  yield (
      (M.qtype.is_dict_qtype(dict_param)),
      f'expect to be a dict, got {name_type_msg(dict_param)}',
  )

  if keys_compatible_with_param is not None:
    common_qtype = M.qtype.common_qtype(
        M.qtype.get_dict_key_qtype(dict_param),
        M.qtype.get_scalar_qtype(keys_compatible_with_param),
    )
    yield (
        common_qtype == M.qtype.get_dict_key_qtype(dict_param),
        (
            'expected a key type compatible with'
            f' {name_type_msg(dict_param)} keys, got'
            f' {name_type_msg(keys_compatible_with_param)}'
        ),
    )


def expect_dict_key_types(param: Placeholder) -> QTypeConstraint:
  """Returns a constraint that param is a dict key compatible scalar or array."""
  scalar_qtype = M.qtype.get_scalar_qtype(param)
  return (
      (
          M.qtype.is_integral_qtype(param)
          | (scalar_qtype == arolla_types.UINT64)
          | (scalar_qtype == arolla_types.BOOLEAN)
          | (scalar_qtype == arolla_types.TEXT)
          | (scalar_qtype == arolla_types.BYTES)
      ),
      (
          'expect value type to be compatible with dict keys, got'
          f' {name_type_msg(param)}'
      ),
  )


def expect_dict_key_types_or_unspecified(param: Placeholder) -> QTypeConstraint:
  """Yields a constraint that a param is a dict key type or a unspecified."""
  cond, err = expect_dict_key_types(param)
  return ((param == arolla_abc.UNSPECIFIED) | cond, err)


def expect_key_to_row_dict(
    dict_param: Placeholder,
    *,
    keys_compatible_with_param: Placeholder | None = None,
) -> Iterator[QTypeConstraint]:
  """Yields constraints that dict is key to row dict and keys are compatible.

  By default, this function emits only a constraint that dict_param holds to
  a key to row dict. If the optional kwarg keys_compatible_with_param is set, it
  also emits constraints that keys_compatible_with_param holds to a type with a
  scalar type compatible with the keys of a given dict.

  Args:
    dict_param: Placeholder with param_name as the key.
    keys_compatible_with_param: Placeholder with param_name as the key.

  Yields:
    A sequence of constraints for the given parameters.
  """
  dict_keys_predicate_expr = M.qtype._get_key_to_row_dict_key_qtype(  # pylint: disable=protected-access
      dict_param
  )
  yield (
      dict_keys_predicate_expr != arolla_abc.NOTHING,
      f'expected a key to row dict, got {name_type_msg(dict_param)}',
  )
  if keys_compatible_with_param is not None:
    keys_predicate_expr = M.qtype.get_scalar_qtype(keys_compatible_with_param)
    common_qtype_predicate_expr = common_qtype_expr(
        dict_keys_predicate_expr, keys_predicate_expr
    )
    msg = f'expected a key type compatible with {name_type_msg(dict_param)}, '
    msg += f'got {name_type_msg(keys_compatible_with_param)}'
    yield (common_qtype_predicate_expr == dict_keys_predicate_expr, msg)


def expect_implicit_cast_compatible(*params: Placeholder) -> QTypeConstraint:
  """Returns a constraint that arguments are implicit cast compatible."""
  assert len(params) >= 2
  predicate_expr = common_qtype_expr(*params) != arolla_abc.NOTHING
  if len(params) == 2:
    error_message = 'incompatible types ' + ' and '.join(
        map(name_type_msg, params)
    )
  else:
    error_message = (
        'incompatible types '
        + ', '.join(map(name_type_msg, params[:-1]))
        + f', and {name_type_msg(params[-1])}'
    )
  return (predicate_expr, error_message)


def expect_broadcast_compatible(*params: Placeholder) -> QTypeConstraint:
  """Returns a constraint that arguments are broadcast compatible.

  Checks that there is a container kind, compatible with all the arguments,
  compatibility of the stored scalars is not needed.

  Example:

    1) broadcast_compatible(
           INT32,
           OPTIONAL_BOOLEAN,
           DENSE_ARRAY_FLOAT32,
       ) is True  # the arguments can be stored as DENSE_ARRAY

    2) broadcast_compatible(
           DENSE_ARRAY_FLOAT32,
           ARRAY_FLOAT32
       ) is False  # not common container for DENSE_ARRAY and ARRAY


  Args:
    *params: Placeholder with param_name as the key.

  Returns:
    A newly constructed qtype constraint.
  """
  assert len(params) >= 2
  predicate_expr = (
      functools.reduce(M.qtype.broadcast_qtype_like, params)
      != arolla_abc.NOTHING
  )
  if len(params) == 2:
    error_message = 'broadcast incompatible types ' + ' and '.join(
        map(name_type_msg, params)
    )
  else:
    error_message = (
        'broadcast incompatible types '
        + ', '.join(map(name_type_msg, params[:-1]))
        + f', and {name_type_msg(params[-1])}'
    )
  return (predicate_expr, error_message)


def expect_shape_compatible(*params: Placeholder) -> QTypeConstraint:
  """Returns a constraint that arguments have the compatible shape.

  Args:
    *params: Placeholder with param_name as the key.

  Returns:
    A newly constructed qtype constraint.
  """
  assert len(params) >= 2
  predicate_expr = M.qtype.get_shape_qtype(
      params[0]
  ) == M.qtype.get_shape_qtype(params[1])
  for param in params[2:]:
    predicate_expr = predicate_expr & (
        M.qtype.get_shape_qtype(params[0]) == M.qtype.get_shape_qtype(param)
    )

  if len(params) == 2:
    error_message = 'expected types with compatible shape, got ' + ' and '.join(
        map(name_type_msg, params)
    )
  else:
    error_message = (
        'expected types with compatible shape, got '
        + ', '.join(map(name_type_msg, params[:-1]))
        + f', and {name_type_msg(params[-1])}'
    )
  return (predicate_expr, error_message)


def expect_edge(
    edge_param: Placeholder,
    *,
    parent_side_param: Placeholder | None = None,
    child_side_param: Placeholder | None = None,
) -> Iterator[QTypeConstraint]:
  """Yields constraints for an edge parameter.

  By default, this function emits only a constraint that edge_param holds to
  an edge. If the optional parent_side_param/child_side_param is/are set, it
  also emits constraints for the corresponding edge sides.

  Args:
    edge_param: Placeholder corresponding to the edge parameter.
    parent_side_param: (optional) Placeholder corresponding to the parent side
      of the edge.
    child_side_param: (optional) Placeholder corresponding to the child side of
      the edge.

  Yields:
    A sequence of constraints for the edge parameter.
  """
  yield (
      M.qtype.is_edge_qtype(edge_param),
      f'expected an edge, got {name_type_msg(edge_param)}',
  )
  if parent_side_param is not None:
    yield (
        M.qtype.get_shape_qtype(M.qtype.optional_like_qtype(parent_side_param))
        == M.qtype.get_parent_shape_qtype(edge_param),
        f'{name_type_msg(parent_side_param)} is not compatible with '
        + f'parent-side of {name_type_msg(edge_param)}',
    )
  if child_side_param is not None:
    yield (
        M.qtype.get_shape_qtype(M.qtype.optional_like_qtype(child_side_param))
        == M.qtype.get_child_shape_qtype(edge_param),
        f'{name_type_msg(child_side_param)} is not compatible with '
        + f'child-side of {name_type_msg(edge_param)}',
    )


def expect_edge_or_unspecified(
    param: Placeholder,
    *,
    parent_side_param: Placeholder | None = None,
    child_side_param: Placeholder | None = None,
) -> Iterator[QTypeConstraint]:
  """Yields constraints that a param is either an edge or a unspecified.

  Args:
    param: Placeholder corresponding to the edge parameter.
    parent_side_param: (optional) Placeholder corresponding to the parent side
      of the edge.
    child_side_param: (optional) Placeholder corresponding to the child side of
      the edge.

  Yields:
    A sequence of constraints for the edge parameter.
  """
  expect_edge_constraints = expect_edge(
      param,
      parent_side_param=parent_side_param,
      child_side_param=child_side_param,
  )
  yield from (
      ((param == arolla_abc.UNSPECIFIED) | cond, err)
      for (cond, err) in expect_edge_constraints
  )
