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

"""(Private) Auto-boxing from a python value to QValue."""

from typing import Any, Callable, Iterable, SupportsIndex

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import array_qtypes
from arolla.types.qtype import casting
from arolla.types.qtype import dense_array_qtypes
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import tuple_qtypes


# NOTE: The numpy dtype characters for intN, uintN, and floatN are
# platform-specific. For this reason, we retrieve (and cache) them at runtime.
_ScalarFactory = Callable[[Any], arolla_abc.AnyQValue]
_scalar_factory_from_numpy_dtype_char_cache: dict[str, _ScalarFactory]
_scalar_qtype_from_numpy_dtype_char_cache: dict[str, arolla_abc.QType]


def _scalar_factory_from_numpy_dtype(dtype) -> _ScalarFactory | None:
  """Returns a scalar factory function corresponding to the specified dtype."""
  global _scalar_factory_from_numpy_dtype_char_cache
  try:
    return _scalar_factory_from_numpy_dtype_char_cache.get(dtype.char)
  except NameError:
    pass
  np = arolla_abc.import_numpy()
  _scalar_factory_from_numpy_dtype_char_cache = {
      '?': scalar_qtypes.boolean,
      'S': scalar_qtypes.bytes_,
      'U': scalar_qtypes.text,
      np.int32(0).dtype.char: scalar_qtypes.int32,
      np.int64(0).dtype.char: scalar_qtypes.int64,
      np.uint64(0).dtype.char: scalar_qtypes.uint64,
      np.float32(0).dtype.char: scalar_qtypes.float32,
      np.float64(0).dtype.char: scalar_qtypes.float64,
  }
  return _scalar_factory_from_numpy_dtype_char_cache.get(dtype.char)


def _scalar_qtype_from_numpy_dtype(dtype) -> arolla_abc.QType | None:
  """Returns a scalar qtype corresponding to the specified dtype."""
  global _scalar_qtype_from_numpy_dtype_char_cache
  try:
    return _scalar_qtype_from_numpy_dtype_char_cache.get(dtype.char)
  except NameError:
    pass
  np = arolla_abc.import_numpy()
  _scalar_qtype_from_numpy_dtype_char_cache = {
      '?': scalar_qtypes.BOOLEAN,
      'S': scalar_qtypes.BYTES,
      'U': scalar_qtypes.TEXT,
      np.int32(0).dtype.char: scalar_qtypes.INT32,
      np.int64(0).dtype.char: scalar_qtypes.INT64,
      np.uint64(0).dtype.char: scalar_qtypes.UINT64,
      np.float32(0).dtype.char: scalar_qtypes.FLOAT32,
      np.float64(0).dtype.char: scalar_qtypes.FLOAT64,
  }
  return _scalar_qtype_from_numpy_dtype_char_cache.get(dtype.char)


def _deduce_scalar_qtype_from_numpy_value(value) -> arolla_abc.QType | None:
  """Returns qtype, if value is a numpy scalar; otherwise returns None."""
  np = arolla_abc.get_numpy_module_or_dummy()
  if isinstance(value, np.generic):
    return _scalar_qtype_from_numpy_dtype(value.dtype)
  return None


def _deduce_scalar_qtype_from_value(value) -> arolla_abc.QType:
  """Returns qtype of a scalar value."""
  if isinstance(value, arolla_abc.QValue):
    value_qtype = value.qtype
    if scalar_qtypes.is_scalar_qtype(value_qtype):
      return value_qtype
    raise ValueError(f'{value.qtype} is not a scalar type')
  value_type = type(value)
  if value_type is float:
    return scalar_qtypes.FLOAT32
  if value_type is int:
    return scalar_qtypes.INT32
  if value_type is bool:
    return scalar_qtypes.BOOLEAN
  if value_type is bytes:
    return scalar_qtypes.BYTES
  if value_type is str:
    return scalar_qtypes.TEXT
  np_result = _deduce_scalar_qtype_from_numpy_value(value)
  if np_result is not None:
    return np_result
  raise TypeError(f'unsupported type: {arolla_abc.get_type_name(type(value))}')


def _deduce_value_qtype_from_values(values: Iterable[Any]) -> arolla_abc.QType:
  """Deduces qtype of values in an iterable object."""
  assert not isinstance(values, arolla_abc.QValue)
  np = arolla_abc.get_numpy_module_or_dummy()
  if isinstance(values, np.ndarray):
    value_qtype = _scalar_qtype_from_numpy_dtype(values.dtype)  # pytype: disable=attribute-error
    if value_qtype is not None:
      return value_qtype
  value_qtypes = set()
  for value in values:
    if isinstance(value, arolla_abc.QValue):
      value_qtypes.add(value.qtype)
    elif value is not None:
      value_qtypes.add(_deduce_scalar_qtype_from_value(value))
  if not value_qtypes:
    raise ValueError('no values, cannot deduce value qtype')
  return casting.common_qtype(*value_qtypes)


def _is_reiterable(values):
  """Indicates whether values can be iterated through multiple times."""
  # NOTE: A more idiomatic variant could look like
  #
  #  isinstance(values, collections.abc.Sequence)
  #
  # Unfortunately, numpy.ndarray is not a python sequence.
  return hasattr(values, '__getitem__')


_DENSE_ARRAY_FACTORIES = {
    scalar_qtypes.UNIT: dense_array_qtypes.dense_array_unit,
    scalar_qtypes.BOOLEAN: dense_array_qtypes.dense_array_boolean,
    scalar_qtypes.BYTES: dense_array_qtypes.dense_array_bytes,
    scalar_qtypes.TEXT: dense_array_qtypes.dense_array_text,
    scalar_qtypes.INT32: dense_array_qtypes.dense_array_int32,
    scalar_qtypes.INT64: dense_array_qtypes.dense_array_int64,
    scalar_qtypes.UINT64: dense_array_qtypes.dense_array_uint64,
    scalar_qtypes.FLOAT32: dense_array_qtypes.dense_array_float32,
    scalar_qtypes.FLOAT64: dense_array_qtypes.dense_array_float64,
    scalar_qtypes.WEAK_FLOAT: dense_array_qtypes.dense_array_weak_float,
}


def dense_array(
    values: Iterable[Any],
    value_qtype: arolla_abc.QType | None = None,
    *,
    ids: Any = None,
    size: SupportsIndex | None = None,
) -> arolla_abc.AnyQValue:
  """Creates a DenseArray.

  DenseArray supports missed values. Analogue of std::vector<OptionalValue<T>>.
  It is implemented on top of Buffer<T>, so values are immutable.

  Additional information on DenseArray:
      //arolla/dense_array/README.md

  Args:
    values: An iterable of values.
    value_qtype: Desirable qtype for values. If missing, the value qtype will be
      deduced from `values`.
    ids: An optional iterable of ids for the values; should be used together
      with `size`.
    size: Optional size, should be used together with `ids`.

  Returns:
    A new dense_array object.
  """
  if value_qtype is None:
    if isinstance(values, arolla_abc.QValue):
      value_qtype = scalar_qtypes.get_scalar_qtype(values.qtype)
    else:
      if not _is_reiterable(values):
        values = list(values)
      value_qtype = _deduce_value_qtype_from_values(values)
  if optional_qtypes.is_optional_qtype(value_qtype):
    fn = _DENSE_ARRAY_FACTORIES.get(value_qtype.value_qtype)
  else:
    fn = _DENSE_ARRAY_FACTORIES.get(value_qtype)
  if fn is None:
    raise ValueError(f'unsupported qtype: {value_qtype}')
  return fn(values=values, ids=ids, size=size)


_ARRAY_FACTORIES = {
    scalar_qtypes.UNIT: array_qtypes.array_unit,
    scalar_qtypes.BOOLEAN: array_qtypes.array_boolean,
    scalar_qtypes.BYTES: array_qtypes.array_bytes,
    scalar_qtypes.TEXT: array_qtypes.array_text,
    scalar_qtypes.INT32: array_qtypes.array_int32,
    scalar_qtypes.INT64: array_qtypes.array_int64,
    scalar_qtypes.UINT64: array_qtypes.array_uint64,
    scalar_qtypes.FLOAT32: array_qtypes.array_float32,
    scalar_qtypes.FLOAT64: array_qtypes.array_float64,
    scalar_qtypes.WEAK_FLOAT: array_qtypes.array_weak_float,
}


def array(
    values: Iterable[Any],
    value_qtype: arolla_abc.QType | None = None,
    *,
    ids: Any = None,
    size: SupportsIndex | None = None,
) -> arolla_abc.AnyQValue:
  """Creates a Array.

  Array is an immutable array with support of missed values. Implemented
  on top of DenseArray. It efficiently represents very sparse data and
  constants, but has bigger fixed overhead than DenseArray.

  The implementation of Array: //arolla/array/array.h

  Args:
    values: An iterable of values.
    value_qtype: Desirable qtype for values. If missing, the value qtype will be
      deduced from `values`.
    ids: An optional iterable of ids for the values; should be used together
      with `size`.
    size: Optional size, should be used together with `ids`.

  Returns:
    A new array object.
  """
  if value_qtype is None:
    if isinstance(values, arolla_abc.QValue):
      value_qtype = scalar_qtypes.get_scalar_qtype(values.qtype)
    else:
      if not _is_reiterable(values):
        values = list(values)
      value_qtype = _deduce_value_qtype_from_values(values)
  if optional_qtypes.is_optional_qtype(value_qtype):
    fn = _ARRAY_FACTORIES.get(value_qtype.value_qtype)
  else:
    fn = _ARRAY_FACTORIES.get(value_qtype)
  if fn is None:
    raise ValueError(f'unsupported qtype: {value_qtype}')
  return fn(values=values, ids=ids, size=size)


def tuple_(*field_values: Any) -> arolla_abc.AnyQValue:
  """Constructs a tuple.

  Args:
    *field_values: Field values.

  Returns:
    A tuple with the given field values.
  """
  return tuple_qtypes.make_tuple_qvalue(*map(as_qvalue, field_values))


def namedtuple(**field_values: Any) -> arolla_abc.AnyQValue:
  """Constructs a named tuple.

  Args:
    **field_values: ordered (see PEP 468) collection of field values.

  Returns:
    A named tuple with the given fields.
  """
  return tuple_qtypes.make_namedtuple_qvalue(
      **{k: as_qvalue(v) for k, v in field_values.items()}
  )


def literal(x: Any) -> arolla_abc.Expr:
  """Returns a literal node holding the value."""
  return arolla_abc.literal(as_qvalue(x))


_unspecified = arolla_abc.unspecified
_as_qvalue_fns = {
    type(None): lambda _, /: _unspecified(),  # pylint: disable=unnecessary-lambda
    float: scalar_qtypes.float32,
    int: scalar_qtypes.int32,
    bool: scalar_qtypes.boolean,
    bytes: scalar_qtypes.bytes_,
    str: scalar_qtypes.text,
    list: array,
    tuple: lambda value, /: tuple_qtypes.make_tuple_qvalue(
        *map(as_qvalue, value)
    ),
}
_QVALUE = arolla_abc.QValue
_EXPR = arolla_abc.Expr
_QVALUE_OR_EXPR = (_EXPR, _QVALUE)


def as_qvalue(value, /) -> arolla_abc.AnyQValue:
  """Creates a qvalue from a value object.

  If `value` is already a qvalue object, this function returns it unchanged.
  Otherwise, the function selects qtype based on the value type, and makes
  the conversion:

  - Python/NumPy scalar types got converted to arolla scalar types

  - Python list and numpy.ndarray -> arolla array

  - Python tuple -> arolla tuple

  Args:
    value: A constant value (or list/tuple).

  Returns:
    A qvalue object.
  """
  if isinstance(value, _QVALUE):
    return value
  fn = _as_qvalue_fns.get(type(value))
  if fn is not None:
    return fn(value)
  np = arolla_abc.get_numpy_module_or_dummy()
  if isinstance(value, np.generic):
    fn = _scalar_factory_from_numpy_dtype(value.dtype)
    if fn is not None:
      return fn(value)
  if isinstance(value, np.ndarray):
    return array(value)
  raise TypeError(f'unsupported type: {arolla_abc.get_type_name(type(value))}')


def _tuple_qvalue_or_expr(value, /) -> arolla_abc.Expr | arolla_abc.AnyQValue:
  items = tuple(map(as_qvalue_or_expr, value))
  if _EXPR in map(type, items):
    return arolla_abc.make_operator_node('core.make_tuple', items)
  return tuple_qtypes.make_tuple_qvalue(*items)


_as_qvalue_or_expr_fns = _as_qvalue_fns | {
    tuple: _tuple_qvalue_or_expr,
}


def as_qvalue_or_expr(value, /) -> arolla_abc.Expr | arolla_abc.AnyQValue:
  """Wraps a value as a qvalue or as an expression.

  Note: This is a relaxed version of as_qvalue() that supports tuples with mixed
  values and liters.

  Args:
    value: A constant value (or list/tuple).

  Returns:
    A qvalue object or an expression.
  """
  if isinstance(value, _QVALUE_OR_EXPR):
    return value
  fn = _as_qvalue_or_expr_fns.get(type(value))
  if fn is not None:
    return fn(value)
  np = arolla_abc.get_numpy_module_or_dummy()
  if isinstance(value, np.generic):
    fn = _scalar_factory_from_numpy_dtype(value.dtype)
    if fn is not None:
      return fn(value)
  if isinstance(value, np.ndarray):
    return array(value)
  raise TypeError(f'unsupported type: {arolla_abc.get_type_name(type(value))}')


def as_expr(value, /) -> arolla_abc.Expr:
  """Wraps a value as literal if needed, or returns expr input as is."""
  # optimization: We can use type(expr) is arolla_abc.Expr instead of isinstance
  # because Expr is a final class.
  if type(value) is _EXPR:  # pylint: disable=unidiomatic-typecheck
    return value
  try:
    result = as_qvalue_or_expr(value)
  except (TypeError, ValueError) as _:
    raise TypeError(
        '; '.join(f'unable to create a literal from: {value!r}'.split('\n'))
    ) from None
  if type(result) is _EXPR:  # pylint: disable=unidiomatic-typecheck  # optimization
    return result
  return arolla_abc.literal(result)


def eval_(expr: Any, /, **leaf_values: Any) -> arolla_abc.AnyQValue:
  """Returns the result of the expression evaluation.

  The difference from the arolla_abc.eval_() is that this function supports
  auto-boxing.

    Example:
      arolla.eval(3.14)            # returns: arolla.float32(3.14)
      arolla.eval(L.x**0.5, x=2.)  # returns: arolla.float32(1.414213562)

  Args:
    expr: An expression for evaluation; can be any expression or a value support
      by arolla.
    **leaf_values: Values for leaf nodes; can be any value supported by arolla.

  Returns:
    The result of evaluation of the given expression with the specified leaf
    values.
  """
  return arolla_abc.vectorcall(
      arolla_abc.eval_expr,
      as_expr(expr),
      *map(as_qvalue, leaf_values.values()),
      tuple(leaf_values),
  )


# Setup the default argument binding policy.
arolla_abc.register_classic_aux_binding_policy_with_custom_boxing(
    '', as_qvalue_or_expr
)


# Setup the 'experimental_kwargs' argument binding policy.
def _experimental_kwargs(*args, **kwargs):
  """Argument binding policy for the `namedtuple.make()` operator.

  This is an experimental binding policy that introduces a 'variadic-keyword'
  parameter for the operator `namedtuple.make` with a classic signature:
  `fields, *values`.

  Importantly, the operator continues to support both the `**kwargs` syntax and
  the classic syntax in Python, and the following calls are equivalent:

    namedtuple.make(x=1, y=2)
    namedtuple.make('x,y', 1, 2)

  Args:
    *args: `(fields, *values)` from the "classic" signature; mutually exclusive
      with `**kwargs`.
    **kwargs: Keyword arguments representing fields.

  Returns:
    A tuple representing the inputs to the `namedtuple.make` operator.
  """
  if args and kwargs:
    raise TypeError(
        'expected only positional or only keyword arguments, got both'
        " (aux_policy='experimental_kwargs')"
    )
  if args:
    return tuple(map(as_qvalue_or_expr, args))
  return (
      scalar_qtypes.text(','.join(kwargs.keys())),
      *map(as_qvalue_or_expr, kwargs.values()),
  )


arolla_abc.register_adhoc_aux_binding_policy(
    'experimental_kwargs', _experimental_kwargs
)


# Setup the 'experimental_format_args' argument binding policy.
def _experimental_format_args(fmt, /, *args, **kwargs):
  """Argument binding policy for the `strings.format()` operator.

  This is an experimental binding policy that introduces a 'variadic-keyword'
  parameter for the operator `strings.format` with a classic signature:
  `fmt, arg_names, **kwargs`.

  Importantly, the operator continues to support both the `**kwargs` syntax and
  the classic syntax in Python, and the following calls are equivalent:

    strings.format('x={x}, y={y}', x=1, y=2)
    strings.format('x={x}, y={y}', 'x,y', 1, 2)

  Args:
    fmt: `fmt` from the "class" signature.
    *args: `(arg_names, *kwargs)` from the "classic" signature; mutually
      exclusive with `**kwargs`.
    **kwargs: Keyword arguments representing substitutions for the `fmt` string.

  Returns:
    A tuple representing the inputs to the `strings.format` operator.
  """
  if args and kwargs:
    raise TypeError(
        'expected exactly one positional arg with keyword arguments, got '
        f'{1 + len(args)}'
        " (aux_policy='experimental_format_args')"
    )
  if args:
    return (as_qvalue_or_expr(fmt), *map(as_qvalue_or_expr, args))
  return (
      as_qvalue_or_expr(fmt),
      scalar_qtypes.text(','.join(kwargs.keys())),
      *map(as_qvalue_or_expr, kwargs.values()),
  )


arolla_abc.register_adhoc_aux_binding_policy(
    'experimental_format_args', _experimental_format_args
)
