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

"""(Private) Array qtypes and corresponding factory functions.

Please avoid using this module directly. Use arolla.rl (preferrably) or
arolla.types.types instead.
"""

import dataclasses
from typing import Any, Callable, SupportsIndex

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import casting
from arolla.types.qtype import clib
from arolla.types.qtype import dense_array_qtype
from arolla.types.qtype import optional_qtype
from arolla.types.qtype import scalar_qtype

# Returns i-th item of the array.
get_array_item = clib.get_array_item

# Returns a list of python values from array.
get_array_py_value = clib.get_array_py_value


def is_array_qtype(qtype: arolla_abc.QType) -> bool:
  """Checks whether qtype is an array (or dense_array)."""
  return casting.get_shape_qtype_or_nothing(qtype) in (
      ARRAY_SHAPE,
      clib.DENSE_ARRAY_SHAPE,
  )


def make_array_qtype(qtype: arolla_abc.QType) -> arolla_abc.QType:
  """Returns the Array QType corresponding to a value QType."""
  if not (
      scalar_qtype.is_scalar_qtype(qtype)
      or optional_qtype.is_optional_qtype(qtype)
  ):
    raise casting.QTypeError(f'expected a scalar qtype, got {qtype=}')
  return casting.broadcast_qtype([clib.ARRAY_UNIT], qtype)


# LINT.IfChange
ARRAY_BOOLEAN = make_array_qtype(scalar_qtype.BOOLEAN)
ARRAY_BYTES = make_array_qtype(scalar_qtype.BYTES)
ARRAY_FLOAT32 = make_array_qtype(scalar_qtype.FLOAT32)
ARRAY_FLOAT64 = make_array_qtype(scalar_qtype.FLOAT64)
ARRAY_INT32 = make_array_qtype(scalar_qtype.INT32)
ARRAY_INT64 = make_array_qtype(scalar_qtype.INT64)
ARRAY_TEXT = make_array_qtype(scalar_qtype.TEXT)
ARRAY_UINT64 = make_array_qtype(scalar_qtype.UINT64)
ARRAY_UNIT = make_array_qtype(scalar_qtype.UNIT)
ARRAY_WEAK_FLOAT = make_array_qtype(scalar_qtype.WEAK_FLOAT)

ARRAY_SHAPE = clib.ARRAY_SHAPE
ARRAY_TO_SCALAR_EDGE = clib.ARRAY_TO_SCALAR_EDGE
ARRAY_EDGE = clib.ARRAY_EDGE
# LINT.ThenChange(//depot/py/arolla/rl.py)

ARRAY_QTYPES = tuple(
    make_array_qtype(tpe) for tpe in scalar_qtype.SCALAR_QTYPES
)

ARRAY_NUMERIC_QTYPES = tuple(
    make_array_qtype(tpe) for tpe in scalar_qtype.NUMERIC_QTYPES
)


def _make_implicit_casting_expr(
    target_qtype: arolla_abc.QType,
) -> arolla_abc.Expr:
  return arolla_abc.unsafe_parse_sexpr((
      'core.cast',
      arolla_abc.leaf('values'),
      target_qtype,
      scalar_qtype.true(),  # implicit_only=True
  ))


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class _ConstructionSpec:
  """A specification for array_from_* functions.

  Attributes:
    qtype: Target array qtype.
    dense_array_fn: A dense array factory.
    optional_qvalue_fn: Unwraps a value into an optional qvalue compatible with
      the target array.
    casting_expr: A casting expression from a qvalue to the target array.
  """

  qtype: arolla_abc.QType
  dense_array_fn: Callable[[Any], arolla_abc.QValue]
  optional_qvalue_fn: Callable[[Any], arolla_abc.QValue]
  casting_expr: arolla_abc.Expr


# Static specs for qvalue construction.
_ARRAY_UNIT_SPEC = _ConstructionSpec(
    qtype=ARRAY_UNIT,
    dense_array_fn=dense_array_qtype.dense_array_unit,
    optional_qvalue_fn=optional_qtype.optional_unit,
    casting_expr=_make_implicit_casting_expr(ARRAY_UNIT),
)
_ARRAY_BOOLEAN_SPEC = _ConstructionSpec(
    qtype=ARRAY_BOOLEAN,
    dense_array_fn=dense_array_qtype.dense_array_boolean,
    optional_qvalue_fn=optional_qtype.optional_boolean,
    casting_expr=_make_implicit_casting_expr(ARRAY_BOOLEAN),
)
_ARRAY_BYTES_SPEC = _ConstructionSpec(
    qtype=ARRAY_BYTES,
    dense_array_fn=dense_array_qtype.dense_array_bytes,
    optional_qvalue_fn=optional_qtype.optional_bytes,
    casting_expr=_make_implicit_casting_expr(ARRAY_BYTES),
)
_ARRAY_TEXT_SPEC = _ConstructionSpec(
    qtype=ARRAY_TEXT,
    dense_array_fn=dense_array_qtype.dense_array_text,
    optional_qvalue_fn=optional_qtype.optional_text,
    casting_expr=_make_implicit_casting_expr(ARRAY_TEXT),
)
_ARRAY_INT32_SPEC = _ConstructionSpec(
    qtype=ARRAY_INT32,
    optional_qvalue_fn=optional_qtype.optional_int32,
    dense_array_fn=dense_array_qtype.dense_array_int32,
    casting_expr=_make_implicit_casting_expr(ARRAY_INT32),
)
_ARRAY_INT64_SPEC = _ConstructionSpec(
    qtype=ARRAY_INT64,
    dense_array_fn=dense_array_qtype.dense_array_int64,
    optional_qvalue_fn=optional_qtype.optional_int64,
    casting_expr=_make_implicit_casting_expr(ARRAY_INT64),
)
_ARRAY_UINT64_SPEC = _ConstructionSpec(
    qtype=ARRAY_UINT64,
    dense_array_fn=dense_array_qtype.dense_array_uint64,
    optional_qvalue_fn=optional_qtype.optional_uint64,
    casting_expr=_make_implicit_casting_expr(ARRAY_UINT64),
)
_ARRAY_FLOAT32_SPEC = _ConstructionSpec(
    qtype=ARRAY_FLOAT32,
    dense_array_fn=dense_array_qtype.dense_array_float32,
    optional_qvalue_fn=optional_qtype.optional_float32,
    casting_expr=_make_implicit_casting_expr(ARRAY_FLOAT32),
)
_ARRAY_FLOAT64_SPEC = _ConstructionSpec(
    qtype=ARRAY_FLOAT64,
    dense_array_fn=dense_array_qtype.dense_array_float64,
    optional_qvalue_fn=optional_qtype.optional_float64,
    casting_expr=_make_implicit_casting_expr(ARRAY_FLOAT64),
)
_ARRAY_WEAK_FLOAT_SPEC = _ConstructionSpec(
    qtype=ARRAY_WEAK_FLOAT,
    dense_array_fn=dense_array_qtype.dense_array_weak_float,
    optional_qvalue_fn=optional_qtype.optional_weak_float,
    casting_expr=_make_implicit_casting_expr(ARRAY_WEAK_FLOAT),
)


_as_array_expr = arolla_abc.unsafe_parse_sexpr(
    ('array.as_array', arolla_abc.leaf('dense_array'))
)


def _array_from_values(
    spec: _ConstructionSpec, values: Any
) -> arolla_abc.AnyQValue:
  """Converts `values` into a array according to `spec`."""
  if isinstance(values, arolla_abc.QValue):
    if values.qtype == spec.qtype:
      return values
    try:
      return arolla_abc.eval_expr(spec.casting_expr, dict(values=values))
    except ValueError as ex:
      raise ValueError(
          f'failed to convert values with {values.qtype=} to '
          f'target_qtype={spec.qtype} using implicit casting rules'
      ) from ex
  return arolla_abc.eval_expr(
      _as_array_expr, dict(dense_array=spec.dense_array_fn(values))
  )


_Size = SupportsIndex
_OptionalSize = _Size | None


def _array_from_ids_and_values(
    spec: _ConstructionSpec, values: Any, ids: Any, size: _Size
) -> arolla_abc.AnyQValue:
  """Converts `ids` and `values` into an array according to `spec`."""
  values = _array_from_values(spec, values)
  ids = _array_from_values(_ARRAY_INT64_SPEC, ids)
  return arolla_abc.invoke_op(
      'array.from_indices_and_values',
      (ids, values, scalar_qtype.int64(size)),
  )


_const_array_expr = arolla_abc.unsafe_parse_sexpr((
    'core.const_with_shape',
    ('array.make_array_shape', arolla_abc.leaf('size')),
    arolla_abc.leaf('value'),
))


def _array_from_size_and_value(
    spec: _ConstructionSpec, value: Any, size: _Size
):
  """Converts `size` and `values` into an array according to `spec`."""
  size = scalar_qtype.int64(size)
  value = spec.optional_qvalue_fn(value)
  return arolla_abc.eval_expr(_const_array_expr, dict(size=size, value=value))


def _array_qvalue_impl(
    spec: _ConstructionSpec,
    values: Any,
    ids: Any = None,
    size: _OptionalSize = None,
) -> arolla_abc.AnyQValue:
  """A helper function for factories like array_{bytes,text,int32,...}."""
  if ids is not None:
    if size is None:
      raise TypeError('missing parameter: size')
    return _array_from_ids_and_values(spec, values, ids, size)
  if size is not None:
    return _array_from_size_and_value(spec, values, size)
  return _array_from_values(spec, values)


def array_unit(values: Any, *, ids: Any = None, size: _OptionalSize = None):
  """Returns ARRAY_UNIT qvalue."""
  return _array_qvalue_impl(_ARRAY_UNIT_SPEC, values, ids, size)


def array_boolean(values: Any, *, ids: Any = None, size: _OptionalSize = None):
  """Returns ARRAY_BOOLEAN qvalue."""
  return _array_qvalue_impl(_ARRAY_BOOLEAN_SPEC, values, ids, size)


def array_bytes(values: Any, *, ids: Any = None, size: _OptionalSize = None):
  """Returns ARRAY_BYTES qvalue."""
  return _array_qvalue_impl(_ARRAY_BYTES_SPEC, values, ids, size)


def array_text(values: Any, *, ids: Any = None, size: _OptionalSize = None):
  """Returns ARRAY_TEXT qvalue."""
  return _array_qvalue_impl(_ARRAY_TEXT_SPEC, values, ids, size)


def array_int32(values: Any, *, ids: Any = None, size: _OptionalSize = None):
  """Returns ARRAY_INT32 qvalue."""
  return _array_qvalue_impl(_ARRAY_INT32_SPEC, values, ids, size)


def array_int64(values: Any, *, ids: Any = None, size: _OptionalSize = None):
  """Returns ARRAY_INT64 qvalue."""
  return _array_qvalue_impl(_ARRAY_INT64_SPEC, values, ids, size)


def array_uint64(values: Any, *, ids: Any = None, size: _OptionalSize = None):
  """Returns ARRAY_UINT64 qvalue."""
  return _array_qvalue_impl(_ARRAY_UINT64_SPEC, values, ids, size)


def array_float32(values: Any, *, ids: Any = None, size: _OptionalSize = None):
  """Returns ARRAY_FLOAT32 qvalue."""
  return _array_qvalue_impl(_ARRAY_FLOAT32_SPEC, values, ids, size)


def array_float64(values: Any, *, ids: Any = None, size: _OptionalSize = None):
  """Returns ARRAY_FLOAT64 qvalue."""
  return _array_qvalue_impl(_ARRAY_FLOAT64_SPEC, values, ids, size)


def array_weak_float(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns ARRAY_WEAK_FLOAT qvalue."""
  return _array_qvalue_impl(_ARRAY_WEAK_FLOAT_SPEC, values, ids, size)


def array_concat(*args: arolla_abc.QValue) -> arolla_abc.AnyQValue:
  """Returns concatenation of the given arrays."""

  def impl(arrays: tuple[arolla_abc.QValue, ...]) -> arolla_abc.QValue:
    if len(arrays) == 1:
      return arrays[0]
    return arolla_abc.invoke_op(
        'array.concat',
        (
            impl(arrays[: len(arrays) // 2]),
            impl(arrays[len(arrays) // 2 :]),
        ),
    )

  if not args:
    raise TypeError('expected at least one argument, got none')
  arg_qtypes = {arg.qtype: None for arg in args}
  for arg_qtype in arg_qtypes.keys():
    if not is_array_qtype(arg_qtype):
      raise ValueError(f'expected all arguments to be arrays, got {arg_qtype}')
  if len(arg_qtypes) != 1:
    raise ValueError(
        'expected all arguments to have the same array type, got: '
        + ', '.join(map(str, arg_qtypes.keys()))
    )
  return impl(args)
