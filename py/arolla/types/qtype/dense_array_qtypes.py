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

"""(Private) DenseArray qtypes and corresponding factory functions."""

import dataclasses
from typing import Any, Callable, SupportsIndex

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import casting
from arolla.types.qtype import clib
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes

# LINT.IfChange
DENSE_ARRAY_BOOLEAN = clib.dense_array_boolean_from_values([]).qtype
DENSE_ARRAY_BYTES = clib.dense_array_bytes_from_values([]).qtype
DENSE_ARRAY_FLOAT32 = clib.dense_array_float32_from_values([]).qtype
DENSE_ARRAY_FLOAT64 = clib.dense_array_float64_from_values([]).qtype
DENSE_ARRAY_INT32 = clib.dense_array_int32_from_values([]).qtype
DENSE_ARRAY_INT64 = clib.dense_array_int64_from_values([]).qtype
DENSE_ARRAY_TEXT = clib.dense_array_text_from_values([]).qtype
DENSE_ARRAY_UINT64 = clib.dense_array_uint64_from_values([]).qtype
DENSE_ARRAY_UNIT = clib.dense_array_unit_from_values([]).qtype
DENSE_ARRAY_WEAK_FLOAT = clib.dense_array_weak_float_from_values([]).qtype

DENSE_ARRAY_SHAPE = clib.DENSE_ARRAY_SHAPE
DENSE_ARRAY_TO_SCALAR_EDGE = clib.DENSE_ARRAY_TO_SCALAR_EDGE
DENSE_ARRAY_EDGE = clib.DENSE_ARRAY_EDGE
# LINT.ThenChange(//depot/py/arolla/arolla.py)


def is_dense_array_qtype(qtype: arolla_abc.QType) -> bool:
  """Checks whether qtype corresponds to a dense_array."""
  return casting.get_shape_qtype_or_nothing(qtype) == DENSE_ARRAY_SHAPE


def make_dense_array_qtype(qtype: arolla_abc.QType) -> arolla_abc.QType:
  """Returns the DenseArray QType corresponding to a value QType."""
  if not (
      scalar_qtypes.is_scalar_qtype(qtype)
      or optional_qtypes.is_optional_qtype(qtype)
  ):
    raise casting.QTypeError(f'expected a scalar qtype, got {qtype=}')
  return casting.broadcast_qtype([DENSE_ARRAY_UNIT], qtype)


DENSE_ARRAY_QTYPES = tuple(
    make_dense_array_qtype(tpe) for tpe in scalar_qtypes.SCALAR_QTYPES
)


DENSE_ARRAY_NUMERIC_QTYPES = tuple(
    make_dense_array_qtype(tpe) for tpe in scalar_qtypes.NUMERIC_QTYPES
)


def _make_implicit_casting_expr(
    target_qtype: arolla_abc.QType,
) -> arolla_abc.Expr:
  return arolla_abc.unsafe_parse_sexpr((
      'core.cast',
      arolla_abc.leaf('values'),
      target_qtype,
      scalar_qtypes.true(),  # implicit_only=True
  ))


def _make_from_numpy_ndarray_fn(
    dtype_name: str,
    from_values_buffer_fn: Callable[[Any], arolla_abc.AnyQValue],
) -> Callable[[Any], arolla_abc.AnyQValue]:
  """Generates a dense_array factory from a numpy.ndarray."""

  def impl(ndarray, /) -> arolla_abc.AnyQValue:
    if ndarray.ndim == 0:
      raise ValueError('expected a single dimensional array, got a scalar')
    if ndarray.ndim > 1:
      raise ValueError(
          'expected a single dimensional array, got an array with'
          f' {ndarray.ndim} dimensions'
      )
    return from_values_buffer_fn(
        ndarray.astype(dtype_name, order='C', copy=False)
    )

  return impl


def _cannot_construct_from_numpy_ndarray(_, /):
  raise NotImplementedError('cannot construct from numpy.ndarray')


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class _ConstructionSpec:
  """A specification for dense_array_from_* functions.

  Attributes:
    qtype: Target array qtype.
    optional_qvalue_fn: Unwraps a value into an optional qvalue compatible with
      the target array.
    casting_expr: A casting expression from a qvalue to the target array.
    from_values: Constructs a dense array from a sequence of values.
    from_numpy_ndarray: Constructs a dense array from a numpy array.
  """

  qtype: arolla_abc.QType
  optional_qvalue_fn: Callable[[Any], arolla_abc.QValue]
  casting_expr: arolla_abc.Expr
  from_values: Callable[[Any], Any]
  from_numpy_ndarray: Callable[[Any], Any]


# Static specs for qvalue construction.
_DENSE_ARRAY_UNIT_SPEC = _ConstructionSpec(
    qtype=DENSE_ARRAY_UNIT,
    optional_qvalue_fn=optional_qtypes.optional_unit,
    casting_expr=_make_implicit_casting_expr(DENSE_ARRAY_UNIT),
    from_values=clib.dense_array_unit_from_values,
    from_numpy_ndarray=_cannot_construct_from_numpy_ndarray,
)
_DENSE_ARRAY_BOOLEAN_SPEC = _ConstructionSpec(
    qtype=DENSE_ARRAY_BOOLEAN,
    optional_qvalue_fn=optional_qtypes.optional_boolean,
    casting_expr=_make_implicit_casting_expr(DENSE_ARRAY_BOOLEAN),
    from_values=clib.dense_array_boolean_from_values,
    from_numpy_ndarray=_make_from_numpy_ndarray_fn(
        '?', clib.dense_array_boolean_from_values_buffer
    ),
)
_DENSE_ARRAY_BYTES_SPEC = _ConstructionSpec(
    qtype=DENSE_ARRAY_BYTES,
    optional_qvalue_fn=optional_qtypes.optional_bytes,
    casting_expr=_make_implicit_casting_expr(DENSE_ARRAY_BYTES),
    from_values=clib.dense_array_bytes_from_values,
    from_numpy_ndarray=_cannot_construct_from_numpy_ndarray,
)
_DENSE_ARRAY_TEXT_SPEC = _ConstructionSpec(
    qtype=DENSE_ARRAY_TEXT,
    optional_qvalue_fn=optional_qtypes.optional_text,
    casting_expr=_make_implicit_casting_expr(DENSE_ARRAY_TEXT),
    from_values=clib.dense_array_text_from_values,
    from_numpy_ndarray=_cannot_construct_from_numpy_ndarray,
)
_DENSE_ARRAY_INT32_SPEC = _ConstructionSpec(
    qtype=DENSE_ARRAY_INT32,
    optional_qvalue_fn=optional_qtypes.optional_int32,
    casting_expr=_make_implicit_casting_expr(DENSE_ARRAY_INT32),
    from_values=clib.dense_array_int32_from_values,
    from_numpy_ndarray=_make_from_numpy_ndarray_fn(
        'i4', clib.dense_array_int32_from_values_buffer
    ),
)
_DENSE_ARRAY_INT64_SPEC = _ConstructionSpec(
    qtype=DENSE_ARRAY_INT64,
    optional_qvalue_fn=optional_qtypes.optional_int64,
    casting_expr=_make_implicit_casting_expr(DENSE_ARRAY_INT64),
    from_values=clib.dense_array_int64_from_values,
    from_numpy_ndarray=_make_from_numpy_ndarray_fn(
        'i8', clib.dense_array_int64_from_values_buffer
    ),
)
_DENSE_ARRAY_UINT64_SPEC = _ConstructionSpec(
    qtype=DENSE_ARRAY_UINT64,
    optional_qvalue_fn=optional_qtypes.optional_uint64,
    casting_expr=_make_implicit_casting_expr(DENSE_ARRAY_UINT64),
    from_values=clib.dense_array_uint64_from_values,
    from_numpy_ndarray=_make_from_numpy_ndarray_fn(
        'u8', clib.dense_array_uint64_from_values_buffer
    ),
)
_DENSE_ARRAY_FLOAT32_SPEC = _ConstructionSpec(
    qtype=DENSE_ARRAY_FLOAT32,
    optional_qvalue_fn=optional_qtypes.optional_float32,
    casting_expr=_make_implicit_casting_expr(DENSE_ARRAY_FLOAT32),
    from_values=clib.dense_array_float32_from_values,
    from_numpy_ndarray=_make_from_numpy_ndarray_fn(
        'f4', clib.dense_array_float32_from_values_buffer
    ),
)
_DENSE_ARRAY_FLOAT64_SPEC = _ConstructionSpec(
    qtype=DENSE_ARRAY_FLOAT64,
    optional_qvalue_fn=optional_qtypes.optional_float64,
    casting_expr=_make_implicit_casting_expr(DENSE_ARRAY_FLOAT64),
    from_values=clib.dense_array_float64_from_values,
    from_numpy_ndarray=_make_from_numpy_ndarray_fn(
        'f8', clib.dense_array_float64_from_values_buffer
    ),
)
_DENSE_ARRAY_WEAK_FLOAT_SPEC = _ConstructionSpec(
    qtype=DENSE_ARRAY_WEAK_FLOAT,
    optional_qvalue_fn=optional_qtypes.optional_weak_float,
    casting_expr=_make_implicit_casting_expr(DENSE_ARRAY_WEAK_FLOAT),
    from_values=clib.dense_array_weak_float_from_values,
    from_numpy_ndarray=_make_from_numpy_ndarray_fn(
        'f8', clib.dense_array_weak_float_from_values_buffer
    ),
)


def _dense_array_from_values(
    spec: _ConstructionSpec, values: Any
) -> arolla_abc.AnyQValue:
  """Converts `values` into a array according to `spec`."""
  if isinstance(values, arolla_abc.QValue):
    if values.qtype == spec.qtype:
      return values
    try:
      return arolla_abc.eval_expr(spec.casting_expr, values=values)
    except ValueError as ex:
      raise ValueError(
          f'failed to convert values with {values.qtype=} to '
          f'target_qtype={spec.qtype} using implicit casting rules'
      ) from ex

  np = arolla_abc.get_numpy_module_or_dummy()
  if isinstance(values, np.ndarray):
    return spec.from_numpy_ndarray(values)
  return spec.from_values(values)


_Size = SupportsIndex
_OptionalSize = _Size | None


def _dense_array_from_ids_and_values(
    spec: _ConstructionSpec, values: Any, ids: Any, size: _Size
) -> arolla_abc.AnyQValue:
  """Converts `ids` and `values` into an array according to `spec`."""
  values = _dense_array_from_values(spec, values)
  ids = _dense_array_from_values(_DENSE_ARRAY_INT64_SPEC, ids)
  return arolla_abc.invoke_op(
      'array.from_indices_and_values',
      (ids, values, scalar_qtypes.int64(size)),
  )


_const_array_expr = arolla_abc.unsafe_parse_sexpr((
    'core.const_with_shape',
    ('array.make_dense_array_shape', arolla_abc.leaf('size')),
    arolla_abc.leaf('value'),
))


def _dense_array_from_size_and_value(
    spec: _ConstructionSpec, value: Any, size: _Size
):
  """Converts `size` and `values` into an array according to `spec`."""
  size = scalar_qtypes.int64(size)
  value = spec.optional_qvalue_fn(value)
  return arolla_abc.eval_expr(_const_array_expr, size=size, value=value)


def _dense_array_qvalue_impl(
    spec: _ConstructionSpec,
    values: Any,
    ids: Any = None,
    size: _OptionalSize = None,
) -> arolla_abc.AnyQValue:
  """A helper function for factories like dense_array_{bytes,text,int32,...}."""
  if ids is not None:
    if size is None:
      raise TypeError('missing parameter: size')
    return _dense_array_from_ids_and_values(spec, values, ids, size)
  if size is not None:
    return _dense_array_from_size_and_value(spec, values, size)
  return _dense_array_from_values(spec, values)


def dense_array_unit(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns DENSE_ARRAY_UNIT qvalue."""
  return _dense_array_qvalue_impl(_DENSE_ARRAY_UNIT_SPEC, values, ids, size)


def dense_array_boolean(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns DENSE_ARRAY_BOOLEAN qvalue."""
  return _dense_array_qvalue_impl(_DENSE_ARRAY_BOOLEAN_SPEC, values, ids, size)


def dense_array_bytes(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns DENSE_ARRAY_BYTES qvalue."""
  return _dense_array_qvalue_impl(_DENSE_ARRAY_BYTES_SPEC, values, ids, size)


def dense_array_text(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns DENSE_ARRAY_TEXT qvalue."""
  return _dense_array_qvalue_impl(_DENSE_ARRAY_TEXT_SPEC, values, ids, size)


def dense_array_int32(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns DENSE_ARRAY_INT32 qvalue."""
  return _dense_array_qvalue_impl(_DENSE_ARRAY_INT32_SPEC, values, ids, size)


def dense_array_int64(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns DENSE_ARRAY_INT64 qvalue."""
  return _dense_array_qvalue_impl(_DENSE_ARRAY_INT64_SPEC, values, ids, size)


def dense_array_uint64(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns DENSE_ARRAY_UINT64 qvalue."""
  return _dense_array_qvalue_impl(_DENSE_ARRAY_UINT64_SPEC, values, ids, size)


def dense_array_float32(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns DENSE_ARRAY_FLOAT32 qvalue."""
  return _dense_array_qvalue_impl(_DENSE_ARRAY_FLOAT32_SPEC, values, ids, size)


def dense_array_float64(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns DENSE_ARRAY_FLOAT64 qvalue."""
  return _dense_array_qvalue_impl(_DENSE_ARRAY_FLOAT64_SPEC, values, ids, size)


def dense_array_weak_float(
    values: Any, *, ids: Any = None, size: _OptionalSize = None
):
  """Returns DENSE_ARRAY_WEAK_FLOAT qvalue."""
  return _dense_array_qvalue_impl(
      _DENSE_ARRAY_WEAK_FLOAT_SPEC, values, ids, size
  )
