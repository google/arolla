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

"""(Private) QValue specialisations for array types."""

from __future__ import annotations

import functools
from typing import Any, SupportsIndex, TypeVar

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import array_qtypes
from arolla.types.qtype import boxing
from arolla.types.qtype import scalar_qtypes
from arolla.types.qvalue import basic_array_helpers
from arolla.types.qvalue import qvalue_mixins


# We purposfully do _not_ use typing.Sequence, as neither
# basic_array_helpers.BasicArray nor np.ndarray implement it.
IntSequence = Any
TSequence = TypeVar('TSequence')

_present_values_as_dense_array_expr = arolla_abc.unsafe_parse_sexpr(
    ('array.as_dense_array', ('array.present_values', arolla_abc.leaf('array')))
)
_present_indices_as_dense_array_expr = arolla_abc.unsafe_parse_sexpr((
    'array.as_dense_array',
    ('array.present_indices', arolla_abc.leaf('array')),
))


class Array(qvalue_mixins.PresenceQValueMixin, basic_array_helpers.BasicArray):
  """QValue specialization for array qtype.

  Array is an immutable array with support of missed values. Implemented
  on top of DenseArray. It efficiently represents very sparse data and
  constants, but has bigger fixed overhead than DenseArray.

  The implementation of Arrays: //arolla/array/array.h
  """

  __slots__ = ()

  def to_ids_and_values(self) -> tuple[list[int], list[Any]]:
    """Returns aligned lists of ids and values."""
    values = arolla_abc.invoke_op('array.present_values', (self,))
    indices = arolla_abc.invoke_op('array.present_indices', (self,))
    return indices.py_value(), values.py_value()


class ArrayInt(qvalue_mixins.IntegralArithmeticQValueMixin, Array):
  """QValue specialisation for ARRAY_INTs."""

  __slots__ = ()


class ArrayFloat(qvalue_mixins.FloatingPointArithmeticQValueMixin, Array):
  """QValue specialisation for ARRAY_FLOATs."""

  __slots__ = ()


class ArrayEdge(arolla_abc.QValue):
  """QValue specialization for ARRAY_EDGE qtype."""

  __slots__ = ()

  def __new__(cls):
    raise NotImplementedError('please use ArrayEdge.from_* instead')

  @classmethod
  def from_sizes(cls, sizes: IntSequence) -> ArrayEdge:
    """Returns a ArrayEdge from group sizes."""
    return arolla_abc.invoke_op(
        'edge.from_sizes', (array_qtypes.array_int64(sizes),)
    )

  @classmethod
  def from_split_points(cls, split_points: IntSequence) -> ArrayEdge:
    """Returns a DenseArrayEdge from group split points."""
    return arolla_abc.invoke_op(
        'edge.from_split_points', (array_qtypes.array_int64(split_points),)
    )

  @classmethod
  def from_mapping(
      cls, mapping: IntSequence, parent_size: SupportsIndex
  ) -> ArrayEdge:
    """Returns a ArrayEdge from a mapping of each child to a parent.

    Args:
      mapping: Maps each child element i to a parent value mapping[i].
      parent_size: The number of parents.
    """
    return arolla_abc.invoke_op(
        'edge.from_mapping',
        (
            array_qtypes.array_int64(mapping),
            scalar_qtypes.int64(parent_size),
        ),
    )

  @classmethod
  def from_keys(
      cls, child_keys: TSequence, parent_keys: TSequence
  ) -> ArrayEdge:
    """Returns a ArrayEdge from child_keys to parent_keys.

    Args:
      child_keys: Maps each child element i to an index j, where child_keys[i]
        == parent_keys[j].
      parent_keys: The ids of the parents.
    """
    return arolla_abc.invoke_op(
        'edge.from_keys',
        (boxing.array(child_keys), boxing.array(parent_keys)),
    )

  @property
  def parent_size(self) -> int:
    return int(arolla_abc.invoke_op('edge.parent_size', (self,)))

  @property
  def child_size(self) -> int:
    return int(arolla_abc.invoke_op('edge.child_size', (self,)))

  def mapping(self) -> arolla_abc.AnyQValue:
    return arolla_abc.invoke_op('edge.mapping', (self,))


# Note: We use lazy evaluation, just in case the corresponding operators are
# unavailable (or not yet available) in the current environment.
_empty_array_empty_shape_fn = functools.cache(
    functools.partial(arolla_abc.invoke_op, 'qtype._const_empty_array_shape')
)


class ArrayShape(arolla_abc.QValue):
  """QValue specialization for ARRAY_SHAPE qtype."""

  __slots__ = ()

  def __new__(cls, size: SupportsIndex) -> ArrayShape:
    """Returns a dense array shape of the given size."""
    return arolla_abc.invoke_op(
        'array.make_array_shape', (scalar_qtypes.int64(size),)
    )

  @property
  def size(self) -> int:
    return int(arolla_abc.invoke_op('array.array_shape_size', (self,)))


class ArrayToScalarEdge(arolla_abc.QValue):
  """QValue specialization for ARRAY_TO_SCALAR_EDGE qtype."""

  __slots__ = ()

  def __new__(cls, size: SupportsIndex) -> ArrayToScalarEdge:
    return arolla_abc.invoke_op('edge.from_shape', (ArrayShape(size),))

  @property
  def child_size(self) -> int:
    return int(arolla_abc.invoke_op('edge.child_size', (self,)))


def _register_qvalue_specializations():
  """Registers qvalue specializations for array types."""
  for qtype in scalar_qtypes.SCALAR_QTYPES:
    if qtype in scalar_qtypes.INTEGRAL_QTYPES:
      arolla_abc.register_qvalue_specialization(
          array_qtypes.make_array_qtype(qtype), ArrayInt
      )
    elif qtype in scalar_qtypes.FLOATING_POINT_QTYPES:
      arolla_abc.register_qvalue_specialization(
          array_qtypes.make_array_qtype(qtype), ArrayFloat
      )
    else:
      arolla_abc.register_qvalue_specialization(
          array_qtypes.make_array_qtype(qtype), Array
      )
  arolla_abc.register_qvalue_specialization(array_qtypes.ARRAY_UINT64, ArrayInt)
  arolla_abc.register_qvalue_specialization(array_qtypes.ARRAY_EDGE, ArrayEdge)
  arolla_abc.register_qvalue_specialization(
      array_qtypes.ARRAY_SHAPE, ArrayShape
  )
  arolla_abc.register_qvalue_specialization(
      array_qtypes.ARRAY_TO_SCALAR_EDGE, ArrayToScalarEdge
  )


_register_qvalue_specializations()
